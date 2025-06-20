# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, cast
import json
import uuid
import datetime
import time
from pydantic import BaseModel, Field, validator
from sqlalchemy import select
from fastapi.security import OAuth2PasswordBearer
import asyncio

from app.core.logging_config import orders_logger
from app.core.security import get_user_from_service_or_user_token, get_current_user, get_user_from_service_token
from app.database.models import Group, ExternalSymbolInfo, User, DemoUser, UserOrder, DemoUserOrder, Wallet
from app.schemas.order import (
    ServiceProviderUpdateRequest, OrderPlacementRequest, OrderResponse, CloseOrderRequest, 
    UpdateStopLossTakeProfitRequest, PendingOrderPlacementRequest, PendingOrderCancelRequest, 
    AddStopLossRequest, AddTakeProfitRequest, CancelStopLossRequest, CancelTakeProfitRequest, 
    HalfSpreadRequest, HalfSpreadResponse, OrderStatusResponse
)
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate
from app.core.cache import publish_account_structure_changed_event

from app.core.cache import (
    set_user_data_cache,
    set_user_portfolio_cache,
    DecimalEncoder,
    get_group_symbol_settings_cache,
    publish_account_structure_changed_event,
    get_user_portfolio_cache,
    get_user_data_cache,
    get_group_settings_cache,
    get_last_known_price, # Added import
    # New cache functions
    set_user_static_orders_cache,
    get_user_static_orders_cache,
    set_user_dynamic_portfolio_cache,
    get_user_dynamic_portfolio_cache,
    # New publish functions
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_group_settings_cache,
    get_adjusted_market_price_cache,
)

from app.utils.validation import enforce_service_user_id_restriction
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution,
    generate_unique_10_digit_id
)
from app.services.portfolio_calculator import _convert_to_usd, calculate_user_portfolio
from app.services.margin_calculator import calculate_single_order_margin, get_live_adjusted_buy_price_for_pair, calculate_pending_order_margin, get_live_adjusted_sell_price_for_pair
from app.services.pending_orders import add_pending_order, remove_pending_order

from app.crud import crud_order, group as crud_group
from app.crud.crud_order import OrderCreateInternal
from app.crud.user import get_user_by_id, get_demo_user_by_id, get_user_by_id_with_lock, get_demo_user_by_id_with_lock, update_user_margin
# Ensure NO import of get_order_model from any module. Only use the local version below.

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from app.schemas.order import OrderResponse

# Robust local get_order_model implementation
from app.database.models import UserOrder, DemoUserOrder, Wallet
from app.services.order_processing import generate_unique_10_digit_id
from app.schemas.wallet import WalletCreate

from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
from app.crud.group import get_all_symbols_for_group
from app.firebase_stream import get_latest_market_data
from app.services.margin_calculator import get_external_symbol_info
from app.core.firebase import send_order_to_firebase

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

async def update_user_static_orders(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user after order changes.
    This includes both open and pending orders.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        orders_logger.info(f"Starting update_user_static_orders for user {user_id}, user_type {user_type}")
        order_model = get_order_model(user_type)
        orders_logger.info(f"Using order model: {order_model.__name__}")
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        orders_logger.info(f"Fetched {len(open_orders_orm)} open orders for user {user_id}")
        open_orders_data = []
        for pos in open_orders_orm:
            pos_dict = {attr: str(v) if isinstance(v := getattr(pos, attr, None), Decimal) else v
                       for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                   'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['commission'] = str(getattr(pos, 'commission', '0.0'))
            open_orders_data.append(pos_dict)
        
        # Get pending orders - always fetch from database to ensure fresh data
        pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
        pending_orders_orm = await crud_order.get_orders_by_user_id_and_statuses(db, user_id, pending_statuses, order_model)
        orders_logger.info(f"Fetched {len(pending_orders_orm)} pending orders for user {user_id}")
        pending_orders_data = []
        for po in pending_orders_orm:
            po_dict = {attr: str(v) if isinstance(v := getattr(po, attr, None), Decimal) else v
                      for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 
                                  'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            po_dict['commission'] = str(getattr(po, 'commission', '0.0'))
            pending_orders_data.append(po_dict)
        
        # Cache the static orders data
        static_orders_data = {
            "open_orders": open_orders_data,
            "pending_orders": pending_orders_data,
            "updated_at": datetime.datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        orders_logger.info(f"Updated static orders cache for user {user_id} with {len(open_orders_data)} open orders and {len(pending_orders_data)} pending orders")
        
        return static_orders_data
    except Exception as e:
        orders_logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.datetime.now().isoformat()}

def get_order_model(user_or_type):
    """
    Returns the correct order model class based on user object or user_type string.
    Accepts a user object (User or DemoUser) or a user_type string.
    """
    # If a string is passed
    if isinstance(user_or_type, str):
        if user_or_type.lower() == 'demo':
            return DemoUserOrder
        elif user_or_type.lower() == 'live':
            return UserOrder
        else:
            return None
    # If a user object is passed
    user_type = getattr(user_or_type, 'user_type', None)
    if user_type and str(user_type).lower() == 'demo':
        return DemoUserOrder
    elif user_type and str(user_type).lower() == 'live':
        return UserOrder
    # Fallback: check class name
    if user_or_type.__class__.__name__ == 'DemoUser':
        return DemoUserOrder
    elif user_or_type.__class__.__name__ == 'User':
        return UserOrder
    return None

# --- New Endpoints for Order Status Filtering ---
@router.get("/pending", response_model=List[OrderResponse], summary="Get all pending orders for the current user")
async def get_pending_orders(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user),
):
    """
    Returns all orders with status 'PENDING' for the current user.
    """
    if not current_user or not hasattr(current_user, "user_type"):
        logger.error(f"[get_pending_orders] Invalid user or missing user_type: {current_user}")
        raise HTTPException(status_code=400, detail="Invalid user or user_type.")
    logger.info(f"[get_pending_orders] user_type: {current_user.user_type}")
    order_model = get_order_model(current_user.user_type)
    if order_model is None:
        logger.error(f"[get_pending_orders] Could not determine order model for user_type: {current_user.user_type}")
        raise HTTPException(status_code=400, detail="Invalid user_type for order model.")
    orders = await crud_order.get_orders_by_user_id_and_statuses(db, current_user.id, ["PENDING"], order_model)
    return orders

@router.get("/closed", response_model=List[OrderResponse], summary="Get all closed orders for the current user")
async def get_closed_orders(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user),
):
    """
    Returns all orders with status 'CLOSED' for the current user.
    """
    if not current_user or not hasattr(current_user, "user_type"):
        logger.error(f"[get_closed_orders] Invalid user or missing user_type: {current_user}")
        raise HTTPException(status_code=400, detail="Invalid user or user_type.")
    logger.info(f"[get_closed_orders] user_type: {current_user.user_type}")
    order_model = get_order_model(current_user.user_type)
    if order_model is None:
        logger.error(f"[get_closed_orders] Could not determine order model for user_type: {current_user.user_type}")
        raise HTTPException(status_code=400, detail="Invalid user_type for order model.")
    orders = await crud_order.get_orders_by_user_id_and_statuses(db, current_user.id, ["CLOSED"], order_model)
    return orders

@router.get("/rejected", response_model=List[OrderResponse], summary="Get all rejected orders for the current user")
async def get_rejected_orders(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user),
):
    """
    Returns all orders with status 'REJECTED' for the current user.
    """
    if not current_user or not hasattr(current_user, "user_type"):
        logger.error(f"[get_rejected_orders] Invalid user or missing user_type: {current_user}")
        raise HTTPException(status_code=400, detail="Invalid user or user_type.")
    logger.info(f"[get_rejected_orders] user_type: {current_user.user_type}")
    order_model = get_order_model(current_user.user_type)
    if order_model is None:
        logger.error(f"[get_rejected_orders] Could not determine order model for user_type: {current_user.user_type}")
        raise HTTPException(status_code=400, detail="Invalid user_type for order model.")
    orders = await crud_order.get_orders_by_user_id_and_statuses(db, current_user.id, ["REJECTED"], order_model)
    return orders

    from app.core.logging_config import orders_logger
    orders_logger.info(f"[get_order_model] called with: {repr(user_or_type)} (type: {type(user_or_type)})")
    # Log attributes if it's an object
    if not isinstance(user_or_type, str):
        orders_logger.info(f"[get_order_model] user_or_type.__class__.__name__: {user_or_type.__class__.__name__}")
        orders_logger.info(f"[get_order_model] user_or_type attributes: {dir(user_or_type)}")
        user_type_attr = getattr(user_or_type, 'user_type', None)
        orders_logger.info(f"[get_order_model] user_type attribute value: {user_type_attr}, type: {type(user_type_attr)}")
    # If a string is passed
    if isinstance(user_or_type, str):
        orders_logger.info("[get_order_model] Branch: isinstance(user_or_type, str)")
        if user_or_type.lower() == 'demo':
            orders_logger.info("[get_order_model] Branch: user_type string is 'demo' -> DemoUserOrder")
            return DemoUserOrder
        orders_logger.info("[get_order_model] Branch: user_type string is not 'demo' -> UserOrder")
        return UserOrder
    # If a user object is passed
    user_type = getattr(user_or_type, 'user_type', None)
    if user_type and str(user_type).lower() == 'demo':
        orders_logger.info("[get_order_model] Branch: user_type attribute is 'demo' -> DemoUserOrder")
        return DemoUserOrder
    # Fallback: check class name
    if user_or_type.__class__.__name__ == 'DemoUser':
        orders_logger.info("[get_order_model] Branch: class name is 'DemoUser' -> DemoUserOrder (FORCED)")
        return DemoUserOrder
    orders_logger.info("[get_order_model] Branch: default -> UserOrder")
    return UserOrder
    
class OrderPlacementRequest(BaseModel):
    # Required fields
    symbol: str  # Corresponds to order_company_name
    order_type: str  # E.g., "MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT"
    order_quantity: Decimal = Field(..., gt=0)
    order_price: Decimal
    user_type: str  # "live" or "demo"
    user_id: int

    # Optional fields with defaults
    order_status: str = "OPEN"  # Default to OPEN for new orders
    status: str = "ACTIVE"  # Default to ACTIVE for new orders
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    contract_value: Optional[Decimal] = None
    margin: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    cancel_id: Optional[str] = None
    close_id: Optional[str] = None
    modify_id: Optional[str] = None
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    stoploss_cancel_id: Optional[str] = None
    takeprofit_cancel_id: Optional[str] = None

    @validator('order_type')
    def validate_order_type(cls, v):
        valid_types = ["MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]
        if v.upper() not in valid_types:
            raise ValueError(f"Invalid order type. Must be one of: {', '.join(valid_types)}")
        return v.upper()

    @validator('user_type')
    def validate_user_type(cls, v):
        valid_types = ["live", "demo"]
        if v.lower() not in valid_types:
            raise ValueError(f"Invalid user type. Must be one of: {', '.join(valid_types)}")
        return v.lower()

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }

@router.post("/", response_model=OrderResponse)
async def place_order(
    order_request: OrderPlacementRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Place a new order.
    """
    from app.core.logging_config import frontend_orders_logger, error_logger
    from app.core.cache import get_adjusted_market_price_cache
    
    try:
        # Log the incoming request
        frontend_orders_logger.info(f"FRONTEND ORDER PLACEMENT - REQUEST: {json.dumps(order_request.dict(), default=str)}")
        
        # Get user's group name for adjusted price lookup
        user_group_name = getattr(current_user, 'group_name', 'default')
        
        # Get adjusted market prices from Redis cache
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, user_group_name, order_request.symbol)
        if not adjusted_prices:
            raise HTTPException(status_code=500, detail="Could not fetch current market prices.")
        
        # Set order price based on order type
        if order_request.order_type == "BUY":
            order_price = adjusted_prices.get('buy')  # Use buy price for BUY orders
            orders_logger.info(f"Using adjusted buy price {order_price} for BUY order")
        elif order_request.order_type == "SELL":
            order_price = adjusted_prices.get('sell')  # Use sell price for SELL orders
            orders_logger.info(f"Using adjusted sell price {order_price} for SELL order")
        else:
            raise HTTPException(status_code=400, detail=f"Invalid order type: {order_request.order_type}")

        if not order_price:
            raise HTTPException(status_code=500, detail="Could not determine order price from market data.")

        try:
            order_price = Decimal(str(order_price))
            if order_price <= Decimal("0"):
                raise HTTPException(status_code=400, detail="Order price must be positive.")
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="Invalid order price format.")
            
        # Update the order request with the fetched price
        order_request.order_price = order_price
        
        # Continue with the existing order placement logic
        orders_logger.info(f"Order placement request received - Symbol: {order_request.symbol}, Type: {order_request.order_type}, Price: {order_price}")
        
        # Convert order request to dict
        order_data = {
            'order_company_name': order_request.symbol,
            'order_type': order_request.order_type,
            'order_quantity': order_request.order_quantity,
            'order_price': order_request.order_price,
            'user_type': order_request.user_type,
            'status': order_request.status,
            'stop_loss': order_request.stop_loss,
            'take_profit': order_request.take_profit
        }

        # Get user and group settings to determine if Barclays live user
        user_id_for_order = current_user.id
        # Use user_type from request (not just current_user)
        user_type = order_request.user_type.lower() if hasattr(order_request, 'user_type') else current_user.user_type
        user_data_cache = await get_user_data_cache(redis_client, user_id_for_order, db, user_type)
        group_name = user_data_cache.get('group_name') if user_data_cache else None
        # Fallback: If group_name is missing, fetch from DB
        if not group_name:
            db_user = await get_user_by_id(db, user_id_for_order, user_type=user_type)
            group_name = getattr(db_user, 'group_name', None) if db_user else None
        group_settings_cache = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders_cache = group_settings_cache.get('sending_orders') if group_settings_cache else None

        # If not found in cache, fetch group settings from DB
        if (group_settings_cache is None or sending_orders_cache is None) and group_name:
            from app.crud import group as crud_group
            db_group_result = await crud_group.get_group_by_name(db, group_name)
            orders_logger.info(f"[DEBUG] db_group_result fetched from DB: {db_group_result}")
            sending_orders_extracted = None
            if db_group_result:
                # If it's a list (multiple group-symbol records), extract from the first
                if isinstance(db_group_result, list):
                    orders_logger.info(f"[DEBUG] Number of group records found: {len(db_group_result)}")
                    if len(db_group_result) > 0 and hasattr(db_group_result[0], 'sending_orders'):
                        sending_orders_extracted = getattr(db_group_result[0], 'sending_orders', None)
                # If it's a single record
                elif hasattr(db_group_result, 'sending_orders'):
                    sending_orders_extracted = getattr(db_group_result, 'sending_orders', None)
            if sending_orders_extracted is not None:
                sending_orders_cache = sending_orders_extracted
                orders_logger.info(f"[DEBUG] sending_orders_cache extracted from DB: {sending_orders_cache}")
            else:
                orders_logger.warning(f"[WARNING] Group '{group_name}' not found in DB or missing 'sending_orders' attribute. db_group_result={db_group_result}")

        # Normalize for robust comparison
        sending_orders_normalized = sending_orders_cache.lower() if isinstance(sending_orders_cache, str) else sending_orders_cache
        orders_logger.info(f"[DEBUG] group_settings_cache: {group_settings_cache}")
        orders_logger.info(f"[DEBUG] sending_orders_cache value: {sending_orders_cache} (type: {type(sending_orders_cache)})")
        is_barclays_live_user = (user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"Order placement: user_id={user_id_for_order}, is_barclays_live_user={is_barclays_live_user}, user_type={user_type}, group_name={group_name}, sending_orders_setting={sending_orders_cache}")

        # Force order_status to 'PROCESSING' for Barclays live users
        if is_barclays_live_user:
            order_data['order_status'] = 'PROCESSING'

        # Prepare order data (all IDs, margin, etc.)
        order_create_internal = await process_new_order(
            db=db,
            redis_client=redis_client,
            user_id=user_id_for_order, # Use the defined user_id
            order_data=order_data,
            user_type=current_user.user_type,
            is_barclays_live_user=is_barclays_live_user # Pass the new flag
        )

        # Create order in database (OrderCreateInternal-compatible)
        order_model = get_order_model(current_user.user_type)
        orders_logger.info(f"[PRE-CRUD] About to call create_order with: {order_create_internal}")
        db_order = await crud_order.create_order(db, order_create_internal, order_model)

        # Log user margin from session before commit
        try:
            user_to_check = await db.get(type(current_user), current_user.id) # Assumes current_user is the correct user model type
            if user_to_check:
                orders_logger.info(f"[MARGIN_COMMIT_CHECK] User {current_user.id} margin in session BEFORE commit: {user_to_check.margin}")
            else:
                orders_logger.warning(f"[MARGIN_COMMIT_CHECK] User {current_user.id} not found in session before commit.")
        except Exception as e_check:
            orders_logger.error(f"[MARGIN_COMMIT_CHECK] Error checking user margin before commit: {e_check}", exc_info=True)
        
        await db.commit()
        await db.refresh(db_order)

        # Margin update for non-Barclays users is now handled within process_new_order.
        # The database commit for the order itself is sufficient here.

        # --- Update user data cache after DB update ---
        try:
            user_id = db_order.order_user_id
            # Fetch the latest user data from DB to update cache
            db_user = None
            if user_type == 'live':
                db_user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                db_user = await get_demo_user_by_id(db, user_id, user_type=user_type)
            
            if db_user:
                user_data_to_cache = {
                    "id": db_user.id,
                    "email": getattr(db_user, 'email', None),
                    "group_name": db_user.group_name,
                    "leverage": db_user.leverage,
                    "user_type": user_type,
                    "account_number": getattr(db_user, 'account_number', None),
                    "wallet_balance": db_user.wallet_balance,
                    "margin": db_user.margin,
                    "first_name": getattr(db_user, 'first_name', None),
                    "last_name": getattr(db_user, 'last_name', None),
                    "country": getattr(db_user, 'country', None),
                    "phone_number": getattr(db_user, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                orders_logger.info(f"User data cache updated for user {user_id} after placing order")
        except Exception as e:
            orders_logger.error(f"Error updating user data cache after order placement: {e}", exc_info=True)

        # --- Portfolio Update & Websocket Event ---
        try:
            user_id = db_order.order_user_id
            user_data = await get_user_data_cache(redis_client, user_id, db, current_user.user_type)
            if user_data:
                group_name = user_data.get('group_name')
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol_key in group_symbol_settings.keys(): # Renamed symbol to symbol_key to avoid conflict with outer scope symbol if any
                        # Assuming symbol_key is the actual symbol string we need for price fetching
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"Portfolio cache updated and websocket event published for user {user_id} after placing order.")
        except Exception as e:
            orders_logger.error(f"Error updating portfolio cache or publishing websocket event after order placement: {e}", exc_info=True)

        # --- Barclays Firebase Push Logic ---
        if is_barclays_live_user:
            try:
                # For Barclays live users, ensure order_status is PROCESSING and do NOT update margin in DB
                # Check margin and send to Firebase if sufficient, but do not update user margin
                user_id = db_order.order_user_id
                user_data = await get_user_data_cache(redis_client, user_id, db, current_user.user_type)
                group_name = user_data.get('group_name') if user_data else None
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL") if group_name else None
                open_positions = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = [
                    {attr: str(getattr(pos, attr)) if isinstance(getattr(pos, attr), Decimal) else getattr(pos, attr)
                     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'commission']}
                    for pos in open_positions
                ]
                adjusted_market_prices = {}
                if group_symbol_settings:
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                portfolio = await calculate_user_portfolio(user_data, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                free_margin = Decimal(str(portfolio.get('free_margin', '0')))
                order_margin = Decimal(str(db_order.margin or 0))
                # Do NOT update user margin for Barclays users; just check
                if free_margin > order_margin:
                    firebase_order_data = {
                        'order_id': db_order.order_id,
                        'order_user_id': db_order.order_user_id,
                        'order_company_name': db_order.order_company_name,
                        'order_type': db_order.order_type,
                        'order_status': db_order.order_status,  # Should be PROCESSING
                        'order_price': db_order.order_price,
                        'order_quantity': db_order.order_quantity,
                        'contract_value': db_order.contract_value,
                        'margin': db_order.margin,
                        'stop_loss': db_order.stop_loss,
                        'take_profit': db_order.take_profit,

                        'status': getattr(db_order, 'status', None),
                    }
                    orders_logger.info(f"[FIREBASE] Payload being sent to Firebase: {firebase_order_data}")
                    await send_order_to_firebase(firebase_order_data, "live")
                    orders_logger.info(f"[FIREBASE] Barclays order sent to Firebase: {db_order.order_id} (order_status=PROCESSING, margin not updated in DB)")
                else:
                    orders_logger.warning(f"[FIREBASE] Barclays order NOT sent to Firebase due to insufficient free margin. free_margin={free_margin}, order_margin={order_margin}")
            except Exception as e:
                orders_logger.error(f"[FIREBASE] Error sending Barclays order to Firebase: {e}", exc_info=True)

        await publish_account_structure_changed_event(redis_client, current_user.id)
        await publish_market_data_trigger(redis_client)
        
        # Update static orders cache - force a fresh fetch from the database
        await update_user_static_orders(user_id, db, redis_client, user_type)
        
        # Publish updates to notify WebSocket clients - make sure these are in the right order
        await publish_order_update(redis_client, user_id)
        await publish_user_data_update(redis_client, user_id)
        
        return OrderResponse(
            order_id=db_order.order_id,
            order_user_id=db_order.order_user_id,
            order_company_name=db_order.order_company_name,
            order_type=db_order.order_type,
            order_quantity=db_order.order_quantity,
            order_price=db_order.order_price,
            status=getattr(db_order, 'status', None) or 'ACTIVE',
            stop_loss=db_order.stop_loss,
            take_profit=db_order.take_profit,
            order_status=db_order.order_status,
            contract_value=db_order.contract_value,
            margin=db_order.margin,
            created_at=getattr(db_order, 'created_at', None).isoformat() if getattr(db_order, 'created_at', None) else None,
            updated_at=getattr(db_order, 'updated_at', None).isoformat() if getattr(db_order, 'updated_at', None) else None,
            net_profit=getattr(db_order, 'net_profit', None),
            close_price=getattr(db_order, 'close_price', None),
            commission=getattr(db_order, 'commission', None),
            swap=getattr(db_order, 'swap', None),
            cancel_message=getattr(db_order, 'cancel_message', None),
            close_message=getattr(db_order, 'close_message', None),
            stoploss_id=getattr(db_order, 'stoploss_id', None),
            takeprofit_id=getattr(db_order, 'takeprofit_id', None),
            close_id=getattr(db_order, 'close_id', None),
        )

    except OrderProcessingError as e:
        orders_logger.error(f"Order processing error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        orders_logger.error(f"Unexpected error in place_order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process order: {str(e)}")

@router.post("/pending-place", response_model=OrderResponse)
async def place_pending_order(
    order_request: PendingOrderPlacementRequest, # Use the new schema here
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Place a new PENDING order (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP).
    """
    try:
        orders_logger.info(f"Pending order placement request received - User ID: {current_user.id}, Symbol: {order_request.symbol}, Type: {order_request.order_type}, Quantity: {order_request.order_quantity}")
        
        user_id_for_order = current_user.id
        authenticated_user_type = 'demo' if isinstance(current_user, DemoUser) else 'live'
        requested_user_type = order_request.user_type.lower()
        
        # Validate user_type matches authenticated user type
        if authenticated_user_type != requested_user_type:
            orders_logger.error(f"User type mismatch: Authenticated as {authenticated_user_type} but request specified {requested_user_type}")
            raise HTTPException(
                status_code=400, 
                detail=f"User type mismatch: You are authenticated as a {authenticated_user_type} user but trying to place an order as a {requested_user_type} user"
            )
        
        # Validate user_id if provided in the request
        if hasattr(order_request, 'user_id') and order_request.user_id is not None:
            if order_request.user_id != user_id_for_order:
                orders_logger.error(f"User ID mismatch: Authenticated as {user_id_for_order} but request specified {order_request.user_id}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"User ID mismatch: You are authenticated as user {user_id_for_order} but trying to place an order for user {order_request.user_id}"
                )
        
        # Use the validated user_type
        user_type = requested_user_type

        # Verify that the user exists in the database before proceeding
        try:
            if user_type == 'live':
                db_user = await get_user_by_id(db, user_id_for_order, user_type=user_type)
            else:
                db_user = await get_demo_user_by_id(db, user_id_for_order, user_type=user_type)
            
            if not db_user:
                orders_logger.error(f"User {user_id_for_order} with type {user_type} not found in database")
                raise HTTPException(
                    status_code=404, 
                    detail=f"User with ID {user_id_for_order} and type {user_type} not found in database"
                )
        except Exception as e:
            orders_logger.error(f"Error verifying user existence: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Error verifying user existence: {str(e)}"
            )

        # Get user data for group name
        user_data = await get_user_data_cache(redis_client, user_id_for_order, db, user_type)
        group_name = user_data.get('group_name') if user_data else None
        
        # Check if user is a Barclays live user
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        is_barclays_live_user = (user_type == 'live' and sending_orders_normalized == 'barclays')
        
        # For non-Barclays users, validate the pending order price based on order type
        if not is_barclays_live_user:
            symbol = order_request.symbol
            order_type = order_request.order_type
            order_price = order_request.order_price
            
            # Get current market prices
            current_buy_price = await get_live_adjusted_buy_price_for_pair(redis_client, symbol, group_name)
            current_sell_price = await get_live_adjusted_sell_price_for_pair(redis_client, symbol, group_name)
            
            if current_buy_price is None or current_sell_price is None:
                orders_logger.error(f"Could not get current market prices for {symbol}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Could not get current market prices for {symbol}"
                )
            
            orders_logger.info(f"Validating pending order price: Order type={order_type}, Order price={order_price}, Current buy price={current_buy_price}, Current sell price={current_sell_price}")
            
            # Validate price based on order type
            if order_type == 'BUY_LIMIT' and order_price >= current_buy_price:
                orders_logger.error(f"Invalid BUY_LIMIT price: {order_price} must be less than current buy price {current_buy_price}")
                raise HTTPException(
                    status_code=400,
                    detail=f"BUY_LIMIT price must be less than the current market price. Your price: {order_price}, Current price: {current_buy_price}"
                )
            elif order_type == 'SELL_STOP' and order_price >= current_sell_price:
                orders_logger.error(f"Invalid SELL_STOP price: {order_price} must be less than current sell price {current_sell_price}")
                raise HTTPException(
                    status_code=400,
                    detail=f"SELL_STOP price must be less than the current market price. Your price: {order_price}, Current price: {current_sell_price}"
                )
            elif order_type == 'SELL_LIMIT' and order_price <= current_sell_price:
                orders_logger.error(f"Invalid SELL_LIMIT price: {order_price} must be greater than current sell price {current_sell_price}")
                raise HTTPException(
                    status_code=400,
                    detail=f"SELL_LIMIT price must be greater than the current market price. Your price: {order_price}, Current price: {current_sell_price}"
                )
            elif order_type == 'BUY_STOP' and order_price <= current_buy_price:
                orders_logger.error(f"Invalid BUY_STOP price: {order_price} must be greater than current buy price {current_buy_price}")
                raise HTTPException(
                    status_code=400,
                    detail=f"BUY_STOP price must be greater than the current market price. Your price: {order_price}, Current price: {current_buy_price}"
                )

        # Generate a unique order_id for the new pending order using the async utility
        order_model = get_order_model(user_type)
        new_order_id = await generate_unique_10_digit_id(db, order_model, 'order_id')

        # Fetch contract_size from ExternalSymbolInfo table
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_request.symbol))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()
        
        if not ext_symbol_info or ext_symbol_info.contract_size is None:
            orders_logger.error(f"Missing critical ExternalSymbolInfo for symbol {order_request.symbol}.")
            raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_request.symbol}.")
        
        # Calculate contract_value = contract_size * order_quantity
        contract_size = Decimal(str(ext_symbol_info.contract_size))
        contract_value = contract_size * order_request.order_quantity
        orders_logger.info(f"Calculated contract_value for pending order: {contract_value} (contract_size: {contract_size} * quantity: {order_request.order_quantity})")

        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_order, db, user_type)
        orders_logger.info(f"[DEBUG] User data from cache: {user_data}")
        
        group_name = user_data.get('group_name') if user_data else None
        orders_logger.info(f"[DEBUG] Group name: {group_name}")
        
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        orders_logger.info(f"[DEBUG] Group settings from cache: {group_settings}")
        
        # If group settings not found in cache, try to fetch from database
        if not group_settings and group_name:
            from app.crud import group as crud_group
            db_group = await crud_group.get_group_by_name(db, group_name)
            orders_logger.info(f"[DEBUG] Group from database: {db_group}")
            
            if db_group:
                # Extract sending_orders from database result
                if isinstance(db_group, list) and len(db_group) > 0:
                    sending_orders_db = getattr(db_group[0], 'sending_orders', None)
                    orders_logger.info(f"[DEBUG] sending_orders from database (list): {sending_orders_db}")
                else:
                    sending_orders_db = getattr(db_group, 'sending_orders', None)
                    orders_logger.info(f"[DEBUG] sending_orders from database (single): {sending_orders_db}")
                
                # Store in cache for future use
                if sending_orders_db is not None:
                    group_settings = {'sending_orders': sending_orders_db}
                    await set_group_settings_cache(redis_client, group_name, group_settings)
                    orders_logger.info(f"[DEBUG] Updated group settings cache with: {group_settings}")
        
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        orders_logger.info(f"[DEBUG] sending_orders value: {sending_orders}, type: {type(sending_orders)}")
        
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        orders_logger.info(f"[DEBUG] sending_orders_normalized: {sending_orders_normalized}")
        
        # TEMPORARY FIX: Force Barclays mode for testing if group_name contains 'barclays' (case insensitive)
        if group_name and 'barclays' in group_name.lower():
            orders_logger.info(f"[DEBUG] Forcing Barclays mode for testing because group_name '{group_name}' contains 'barclays'")
            sending_orders_normalized = 'barclays'
        
        is_barclays_live_user = (user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_order} is_barclays_live_user: {is_barclays_live_user} (user_type: {user_type}, sending_orders_normalized: {sending_orders_normalized})")

        # Generate SL/TP IDs if values are provided
        stoploss_id = None
        if order_request.stop_loss is not None:
            stoploss_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_id')

        takeprofit_id = None
        if order_request.take_profit is not None:
            takeprofit_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_id')

        # Prepare order data for internal processing
        order_data_for_internal_processing = {
            'order_id': new_order_id, # Assign the generated order_id
            'order_company_name': order_request.symbol,
            'order_type': order_request.order_type,
            'order_quantity': order_request.order_quantity,
            'order_price': order_request.order_price, # This is the limit/stop price
            'user_type': user_type,
            'status': order_request.status,
            'stop_loss': order_request.stop_loss,
            'take_profit': order_request.take_profit,
            'stoploss_id': stoploss_id,
            'takeprofit_id': takeprofit_id,
            'order_user_id': user_id_for_order,  # Always current_user.id
            'order_status': "PENDING_PROCESSING" if is_barclays_live_user else order_request.order_status, # Use PENDING_PROCESSING for Barclays users
            'contract_value': contract_value, # Store calculated contract_value
            'margin': None,         # Still None for pending orders
            'open_time': None # Not open yet
        }

        # Log for debugging
        orders_logger.info(f"[DEBUG][pending_order] Prepared order_data_for_internal_processing: {order_data_for_internal_processing}")
        if is_barclays_live_user:
            orders_logger.info(f"[BARCLAYS] Setting initial order_status to PENDING_PROCESSING for Barclays user. Will be updated to PENDING after service provider confirmation.")

        # Defensive check: ensure user_id is valid
        if not user_id_for_order:
            orders_logger.error("[ERROR][pending_order] current_user.id is missing or invalid!")
            raise HTTPException(status_code=400, detail="Authenticated user not found. Cannot place pending order.")

        orders_logger.info(f"Placing PENDING order: {order_request.order_type} for user {user_id_for_order} at price {order_request.order_price}")

        # Create order in database with PENDING status
        # Create the OrderCreateInternal model (margin is None)
        order_create_internal = OrderCreateInternal(**order_data_for_internal_processing)
        # order_model already set above
        # Convert to dict before passing to crud_order.create_order
        db_order = await crud_order.create_order(db, order_create_internal.model_dump(), order_model) 
        await db.commit()
        await db.refresh(db_order)
        
        # Ensure the database transaction is fully committed before proceeding
        # Use a more robust verification approach with multiple retries
        max_verification_attempts = 3
        verification_delay = 0.5
        verification_order = None
        
        for attempt in range(max_verification_attempts):
            try:
                # Create a new connection to the database to verify the order is truly committed
                from app.database.session import AsyncSessionLocal
                async with AsyncSessionLocal() as verify_db:
                    verification_order = await crud_order.get_order_by_id(verify_db, db_order.order_id, order_model)
                    if verification_order:
                        orders_logger.info(f"Order {db_order.order_id} verified in database on attempt {attempt + 1} before adding to Redis.")
                        break
            except Exception as e:
                orders_logger.error(f"Error during verification attempt {attempt + 1}: {str(e)}", exc_info=True)
            
            # If verification failed and we have more attempts, wait and try again
            if attempt < max_verification_attempts - 1:
                orders_logger.warning(f"Order {db_order.order_id} not verified on attempt {attempt + 1}, waiting {verification_delay}s before retry...")
                await asyncio.sleep(verification_delay)
                verification_delay *= 2  # Exponential backoff for verification
        
        if not verification_order:
            orders_logger.error(f"Order {db_order.order_id} could not be verified in database after {max_verification_attempts} attempts. Not adding to Redis.")
            raise HTTPException(status_code=500, detail="Order created but could not be verified in database.")

        # For Barclays live users, send to Firebase
        if is_barclays_live_user:
            orders_logger.info(f"Barclays live user detected. Sending pending order to Firebase.")
            
            # Prepare data to send to Firebase
            firebase_order_data = {
                'order_id': db_order.order_id,
                'order_user_id': db_order.order_user_id,
                'order_company_name': db_order.order_company_name,
                'order_type': db_order.order_type,
                'order_status': db_order.order_status,  # Will be PENDING_PROCESSING for Barclays users
                'order_price': str(db_order.order_price),
                'order_quantity': str(db_order.order_quantity),
                'contract_value': str(db_order.contract_value),
                'margin': str(db_order.margin) if db_order.margin else None,
                'stop_loss': str(db_order.stop_loss) if db_order.stop_loss else None,
                'take_profit': str(db_order.take_profit) if db_order.take_profit else None,
                'stoploss_id': db_order.stoploss_id,
                'takeprofit_id': db_order.takeprofit_id,
                'status': getattr(db_order, 'status', None),
                'action': 'pending_order',
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            
            orders_logger.info(f"[FIREBASE] Prepared Firebase payload for pending order: {firebase_order_data}")
            
            try:
                # Send to Firebase
                # Note: For Barclays users, the order starts with status PENDING_PROCESSING
                # The service provider should confirm receipt and execution, after which
                # the status will be changed to PENDING through a separate API call
                result = await send_order_to_firebase(firebase_order_data, "live")
                if result:
                    orders_logger.info(f"[FIREBASE] Successfully sent pending order {db_order.order_id} to Firebase for Barclays user {user_id_for_order}")
                else:
                    orders_logger.error(f"[FIREBASE] Failed to send pending order {db_order.order_id} to Firebase for Barclays user {user_id_for_order}")
            except Exception as e:
                orders_logger.error(f"[FIREBASE] Exception while sending pending order to Firebase: {str(e)}", exc_info=True)
        
        # For non-Barclays users or as a backup for all users, add to Redis pending orders
        # Ensure the order dict passed to add_pending_order has all necessary fields
        order_dict_for_redis = {
            'order_id': db_order.order_id,
            'order_user_id': db_order.order_user_id,
            'order_company_name': db_order.order_company_name,
            'order_type': db_order.order_type,
            'order_status': db_order.order_status, # Should be PENDING
            'order_price': str(db_order.order_price), # Store as string for JSON serialization
            'order_quantity': str(db_order.order_quantity), # Store as string
            'contract_value': str(db_order.contract_value) if db_order.contract_value else None,
            'margin': str(db_order.margin) if db_order.margin else None,
            'stop_loss': str(db_order.stop_loss) if db_order.stop_loss else None,
            'take_profit': str(db_order.take_profit) if db_order.take_profit else None,
            'stoploss_id': db_order.stoploss_id,
            'takeprofit_id': db_order.takeprofit_id,
            'user_type': user_type,
            'status': db_order.status,
            'created_at': getattr(db_order, 'created_at', None).isoformat() if getattr(db_order, 'created_at', None) else None,
            'updated_at': getattr(db_order, 'updated_at', None).isoformat() if getattr(db_order, 'updated_at', None) else None,
            # Add any other fields that might be needed by trigger_pending_order
        }
        
        # We've already verified the order exists, so we can proceed with adding to Redis
        # Only store non-Barclays users' pending orders in Redis for price comparison
        if not is_barclays_live_user or user_type == 'demo':
            await add_pending_order(redis_client, order_dict_for_redis)
            orders_logger.info(f"Pending order {db_order.order_id} added to Redis for non-Barclays user or demo user.")
        else:
            orders_logger.info(f"Skipping Redis storage for Barclays user pending order {db_order.order_id}")

        # --- Update user data cache after DB update ---
        try:
            # Fetch the latest user data from DB to update cache
            db_user = None
            if user_type == 'live':
                db_user = await get_user_by_id(db, user_id_for_order, user_type=user_type)
            else:
                db_user = await get_demo_user_by_id(db, user_id_for_order, user_type=user_type)
            
            if db_user:
                user_data_to_cache = {
                    "id": db_user.id,
                    "email": getattr(db_user, 'email', None),
                    "group_name": db_user.group_name,
                    "leverage": db_user.leverage,
                    "user_type": user_type,
                    "account_number": getattr(db_user, 'account_number', None),
                    "wallet_balance": db_user.wallet_balance,
                    "margin": db_user.margin,
                    "first_name": getattr(db_user, 'first_name', None),
                    "last_name": getattr(db_user, 'last_name', None),
                    "country": getattr(db_user, 'country', None),
                    "phone_number": getattr(db_user, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, user_id_for_order, user_data_to_cache)
                orders_logger.info(f"User data cache updated for user {user_id_for_order} after placing pending order")
        except Exception as e:
            orders_logger.error(f"Error updating user data cache after pending order placement: {e}", exc_info=True)

        # Update static orders cache - force a fresh fetch from the database
        await update_user_static_orders(user_id_for_order, db, redis_client, user_type)
        
        # Publish updates to notify WebSocket clients - make sure these are in the right order
        await publish_order_update(redis_client, user_id_for_order)
        await publish_user_data_update(redis_client, user_id_for_order)
        
        return OrderResponse(
            order_id=db_order.order_id,
            order_user_id=db_order.order_user_id,
            order_company_name=db_order.order_company_name,
            order_type=db_order.order_type,
            order_quantity=db_order.order_quantity,
            order_price=db_order.order_price,
            status=db_order.status,
            stop_loss=db_order.stop_loss,
            take_profit=db_order.take_profit,
            order_status=db_order.order_status,
            contract_value=db_order.contract_value if db_order.contract_value is not None else None,
            margin=db_order.margin if db_order.margin is not None else None,
            created_at=getattr(db_order, 'created_at', None).isoformat() if getattr(db_order, 'created_at', None) else None,
            updated_at=getattr(db_order, 'updated_at', None).isoformat() if getattr(db_order, 'updated_at', None) else None,
        )

    except OrderProcessingError as e:
        orders_logger.error(f"Order processing error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        orders_logger.error(f"Unexpected error in place_pending_order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process pending order: {str(e)}")


@router.post("/close", response_model=OrderResponse)
async def close_order(
    close_request: CloseOrderRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token),
    token: str = Depends(oauth2_scheme)
):
    """
    Close an existing order.
    """
    from app.core.logging_config import frontend_orders_logger, error_logger
    from app.core.cache import get_adjusted_market_price_cache
    
    try:
        # Log the incoming request
        frontend_orders_logger.info(f"FRONTEND ORDER CLOSE - REQUEST: {json.dumps(close_request.dict(), default=str)}")
        orders_logger.info(f"Order close request received - Order ID: {close_request.order_id}, User ID: {close_request.user_id}")
        
        # Validate the request
        if not close_request.order_id:
            error_msg = "Order ID is required"
            frontend_orders_logger.error(f"FRONTEND ORDER CLOSE ERROR: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
        
        target_user_id_to_operate_on = current_user.id
        user_to_operate_on = current_user

        if close_request.user_id is not None:
            is_service_account = getattr(current_user, 'is_service_account', False)
            if is_service_account:
                orders_logger.info(f"Service account operation - Target user ID: {close_request.user_id}")
                enforce_service_user_id_restriction(close_request.user_id, token)
                _user = await get_user_by_id(db, close_request.user_id)
                if _user:
                    user_to_operate_on = _user
                else:
                    _demo_user = await get_demo_user_by_id(db, close_request.user_id)
                    if _demo_user:
                        user_to_operate_on = _demo_user
                    else:
                        orders_logger.error(f"Target user not found for service operation - User ID: {close_request.user_id}")
                        raise HTTPException(status_code=404, detail="Target user not found for service op.")
                target_user_id_to_operate_on = close_request.user_id
            else:
                if close_request.user_id != current_user.id:
                    orders_logger.error(f"Unauthorized user_id specification - Current user: {current_user.id}, Requested user: {close_request.user_id}")
                    raise HTTPException(status_code=403, detail="Not authorized to specify user_id.")
        
        orders_logger.info(f"user_to_operate_on: {user_to_operate_on}, type: {type(user_to_operate_on)}, attrs: {dir(user_to_operate_on)}")
        order_model_class = get_order_model(user_to_operate_on)
        orders_logger.info(f"Using order model: {getattr(order_model_class, '__tablename__', str(order_model_class))} for user {user_to_operate_on.id} ({type(user_to_operate_on).__name__})")
        order_id = close_request.order_id

        # Get the order first to determine the order type and symbol
        db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
        if not db_order:
            raise HTTPException(status_code=404, detail="Order not found.")

        # Get user's group name for adjusted price lookup
        user_group_name = getattr(user_to_operate_on, 'group_name', 'default')
        
        # Get adjusted market prices from Redis cache
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, user_group_name, db_order.order_company_name)
        if not adjusted_prices:
            raise HTTPException(status_code=500, detail="Could not fetch current market prices.")

        # Set close price based on order type
        if db_order.order_type == "BUY":
            close_price = adjusted_prices.get('sell')  # Use sell price to close a buy position
            orders_logger.info(f"Using adjusted sell price {close_price} to close BUY order {order_id}")
        elif db_order.order_type == "SELL":
            close_price = adjusted_prices.get('buy')   # Use buy price to close a sell position
            orders_logger.info(f"Using adjusted buy price {close_price} to close SELL order {order_id}")
        else:
            raise HTTPException(status_code=400, detail=f"Invalid order type: {db_order.order_type}")

        if not close_price:
            raise HTTPException(status_code=500, detail="Could not determine closing price from market data.")

        try:
            close_price = Decimal(str(close_price))
            if close_price <= Decimal("0"):
                raise HTTPException(status_code=400, detail="Close price must be positive.")
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="Invalid close price format.")

        orders_logger.info(f"Request to close order {order_id} for user {user_to_operate_on.id} ({type(user_to_operate_on).__name__}) with price {close_price}. Frontend provided type: {close_request.order_type}, company: {close_request.order_company_name}, status: {close_request.order_status}, frontend_status: {close_request.status}.")

        from app.services.order_processing import generate_unique_10_digit_id
        close_id = await generate_unique_10_digit_id(db, order_model_class, 'close_id')

        try:
            if isinstance(user_to_operate_on, User):
                user_type = 'live'
                group_name = user_to_operate_on.group_name
                
                sending_orders_normalized = None
                if group_name:
                    group_settings = await get_group_settings_cache(redis_client, group_name)
                    sending_orders = group_settings.get('sending_orders') if group_settings else None
                    if sending_orders:
                        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
                    else:
                        # Fallback to DB if not in cache
                        db_group = await crud_group.get_group_by_name(db, group_name)
                        if db_group:
                            sending_orders_db = getattr(db_group[0] if isinstance(db_group, list) else db_group, 'sending_orders', None)
                            if sending_orders_db:
                                sending_orders_normalized = sending_orders_db.lower() if isinstance(sending_orders_db, str) else sending_orders_db

                orders_logger.info(f"User group: {group_name}, sending_orders setting: {sending_orders_normalized}")
                
                is_barclays_live_user = (user_type == 'live' and sending_orders_normalized == 'barclays')
                if is_barclays_live_user:
                    orders_logger.info(f"Live user {user_to_operate_on.id} from group '{group_name}' has 'sending_orders' set to 'barclays'. Pushing close request to Firebase and skipping local DB update.")
                    
                    # Fetch the order from DB to get all necessary details for Firebase
                    db_order_for_firebase = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                    if not db_order_for_firebase:
                        raise HTTPException(status_code=404, detail="Order to be closed not found in database for Firebase operation.")

                    # Check for existing stop loss and take profit before closing the order
                    # Generate cancel IDs if SL/TP exist and send individual cancellation messages
                    stoploss_cancel_id = None
                    takeprofit_cancel_id = None
                    
                    # Flag to track if SL or TP exists
                    has_sl_or_tp = False
                    
                    # Check if stop_loss exists and is > 0
                    if db_order_for_firebase.stop_loss is not None and db_order_for_firebase.stop_loss > 0:
                        has_sl_or_tp = True
                        stoploss_cancel_id = await generate_unique_10_digit_id(db, order_model_class, 'stoploss_cancel_id')
                        orders_logger.info(f"Generating stoploss_cancel_id: {stoploss_cancel_id} for order {order_id}")
                        
                        # Send stop loss cancellation to Firebase
                        sl_cancel_payload = {
                            "action": "cancel_stoploss",
                            "status": "TP/SL-CLOSED",  # Updated status for SL cancellation
                            "stop_loss": db_order_for_firebase.stop_loss,
                            "order_id": db_order_for_firebase.order_id,
                            "user_id": user_to_operate_on.id,
                            "stoploss_id": db_order_for_firebase.stoploss_id,
                            "stoploss_cancel_id": stoploss_cancel_id,
                            "symbol": db_order_for_firebase.order_company_name,
                            "order_type": db_order_for_firebase.order_type
                        }
                        orders_logger.info(f"[FIREBASE_SL_CANCEL] Sending stop loss cancellation: {sl_cancel_payload}")
                        background_tasks.add_task(send_order_to_firebase, sl_cancel_payload, "live")
                    
                    # Check if take_profit exists and is > 0
                    if db_order_for_firebase.take_profit is not None and db_order_for_firebase.take_profit > 0:
                        has_sl_or_tp = True
                        takeprofit_cancel_id = await generate_unique_10_digit_id(db, order_model_class, 'takeprofit_cancel_id')
                        orders_logger.info(f"Generating takeprofit_cancel_id: {takeprofit_cancel_id} for order {order_id}")
                        
                        # Send take profit cancellation to Firebase
                        tp_cancel_payload = {
                            "action": "cancel_takeprofit",
                            "status": "TP/SL-CLOSED",  # Updated status for TP cancellation
                            "take_profit": db_order_for_firebase.take_profit,
                            "order_id": db_order_for_firebase.order_id,
                            "user_id": user_to_operate_on.id,
                            "takeprofit_id": db_order_for_firebase.takeprofit_id,
                            "takeprofit_cancel_id": takeprofit_cancel_id,
                            "symbol": db_order_for_firebase.order_company_name,
                            "order_type": db_order_for_firebase.order_type
                        }
                        orders_logger.info(f"[FIREBASE_TP_CANCEL] Sending take profit cancellation: {tp_cancel_payload}")
                        background_tasks.add_task(send_order_to_firebase, tp_cancel_payload, "live")

                    # Set the status for the close request based on whether SL/TP exists
                    close_request_status = "TP/SL-CLOSED" if has_sl_or_tp else close_request.status

                    firebase_close_data = {
                        "order_id": db_order_for_firebase.order_id,
                        "close_price": str(close_request.close_price),
                        "user_id": user_to_operate_on.id,
                        "order_type": db_order_for_firebase.order_type,
                        "order_company_name": db_order_for_firebase.order_company_name,
                        "order_quantity": str(db_order_for_firebase.order_quantity),
                        "contract_value": str(db_order_for_firebase.contract_value),
                        "order_status": close_request.order_status, # Status from the request indicating the action
                        "status": close_request_status, # Use the status based on SL/TP existence
                        "action": "close_order",
                        "close_id": close_id
                    }

                    orders_logger.info(f"[FIREBASE_CLOSE_REQUEST] Preparing to send payload for user-initiated close: {firebase_close_data}")
                    background_tasks.add_task(send_order_to_firebase, firebase_close_data, "live")
                    
                    db_order_for_response = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                    if db_order_for_response:
                        # Per request, do not change the status. Keep it OPEN until the provider confirms.
                        # db_order_for_response.order_status = "PENDING_CLOSE" 
                        db_order_for_response.close_message = "Close request sent to service provider."
                        db_order_for_response.close_id = close_id # Save close_id in DB
                        
                        # Update status field if order has SL or TP
                        if has_sl_or_tp:
                            db_order_for_response.status = "TP/SL-CLOSED"
                        
                        # Save the cancel IDs if they were generated
                        if stoploss_cancel_id:
                            db_order_for_response.stoploss_cancel_id = stoploss_cancel_id
                        if takeprofit_cancel_id:
                            db_order_for_response.takeprofit_cancel_id = takeprofit_cancel_id
                        
                        await db.commit()
                        await db.refresh(db_order_for_response)
                        
                        # Log action in OrderActionHistory
                        user_type_str = "live" if isinstance(user_to_operate_on, User) else "demo"
                        
                        # The OrderUpdateRequest was not defined; using a dict instead for tracking.
                        update_fields_for_history = {
                            "close_id": close_id,
                            "close_message": "Close request sent to service provider.",
                            "close_price": close_price,
                        }
                        if has_sl_or_tp:
                            update_fields_for_history['status'] = "TP/SL-CLOSED"
                        elif close_request.status is not None:
                            update_fields_for_history['status'] = close_request.status
                        if stoploss_cancel_id:
                            update_fields_for_history['stoploss_cancel_id'] = stoploss_cancel_id
                        if takeprofit_cancel_id:
                            update_fields_for_history['takeprofit_cancel_id'] = takeprofit_cancel_id
                        
                        await crud_order.update_order_with_tracking(
                            db,
                            db_order_for_response,
                            update_fields_for_history,
                            user_id=user_to_operate_on.id,
                            user_type=user_type_str,
                            action_type="CLOSE_REQUESTED"
                        )
                        await db.commit()
                        await db.refresh(db_order_for_response)
                        return OrderResponse.model_validate(db_order_for_response, from_attributes=True)
                    else:
                        raise HTTPException(status_code=404, detail="Order not found for external closure processing.")
                else:
                    # Fetch user_group by group_name before using it in logs
                    user_group = await crud_group.get_group_by_name(db, getattr(user_to_operate_on, 'group_name', None))
                    group_name_str = user_group.group_name if user_group and hasattr(user_group, 'group_name') else 'default'
                    orders_logger.info(f"Live user {user_to_operate_on.id} from group '{group_name_str}' ('sending_orders' is NOT 'barclays'). Processing close locally.")
                    async with db.begin_nested():
                        db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                        if db_order is None:
                            raise HTTPException(status_code=404, detail="Order not found.")
                        if db_order.order_user_id != user_to_operate_on.id and not getattr(current_user, 'is_admin', False):
                            raise HTTPException(status_code=403, detail="Not authorized to close this order.")
                        if db_order.order_status != 'OPEN':
                            raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

                        order_symbol = db_order.order_company_name.upper()
                        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
                        symbol_info_result = await db.execute(symbol_info_stmt)
                        ext_symbol_info = symbol_info_result.scalars().first()
                        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                            raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
                        contract_size = Decimal(str(ext_symbol_info.contract_size))
                        profit_currency = ext_symbol_info.profit.upper()

                        group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
                        if not group_settings:
                            raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
                        
                        commission_type = int(group_settings.get('commision_type', -1))
                        commission_value_type = int(group_settings.get('commision_value_type', -1))
                        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                        
                        # Get existing entry commission from the order
                        existing_entry_commission = Decimal(str(db_order.commission or "0.0"))
                        orders_logger.info(f"Existing entry commission for order {order_id}: {existing_entry_commission}")
                        
                        # Only calculate exit commission if applicable
                        exit_commission = Decimal("0.0")
                        if commission_type in [0, 2]:  # "Every Trade" or "Out"
                            if commission_value_type == 0:  # Per lot
                                exit_commission = quantity * commission_rate
                            elif commission_value_type == 1:  # Percent of price
                                calculated_exit_contract_value = quantity * contract_size * close_price
                                if calculated_exit_contract_value > Decimal("0.0"):
                                    exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                        
                        # Total commission is existing entry commission plus exit commission
                        total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        orders_logger.info(f"Commission calculation for order {order_id}: entry={existing_entry_commission}, exit={exit_commission}, total={total_commission_for_trade}")

                        if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
                        elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
                        else: raise HTTPException(status_code=500, detail="Invalid order type.")
                        
                        profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db, redis_client=redis_client)
                        if profit_currency != "USD" and profit_usd == profit: 
                            orders_logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                            raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

                        db_order.order_status = "CLOSED"
                        db_order.close_price = close_price
                        db_order.net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        db_order.swap = db_order.swap or Decimal("0.0")
                        db_order.commission = total_commission_for_trade
                        db_order.close_id = close_id # Save close_id in DB

                        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
                        swap_amount = db_order.swap
                        db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

                        transaction_time = datetime.datetime.now(datetime.timezone.utc)
                        wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time, "order_id": db_order.order_id}
                        if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
                        else: wallet_common_data["user_id"] = db_user_locked.id
                        if db_order.net_profit != Decimal("0.0"):
                            transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
                        if total_commission_for_trade > Decimal("0.0"):
                            transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
                        if swap_amount != Decimal("0.0"):
                            transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                            db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))

                        await db.commit()
                        await db.refresh(db_order)
                        
                        # Log the user's wallet balance and margin after commit
                        orders_logger.info(f"AFTER COMMIT: User {db_user_locked.id} wallet_balance={db_user_locked.wallet_balance}, margin={db_user_locked.margin}")
                        
                        # Define variables needed for WebSocket updates
                        user_id = db_order.order_user_id
                        user_type_str = 'demo' if isinstance(db_user_locked, DemoUser) else 'live'
                        
                        # Update user data cache with the latest values from db_user_locked
                        user_data_to_cache = {
                            "id": db_user_locked.id,
                            "email": getattr(db_user_locked, 'email', None),
                            "group_name": db_user_locked.group_name,
                            "leverage": db_user_locked.leverage,
                            "user_type": user_type_str,
                            "account_number": getattr(db_user_locked, 'account_number', None),
                            "wallet_balance": db_user_locked.wallet_balance,
                            "margin": db_user_locked.margin,
                            "first_name": getattr(db_user_locked, 'first_name', None),
                            "last_name": getattr(db_user_locked, 'last_name', None),
                            "country": getattr(db_user_locked, 'country', None),
                            "phone_number": getattr(db_user_locked, 'phone_number', None)
                        }
                        orders_logger.info(f"Setting user data cache for user {user_id} with wallet_balance={user_data_to_cache['wallet_balance']}, margin={user_data_to_cache['margin']}")
                        await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                        orders_logger.info(f"User data cache updated for user {user_id}")
                        
                        await update_user_static_orders(user_id, db, redis_client, user_type_str)
                        
                        # Publish updates in the correct order
                        orders_logger.info(f"Publishing order update for user {user_id}")
                        await publish_order_update(redis_client, user_id)
                        
                        orders_logger.info(f"Publishing user data update for user {user_id}")
                        await publish_user_data_update(redis_client, user_id)
                        
                        orders_logger.info(f"Publishing market data trigger")
                        await publish_market_data_trigger(redis_client)
                        
                        return OrderResponse.model_validate(db_order, from_attributes=True)
            else:
                # Always fetch user_group before logging
                user_group = await crud_group.get_group_by_name(db, getattr(user_to_operate_on, 'group_name', None))
                group_name_str = (
                    user_group[0].group_name if user_group and isinstance(user_group, list) and len(user_group) > 0 and hasattr(user_group[0], 'group_name')
                    else 'default'
                )
                user_type_str = 'Demo user' if isinstance(user_to_operate_on, DemoUser) else 'Live user'
                orders_logger.info(f"{user_type_str} {user_to_operate_on.id} from group '{group_name_str}' ('sending_orders' is NOT 'barclays'). Processing close locally.")
                db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                if db_order is None:
                    raise HTTPException(status_code=404, detail="Order not found.")
                if db_order.order_user_id != user_to_operate_on.id and not getattr(current_user, 'is_admin', False):
                    raise HTTPException(status_code=403, detail="Not authorized to close this order.")
                if db_order.order_status != 'OPEN':
                    raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

                order_symbol = db_order.order_company_name.upper()
                quantity = Decimal(str(db_order.order_quantity))
                entry_price = Decimal(str(db_order.order_price))
                order_type_db = db_order.order_type.upper()
                user_group_name = getattr(user_to_operate_on, 'group_name', 'default')
                # Use correct lock function for user type
                if isinstance(user_to_operate_on, DemoUser):
                    db_user_locked = await get_demo_user_by_id_with_lock(db, user_to_operate_on.id)
                    if db_user_locked is None:
                        # Debug fallback: try plain fetch
                        from app.crud.user import get_demo_user_by_id
                        fallback_demo_user = await get_demo_user_by_id(db, user_to_operate_on.id)
                        if fallback_demo_user is None:
                            orders_logger.error(f"[DEBUG] DemoUser with ID {user_to_operate_on.id} does NOT exist in DB (plain fetch also failed).")
                        else:
                            orders_logger.error(f"[DEBUG] DemoUser with ID {user_to_operate_on.id} exists in DB WITHOUT lock. Problem is with locking.")
                else:
                    db_user_locked = await get_user_by_id_with_lock(db, user_to_operate_on.id)
                if db_user_locked is None:
                    orders_logger.error(f"Could not retrieve and lock user record for user ID: {user_to_operate_on.id}")
                    raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

                all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                    db=db, user_id=db_user_locked.id, symbol=order_symbol, order_model=order_model_class
                )
                margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
                    db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                    open_positions_for_symbol=all_open_orders_for_symbol, order_model=order_model_class
                )
                margin_before_recalc = margin_before_recalc_dict["total_margin"]
                current_overall_margin = Decimal(str(db_user_locked.margin))
                non_symbol_margin = current_overall_margin - margin_before_recalc
                remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
                margin_after_symbol_recalc_dict = await calculate_total_symbol_margin_contribution(
                    db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                    open_positions_for_symbol=remaining_orders_for_symbol_after_close, order_model=order_model_class
                )
                margin_after_symbol_recalc = margin_after_symbol_recalc_dict["total_margin"]
                db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

                symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
                symbol_info_result = await db.execute(symbol_info_stmt)
                ext_symbol_info = symbol_info_result.scalars().first()
                if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                    raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
                contract_size = Decimal(str(ext_symbol_info.contract_size))
                profit_currency = ext_symbol_info.profit.upper()

                group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
                if not group_settings:
                    raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
                
                commission_type = int(group_settings.get('commision_type', -1))
                commission_value_type = int(group_settings.get('commision_value_type', -1))
                commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                
                # Get existing entry commission from the order
                existing_entry_commission = Decimal(str(db_order.commission or "0.0"))
                orders_logger.info(f"Existing entry commission for order {order_id}: {existing_entry_commission}")
                
                # Only calculate exit commission if applicable
                exit_commission = Decimal("0.0")
                if commission_type in [0, 2]:  # "Every Trade" or "Out"
                    if commission_value_type == 0:  # Per lot
                        exit_commission = quantity * commission_rate
                    elif commission_value_type == 1:  # Percent of price
                        calculated_exit_contract_value = quantity * contract_size * close_price
                        if calculated_exit_contract_value > Decimal("0.0"):
                            exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                
                # Total commission is existing entry commission plus exit commission
                total_commission_for_trade = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                orders_logger.info(f"Commission calculation for order {order_id}: entry={existing_entry_commission}, exit={exit_commission}, total={total_commission_for_trade}")

                if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
                elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
                else: raise HTTPException(status_code=500, detail="Invalid order type.")
                
                profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db, redis_client=redis_client)
                if profit_currency != "USD" and profit_usd == profit: 
                    orders_logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                    raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

                db_order.order_status = "CLOSED"
                db_order.close_price = close_price
                db_order.net_profit = (profit_usd - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                db_order.swap = db_order.swap or Decimal("0.0")
                db_order.commission = total_commission_for_trade
                db_order.close_id = close_id # Save close_id in DB

                original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
                swap_amount = db_order.swap
                db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

                transaction_time = datetime.datetime.now(datetime.timezone.utc)
                wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time, "order_id": db_order.order_id}
                if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
                else: wallet_common_data["user_id"] = db_user_locked.id
                if db_order.net_profit != Decimal("0.0"):
                    transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_profit))
                if total_commission_for_trade > Decimal("0.0"):
                    transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_commission))
                if swap_amount != Decimal("0.0"):
                    transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=transaction_id_swap))

                await db.commit()
                await db.refresh(db_order)
                
                # Log the user's wallet balance and margin after commit
                orders_logger.info(f"AFTER COMMIT: User {db_user_locked.id} wallet_balance={db_user_locked.wallet_balance}, margin={db_user_locked.margin}")
                
                # Define variables needed for WebSocket updates
                user_id = db_order.order_user_id
                user_type_str = 'demo' if isinstance(db_user_locked, DemoUser) else 'live'
                
                # Update user data cache with the latest values from db_user_locked
                user_data_to_cache = {
                    "id": db_user_locked.id,
                    "email": getattr(db_user_locked, 'email', None),
                    "group_name": db_user_locked.group_name,
                    "leverage": db_user_locked.leverage,
                    "user_type": user_type_str,
                    "account_number": getattr(db_user_locked, 'account_number', None),
                    "wallet_balance": db_user_locked.wallet_balance,
                    "margin": db_user_locked.margin,
                    "first_name": getattr(db_user_locked, 'first_name', None),
                    "last_name": getattr(db_user_locked, 'last_name', None),
                    "country": getattr(db_user_locked, 'country', None),
                    "phone_number": getattr(db_user_locked, 'phone_number', None),
                }
                orders_logger.info(f"Setting user data cache for user {user_id} with wallet_balance={user_data_to_cache['wallet_balance']}, margin={user_data_to_cache['margin']}")
                await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                orders_logger.info(f"User data cache updated for user {user_id}")
                
                await update_user_static_orders(user_id, db, redis_client, user_type_str)
                
                # Publish updates in the correct order
                orders_logger.info(f"Publishing order update for user {user_id}")
                await publish_order_update(redis_client, user_id)
                
                orders_logger.info(f"Publishing user data update for user {user_id}")
                await publish_user_data_update(redis_client, user_id)
                
                orders_logger.info(f"Publishing market data trigger")
                await publish_market_data_trigger(redis_client)
                
                return OrderResponse.model_validate(db_order, from_attributes=True)
        except Exception as e:
            orders_logger.error(f"Error processing close order: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error processing close order: {str(e)}")
    except Exception as e:
        orders_logger.error(f"Error in close_order endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error in close_order endpoint: {str(e)}")



from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
from decimal import Decimal, InvalidOperation
from pydantic import BaseModel, Field
import uuid
import time

from app.dependencies.redis_client import get_redis_client
from app.database.session import get_db
from app.core.security import get_current_user
from app.crud import crud_order, user as crud_user, group as crud_group
from app.database.models import User, DemoUser
from app.api.v1.endpoints.orders import get_order_model
from app.core.firebase import send_order_to_firebase
from app.core.cache import get_user_data_cache, get_group_settings_cache
from app.services.pending_orders import remove_pending_order, add_pending_order

class ModifyPendingOrderRequest(BaseModel):
    order_id: str
    order_type: str
    order_price: Decimal = Field(..., gt=0, description="The new price for the pending order (required)")
    order_quantity: Decimal = Field(..., gt=0, description="The new quantity for the pending order (required)")
    order_company_name: str
    user_id: int
    user_type: str
    order_status: str
    status: str  # No close_price field; order_price and order_quantity are required for modification

@router.post("/modify-pending")
async def modify_pending_order(
    modify_request: ModifyPendingOrderRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user),
):
    try:
        order_model = get_order_model(modify_request.user_type)
        db_order = await crud_order.get_order_by_id_and_user_id(
            db,
            modify_request.order_id,
            modify_request.user_id,
            order_model
        )

        if not db_order:
            raise HTTPException(status_code=404, detail="Order not found")

        if db_order.order_status != "PENDING":
            raise HTTPException(status_code=400, detail="Only PENDING orders can be modified")

        user_data = await get_user_data_cache(redis_client, modify_request.user_id, db, modify_request.user_type)
        group_name = user_data.get("group_name") if user_data else None

        sending_orders_normalized = None
        if group_name:
            group_settings = await get_group_settings_cache(redis_client, group_name)
            if group_settings:
                sending_orders = group_settings.get("sending_orders")
                sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders

        is_barclays_live_user = (modify_request.user_type == 'live' and sending_orders_normalized == 'barclays')

        order_model = get_order_model(modify_request.user_type)
        modify_id = await generate_unique_10_digit_id(db, order_model, 'order_id')

        if is_barclays_live_user:
            firebase_modify_data = {
                "order_id": modify_request.order_id,
                "order_user_id": modify_request.user_id,
                "order_company_name": modify_request.order_company_name,
                "order_type": modify_request.order_type,
                "order_status": modify_request.order_status,
                "order_price": str(modify_request.order_price),
                "order_quantity": str(modify_request.order_quantity),
                "status": modify_request.status,
                "modify_id": modify_id,
                "action": "modify_order"
            }
            await send_order_to_firebase(firebase_modify_data, "live")
            
            # Also update the local order with the new status and modify_id
            update_data = {
                "modify_id": modify_id,
                "status": modify_request.status
            }
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_data,
                user_id=modify_request.user_id,
                user_type=modify_request.user_type,
                action_type="MODIFY_PENDING_REQUESTED"
            )
            
            return {"message": "Order modification request sent to external service (Barclays)."}

        # For non-Barclays users, update the order
        update_data = {
            "order_price": modify_request.order_price,
            "order_quantity": modify_request.order_quantity,
            "modify_id": modify_id,
            "status": modify_request.status,
            "order_status": modify_request.order_status
        }

        updated_order = await crud_order.update_order_with_tracking(
            db,
            db_order,
            update_fields=update_data,
            user_id=modify_request.user_id,
            user_type=modify_request.user_type,
            action_type="MODIFY_PENDING"
        )

        # --- Update Redis Cache ---
        await remove_pending_order(
            redis_client,
            modify_request.order_id,
            modify_request.order_company_name,
            modify_request.order_type,
            str(modify_request.user_id)
        )

        new_pending_order_data = {
            "order_id": updated_order.order_id,
            "order_user_id": updated_order.order_user_id,
            "order_company_name": updated_order.order_company_name,
            "order_type": updated_order.order_type,
            "order_status": updated_order.order_status,
            "order_price": str(updated_order.order_price),
            "order_quantity": str(updated_order.order_quantity),
            "contract_value": str(updated_order.contract_value) if updated_order.contract_value else None,
            "margin": str(updated_order.margin) if updated_order.margin is not None else None,
            "stop_loss": str(updated_order.stop_loss) if updated_order.stop_loss is not None else None,
            "take_profit": str(updated_order.take_profit) if updated_order.take_profit is not None else None,
            "stoploss_id": updated_order.stoploss_id,
            "takeprofit_id": updated_order.takeprofit_id,
            "user_type": modify_request.user_type,
            "status": updated_order.status,
            "created_at": updated_order.created_at.isoformat() if updated_order.created_at else None,
            "updated_at": updated_order.updated_at.isoformat() if updated_order.updated_at else None,
        }
        await add_pending_order(redis_client, new_pending_order_data)

        # --- Update user data cache after DB update ---
        try:
            # Fetch the latest user data from DB to update cache
            db_user = None
            if modify_request.user_type == 'live':
                db_user = await get_user_by_id(db, modify_request.user_id)
            else:
                db_user = await get_demo_user_by_id(db, modify_request.user_id)
            
            if db_user:
                user_data_to_cache = {
                    "id": db_user.id,
                    "email": getattr(db_user, 'email', None),
                    "group_name": db_user.group_name,
                    "leverage": db_user.leverage,
                    "user_type": modify_request.user_type,
                    "account_number": getattr(db_user, 'account_number', None),
                    "wallet_balance": db_user.wallet_balance,
                    "margin": db_user.margin,
                    "first_name": getattr(db_user, 'first_name', None),
                    "last_name": getattr(db_user, 'last_name', None),
                    "country": getattr(db_user, 'country', None),
                    "phone_number": getattr(db_user, 'phone_number', None),
                }
                await set_user_data_cache(redis_client, modify_request.user_id, user_data_to_cache)
                orders_logger.info(f"User data cache updated for user {modify_request.user_id} after modifying pending order")
        except Exception as e:
            orders_logger.error(f"Error updating user data cache after pending order modification: {e}", exc_info=True)

        # Update static orders cache - force a fresh fetch from the database
        await update_user_static_orders(modify_request.user_id, db, redis_client, modify_request.user_type)

        # Publish updates to notify WebSocket clients - make sure these are in the right order
        await publish_order_update(redis_client, modify_request.user_id)
        await publish_user_data_update(redis_client, modify_request.user_id)
        
        return {
            "order_id": updated_order.order_id,
            "order_price": updated_order.order_price,
            "order_quantity": updated_order.order_quantity,
            "order_status": updated_order.order_status,
            "modify_id": updated_order.modify_id,
            "message": "Pending order successfully modified"
        }

    except InvalidOperation:
        raise HTTPException(status_code=400, detail="Invalid decimal value for price or quantity")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to modify pending order: {str(e)}")







# from fastapi import APIRouter, Depends, HTTPException
# from sqlalchemy.ext.asyncio import AsyncSession
# from redis.asyncio import Redis
# from app.crud import crud_order
# from app.dependencies.redis_client import get_redis_client
# from app.database.session import get_db
# from app.schemas.order import OrderUpdateRequest
# from app.core.security import get_user_from_service_or_user_token


# async def handle_barclays_order_open_transition(
#     db: AsyncSession,
#     redis_client: Redis,
#     db_order,
#     user_type: str
# ):
#     from app.crud import crud_order
#     from app.crud.user import update_user_margin
#     from app.services.margin_calculator import calculate_total_symbol_margin_contribution
#     from app.core.cache import get_user_data_cache
#     from decimal import Decimal
#     import logging

#     logger = logging.getLogger(__name__)

#     try:
#         user_id = db_order.order_user_id
#         symbol = db_order.order_company_name.upper()
#         order_model = get_order_model(user_type)

#         # Fetch all current open orders for the symbol (including the one being transitioned)
#         open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
#         if db_order not in open_orders:
#             open_orders.append(db_order)

#         # Calculate total margin using hedging logic
#         total_margin = await calculate_total_symbol_margin_contribution(
#             db=db,
#             redis_client=redis_client,
#             user_id=user_id,
#             symbol=symbol,
#             open_positions_for_symbol=open_orders,
#             order_model_for_calc=order_model
#             contract_size = Decimal(str(ext_symbol_info.contract_size))
#             profit_currency = ext_symbol_info.profit.upper()

#             group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
#             if not group_settings:
#                 raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
            
#             commission_type = int(group_settings.get('commision_type', -1))
#             commission_value_type = int(group_settings.get('commision_value_type', -1))
#             commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
#             exit_commission = Decimal("0.0")
#             if commission_type in [0, 2]:
#                 if commission_value_type == 0: exit_commission = quantity * commission_rate
#                 elif commission_value_type == 1:
#                     calculated_exit_contract_value = quantity * contract_size * close_price
#                     if calculated_exit_contract_value > Decimal("0.0"):
#                         exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
#             entry_commission_for_total = Decimal("0.0")
#             if commission_type in [0, 1]:
#                 if commission_value_type == 0: entry_commission_for_total = quantity * commission_rate
#                 elif commission_value_type == 1:
#                     initial_contract_value = quantity * contract_size * entry_price
#                     if initial_contract_value > Decimal("0.0"):
#                         entry_commission_for_total = (commission_rate / Decimal("100")) * initial_contract_value
#             total_commission_for_trade = (entry_commission_for_total + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

#             if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
#             elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
#             else: raise HTTPException(status_code=500, detail="Invalid order type.")
            
#             profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db) 
#             if profit_currency != "USD" and profit_usd == profit: 
#                 orders_logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
#                 raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

#                     db_order.order_status = "CLOSED"
#                     db_order.close_price = close_price
#                     db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
#                     db_order.swap = db_order.swap or Decimal("0.0")
#                     db_order.commission = total_commission_for_trade
#                     db_order.close_id = close_id # Save close_id in DB

#                     original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
#                     swap_amount = db_order.swap
#                     db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

#                     transaction_time = datetime.datetime.now(datetime.timezone.utc)
#                     wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time}
#                     if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
#                     else: wallet_common_data["user_id"] = db_user_locked.id
#                     if db_order.net_profit != Decimal("0.0"):
#                         db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
#                     if total_commission_for_trade > Decimal("0.0"):
#                         db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
#                     if swap_amount != Decimal("0.0"):
#                         db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))

#                     await db.commit()
#                     await db.refresh(db_order)
#                     return OrderResponse.model_validate(db_order, from_attributes=True)        )

#         # Update user total margin in DB
#         await update_user_margin(db, user_id, user_type, total_margin)
#         logger.info(f"Margin updated for Barclays user {user_id} on symbol {symbol} to {total_margin}")

#     except Exception as e:
#         logger.error(f"Error in Barclays margin update during OPEN transition: {e}", exc_info=True)
#         raise


# @router.patch("/orders/{order_id}/status")
# async def update_order_status(
#     order_id: str,
#     update_request: OrderUpdateRequest,
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user = Depends(get_user_from_service_or_user_token)
# ):
#     user_type = current_user.user_type
#     order_model = get_order_model(user_type)

#     db_order = await crud_order.get_order_by_id(db, order_id, order_model)
#     if not db_order:
#         raise HTTPException(status_code=404, detail="Order not found")

#     if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
#         raise HTTPException(status_code=403, detail="Not authorized to modify this order")

#     # Check and apply transition logic
#     if db_order.order_status == "PROCESSING" and update_request.order_status == "OPEN":
#         await handle_barclays_order_open_transition(db, redis_client, db_order, user_type)

#     update_fields = update_request.model_dump(exclude_unset=True)
#     updated_order = await crud_order.update_order_with_tracking(
#         db=db,
#         db_order=db_order,
#         update_fields=update_fields,
#         user_id=current_user.id,
#         user_type=user_type
#     )
#     return {"status": "success", "updated_order": updated_order.order_id}



@router.post("/debug/trigger-order-update/{user_id}")
async def debug_trigger_order_update(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Debug endpoint to manually trigger an order update for a user.
    This is useful for testing WebSocket functionality.
    """
    try:
        # Check if the user has admin privileges or is requesting their own update
        if not getattr(current_user, 'is_admin', False) and current_user.id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to trigger updates for other users")
        
        # Get user type
        user_type = 'demo' if isinstance(current_user, DemoUser) else 'live'
        
        # Update static orders cache
        orders_logger.info(f"DEBUG: Manually triggering order update for user {user_id}")
        await update_user_static_orders(user_id, db, redis_client, user_type)
        
        # Publish updates
        orders_logger.info(f"DEBUG: Publishing order update for user {user_id}")
        await publish_order_update(redis_client, user_id)
        await publish_user_data_update(redis_client, user_id)
        await publish_market_data_trigger(redis_client)
        
        return {"status": "success", "message": f"Order update triggered for user {user_id}"}
    except Exception as e:
        orders_logger.error(f"Error in debug_trigger_order_update: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error triggering order update: {str(e)}")

@router.post("/cancel-pending", response_model=dict)
async def cancel_pending_order(
    cancel_request: PendingOrderCancelRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Cancel a pending order.
    For Barclays users: Store cancel_id and send order to Firebase without changing status
    For non-Barclays users: Cancel the order immediately by updating its status
    """
    try:
        orders_logger.info(f"Cancel pending order request received - Order ID: {cancel_request.order_id}, User ID: {current_user.id}")
        
        # Use the authenticated user's ID instead of the one from the request
        user_id_for_operation = current_user.id
        
        # Check if user is admin and trying to cancel another user's order
        if cancel_request.user_id != user_id_for_operation and getattr(current_user, 'is_admin', False):
            # Admin can cancel other users' orders
            user_id_for_operation = cancel_request.user_id
            orders_logger.info(f"Admin user {current_user.id} cancelling order for user {user_id_for_operation}")
        elif cancel_request.user_id != user_id_for_operation:
            # Non-admin users can only cancel their own orders
            orders_logger.warning(f"User {current_user.id} attempted to cancel order for user {cancel_request.user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to cancel orders for other users")
        
        # Get the order model based on user type
        order_model = get_order_model(cancel_request.user_type)
        
        # Find the order
        db_order = await crud_order.get_order_by_id_and_user_id(
            db, 
            cancel_request.order_id, 
            user_id_for_operation,  # Use the determined user ID
            order_model
        )
        
        if not db_order:
            orders_logger.warning(f"Order {cancel_request.order_id} not found for user {user_id_for_operation}")
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Check if order is in PENDING status
        if db_order.order_status != "PENDING":
            orders_logger.warning(f"Cannot cancel order {cancel_request.order_id} with status {db_order.order_status}. Only PENDING orders can be cancelled.")
            raise HTTPException(status_code=400, detail="Only PENDING orders can be cancelled")
        
        # Generate a cancel_id
        cancel_id = await generate_unique_10_digit_id(db, order_model, 'cancel_id')
        orders_logger.info(f"Generated cancel_id: {cancel_id} for order {cancel_request.order_id}")
        
        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_operation, db, cancel_request.user_type)
        group_name = user_data.get('group_name') if user_data else None
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        
        is_barclays_live_user = (cancel_request.user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_operation} is_barclays_live_user: {is_barclays_live_user}")
        
        if is_barclays_live_user:
            # For Barclays users, just store cancel_id and send to Firebase
            orders_logger.info(f"Barclays user detected. Sending cancel request to Firebase for order {cancel_request.order_id}")
            
            # Update the order with cancel_id without changing status
            update_fields = {
                "cancel_id": cancel_id,
                "cancel_message": cancel_request.cancel_message or "Cancellation requested"
            }
            
            # Update order with tracking
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=cancel_request.user_type,
                action_type="CANCEL_REQUESTED"
            )
            
            # Send to Firebase
            firebase_cancel_data = {
                "order_id": cancel_request.order_id,
                "cancel_id": cancel_id,
                "user_id": user_id_for_operation,
                "symbol": cancel_request.symbol,
                "order_type": cancel_request.order_type,
                "action": "cancel_order",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "cancel_message": cancel_request.cancel_message,
                "status": cancel_request.status or db_order.status,
                "order_quantity": str(cancel_request.order_quantity or db_order.order_quantity),
                "order_status": cancel_request.order_status or db_order.order_status,
                "contract_value": str(db_order.contract_value) if db_order.contract_value else None
            }
            
            await send_order_to_firebase(firebase_cancel_data, "live")
            orders_logger.info(f"Cancel request sent to Firebase for order {cancel_request.order_id}")
            
            return {
                "order_id": db_order.order_id,
                "cancel_id": cancel_id,
                "status": "PENDING_CANCELLATION",
                "message": "Cancellation request sent to service provider"
            }
        else:
            # For non-Barclays users, cancel the order immediately
            orders_logger.info(f"Non-Barclays user. Cancelling order {cancel_request.order_id} immediately")
            
            # Update the order status to CANCELLED
            update_fields = {
                "order_status": "CANCELLED",
                "cancel_id": cancel_id,
                "cancel_message": cancel_request.cancel_message or "Order cancelled by user"
            }
            
            # Update order with tracking
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=cancel_request.user_type,
                action_type="CANCEL"
            )
            
            # Remove from Redis pending orders
            await remove_pending_order(
                redis_client,
                cancel_request.order_id,
                cancel_request.symbol,
                cancel_request.order_type,
                str(user_id_for_operation)
            )
            
            # Update static orders cache
            await update_user_static_orders(user_id_for_operation, db, redis_client, cancel_request.user_type)
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id_for_operation)
            await publish_user_data_update(redis_client, user_id_for_operation)
            
            return {
                "order_id": updated_order.order_id,
                "cancel_id": updated_order.cancel_id,
                "status": updated_order.order_status,
                "message": "Order cancelled successfully"
            }
    
    except Exception as e:
        orders_logger.error(f"Error cancelling pending order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel order: {str(e)}")

@router.post("/add-stoploss", response_model=dict)
async def add_stoploss(
    request: AddStopLossRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Add or update stop loss for an existing order.
    For Barclays users: Store stoploss_id and send to Firebase without changing order
    For non-Barclays users: Update the order directly
    """
    try:
        orders_logger.info(f"Add stoploss request received - Order ID: {request.order_id}, User ID: {current_user.id}")
        
        # Use the authenticated user's ID instead of the one from the request
        user_id_for_operation = current_user.id
        
        # Check if user is admin and trying to modify another user's order
        if request.user_id != user_id_for_operation and getattr(current_user, 'is_admin', False):
            # Admin can modify other users' orders
            user_id_for_operation = request.user_id
            orders_logger.info(f"Admin user {current_user.id} adding stoploss for user {user_id_for_operation}")
        elif request.user_id != user_id_for_operation:
            # Non-admin users can only modify their own orders
            orders_logger.warning(f"User {current_user.id} attempted to add stoploss for user {request.user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to modify orders for other users")
        
        # Get the order model based on user type
        order_model = get_order_model(request.user_type)
        
        # Find the order
        db_order = await crud_order.get_order_by_id_and_user_id(
            db, 
            request.order_id, 
            user_id_for_operation,
            order_model
        )
        
        if not db_order:
            orders_logger.warning(f"Order {request.order_id} not found for user {user_id_for_operation}")
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Check if order is in OPEN status
        if db_order.order_status != "OPEN":
            orders_logger.warning(f"Cannot add stoploss to order {request.order_id} with status {db_order.order_status}. Only OPEN orders can have stoploss.")
            raise HTTPException(status_code=400, detail="Only OPEN orders can have stoploss")

        # Validate stop loss based on order type
        order_price = Decimal(str(db_order.order_price))
        stop_loss = Decimal(str(request.stop_loss))
        
        if db_order.order_type == "BUY" and stop_loss >= order_price:
            orders_logger.warning(f"Invalid stop loss {stop_loss} for BUY order {request.order_id}. Stop loss must be lower than order price {order_price}")
            raise HTTPException(status_code=400, detail=f"For BUY orders, stop loss ({stop_loss}) must be lower than order price ({order_price})")
        elif db_order.order_type == "SELL" and stop_loss <= order_price:
            orders_logger.warning(f"Invalid stop loss {stop_loss} for SELL order {request.order_id}. Stop loss must be greater than order price {order_price}")
            raise HTTPException(status_code=400, detail=f"For SELL orders, stop loss ({stop_loss}) must be greater than order price ({order_price})")
        
        # Generate a stoploss_id
        stoploss_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_id')
        orders_logger.info(f"Generated stoploss_id: {stoploss_id} for order {request.order_id}")
        
        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_operation, db, request.user_type)
        group_name = user_data.get('group_name') if user_data else None
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        
        is_barclays_live_user = (request.user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_operation} is_barclays_live_user: {is_barclays_live_user}")
        
        if is_barclays_live_user:
            # For Barclays users, just store stoploss_id and send to Firebase
            orders_logger.info(f"Barclays user detected. Sending stoploss request to Firebase for order {request.order_id}")
            
            # Update the order with stoploss_id without changing stop_loss value
            update_fields = {
                "stoploss_id": stoploss_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            # Update order with tracking
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="STOPLOSS_REQUESTED"
            )
            
            # Get contract value from the order or calculate it if needed
            contract_value = str(db_order.contract_value) if db_order.contract_value else None
            
            # Send to Firebase with all required fields
            firebase_stoploss_data = {
                "order_id": request.order_id,
                "stoploss_id": stoploss_id,
                "user_id": user_id_for_operation,
                "order_company_name": request.symbol,
                "order_type": request.order_type,
                "stop_loss": str(request.stop_loss),
                "action": "add_stoploss",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "contract_value": contract_value,
                "order_quantity": str(request.order_quantity),
                "order_status": request.order_status,
                "status": request.status
            }
            
            await send_order_to_firebase(firebase_stoploss_data, "live")
            orders_logger.info(f"Stoploss request sent to Firebase for order {request.order_id}")
            
            return {
                "order_id": db_order.order_id,
                "stoploss_id": stoploss_id,
                "status": "PENDING",
                "message": "Stoploss request sent to service provider"
            }
        else:
            # For non-Barclays users, update the order immediately
            orders_logger.info(f"Non-Barclays user. Adding stoploss to order {request.order_id} immediately")
            
            # Update the order with stop_loss and stoploss_id
            update_fields = {
                "stop_loss": request.stop_loss,
                "stoploss_id": stoploss_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            # Update order with tracking
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="STOPLOSS_ADDED"
            )
            
            # Update static orders cache
            await update_user_static_orders(user_id_for_operation, db, redis_client, request.user_type)
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id_for_operation)
            await publish_user_data_update(redis_client, user_id_for_operation)
            
            return {
                "order_id": updated_order.order_id,
                "stoploss_id": updated_order.stoploss_id,
                "stop_loss": str(updated_order.stop_loss),
                "message": "Stoploss added successfully"
            }
    
    except Exception as e:
        orders_logger.error(f"Error adding stoploss: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add stoploss: {str(e)}")

@router.post("/add-takeprofit", response_model=dict)
async def add_takeprofit(
    request: AddTakeProfitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Add or update take profit for an existing order.
    For Barclays users: Store takeprofit_id and send to Firebase without changing order
    For non-Barclays users: Update the order directly
    """
    try:
        orders_logger.info(f"Add takeprofit request received - Order ID: {request.order_id}, User ID: {current_user.id}")
        
        # Use the authenticated user's ID instead of the one from the request
        user_id_for_operation = current_user.id
        
        # Check if user is admin and trying to modify another user's order
        if request.user_id != user_id_for_operation and getattr(current_user, 'is_admin', False):
            # Admin can modify other users' orders
            user_id_for_operation = request.user_id
            orders_logger.info(f"Admin user {current_user.id} adding takeprofit for user {user_id_for_operation}")
        elif request.user_id != user_id_for_operation:
            # Non-admin users can only modify their own orders
            orders_logger.warning(f"User {current_user.id} attempted to add takeprofit for user {request.user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to modify orders for other users")
        
        # Get the order model based on user type
        order_model = get_order_model(request.user_type)
        
        # Find the order
        db_order = await crud_order.get_order_by_id_and_user_id(
            db, 
            request.order_id, 
            user_id_for_operation,
            order_model
        )
        
        if not db_order:
            orders_logger.warning(f"Order {request.order_id} not found for user {user_id_for_operation}")
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Check if order is in OPEN status
        if db_order.order_status != "OPEN":
            orders_logger.warning(f"Cannot add takeprofit to order {request.order_id} with status {db_order.order_status}. Only OPEN orders can have takeprofit.")
            raise HTTPException(status_code=400, detail="Only OPEN orders can have takeprofit")

        # Validate take profit based on order type
        order_price = Decimal(str(db_order.order_price))
        take_profit = Decimal(str(request.take_profit))
        
        if db_order.order_type == "BUY" and take_profit <= order_price:
            orders_logger.warning(f"Invalid take profit {take_profit} for BUY order {request.order_id}. Take profit must be greater than order price {order_price}")
            raise HTTPException(status_code=400, detail=f"For BUY orders, take profit ({take_profit}) must be greater than order price ({order_price})")
        elif db_order.order_type == "SELL" and take_profit >= order_price:
            orders_logger.warning(f"Invalid take profit {take_profit} for SELL order {request.order_id}. Take profit must be lower than order price {order_price}")
            raise HTTPException(status_code=400, detail=f"For SELL orders, take profit ({take_profit}) must be lower than order price ({order_price})")
        
        # Generate a takeprofit_id
        takeprofit_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_id')
        orders_logger.info(f"Generated takeprofit_id: {takeprofit_id} for order {request.order_id}")
        
        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_operation, db, request.user_type)
        group_name = user_data.get('group_name') if user_data else None
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        
        is_barclays_live_user = (request.user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_operation} is_barclays_live_user: {is_barclays_live_user}")
        
        if is_barclays_live_user:
            # For Barclays users, just store takeprofit_id and send to Firebase
            orders_logger.info(f"Barclays user detected. Sending takeprofit request to Firebase for order {request.order_id}")
            
            # Update the order with takeprofit_id without changing take_profit value
            update_fields = {
                "takeprofit_id": takeprofit_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            # Update order with tracking
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="TAKEPROFIT_REQUESTED"
            )
            
            # Get contract value from the order or calculate it if needed
            contract_value = str(db_order.contract_value) if db_order.contract_value else None
            
            # Send to Firebase with all required fields
            firebase_takeprofit_data = {
                "order_id": request.order_id,
                "takeprofit_id": takeprofit_id,
                "user_id": user_id_for_operation,
                "order_company_name": request.symbol,
                "order_type": request.order_type,
                "take_profit": str(request.take_profit),
                "action": "add_takeprofit",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "contract_value": contract_value,
                "order_quantity": str(request.order_quantity),
                "order_status": request.order_status,
                "status": request.status
            }
            
            await send_order_to_firebase(firebase_takeprofit_data, "live")
            orders_logger.info(f"Takeprofit request sent to Firebase for order {request.order_id}")
            
            return {
                "order_id": db_order.order_id,
                "takeprofit_id": takeprofit_id,
                "status": "PENDING",
                "message": "Takeprofit request sent to service provider"
            }
        else:
            # For non-Barclays users, update the order immediately
            orders_logger.info(f"Non-Barclays user. Adding takeprofit to order {request.order_id} immediately")
            
            # Update the order with take_profit and takeprofit_id
            update_fields = {
                "take_profit": request.take_profit,
                "takeprofit_id": takeprofit_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            # Update order with tracking
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="TAKEPROFIT_ADDED"
            )
            
            # Update static orders cache
            await update_user_static_orders(user_id_for_operation, db, redis_client, request.user_type)
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id_for_operation)
            await publish_user_data_update(redis_client, user_id_for_operation)
            
            return {
                "order_id": updated_order.order_id,
                "takeprofit_id": updated_order.takeprofit_id,
                "take_profit": str(updated_order.take_profit),
                "message": "Takeprofit added successfully"
            }
    
    except Exception as e:
        orders_logger.error(f"Error adding takeprofit: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add takeprofit: {str(e)}")

# Endpoints to cancel stoploss and takeprofit
@router.post("/cancel-stoploss", response_model=dict)
async def cancel_stoploss(
    request: CancelStopLossRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Cancel a stop loss for an existing order.
    For Barclays users: Store stoploss_cancel_id and send to Firebase
    For non-Barclays users: Update the order directly
    """
    try:
        orders_logger.info(f"Cancel stoploss request received - Order ID: {request.order_id}, User ID: {current_user.id}")
        
        # Use the authenticated user's ID unless admin
        user_id_for_operation = current_user.id
        if request.user_id != user_id_for_operation and getattr(current_user, 'is_admin', False):
            user_id_for_operation = request.user_id
            orders_logger.info(f"Admin user {current_user.id} cancelling stoploss for user {user_id_for_operation}")
        elif request.user_id != user_id_for_operation:
            orders_logger.warning(f"User {current_user.id} attempted to cancel stoploss for user {request.user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to modify orders for other users")
        
        # Get the order model based on user type
        order_model = get_order_model(request.user_type)
        
        # Find the order
        db_order = await crud_order.get_order_by_id_and_user_id(
            db, 
            request.order_id, 
            user_id_for_operation,
            order_model
        )
        
        if not db_order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        if not db_order.stoploss_id:
            raise HTTPException(status_code=400, detail="No stop loss exists for this order")
        
        # Generate a stoploss_cancel_id
        stoploss_cancel_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_cancel_id')
        
        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_operation, db, request.user_type)
        group_name = user_data.get('group_name') if user_data else None
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        
        is_barclays_live_user = (request.user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_operation} is_barclays_live_user: {is_barclays_live_user}")
        
        if is_barclays_live_user:
            # For Barclays users, just store stoploss_cancel_id and send to Firebase
            update_fields = {
                "stoploss_cancel_id": stoploss_cancel_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="STOPLOSS_CANCEL_REQUESTED"
            )
            
            # Send to Firebase
            firebase_data = {
                "order_id": request.order_id,
                "stoploss_id": db_order.stoploss_id,
                "stoploss_cancel_id": stoploss_cancel_id,
                "user_id": user_id_for_operation,
                "symbol": request.symbol,
                "order_type": request.order_type,
                "order_status": request.order_status,
                "status": request.status or db_order.status,
                "action": "cancel_stoploss",
                "cancel_message": request.cancel_message,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            
            await send_order_to_firebase(firebase_data, "live")
            orders_logger.info(f"Stoploss cancel request sent to Firebase for order {request.order_id}")
            
            return {
                "order_id": request.order_id,
                "stoploss_cancel_id": stoploss_cancel_id,
                "status": "PENDING",
                "message": "Stop loss cancellation request sent to service provider"
            }
        else:
            # For non-Barclays users, update the order immediately
            update_fields = {
                "stop_loss": None,
                "stoploss_id": None,
                "stoploss_cancel_id": stoploss_cancel_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="STOPLOSS_CANCELLED"
            )
            
            # Update static orders cache
            await update_user_static_orders(user_id_for_operation, db, redis_client, request.user_type)
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id_for_operation)
            
            return {
                "order_id": request.order_id,
                "stoploss_cancel_id": stoploss_cancel_id,
                "message": "Stop loss cancelled successfully"
            }
    
    except Exception as e:
        orders_logger.error(f"Error cancelling stoploss: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel stop loss: {str(e)}")

@router.post("/cancel-takeprofit", response_model=dict)
async def cancel_takeprofit(
    request: CancelTakeProfitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_current_user)
):
    """
    Cancel a take profit for an existing order.
    For Barclays users: Store takeprofit_cancel_id and send to Firebase
    For non-Barclays users: Update the order directly
    """
    try:
        orders_logger.info(f"Cancel takeprofit request received - Order ID: {request.order_id}, User ID: {current_user.id}")
        
        # Use the authenticated user's ID unless admin
        user_id_for_operation = current_user.id
        if request.user_id != user_id_for_operation and getattr(current_user, 'is_admin', False):
            user_id_for_operation = request.user_id
            orders_logger.info(f"Admin user {current_user.id} cancelling takeprofit for user {user_id_for_operation}")
        elif request.user_id != user_id_for_operation:
            orders_logger.warning(f"User {current_user.id} attempted to cancel takeprofit for user {request.user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to modify orders for other users")
        
        # Get the order model based on user type
        order_model = get_order_model(request.user_type)
        
        # Find the order
        db_order = await crud_order.get_order_by_id_and_user_id(
            db, 
            request.order_id, 
            user_id_for_operation,
            order_model
        )
        
        if not db_order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        if not db_order.takeprofit_id:
            raise HTTPException(status_code=400, detail="No take profit exists for this order")
        
        # Generate a takeprofit_cancel_id
        takeprofit_cancel_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_cancel_id')
        
        # Check if user is a Barclays live user
        user_data = await get_user_data_cache(redis_client, user_id_for_operation, db, request.user_type)
        group_name = user_data.get('group_name') if user_data else None
        group_settings = await get_group_settings_cache(redis_client, group_name) if group_name else None
        sending_orders = group_settings.get('sending_orders') if group_settings else None
        sending_orders_normalized = sending_orders.lower() if isinstance(sending_orders, str) else sending_orders
        
        is_barclays_live_user = (request.user_type == 'live' and sending_orders_normalized == 'barclays')
        orders_logger.info(f"User {user_id_for_operation} is_barclays_live_user: {is_barclays_live_user}")
        
        if is_barclays_live_user:
            # For Barclays users, just store takeprofit_cancel_id and send to Firebase
            update_fields = {
                "takeprofit_cancel_id": takeprofit_cancel_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="TAKEPROFIT_CANCEL_REQUESTED"
            )
            
            # Send to Firebase
            firebase_data = {
                "order_id": request.order_id,
                "takeprofit_id": db_order.takeprofit_id,
                "takeprofit_cancel_id": takeprofit_cancel_id,
                "user_id": user_id_for_operation,
                "symbol": request.symbol,
                "order_type": request.order_type,
                "order_status": request.order_status,
                "status": request.status or db_order.status,
                "action": "cancel_takeprofit",
                "cancel_message": request.cancel_message,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            
            await send_order_to_firebase(firebase_data, "live")
            orders_logger.info(f"Takeprofit cancel request sent to Firebase for order {request.order_id}")
            
            return {
                "order_id": request.order_id,
                "takeprofit_cancel_id": takeprofit_cancel_id,
                "status": "PENDING",
                "message": "Take profit cancellation request sent to service provider"
            }
        else:
            # For non-Barclays users, update the order immediately
            update_fields = {
                "take_profit": None,
                "takeprofit_id": None,
                "takeprofit_cancel_id": takeprofit_cancel_id
            }
            if request.status is not None:
                update_fields['status'] = request.status
            
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields=update_fields,
                user_id=user_id_for_operation,
                user_type=request.user_type,
                action_type="TAKEPROFIT_CANCELLED"
            )
            
            # Update static orders cache
            await update_user_static_orders(user_id_for_operation, db, redis_client, request.user_type)
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id_for_operation)
            
            return {
                "order_id": request.order_id,
                "takeprofit_cancel_id": takeprofit_cancel_id,
                "message": "Take profit cancelled successfully"
            }
    
    except Exception as e:
        orders_logger.error(f"Error cancelling takeprofit: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel take profit: {str(e)}")

@router.post("/service-provider-update", response_model=OrderResponse)
async def update_order_by_service_provider(
    update_request: ServiceProviderUpdateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_user_from_service_token)
):
    """
    Endpoint for service providers to update orders for Barclays users.
    Handles different order status transitions and performs necessary calculations.
    """
    try:
        # Ensure the user is a service account
        if not getattr(current_user, 'is_service_account', False):
            orders_logger.error(f"Non-service account attempted to use service provider endpoint: {current_user.id}")
            raise HTTPException(status_code=403, detail="Only service accounts can use this endpoint")
        
        orders_logger.info(f"Service provider update request received: {update_request.model_dump_json(exclude_unset=True)}")
        
        # Extract the single ID provided in the request
        id_to_find = (
            update_request.order_id or
            update_request.cancel_id or
            update_request.close_id or
            update_request.modify_id or
            update_request.stoploss_id or
            update_request.takeprofit_id or
            update_request.stoploss_cancel_id or
            update_request.takeprofit_cancel_id
        )
        if not id_to_find:
            raise HTTPException(status_code=400, detail="An order identifier must be provided.")

        # Get the order from the database using the new generic search function
        order_model = UserOrder  # Barclays users are always live users
        db_order = await crud_order.get_order_by_any_id(db, id_to_find, order_model)
        
        if not db_order:
            orders_logger.error(f"Order not found with provided identifier '{id_to_find}' in request: {update_request.model_dump_json(exclude_unset=True)}")
            raise HTTPException(status_code=404, detail="Order not found with provided ID")
        
        # Store original status for comparison after update
        original_order_status = db_order.order_status
        orders_logger.info(f"Original order status: {original_order_status}")
        
        # Extract update fields from request (only fields that are provided)
        update_fields = update_request.model_dump(exclude_unset=True)
        orders_logger.info(f"Update fields: {update_fields}")
        
        # Handle special status transitions
        new_order_status = update_fields.get('order_status')
        
        # Case 1: PROCESSING -> OPEN transition (order confirmation)
        if original_order_status == "PROCESSING" and new_order_status == "OPEN":
            orders_logger.info(f"Processing PROCESSING -> OPEN transition for order {db_order.order_id}")
            
            # Get user data
            user_id = db_order.order_user_id
            db_user = await get_user_by_id_with_lock(db, user_id)
            if not db_user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Get symbol information
            symbol = db_order.order_company_name
            order_type = db_order.order_type
            quantity = Decimal(str(db_order.order_quantity))
            
            # Get external symbol info
            symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
            symbol_info_result = await db.execute(symbol_info_stmt)
            ext_symbol_info = symbol_info_result.scalars().first()
            
            if not ext_symbol_info:
                raise HTTPException(status_code=500, detail=f"Symbol information not found for {symbol}")
            
            # Get group settings
            user_data = await get_user_data_cache(redis_client, user_id, db, 'live')
            group_name = user_data.get('group_name') if user_data else db_user.group_name
            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
            
            if not group_settings:
                raise HTTPException(status_code=500, detail="Group settings not found")
            
            # Calculate margin based on the updated price
            order_price = Decimal(str(update_fields.get('order_price', db_order.order_price)))
            contract_size = Decimal(str(ext_symbol_info.contract_size))
            user_leverage = Decimal(str(db_user.leverage))
            
            # Calculate contract value (contract_size * quantity)
            contract_value = contract_size * quantity
            
            # Calculate margin ((contract_value * price) / leverage)
            margin_raw = (contract_value * order_price) / user_leverage
            margin = margin_raw.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            # Convert margin to USD if profit_currency is not USD
            profit_currency = ext_symbol_info.profit.upper()
            if profit_currency != 'USD':
                margin_usd = await _convert_to_usd(
                    margin, 
                    profit_currency, 
                    user_id, 
                    db_order.order_id, 
                    f"margin for {symbol} {order_type} order", 
                    db, 
                    redis_client
                )
                margin = margin_usd
            
            # Update margin in the order
            update_fields['margin'] = margin
            update_fields['contract_value'] = contract_value
            
            # Calculate commission if applicable
            commission = Decimal('0.0')
            commission_type = int(group_settings.get('commision_type', 0))
            commission_value_type = int(group_settings.get('commision_value_type', 0))
            commission_rate = Decimal(str(group_settings.get('commision', '0.0')))
            
            if commission_type in [0, 1]:  # "Every Trade" or "In"
                if commission_value_type == 0:  # Per lot
                    commission = quantity * commission_rate
                elif commission_value_type == 1:  # Percent of price
                    commission = (commission_rate / Decimal('100')) * contract_value * order_price
            
            commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            update_fields['commission'] = commission
            
            # Get all open orders for the symbol to calculate hedging
            open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
            
            # Calculate margin before adding this order
            margin_before_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders, 'live'
            )
            margin_before = margin_before_data["total_margin"]
            
            # Add this order to the calculation
            simulated_order = type('Obj', (object,), {
                'order_quantity': quantity,
                'order_type': order_type,
                'margin': margin
            })()
            
            margin_after_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders + [simulated_order], 'live'
            )
            margin_after = margin_after_data["total_margin"]
            
            # Calculate additional margin needed
            additional_margin = max(Decimal("0.0"), margin_after - margin_before)
            
            # Update user's margin
            original_margin = db_user.margin
            db_user.margin = (Decimal(str(original_margin)) + additional_margin).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            
            orders_logger.info(f"Updating user {user_id} margin from {original_margin} to {db_user.margin}")
            
            # Update the order with the new fields
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields,
                current_user.id,
                'live',
                action_type="SERVICE_PROVIDER_CONFIRM"
            )
            
            # Commit changes
            await db.commit()
            
            # Update user data cache
            user_data_to_cache = {
                "id": db_user.id,
                "email": getattr(db_user, 'email', None),
                "group_name": db_user.group_name,
                "leverage": db_user.leverage,
                "user_type": 'live',
                "account_number": getattr(db_user, 'account_number', None),
                "wallet_balance": db_user.wallet_balance,
                "margin": db_user.margin,
                "first_name": getattr(db_user, 'first_name', None),
                "last_name": getattr(db_user, 'last_name', None),
                "country": getattr(db_user, 'country', None),
                "phone_number": getattr(db_user, 'phone_number', None),
            }
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            
            # Update static orders cache
            await update_user_static_orders(user_id, db, redis_client, 'live')
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id)
            await publish_user_data_update(redis_client, user_id)
            await publish_market_data_trigger(redis_client)
            
        # Case 2: OPEN -> CLOSED transition (order closure)
        elif original_order_status == "OPEN" and new_order_status == "CLOSED":
            orders_logger.info(f"Processing OPEN -> CLOSED transition for order {db_order.order_id}")
            
            # Get user data
            user_id = db_order.order_user_id
            db_user = await get_user_by_id_with_lock(db, user_id)
            if not db_user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Get symbol information
            symbol = db_order.order_company_name.upper()
            order_type = db_order.order_type.upper()
            quantity = Decimal(str(db_order.order_quantity))
            entry_price = Decimal(str(db_order.order_price))
            
            # Ensure close_price is provided
            if 'close_price' not in update_fields or not update_fields['close_price']:
                raise HTTPException(status_code=400, detail="close_price is required for closing an order")
            
            close_price = Decimal(str(update_fields['close_price']))
            
            # Get external symbol info
            symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
            symbol_info_result = await db.execute(symbol_info_stmt)
            ext_symbol_info = symbol_info_result.scalars().first()
            
            if not ext_symbol_info:
                raise HTTPException(status_code=500, detail=f"Symbol information not found for {symbol}")
            
            contract_size = Decimal(str(ext_symbol_info.contract_size))
            profit_currency = ext_symbol_info.profit.upper()
            
            # Get group settings
            user_data = await get_user_data_cache(redis_client, user_id, db, 'live')
            group_name = user_data.get('group_name') if user_data else db_user.group_name
            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
            
            if not group_settings:
                raise HTTPException(status_code=500, detail="Group settings not found")
            
            # Calculate commission
            commission_type = int(group_settings.get('commision_type', 0))
            commission_value_type = int(group_settings.get('commision_value_type', 0))
            commission_rate = Decimal(str(group_settings.get('commision', '0.0')))
            
            # Get existing entry commission from the order
            existing_entry_commission = Decimal(str(db_order.commission or "0.0"))
            
            # Calculate exit commission if applicable
            exit_commission = Decimal("0.0")
            if commission_type in [0, 2]:  # "Every Trade" or "Out"
                if commission_value_type == 0:  # Per lot
                    exit_commission = quantity * commission_rate
                elif commission_value_type == 1:  # Percent of price
                    calculated_exit_contract_value = quantity * contract_size * close_price
                    if calculated_exit_contract_value > Decimal("0.0"):
                        exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
            
            # Total commission is existing entry commission plus exit commission
            total_commission = (existing_entry_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            update_fields['commission'] = total_commission
            
            # Calculate profit/loss
            if order_type == "BUY":
                profit = (close_price - entry_price) * quantity * contract_size
            elif order_type == "SELL":
                profit = (entry_price - close_price) * quantity * contract_size
            else:
                raise HTTPException(status_code=500, detail="Invalid order type")
            
            # Convert profit to USD if necessary
            profit_usd = await _convert_to_usd(
                profit,
                profit_currency,
                user_id,
                db_order.order_id,
                "PnL on Close",
                db,
                redis_client
            )
            
            update_fields['net_profit'] = (profit_usd - total_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            
            # Get all open orders for the symbol to recalculate margin
            all_open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
            margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, all_open_orders, 'live'
            )
            margin_before_recalc = margin_before_recalc_dict["total_margin"]
            
            # Calculate current overall margin and non-symbol margin
            current_overall_margin = Decimal(str(db_user.margin))
            non_symbol_margin = current_overall_margin - margin_before_recalc
            
            # Calculate remaining orders after closing this one
            remaining_orders = [o for o in all_open_orders if o.order_id != db_order.order_id]
            margin_after_recalc_dict = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, remaining_orders, 'live'
            )
            margin_after_recalc = margin_after_recalc_dict["total_margin"]
            
            # Update user margin
            db_user.margin = max(Decimal(0), (non_symbol_margin + margin_after_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            
            # Update user wallet balance
            swap_amount = Decimal(str(update_fields.get('swap', db_order.swap or "0.0")))
            db_user.wallet_balance = (
                Decimal(str(db_user.wallet_balance)) + 
                update_fields['net_profit'] - 
                swap_amount
            ).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
            
            # Create wallet transactions for profit/loss, commission, and swap
            transaction_time = datetime.datetime.now(datetime.timezone.utc)
            wallet_common_data = {
                "symbol": symbol,
                "order_quantity": quantity,
                "is_approved": 1,
                "order_type": order_type,
                "transaction_time": transaction_time,
                "order_id": db_order.order_id,
                "user_id": user_id
            }
            
            # Add profit/loss transaction
            if profit_usd != Decimal("0.0"):
                transaction_id_profit = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                db.add(Wallet(
                    **WalletCreate(
                        **wallet_common_data,
                        transaction_type="Profit/Loss",
                        transaction_amount=profit_usd,
                        description=f"P/L for closing order {db_order.order_id}"
                    ).model_dump(exclude_none=True),
                    transaction_id=transaction_id_profit
                ))
            
            # Add commission transaction
            if total_commission > Decimal("0.0"):
                transaction_id_commission = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                db.add(Wallet(
                    **WalletCreate(
                        **wallet_common_data,
                        transaction_type="Commission",
                        transaction_amount=-total_commission,
                        description=f"Commission for closing order {db_order.order_id}"
                    ).model_dump(exclude_none=True),
                    transaction_id=transaction_id_commission
                ))
            
            # Add swap transaction
            if swap_amount != Decimal("0.0"):
                transaction_id_swap = await generate_unique_10_digit_id(db, Wallet, "transaction_id")
                db.add(Wallet(
                    **WalletCreate(
                        **wallet_common_data,
                        transaction_type="Swap",
                        transaction_amount=-swap_amount,
                        description=f"Swap for closing order {db_order.order_id}"
                    ).model_dump(exclude_none=True),
                    transaction_id=transaction_id_swap
                ))
            
            # Update the order with all the new fields
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields,
                current_user.id,
                'live',
                action_type="SERVICE_PROVIDER_CLOSE"
            )
            
            # Commit changes
            await db.commit()
            
            # Update user data cache
            user_data_to_cache = {
                "id": db_user.id,
                "email": getattr(db_user, 'email', None),
                "group_name": db_user.group_name,
                "leverage": db_user.leverage,
                "user_type": 'live',
                "account_number": getattr(db_user, 'account_number', None),
                "wallet_balance": db_user.wallet_balance,
                "margin": db_user.margin,
                "first_name": getattr(db_user, 'first_name', None),
                "last_name": getattr(db_user, 'last_name', None),
                "country": getattr(db_user, 'country', None),
                "phone_number": getattr(db_user, 'phone_number', None),
            }
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            
            # Update static orders cache
            await update_user_static_orders(user_id, db, redis_client, 'live')
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id)
            await publish_user_data_update(redis_client, user_id)
            await publish_market_data_trigger(redis_client)
            
        # Case 3: PENDING -> OPEN transition (pending order activation)
        elif original_order_status == "PENDING" and new_order_status == "OPEN":
            orders_logger.info(f"Processing PENDING -> OPEN transition for order {db_order.order_id}")
            
            # Get user data
            user_id = db_order.order_user_id
            db_user = await get_user_by_id_with_lock(db, user_id)
            if not db_user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Get symbol information
            symbol = db_order.order_company_name
            order_type = db_order.order_type
            quantity = Decimal(str(db_order.order_quantity))
            
            # Get external symbol info
            symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
            symbol_info_result = await db.execute(symbol_info_stmt)
            ext_symbol_info = symbol_info_result.scalars().first()
            
            if not ext_symbol_info:
                raise HTTPException(status_code=500, detail=f"Symbol information not found for {symbol}")
            
            # Get group settings
            user_data = await get_user_data_cache(redis_client, user_id, db, 'live')
            group_name = user_data.get('group_name') if user_data else db_user.group_name
            group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
            
            if not group_settings:
                raise HTTPException(status_code=500, detail="Group settings not found")
            
            # Calculate margin based on the updated price
            order_price = Decimal(str(update_fields.get('order_price', db_order.order_price)))
            contract_size = Decimal(str(ext_symbol_info.contract_size))
            user_leverage = Decimal(str(db_user.leverage))
            
            # Calculate contract value (contract_size * quantity)
            contract_value = contract_size * quantity
            
            # Calculate margin ((contract_value * price) / leverage)
            margin_raw = (contract_value * order_price) / user_leverage
            margin = margin_raw.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            # Convert margin to USD if profit_currency is not USD
            profit_currency = ext_symbol_info.profit.upper()
            if profit_currency != 'USD':
                margin_usd = await _convert_to_usd(
                    margin, 
                    profit_currency, 
                    user_id, 
                    db_order.order_id, 
                    f"margin for {symbol} {order_type} order", 
                    db, 
                    redis_client
                )
                margin = margin_usd
            
            # Update margin in the order
            update_fields['margin'] = margin
            update_fields['contract_value'] = contract_value
            
            # Calculate commission if applicable
            commission = Decimal('0.0')
            commission_type = int(group_settings.get('commision_type', 0))
            commission_value_type = int(group_settings.get('commision_value_type', 0))
            commission_rate = Decimal(str(group_settings.get('commision', '0.0')))
            
            if commission_type in [0, 1]:  # "Every Trade" or "In"
                if commission_value_type == 0:  # Per lot
                    commission = quantity * commission_rate
                elif commission_value_type == 1:  # Percent of price
                    commission = (commission_rate / Decimal('100')) * contract_value * order_price
            
            commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            update_fields['commission'] = commission
            
            # Get all open orders for the symbol to calculate hedging
            open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
            
            # Calculate margin before adding this order
            margin_before_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders, 'live'
            )
            margin_before = margin_before_data["total_margin"]
            
            # Add this order to the calculation
            simulated_order = type('Obj', (object,), {
                'order_quantity': quantity,
                'order_type': order_type,
                'margin': margin
            })()
            
            margin_after_data = await calculate_total_symbol_margin_contribution(
                db, redis_client, user_id, symbol, open_orders + [simulated_order], 'live'
            )
            margin_after = margin_after_data["total_margin"]
            
            # Calculate additional margin needed
            additional_margin = max(Decimal("0.0"), margin_after - margin_before)
            
            # Update user's margin
            original_margin = db_user.margin
            db_user.margin = (Decimal(str(original_margin)) + additional_margin).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            
            orders_logger.info(f"Updating user {user_id} margin from {original_margin} to {db_user.margin}")
            
            # Remove from pending orders in Redis
            await remove_pending_order(
                redis_client,
                db_order.order_id,
                symbol,
                order_type,
                str(user_id)
            )
            
            # Update the order with the new fields
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields,
                current_user.id,
                'live',
                action_type="SERVICE_PROVIDER_PENDING_ACTIVATE"
            )
            
            # Commit changes
            await db.commit()
            
            # Update user data cache
            user_data_to_cache = {
                "id": db_user.id,
                "email": getattr(db_user, 'email', None),
                "group_name": db_user.group_name,
                "leverage": db_user.leverage,
                "user_type": 'live',
                "account_number": getattr(db_user, 'account_number', None),
                "wallet_balance": db_user.wallet_balance,
                "margin": db_user.margin,
                "first_name": getattr(db_user, 'first_name', None),
                "last_name": getattr(db_user, 'last_name', None),
                "country": getattr(db_user, 'country', None),
                "phone_number": getattr(db_user, 'phone_number', None),
            }
            await set_user_data_cache(redis_client, user_id, user_data_to_cache)
            
            # Update static orders cache
            await update_user_static_orders(user_id, db, redis_client, 'live')
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id)
            await publish_user_data_update(redis_client, user_id)
            await publish_market_data_trigger(redis_client)
            
        # Case 4: PENDING -> CANCELLED transition (pending order cancellation)
        elif original_order_status == "PENDING" and new_order_status == "CANCELLED":
            orders_logger.info(f"Processing PENDING -> CANCELLED transition for order {db_order.order_id}")
            
            # Get user ID
            user_id = db_order.order_user_id
            
            # Remove from pending orders in Redis
            await remove_pending_order(
                redis_client,
                db_order.order_id,
                db_order.order_company_name,
                db_order.order_type,
                str(user_id)
            )
            
            # Update the order with the new fields
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields,
                current_user.id,
                'live',
                action_type="SERVICE_PROVIDER_CANCEL_PENDING"
            )
            
            # Commit changes
            await db.commit()
            
            # Update static orders cache
            await update_user_static_orders(user_id, db, redis_client, 'live')
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, user_id)
            await publish_user_data_update(redis_client, user_id)
            
        # Default case: Just update the order with the provided fields
        else:
            orders_logger.info(f"Processing regular update for order {db_order.order_id}")
            
            # Update the order with the new fields
            updated_order = await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields,
                current_user.id,
                'live',
                action_type="SERVICE_PROVIDER_UPDATE"
            )
            
            # Commit changes
            await db.commit()
            
            # Update static orders cache
            await update_user_static_orders(db_order.order_user_id, db, redis_client, 'live')
            
            # Publish updates to notify WebSocket clients
            await publish_order_update(redis_client, db_order.order_user_id)
            
        # Return the updated order
        await db.refresh(db_order)
        return OrderResponse.model_validate(db_order, from_attributes=True)
        
    except Exception as e:
        orders_logger.error(f"Error in service provider update endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update order: {str(e)}")



async def _handle_order_open_transition(
    db: AsyncSession,
    redis_client: Redis,
    db_order: UserOrder,
    update_fields: Dict[str, Any],
    current_user: User,
    action_type: str
) -> Dict[str, Any]:
    """
    Handles the logic for when an order transitions to an OPEN state
    from a service provider confirmation. This includes margin calculation
    and updating the user's overall margin based on hedging.
    
    Note: For Barclays service provider, we always use the UserOrder model as they only handle live users.
    """
    orders_logger.info(f"Order {db_order.order_id} transitioning to OPEN. Calculating margin.")
    
    # For Barclays service integration: always use UserOrder model
    order_model_class = UserOrder
    orders_logger.info(f"Using fixed UserOrder model for Barclays service provider integration")
    
    # Get the user's group_name from cache or database
    user_data = await get_user_data_cache(redis_client, db_order.order_user_id, db, 'live')
    group_name = user_data.get("group_name") if user_data else None
    
    if not group_name:
        # Fetch from database directly
        db_user = await get_user_by_id(db, db_order.order_user_id, user_type='live')
        group_name = getattr(db_user, 'group_name', None)
        
    if not group_name:
        raise HTTPException(status_code=400, detail=f"Cannot process order: Group name not found for user {db_order.order_user_id}")
    
    orders_logger.info(f"Using group_name: {group_name} for order {db_order.order_id}")
    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, "ALL")
    symbol_settings = group_settings.get(db_order.order_company_name.upper())

    if not symbol_settings:
        raise HTTPException(status_code=400, detail=f"Symbol settings not found for {db_order.order_company_name} in group {group_name}")

    # Calculate margin for this specific order using the price from the service provider
    orders_logger.info(f"Calculating new margin for order {db_order.order_id} with price {update_fields.get('order_price', db_order.order_price)}")
    try:
        # Get market data for price calculation
        try:
            # The get_latest_market_data function from firebase_stream is not async
            raw_market_data = get_latest_market_data()
            
            if not raw_market_data:
                orders_logger.error(f"Failed to get market data for margin calculation")
                raw_market_data = {}
        except Exception as e:
            orders_logger.error(f"Error getting market data: {e}", exc_info=True)
            raw_market_data = {}
            
        # Get external symbol info
        ext_symbol_info = await get_external_symbol_info(db, db_order.order_company_name)
        if not ext_symbol_info:
            orders_logger.error(f"External symbol info not found for {db_order.order_company_name}")
            ext_symbol_info = {
                'contract_size': 100000,
                'profit_currency': 'USD',
                'digit': 5
            }
            
        # Add group_name to symbol_settings for proper price lookup
        symbol_settings['group_name'] = group_name
        
        # Get user's leverage from cache or database
        user_leverage = Decimal('100')  # Default fallback leverage
        if user_data and user_data.get('leverage') and Decimal(str(user_data.get('leverage'))) > 0:
            user_leverage = Decimal(str(user_data.get('leverage')))
            orders_logger.info(f"Using user's leverage from cache: {user_leverage}")
        else:
            # Try to get from database
            try:
                actual_user = await get_user_by_id(db, db_order.order_user_id, user_type='live')
                if actual_user and actual_user.leverage and actual_user.leverage > 0:
                    user_leverage = actual_user.leverage
                    orders_logger.info(f"Using user's leverage from DB: {user_leverage}")
            except Exception as e:
                orders_logger.error(f"Error getting user leverage: {e}")
                # Keep default leverage = 100
        
        # Use calculate_single_order_margin which is more reliable
        margin_result, price, contract_value, commission = await calculate_single_order_margin(
            redis_client,  # Pass redis_client as positional argument, not keyword
            db_order.order_company_name,  # symbol
            db_order.order_type,  # order_type
            db_order.order_quantity,  # quantity
            user_leverage,  # Use the user's actual leverage
            symbol_settings,  # group_settings with group_name added
            ext_symbol_info,  # external_symbol_info
            raw_market_data,  # raw_market_data
            db,  # db session
            db_order.order_user_id  # user_id
        )
        
        if margin_result is None:
            orders_logger.error(f"Failed to calculate margin, using fallback calculation")
            # Fallback to a simplified calculation - get a more reasonable number
            contract_size = ext_symbol_info.get('contract_size', 100000)
            order_price = update_fields.get('order_price', db_order.order_price)
            
            # Calculate realistic margin
            new_order_margin = (Decimal(str(contract_size)) * db_order.order_quantity * order_price) / user_leverage
            orders_logger.info(f"Fallback margin calculation: ({contract_size} * {db_order.order_quantity} * {order_price}) / {user_leverage} = {new_order_margin}")
        else:
            new_order_margin = margin_result
            
        orders_logger.info(f"New margin calculated: {new_order_margin}")
    except Exception as e:
        orders_logger.error(f"Error calculating margin: {e}", exc_info=True)
        raise
    
    update_fields['margin'] = new_order_margin
    update_fields['contract_value'] = contract_value
    
    # --- Correct Hedged Margin Calculation (Mirrors place_order) ---
    orders_logger.info(f"Recalculating total hedged margin for user {db_order.order_user_id} on symbol {db_order.order_company_name}")

    # 1. Get all OPEN positions for the symbol BEFORE the new one is included
    orders_logger.info(f"Fetching open positions for user {db_order.order_user_id}, symbol {db_order.order_company_name} using UserOrder model")
    try:
        open_positions_before = await crud_order.get_open_orders_by_user_id_and_symbol(
            db, user_id=db_order.order_user_id, symbol=db_order.order_company_name, order_model=order_model_class
        )
        orders_logger.info(f"Found {len(open_positions_before)} existing open positions for this symbol")
    except Exception as e:
        orders_logger.error(f"Error fetching open positions: {e}", exc_info=True)
        raise
    
    # 2. Calculate the margin contribution of the symbol BEFORE this order was opened
    margin_before_data = await calculate_total_symbol_margin_contribution(
        db, redis_client, db_order.order_user_id, db_order.order_company_name, 
        open_positions_before, order_model_class, 'live'
    )
    margin_before = margin_before_data["total_margin"]
    orders_logger.info(f"Old total margin for {db_order.order_company_name}: {margin_before}")

    # 3. Create a temporary order object with the new margin
    simulated_order = type('Obj', (object,), {
        'order_quantity': db_order.order_quantity,
        'order_type': db_order.order_type,
        'margin': new_order_margin
    })()

    # 4. Calculate the new total margin including the new order
    margin_after_data = await calculate_total_symbol_margin_contribution(
        db, redis_client, db_order.order_user_id, db_order.order_company_name,
        open_positions_before + [simulated_order], order_model_class, 'live'
    )
    margin_after = margin_after_data["total_margin"]
    orders_logger.info(f"New total margin for {db_order.order_company_name}: {margin_after}")
    
    # 5. Calculate the change and apply it to the user's overall margin
    margin_change = margin_after - margin_before
    orders_logger.info(f"Margin change for user {db_order.order_user_id}: {margin_change}")
    
    # Get the actual user, not the service account
    actual_user = await get_user_by_id_with_lock(db, db_order.order_user_id)
    if actual_user:
        # Record original margin for verification
        original_margin = actual_user.margin or Decimal('0')
        new_margin = original_margin + margin_change
        
        # Check if user has enough free margin
        if actual_user.wallet_balance < new_margin:
            orders_logger.error(f"User {db_order.order_user_id} does not have enough wallet balance to cover margin. wallet_balance={actual_user.wallet_balance}, new_margin={new_margin}")
            raise HTTPException(status_code=400, detail="Insufficient funds to cover margin")
        
        # Update the user's margin in database
        actual_user.margin = new_margin
        db.add(actual_user)
        update_fields['user_margin_after'] = new_margin
        orders_logger.info(f"User {db_order.order_user_id} margin updated from {original_margin} to {new_margin}")
    else:
        orders_logger.error(f"Could not find user with ID {db_order.order_user_id} to update margin")
        raise HTTPException(status_code=404, detail=f"User {db_order.order_user_id} not found")

    # Update the order in the database with all new fields
    updated_order_db = await crud_order.update_order_with_tracking(
        db,
        db_order,
        update_fields,
        current_user.id,
        'live',  # For Barclays service provider, always use 'live'
        action_type=action_type
    )

    await db.commit()
    await db.refresh(updated_order_db)

    # Update caches and publish events
    # Get the user to access their group_name and other attributes
    actual_user = await get_user_by_id(db, updated_order_db.order_user_id, user_type='live')
    
    user_data_to_cache = {
        "id": updated_order_db.order_user_id, 
        "email": getattr(actual_user, 'email', None),
        "group_name": getattr(actual_user, 'group_name', None),
        "leverage": getattr(actual_user, 'leverage', Decimal('100')),
        "user_type": 'live', 
        "account_number": getattr(actual_user, 'account_number', None),
        "wallet_balance": getattr(actual_user, 'wallet_balance', Decimal('0')),
        "margin": getattr(actual_user, 'margin', Decimal('0')),
        "first_name": getattr(actual_user, 'first_name', None),
        "last_name": getattr(actual_user, 'last_name', None),
    }
    await set_user_data_cache(redis_client, updated_order_db.order_user_id, user_data_to_cache)
    await update_user_static_orders(updated_order_db.order_user_id, db, redis_client, 'live')
    await publish_order_update(redis_client, updated_order_db.order_user_id)
    await publish_user_data_update(redis_client, updated_order_db.order_user_id)
    await publish_market_data_trigger(redis_client)
    
    return updated_order_db


@router.post("/service-provider/order-execution", response_model=OrderResponse)
async def service_provider_order_execution(
    request: ServiceProviderUpdateRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_user_from_service_token)
):
    """
    Endpoint for service providers to execute orders.
    This handles both opening new orders and closing existing ones.
    """
    from app.core.logging_config import service_provider_logger, frontend_orders_logger, error_logger
    
    # Log the incoming request
    service_provider_logger.info(f"SERVICE PROVIDER ORDER EXECUTION - REQUEST: {json.dumps(request.dict(), default=str)}")
    
    try:
        # Extract the ID to find the order
        id_to_find = request.order_id
        if not id_to_find:
            error_msg = "No order_id provided in request"
            service_provider_logger.error(f"SERVICE PROVIDER ERROR: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)

        # Find the order by ID (could be any ID field)
        db_order = await crud_order.get_order_by_any_id(db, id_to_find, UserOrder)
        if not db_order:
            error_msg = f"Order with ID {id_to_find} not found"
            service_provider_logger.error(f"SERVICE PROVIDER ERROR: {error_msg}")
            raise HTTPException(status_code=404, detail=error_msg)

        # Log the found order
        service_provider_logger.info(f"SERVICE PROVIDER ORDER FOUND: order_id={db_order.order_id}, status={db_order.order_status}, user_id={db_order.order_user_id}")
        
        # Extract the original status for comparison
        original_status = db_order.order_status
        
        # Extract fields to update from the request
        update_fields = request.dict(exclude_unset=True)
        new_status = update_fields.get("order_status", original_status)
        
        # Log the status transition
        service_provider_logger.info(f"SERVICE PROVIDER STATUS TRANSITION: {original_status} -> {new_status}")

        # Do not update any ID fields from the service provider request. They are for lookup only.
        id_fields_to_ignore = [
            'order_id', 'cancel_id', 'close_id', 'modify_id', 'stoploss_id',
            'takeprofit_id', 'stoploss_cancel_id', 'takeprofit_cancel_id'
        ]
        for field in id_fields_to_ignore:
            if field in update_fields:
                del update_fields[field]

        # Add id_to_find to update_fields for _handle_order_close_transition
        update_fields["_id_used_for_lookup"] = id_to_find
        
        # Handle different status transitions
        if new_status == "OPEN" and original_status in ["PROCESSING", "PENDING"]:
            updated_order = await _handle_order_open_transition(
                db=db,
                redis_client=redis_client,
                db_order=db_order,
                update_fields=update_fields,
                current_user=current_user,
                action_type="SERVICE_PROVIDER_EXECUTION"
            )
            response = OrderResponse.model_validate(updated_order, from_attributes=True)
            service_provider_logger.info(f"SERVICE PROVIDER ORDER OPENED: order_id={response.order_id}, status={response.order_status}")
            return response
        elif new_status == "CLOSED" and original_status == "OPEN":
            updated_order = await _handle_order_close_transition(
                db=db,
                redis_client=redis_client,
                db_order=db_order,
                update_fields=update_fields,
                current_user=current_user
            )
            response = OrderResponse.model_validate(updated_order, from_attributes=True)
            service_provider_logger.info(f"SERVICE PROVIDER ORDER CLOSED: order_id={response.order_id}, status={response.order_status}, net_profit={response.net_profit}")
            return response
        elif new_status == "REJECTED" and original_status in ["PROCESSING", "PENDING"]:
            # Handle rejection (update status and potentially release margin)
            db_order.order_status = "REJECTED"
            db_order.cancel_message = update_fields.get("cancel_message", "Rejected by service provider")
            db_order.cancel_id = await generate_unique_10_digit_id(db, UserOrder, 'cancel_id')
            
            await db.commit()
            await db.refresh(db_order)
            
            response = OrderResponse.model_validate(db_order, from_attributes=True)
            service_provider_logger.info(f"SERVICE PROVIDER ORDER REJECTED: order_id={response.order_id}, reason={db_order.cancel_message}")
            return response
        else:
            error_msg = f"Invalid status transition from {original_status} to {new_status}"
            service_provider_logger.error(f"SERVICE PROVIDER ERROR: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        error_msg = f"Error in service_provider_order_execution: {str(e)}"
        error_logger.error(error_msg, exc_info=True)
        service_provider_logger.error(f"SERVICE PROVIDER EXCEPTION: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/service-provider/order-update", response_model=OrderResponse)
async def service_provider_order_update(
    update_request: ServiceProviderUpdateRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_user_from_service_token)
):
    """
    Endpoint for service providers to update orders.
    This can handle various updates including status changes, price updates, etc.
    """
    from app.core.logging_config import service_provider_logger, frontend_orders_logger, error_logger
    
    # Log the incoming request
    service_provider_logger.info(f"SERVICE PROVIDER ORDER UPDATE - REQUEST: {json.dumps(update_request.dict(), default=str)}")
    
    try:
        # Find the order by any ID field
        id_to_find = None
        for id_field in ['order_id', 'close_id', 'cancel_id', 'modify_id', 'stoploss_id', 'takeprofit_id']:
            if hasattr(update_request, id_field) and getattr(update_request, id_field):
                id_to_find = getattr(update_request, id_field)
                break
        
        if not id_to_find:
            error_msg = "No valid ID field provided in update request"
            service_provider_logger.error(f"SERVICE PROVIDER ERROR: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
        
        db_order = await crud_order.get_order_by_any_id(db, id_to_find, UserOrder)
        if not db_order:
            error_msg = f"Order with ID {id_to_find} not found"
            service_provider_logger.error(f"SERVICE PROVIDER ERROR: {error_msg}")
            raise HTTPException(status_code=404, detail=error_msg)
        
        # Log the found order
        service_provider_logger.info(f"SERVICE PROVIDER ORDER FOUND: order_id={db_order.order_id}, status={db_order.order_status}, user_id={db_order.order_user_id}")
        
        # Extract the original status for comparison
        original_status = db_order.order_status
        
        # Extract fields to update from the request
        update_fields = update_request.dict(exclude_unset=True)
        new_status = update_fields.get("order_status", original_status)
        
        # Log the status transition if status is changing
        if new_status != original_status:
            service_provider_logger.info(f"SERVICE PROVIDER STATUS TRANSITION: {original_status} -> {new_status}")
        
        # Do not update any ID fields from the service provider request. They are for lookup only.
        id_fields_to_ignore = [
            'order_id', 'cancel_id', 'close_id', 'modify_id', 'stoploss_id',
            'takeprofit_id', 'stoploss_cancel_id', 'takeprofit_cancel_id'
        ]
        for field in id_fields_to_ignore:
            if field in update_fields:
                del update_fields[field]

        # If order is being closed, add the id_to_find to update_fields for SL/TP cancellation checks
        if update_fields.get("order_status") == "CLOSED" and original_status == "OPEN":
            update_fields["_id_used_for_lookup"] = id_to_find
            orders_logger.info(f"Service provider is closing order {db_order.order_id}. Calling _handle_order_close_transition.")
            updated_order = await _handle_order_close_transition(
                db=db,
                redis_client=redis_client,
                db_order=db_order,
                update_fields=update_fields,
                current_user=current_user
            )
            response = OrderResponse.model_validate(updated_order, from_attributes=True)
            service_provider_logger.info(f"SERVICE PROVIDER ORDER CLOSED: order_id={response.order_id}, status={response.order_status}, net_profit={response.net_profit}")
            return response
            
        # Specific logic for pending order cancellation
        if update_fields.get("order_status") == "CANCELLED" and original_status == "PENDING":
            db_order.order_status = "CANCELLED"
            db_order.cancel_message = update_fields.get("cancel_message", "Cancelled by service provider")
            db_order.cancel_id = await generate_unique_10_digit_id(db, UserOrder, 'cancel_id')
            
            await db.commit()
            await db.refresh(db_order)
            
            response = OrderResponse.model_validate(db_order, from_attributes=True)
            service_provider_logger.info(f"SERVICE PROVIDER PENDING ORDER CANCELLED: order_id={response.order_id}")
            return response
        
        # For other updates, apply them directly
        for key, value in update_fields.items():
            if hasattr(db_order, key):
                setattr(db_order, key, value)
        
        await db.commit()
        await db.refresh(db_order)
        
        response = OrderResponse.model_validate(db_order, from_attributes=True)
        service_provider_logger.info(f"SERVICE PROVIDER ORDER UPDATED: order_id={response.order_id}, status={response.order_status}")
        return response
    except Exception as e:
        error_msg = f"Error in service_provider_order_update: {str(e)}"
        error_logger.error(error_msg, exc_info=True)
        service_provider_logger.error(f"SERVICE PROVIDER EXCEPTION: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)



async def _handle_order_close_transition(
    db: AsyncSession,
    redis_client: Redis,
    db_order: UserOrder,
    update_fields: Dict[str, Any],
    current_user: User
) -> UserOrder:
    """
    Handles the logic for an order transitioning to CLOSED status by a service provider.
    Calculates P/L, updates user margin and wallet, and cancels any existing SL/TP.
    """
    user_id = db_order.order_user_id
    db_user = await get_user_by_id_with_lock(db, user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    order_model = UserOrder # Service provider actions are on live users
    
    # Get the ID used to find this order
    id_to_find = update_fields.get('_id_used_for_lookup')
    
    # Check if the order_id matches with stoploss_id or takeprofit_id
    if id_to_find:
        # If order_id matches with stoploss_id, check if take_profit exists
        if id_to_find == db_order.stoploss_id and db_order.take_profit is not None and db_order.take_profit > 0 and db_order.takeprofit_id:
            orders_logger.info(f"Order ID {id_to_find} matches stoploss_id. Sending takeprofit cancellation.")
            takeprofit_cancel_id = await generate_unique_10_digit_id(db, order_model, 'takeprofit_cancel_id')
            
            # Send take profit cancellation to Firebase
            tp_cancel_payload = {
                "order_id": db_order.order_id,
                "takeprofit_id": db_order.takeprofit_id,
                "takeprofit_cancel_id": takeprofit_cancel_id,
                "user_id": user_id,
                "symbol": db_order.order_company_name,
                "order_type": db_order.order_type,
                "order_status": "CLOSED",
                "status": "TAKEPROFIT-CANCEL"
            }
            await send_order_to_firebase(tp_cancel_payload, "live")
            orders_logger.info(f"Sent TP cancellation to Firebase for order {db_order.order_id}")
            
            # Update the order with the cancel ID
            update_fields["takeprofit_cancel_id"] = takeprofit_cancel_id
            
        # If order_id matches with takeprofit_id, check if stop_loss exists
        elif id_to_find == db_order.takeprofit_id and db_order.stop_loss is not None and db_order.stop_loss > 0 and db_order.stoploss_id:
            orders_logger.info(f"Order ID {id_to_find} matches takeprofit_id. Sending stoploss cancellation.")
            stoploss_cancel_id = await generate_unique_10_digit_id(db, order_model, 'stoploss_cancel_id')
            
            # Send stop loss cancellation to Firebase
            sl_cancel_payload = {
                "order_id": db_order.order_id,
                "stoploss_id": db_order.stoploss_id,
                "stoploss_cancel_id": stoploss_cancel_id,
                "user_id": user_id,
                "symbol": db_order.order_company_name,
                "order_type": db_order.order_type,
                "order_status": "CLOSED",
                "status": "STOPLOSS-CANCEL"
            }
            await send_order_to_firebase(sl_cancel_payload, "live")
            orders_logger.info(f"Sent SL cancellation to Firebase for order {db_order.order_id}")
            
            # Update the order with the cancel ID
            update_fields["stoploss_cancel_id"] = stoploss_cancel_id
    
    # Remove the temporary field
    if '_id_used_for_lookup' in update_fields:
        del update_fields['_id_used_for_lookup']

    # --- P&L and Commission Calculation ---
    symbol = db_order.order_company_name.upper()
    quantity = Decimal(str(db_order.order_quantity))
    entry_price = Decimal(str(db_order.order_price))
    
    close_price = update_fields.get('close_price')
    if close_price is None:
        raise HTTPException(status_code=400, detail="close_price is required to close an order.")
    close_price = Decimal(str(close_price))
    
    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
    symbol_info_result = await db.execute(symbol_info_stmt)
    ext_symbol_info = symbol_info_result.scalars().first()
    if not ext_symbol_info:
        raise HTTPException(status_code=500, detail=f"Symbol info not found for {symbol}")

    contract_size = Decimal(str(ext_symbol_info.contract_size))
    profit_currency = ext_symbol_info.profit.upper()

    user_data = await get_user_data_cache(redis_client, user_id, db, 'live')
    group_name = user_data.get('group_name') if user_data else db_user.group_name
    group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
    if not group_settings:
        raise HTTPException(status_code=500, detail="Group settings not found")

    # Profit calculation
    if db_order.order_type == "BUY":
        profit = (close_price - entry_price) * quantity * contract_size
    else: # SELL
        profit = (entry_price - close_price) * quantity * contract_size
    profit_usd = await _convert_to_usd(profit, profit_currency, user_id, db_order.order_id, "PnL", db, redis_client)

    # Commission calculation
    existing_commission = Decimal(str(db_order.commission or "0.0"))
    exit_commission = Decimal("0.0")
    commission_type = int(group_settings.get('commision_type', -1))
    if commission_type in [0, 2]: # Every Trade or Out
        commission_value_type = int(group_settings.get('commision_value_type', -1))
        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
        if commission_value_type == 0: # Per lot
            exit_commission = quantity * commission_rate
        elif commission_value_type == 1: # Percent
            exit_commission = (commission_rate / Decimal("100")) * (quantity * contract_size * close_price)
    
    total_commission = (existing_commission + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net_profit = (profit_usd - total_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # --- Margin Recalculation ---
    all_open_orders = await crud_order.get_open_orders_by_user_id_and_symbol(db, user_id, symbol, order_model)
    margin_before_dict = await calculate_total_symbol_margin_contribution(db, redis_client, user_id, symbol, all_open_orders, 'live')
    margin_before = margin_before_dict["total_margin"]
    
    non_symbol_margin = Decimal(str(db_user.margin)) - margin_before
    
    remaining_orders = [o for o in all_open_orders if o.order_id != db_order.order_id]
    margin_after_dict = await calculate_total_symbol_margin_contribution(db, redis_client, user_id, symbol, remaining_orders, 'live')
    margin_after = margin_after_dict["total_margin"]

    db_user.margin = max(Decimal(0), (non_symbol_margin + margin_after).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    
    swap = Decimal(str(update_fields.get("swap", "0.0")))

    # --- Wallet Update ---
    db_user.wallet_balance = (Decimal(str(db_user.wallet_balance)) + net_profit - swap).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    
    # --- Create Wallet Transactions ---
    transaction_time = datetime.datetime.now(datetime.timezone.utc)
    wallet_common = {"symbol": symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time, "order_id": db_order.order_id, "user_id": user_id}
    if net_profit != Decimal("0.0"):
        db.add(Wallet(transaction_id=await generate_unique_10_digit_id(db, Wallet, "transaction_id"), **WalletCreate(**wallet_common, transaction_type="Profit/Loss", transaction_amount=net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True)))
    if total_commission > Decimal("0.0"):
        db.add(Wallet(transaction_id=await generate_unique_10_digit_id(db, Wallet, "transaction_id"), **WalletCreate(**wallet_common, transaction_type="Commission", transaction_amount=-total_commission, description=f"Commission for closing order {db_order.order_id}").model_dump(exclude_none=True)))
    if swap != Decimal("0.0"):
        db.add(Wallet(transaction_id=await generate_unique_10_digit_id(db, Wallet, "transaction_id"), **WalletCreate(**wallet_common, transaction_type="Swap", transaction_amount=-swap, description=f"Swap for order {db_order.order_id}").model_dump(exclude_none=True)))

    # --- Update Order Record ---
    update_fields.update({
        "order_status": "CLOSED", 
        "net_profit": net_profit,
        "swap": swap, 
        "commission": total_commission,
        "close_id": await generate_unique_10_digit_id(db, order_model, 'close_id')
    })

    updated_order = await crud_order.update_order_with_tracking(db, db_order, update_fields, current_user.id, 'live', "SP_CLOSE")
    
    await db.commit()
    await db.refresh(db_user)
    await db.refresh(updated_order)

    # --- Finalize: Caches and Websockets ---
    user_data_to_cache = {
        "id": db_user.id, "email": getattr(db_user, 'email', None), "group_name": db_user.group_name,
        "leverage": db_user.leverage, "user_type": 'live', "account_number": getattr(db_user, 'account_number', None),
        "wallet_balance": db_user.wallet_balance, "margin": db_user.margin, "first_name": getattr(db_user, 'first_name', None),
        "last_name": getattr(db_user, 'last_name', None), "country": getattr(db_user, 'country', None),
        "phone_number": getattr(db_user, 'phone_number', None),
    }
    await set_user_data_cache(redis_client, user_id, user_data_to_cache)
    await update_user_static_orders(user_id, db, redis_client, 'live')
    await publish_order_update(redis_client, user_id)
    await publish_user_data_update(redis_client, user_id)
    await publish_market_data_trigger(redis_client)

    return updated_order


@router.post("/service-provider/calculate-half-spread", response_model=HalfSpreadResponse, summary="Calculate Half-Spread for a Symbol in a User's Group")
async def calculate_half_spread_for_service_provider(
    request: HalfSpreadRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_user_from_service_token)
):
    """
    Calculates the half-spread for a given symbol based on the user's group settings,
    identified by any of the order's unique IDs. This endpoint is for use by service providers.
    
    The formula used is: `half_spread = (spread * spread_pip) / 2`
    """
    orders_logger.info(f"Half-spread calculation request for order_id: {request.order_id}, symbol: {request.symbol}")

    # 1. Find the order to get the user_id, using any provided ID.
    # Service providers only operate on live users, so we use UserOrder model.
    order_model = UserOrder
    db_order = await crud_order.get_order_by_any_id(db, generic_id=request.order_id, order_model=order_model)
    if not db_order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order with ID '{request.order_id}' not found.")

    # 2. Find the user from the order to get the group_name
    user_id = db_order.order_user_id
    db_user = await crud_user.get_user_by_id(db, user_id=user_id, user_type='live')
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID {user_id} associated with the order not found.")

    group_name = db_user.group_name
    if not group_name:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} does not belong to any group.")

    # 3. Find Group settings for the specific symbol and group name
    group_settings = await crud_group.get_group_by_symbol_and_name(db, symbol=request.symbol, name=group_name)
    if not group_settings:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No specific settings found for symbol '{request.symbol}' in group '{group_name}'.")

    # 4. Extract spread and spread_pip
    spread = getattr(group_settings, 'spread', None)
    spread_pip = getattr(group_settings, 'spread_pip', None)

    if spread is None or spread_pip is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Incomplete spread settings for symbol '{request.symbol}' in group '{group_name}'.")

    # 5. Calculate half_spread
    try:
        spread_dec = Decimal(str(spread))
        spread_pip_dec = Decimal(str(spread_pip))
        half_spread = (spread_dec * spread_pip_dec) / Decimal(2)
    except (TypeError, InvalidOperation) as e:
        orders_logger.error(f"Error during spread calculation for symbol '{request.symbol}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error during spread calculation: {e}")

    # 6. Return response
    return HalfSpreadResponse(
        symbol=request.symbol,
        half_spread=half_spread
    )

@router.get("/service-provider/status", response_model=OrderStatusResponse, summary="Get the status of an order by any ID")
async def get_order_status_by_service_provider(
    id: str = Query(..., description="The ID to search for (can be order_id, close_id, cancel_id, etc.)"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_user_from_service_token)
):
    """
    Allows a service provider to retrieve the status of an order using any of its unique identifiers.
    This endpoint searches across all live user orders.
    """
    orders_logger.info(f"Service provider status request received for ID: {id}")

    # The service provider only deals with live users, so we use the UserOrder model.
    order_model = UserOrder
    
    # Use the generic lookup function to find the order by any of its IDs.
    db_order = await crud_order.get_order_by_any_id(db, generic_id=id, order_model=order_model)

    if not db_order:
        orders_logger.warning(f"Order not found with provided identifier '{id}' for service provider status check.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order not found with provided ID: {id}"
        )

    orders_logger.info(f"Found order {db_order.order_id} with status: '{db_order.status}' and order_status: '{db_order.order_status}'")

    return OrderStatusResponse(
        order_id=db_order.order_id,
        status=db_order.status,
        order_status=db_order.order_status
    )


