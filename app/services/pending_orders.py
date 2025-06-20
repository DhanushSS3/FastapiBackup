# app/services/pending_orders.py

from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation
from redis.asyncio import Redis
import logging
from datetime import datetime, timezone
import json
from pydantic import BaseModel 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select  # Add this import
import asyncio
import time
import uuid

from app.core.cache import (
    get_live_adjusted_buy_price_for_pair,
    get_user_data_cache,
    get_group_settings_cache,
    set_user_data_cache,
    set_user_portfolio_cache,
    publish_account_structure_changed_event,
    get_group_symbol_settings_cache, 
    get_last_known_price,
    get_adjusted_market_price_cache,
    publish_order_update,
    publish_user_data_update,
    publish_market_data_trigger,
    set_user_static_orders_cache,
    get_user_static_orders_cache,
    get_user_dynamic_portfolio_cache,
    DecimalEncoder  # Import for JSON serialization of decimals
)
from app.services.margin_calculator import calculate_single_order_margin, calculate_pending_order_margin, get_external_symbol_info
from app.services.portfolio_calculator import calculate_user_portfolio, _convert_to_usd
from app.core.firebase import send_order_to_firebase, get_latest_market_data
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder
from app.crud import crud_order
from app.crud.crud_order import get_order_model
from app.crud.user import update_user_margin 

from app.schemas.order import PendingOrderPlacementRequest, OrderPlacementRequest

logger = logging.getLogger("pending_orders")

# Redis key prefix for pending orders
REDIS_PENDING_ORDERS_PREFIX = "pending_orders"


async def remove_pending_order(redis_client: Redis, order_id: str, symbol: str, order_type: str, user_id: str):
    """Removes a single pending order from Redis."""
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"
    try:
        # Fetch all orders for the key
        all_user_orders_json = await redis_client.hgetall(redis_key)
        
        updated_user_orders = {}
        order_removed = False

        for user_key, orders_list_str in all_user_orders_json.items():
            # Handle both bytes and string user keys
            if isinstance(user_key, bytes):
                current_user_id = user_key.decode('utf-8')
            else:
                current_user_id = user_key
                
            # Handle both bytes and string order lists
            if isinstance(orders_list_str, bytes):
                orders_list_str = orders_list_str.decode('utf-8')
                
            orders_list = json.loads(orders_list_str)
            
            # Filter out the specific order to be removed
            filtered_orders = [order for order in orders_list if order.get('order_id') != order_id]
            
            if len(filtered_orders) < len(orders_list):
                order_removed = True
            
            if filtered_orders:
                updated_user_orders[user_key] = json.dumps(filtered_orders)
        
        if order_removed:
            if updated_user_orders:
                # Atomically update the hash for the users whose orders changed
                pipe = redis_client.pipeline()
                for user_key, orders_json in updated_user_orders.items():
                    pipe.hset(redis_key, user_key, orders_json)
                await pipe.execute()
            else:
                # If no orders remain for this key after filtering, delete the hash
                await redis_client.delete(redis_key)
            logger.info(f"Pending order {order_id} removed from Redis for symbol {symbol} type {order_type} and user {user_id}.")
        else:
            logger.warning(f"Attempted to remove pending order {order_id} from Redis, but it was not found for symbol {symbol} type {order_type} and user {user_id}.")

    except Exception as e:
        logger.error(f"Error removing pending order {order_id} from Redis: {e}", exc_info=True)


async def add_pending_order(
    redis_client: Redis, 
    pending_order_data: Dict[str, Any]
) -> None:
    """Adds a pending order to Redis."""
    symbol = pending_order_data['order_company_name']
    order_type = pending_order_data['order_type']
    user_id = str(pending_order_data['order_user_id'])  # Ensure user_id is a string
    redis_key = f"{REDIS_PENDING_ORDERS_PREFIX}:{symbol}:{order_type}"

    try:
        all_user_orders_json = await redis_client.hget(redis_key, user_id)
        
        # Handle both bytes and string JSON
        if all_user_orders_json:
            if isinstance(all_user_orders_json, bytes):
                all_user_orders_json = all_user_orders_json.decode('utf-8')
            current_orders = json.loads(all_user_orders_json)
        else:
            current_orders = []

        # Check if an order with the same ID already exists
        if any(order.get('order_id') == pending_order_data['order_id'] for order in current_orders):
            logger.warning(f"Pending order {pending_order_data['order_id']} already exists in Redis. Skipping add.")
            return

        current_orders.append(pending_order_data)
        await redis_client.hset(redis_key, user_id, json.dumps(current_orders))
        logger.info(f"Pending order {pending_order_data['order_id']} added to Redis for user {user_id} under key {redis_key}.")
    except Exception as e:
        logger.error(f"Error adding pending order {pending_order_data['order_id']} to Redis: {e}", exc_info=True)
        raise

async def trigger_pending_order(
    db,
    redis_client: Redis,
    order: Dict[str, Any],
    current_price: Decimal
) -> None:
    """
    Trigger a pending order for any user type.
    Updates the order status to 'OPEN' in the database,
    adjusts user margin, and updates portfolio caches.
    """
    order_id = order['order_id']
    user_id = order['order_user_id']
    user_type = order['user_type']
    symbol = order['order_company_name']
    order_type_original = order['order_type'] # Store original order_type
    from app.core.logging_config import orders_logger
    
    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Starting trigger_pending_order for order {order_id}, user {user_id}, symbol {symbol}, type {order_type_original}")
    
    try:
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            user_model = User if user_type == 'live' else DemoUser
            user_data = await user_model.by_id(db, user_id)
            if not user_data:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] User data not found for user {user_id} when triggering order {order_id}. Skipping.")
                return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] User data loaded for user {user_id}: group={user_data.get('group_name')}, wallet_balance={user_data.get('wallet_balance')}")

        group_name = user_data.get('group_name')
        group_settings = await get_group_settings_cache(redis_client, group_name)
        if not group_settings:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Group settings not found for group {group_name} when triggering order {order_id}. Skipping.")
            return

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Group settings loaded for group {group_name}")

        order_model = get_order_model(user_type)
        
        # Add enhanced retry logic for database order fetch
        max_retries = 5  # Increase from 3 to 5
        initial_retry_delay = 0.5  # Start with a shorter delay (seconds)
        retry_delay = initial_retry_delay
        db_order = None
        
        for retry_count in range(max_retries):
            # Try the standard method first
            db_order = await crud_order.get_order_by_id(db, order_id, order_model)
            
            if db_order:
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} found on attempt {retry_count + 1}")
                break
                
            # On the last attempt, try a direct SQL query as a backup method
            if retry_count == max_retries - 1:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Last attempt: trying direct SQL query for order {order_id}")
                try:
                    from sqlalchemy import text
                    table_name = "demo_user_orders" if user_type == "demo" else "user_orders"
                    result = await db.execute(text(f"SELECT * FROM {table_name} WHERE order_id = :order_id"), {"order_id": order_id})
                    row = result.fetchone()
                    if row:
                        # Create order object from raw SQL result
                        db_order = order_model()
                        for key, value in row._mapping.items():
                            setattr(db_order, key, value)
                        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Found order {order_id} with direct SQL query")
                        break
                except Exception as sql_error:
                    orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error in direct SQL query: {str(sql_error)}", exc_info=True)
            
            if retry_count < max_retries - 1:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} not found on attempt {retry_count + 1}/{max_retries}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 1.5  # Less aggressive exponential backoff

        if not db_order:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order {order_id} not found after {max_retries} attempts when triggering pending order. Skipping.")
            # Remove the order from Redis since it cannot be found in the database
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Removing order {order_id} from Redis since it cannot be found in the database.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            return

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Database order loaded: status={db_order.order_status}, price={db_order.order_price}")

        # Ensure atomicity: only update if still PENDING
        if db_order.order_status != 'PENDING':
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} already processed (status: {db_order.order_status}). Skipping trigger.")
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id) 
            return

        # Get the adjusted buy price (ask price) for the symbol from cache
        group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, group_name, symbol)
        
        if not adjusted_prices:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted market prices not found for symbol {symbol} when triggering order {order_id}. Skipping.")
            return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted prices loaded: {adjusted_prices}")
        
        # Use the adjusted buy price for all trigger conditions
        adjusted_buy_price = adjusted_prices.get('buy')
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        adjusted_sell_price = adjusted_prices.get('sell')
        if not adjusted_sell_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted sell price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return

        # Normalize decimal values for comparison - round to 5 decimal places
        try:
            order_price = Decimal(str(db_order.order_price))
            # Round to 5 decimal places for consistent comparison
            order_price_normalized = Decimal(str(round(order_price, 5)))
            
            adjusted_buy_price_str = str(adjusted_buy_price)
            # Ensure the price has at least 5 decimal places
            if '.' in adjusted_buy_price_str:
                integer_part, decimal_part = adjusted_buy_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_buy_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_buy_price_normalized = Decimal(str(round(Decimal(adjusted_buy_price_str), 5)))
            
            # Similar normalization for adjusted_sell_price if needed
            adjusted_sell_price_str = str(adjusted_sell_price)
            if '.' in adjusted_sell_price_str:
                integer_part, decimal_part = adjusted_sell_price_str.split('.')
                if len(decimal_part) < 5:
                    decimal_part = decimal_part.ljust(5, '0')
                adjusted_sell_price_str = f"{integer_part}.{decimal_part}"
            
            adjusted_sell_price_normalized = Decimal(str(round(Decimal(adjusted_sell_price_str), 5)))
            
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Normalized prices for comparison - order_price: {order_price_normalized}, adjusted_buy_price: {adjusted_buy_price_normalized}, adjusted_sell_price: {adjusted_sell_price_normalized}")
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error normalizing prices for comparison: {str(e)}", exc_info=True)
            return
        
        # Only use adjusted_buy_price for all pending order types
        if not adjusted_buy_price:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Adjusted buy price missing for symbol {symbol} when checking order {order_id}. Skipping execution.")
            return
        
        # Determine if the order should be triggered
        should_trigger = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger = adjusted_buy_price_normalized <= order_price_normalized
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] BUY_LIMIT/SELL_STOP check: {adjusted_buy_price_normalized} <= {order_price_normalized} = {should_trigger}")
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger = adjusted_buy_price_normalized >= order_price_normalized
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] SELL_LIMIT/BUY_STOP check: {adjusted_buy_price_normalized} >= {order_price_normalized} = {should_trigger}")
        else:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Unknown order type {order_type_original} for order {order_id}. Skipping execution.")
            return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Checking order {order_id}: type={order_type_original}, adjusted_buy_price={adjusted_buy_price_normalized}, order_price={order_price_normalized}, should_trigger={should_trigger}")
        
        # Additional debug logging for price comparison
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Price comparison values - adjusted_buy_price_normalized: {adjusted_buy_price_normalized} ({type(adjusted_buy_price_normalized)}), order_price_normalized: {order_price_normalized} ({type(order_price_normalized)})")
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Raw price values - adjusted_buy_price: {adjusted_buy_price} ({type(adjusted_buy_price)}), order_price: {order_price} ({type(order_price)})")
        
        # Convert to strings for consistent comparison (handles potential float vs Decimal issues)
        adjusted_price_str = str(adjusted_buy_price_normalized)
        order_price_str = str(order_price_normalized)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] String comparison for prices: '{adjusted_price_str}' vs '{order_price_str}'")
        
        # Compare numeric difference
        price_diff = abs(adjusted_buy_price_normalized - order_price_normalized)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Absolute price difference: {price_diff}")

        # Compare with small epsilon tolerance to catch very close values
        epsilon = Decimal('0.00001')  # Small tolerance
        is_close = price_diff < epsilon
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Prices within epsilon tolerance: {is_close} (epsilon={epsilon})")
        
        # Consider using epsilon for near-exact matches
        should_trigger_with_epsilon = False
        if order_type_original in ['BUY_LIMIT', 'SELL_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized <= order_price_normalized) or is_close
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] BUY_LIMIT/SELL_STOP with epsilon: {should_trigger_with_epsilon}")
        elif order_type_original in ['SELL_LIMIT', 'BUY_STOP']:
            should_trigger_with_epsilon = (adjusted_buy_price_normalized >= order_price_normalized) or is_close
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] SELL_LIMIT/BUY_STOP with epsilon: {should_trigger_with_epsilon}")
        
        # Use epsilon-based trigger when prices are very close
        if should_trigger_with_epsilon and not should_trigger:
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Using epsilon-based trigger since prices are very close: {adjusted_buy_price_normalized} vs {order_price_normalized}")
            should_trigger = True
        
        if not should_trigger:
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} conditions not met for execution. Skipping.")
            return
        
        # Calculate the required margin for the order using calculate_single_order_margin
        order_quantity_decimal = Decimal(str(db_order.order_quantity))
        user_leverage = Decimal(str(user_data.get('leverage', '1.0')))
        # Get external symbol info from DB
        external_symbol_info = await get_external_symbol_info(db, symbol)
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] External symbol info loaded: {external_symbol_info}")
        
        # Get raw market data (for margin calculation) using the synchronous version
        from app.firebase_stream import get_latest_market_data as get_latest_market_data_sync
        raw_market_data = get_latest_market_data_sync(symbol)
        if not raw_market_data or not raw_market_data.get('o'):
            # Fallback to last known price from Redis
            from app.core.cache import get_last_known_price
            last_known = await get_last_known_price(redis_client, symbol)
            if last_known:
                raw_market_data = last_known
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Fallback to last known price for {symbol}: {raw_market_data}")
            else:
                orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] No market data or last known price for {symbol}. Cannot calculate margin for order {order_id}.")
                return
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Raw market data loaded: {raw_market_data}")
        
        try:
            # Wrap the calculate_single_order_margin call in a try block to catch any exceptions
            margin, exec_price, contract_value, commission = await calculate_single_order_margin(
                redis_client=redis_client,
                symbol=symbol,
                order_type=order_type_original,
                quantity=order_quantity_decimal,
                user_leverage=user_leverage,
                group_settings=group_symbol_settings,
                external_symbol_info=external_symbol_info or {},
                raw_market_data={symbol: raw_market_data},
                db=db,
                user_id=user_id,
                order_price=order_price_normalized  # Pass the normalized order price
            )
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Margin calculated successfully for order {order_id}: margin={margin}, exec_price={exec_price}, contract_value={contract_value}")
            
            # Implement hedging logic: check for hedging positions that would reduce required margin
            try:
                # For hedging: Check if there are existing positions for the same symbol with opposite direction
                open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                hedge_offset = Decimal('0')
                
                # Get the direction of the current order for comparison
                is_buy = order_type_original.startswith('BUY')
                is_sell = order_type_original.startswith('SELL')
                
                # Check for opposing positions
                for existing_order in open_orders:
                    if existing_order.order_company_name == symbol:
                        existing_is_buy = existing_order.order_type.startswith('BUY')
                        existing_is_sell = existing_order.order_type.startswith('SELL')
                        
                        # If directions are opposite, we have a hedging situation
                        if (is_buy and existing_is_sell) or (is_sell and existing_is_buy):
                            # For a hedge position, reduce the margin by the minimum of the two quantities
                            hedge_quantity = min(order_quantity_decimal, existing_order.order_quantity)
                            hedge_margin = (hedge_quantity / order_quantity_decimal) * margin
                            hedge_offset += hedge_margin
                            
                            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Found hedging position for order {order_id}: "
                                             f"existing order {existing_order.order_id} with {existing_order.order_type}, "
                                             f"reducing margin by {hedge_margin} (hedge ratio: {hedge_quantity}/{order_quantity_decimal})")
                
                # Apply the hedge offset to reduce required margin
                if hedge_offset > Decimal('0'):
                    original_margin = margin
                    # Don't let hedge_offset reduce margin below zero
                    margin = max(margin - hedge_offset, Decimal('0'))
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Applied hedging offset: original margin={original_margin}, "
                                     f"hedge offset={hedge_offset}, final margin={margin}")
            except Exception as hedge_error:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error calculating hedge offset for user {user_id}: {str(hedge_error)}", exc_info=True)
                # Continue with original margin if hedging calculation fails
        except Exception as margin_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error calculating margin for order {order_id}: {str(margin_error)}", exc_info=True)
            return
        
        # Fetch the latest dynamic portfolio to get up-to-date free_margin
        try:
            dynamic_portfolio = await get_user_dynamic_portfolio_cache(redis_client, user_id)
            user_free_margin = dynamic_portfolio.get('free_margin', 'N/A') if dynamic_portfolio else 'N/A'
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Latest free_margin for user {user_id}: {user_free_margin}")
        except Exception as portfolio_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error fetching dynamic portfolio for user {user_id}: {str(portfolio_error)}", exc_info=True)
            return
        
        # We'll skip the free margin check since we're already handling the hedging logic
        # and checking against wallet_balance later in the function
        
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Executing order {order_id} for user {user_id} at price {adjusted_buy_price_normalized} (order_price={order_price_normalized}, type={order_type_original})")
        
        # Get contract size and profit currency from symbol settings
        contract_size = Decimal(str(group_symbol_settings.get('contract_size', 100000)))
        profit_currency = group_symbol_settings.get('profit', 'USD')
        
        # Calculate contract value using the CORRECT formula - this should match what we calculated above
        contract_value = order_quantity_decimal * contract_size
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Contract value calculated: {contract_value}")

        # Update the order properties with the calculated values
        try:
            # Store the original values for logging
            original_margin = db_order.margin if db_order.margin else "None"
            
            # Update the order with calculated values from margin calculation
            db_order.margin = margin
            db_order.contract_value = contract_value
            db_order.commission = commission
            db_order.order_price = exec_price  # Use the execution price from margin calculation
            
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated order object with calculated values: " 
                             f"margin={margin} (was {original_margin}), contract_value={contract_value}, "
                             f"commission={commission}, execution_price={exec_price}")
        except Exception as value_update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating order with calculated values: {str(value_update_error)}", exc_info=True)

        # Determine the new order type (removing LIMIT/STOP)
        try:
            new_order_type = 'BUY' if order_type_original in ['BUY_LIMIT', 'BUY_STOP'] else 'SELL'
            db_order.open_time = datetime.now() 
            db_order.order_type = new_order_type
            # Set stop_loss and take_profit to None
            db_order.stop_loss = None
            db_order.take_profit = None
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated order object with new values and cleared SL/TP")
        except Exception as update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating order object with new values: {str(update_error)}", exc_info=True)
            return

        # Non-Barclays user
        current_wallet_balance_decimal = Decimal(str(user_data.get('wallet_balance', '0')))
        current_total_margin_decimal = Decimal(str(user_data.get('margin', '0')))

        new_total_margin = current_total_margin_decimal + margin
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Margin check: current_margin={current_total_margin_decimal}, new_margin={new_total_margin}, wallet_balance={current_wallet_balance_decimal}")
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Using margin with hedging applied: {margin}. Adding to current total margin: {current_total_margin_decimal}")
        
        if new_total_margin > current_wallet_balance_decimal:
            orders_logger.warning(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} for user {user_id} canceled due to insufficient margin. Required: {new_total_margin}, Available: {current_wallet_balance_decimal}")
            db_order.order_status = 'CANCELLED'
            db_order.cancel_message = "InsufficientFreeMargin"
            try:
                await db.commit()
                await db.refresh(db_order)
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} status updated to CANCELLED in database")
            except Exception as commit_error:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error committing order cancellation to database: {str(commit_error)}", exc_info=True)
            return

        # These lines were unindented and caused the error, they should be part of the outer try block
        from decimal import ROUND_HALF_UP # Moved import here to resolve potential circular dependency for calculation below
        try:
            # Get all open orders for the symbol to recalculate margin
            all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                db=db, user_id=user_id, symbol=symbol, order_model=order_model
            )
            
            # Calculate margin before closing this order (this is referring to the margin currently held by existing open orders)
            # This function needs to be defined or imported. Assuming it's calculate_total_symbol_margin_contribution from elsewhere.
            # If not defined, this will cause a NameError.
            # For the purpose of fixing indentation, I'll assume it's correctly defined/imported.
            # I'm adding a placeholder import if it's meant to be in pending_orders.py
            # If this function exists outside this file, ensure it's imported at the top.
            from app.services.margin_calculator import calculate_total_symbol_margin_contribution
            
            margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
                db=db,
                redis_client=redis_client,
                user_id=user_id,
                symbol=symbol,
                open_positions_for_symbol=all_open_orders_for_symbol,
                order_model=order_model
            )
            margin_before_recalc = margin_before_recalc_dict["total_margin"]
            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Margin before recalc: {margin_before_recalc}")
            
            # Calculate current overall margin and non-symbol margin
            current_overall_margin = Decimal(str(user_data.get('margin', '0'))) # Use user_data here
            non_symbol_margin = current_overall_margin - margin_before_recalc
            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Current overall margin: {current_overall_margin}, Non-symbol margin: {non_symbol_margin}")
            
            # Calculate remaining orders after *this* pending order opens (not closes)
            # This logic needs adjustment. When a pending order *opens*, its margin is added.
            # The calculation of new_total_margin = current_total_margin_decimal + margin (from the pending order)
            # is what's needed for the check against wallet_balance.
            # The subsequent recalculation of `all_open_orders_for_symbol` should include the *newly opened* order.
            # The current structure of this block seems to be copied from a "closing order" context.
            # I will adjust the logic to reflect *adding* the margin of the triggered order.
            
            # Instead of recalculating based on remaining, we need to add the margin of the *current* triggered order
            # The 'margin' variable already holds the calculated margin for this specific order, potentially hedged.
            new_total_margin_after_open = current_overall_margin + margin # 'margin' is from calculate_single_order_margin
            
            # Update user's margin with the new total
            await update_user_margin(
                db,
                user_id,
                user_type,
                new_total_margin_after_open.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Round for precision
            )
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] User margin updated successfully for user {user_id}: {new_total_margin_after_open} (includes hedging adjustments for this order)")
            
            # Refresh user data in the cache to reflect the updated margin
            try:
                # Get updated user data from the database
                user_model = User if user_type == 'live' else DemoUser
                updated_user = await db.execute(select(user_model).filter(user_model.id == user_id))
                updated_user = updated_user.scalars().first()
                
                if updated_user:
                    # Update the user data cache with fresh data from database
                    user_data_to_cache = {
                        "id": updated_user.id,
                        "email": getattr(updated_user, 'email', None),
                        "group_name": updated_user.group_name,
                        "leverage": updated_user.leverage,
                        "user_type": user_type,
                        "account_number": getattr(updated_user, 'account_number', None),
                        "wallet_balance": updated_user.wallet_balance,
                        "margin": updated_user.margin,  # This contains the new margin value
                        "first_name": getattr(updated_user, 'first_name', None),
                        "last_name": getattr(updated_user, 'last_name', None),
                        "country": getattr(updated_user, 'country', None),
                        "phone_number": getattr(updated_user, 'phone_number', None),
                    }
                    await set_user_data_cache(redis_client, user_id, user_data_to_cache)
                    orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Updated user data cache with new margin value: {updated_user.margin}")
            except Exception as cache_error:
                orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating user data cache: {str(cache_error)}", exc_info=True)
            
            # Publish user data update notification to WebSocket clients using the existing cache function
            await publish_user_data_update(redis_client, user_id)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Published user data update notification for user {user_id}")
        except Exception as margin_update_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating user margin: {str(margin_update_error)}", exc_info=True)
            return
        
        db_order.order_status = 'OPEN'
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Pending order {order_id} for user {user_id} opened successfully. New total margin: {new_total_margin}") # Use new_total_margin_after_open if you want the actual calculated margin after this order

        # Commit DB changes for the order status and updated fields
        try:
            await db.commit()
            await db.refresh(db_order)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Order {order_id} status updated to OPEN in database")
        except Exception as commit_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error committing order status change to database: {str(commit_error)}", exc_info=True)
            return

        try:
            # --- Portfolio Update & Websocket Event ---
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Starting portfolio update for user {user_id}")
            user_data_for_portfolio = await get_user_data_cache(redis_client, user_id, db, user_type) # Re-fetch updated user data
            if user_data_for_portfolio:
                open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
                open_positions_dicts = []
                
                # Convert order objects to dictionaries safely
                for o in open_orders:
                    if hasattr(o, 'to_dict'):
                        open_positions_dicts.append(o.to_dict())
                    else:
                        # Create a dictionary manually if to_dict method is not available
                        order_dict = {
                            'order_id': getattr(o, 'order_id', None),
                            'order_company_name': getattr(o, 'order_company_name', None),
                            'order_type': getattr(o, 'order_type', None),
                            'order_quantity': getattr(o, 'order_quantity', None),
                            'order_price': getattr(o, 'order_price', None),
                            'margin': getattr(o, 'margin', None),
                            'contract_value': getattr(o, 'contract_value', None),
                            'stop_loss': getattr(o, 'stop_loss', None),
                            'take_profit': getattr(o, 'take_profit', None),
                            'commission': getattr(o, 'commission', '0.0'),
                            'order_status': getattr(o, 'order_status', None),
                            'order_user_id': getattr(o, 'order_user_id', None)
                        }
                        open_positions_dicts.append(order_dict)
                
                # Fetch current prices for all open positions to calculate portfolio correctly
                adjusted_market_prices = {}
                if group_symbol_settings: # Ensure group_symbol_settings is not None
                    for symbol_key in group_symbol_settings.keys():
                        prices = await get_last_known_price(redis_client, symbol_key)
                        if prices:
                            adjusted_market_prices[symbol_key] = prices
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Fetched market prices for {len(adjusted_market_prices)} symbols")
                
                portfolio = await calculate_user_portfolio(user_data_for_portfolio, open_positions_dicts, adjusted_market_prices, group_symbol_settings or {}, redis_client)
                await set_user_portfolio_cache(redis_client, user_id, portfolio)
                await publish_account_structure_changed_event(redis_client, user_id)
                orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Portfolio cache updated and websocket event published for user {user_id}")
        except Exception as e:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error updating portfolio cache or publishing websocket event: {str(e)}", exc_info=True)
            # Continue execution - don't return here as the order is already opened

        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Successfully processed triggered pending order {order_id} for user {user_id}. Status set to {db_order.order_status}. Order Type changed from {order_type_original} to {new_order_type}.")
        
        # Remove the order from Redis pending list AFTER successful processing
        # Use the original_order_type for removal as that's how it's stored in Redis
        try:
            await remove_pending_order(redis_client, order_id, symbol, order_type_original, user_id)
            orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Successfully removed order {order_id} from Redis pending orders")
            
            # Notify clients about order execution through websockets
            await publish_order_execution_notification(redis_client, user_id, order_id, symbol, new_order_type, exec_price)
        except Exception as remove_error:
            orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error removing order from Redis: {str(remove_error)}", exc_info=True)

    except Exception as e:
        orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Critical error in trigger_pending_order for order {order.get('order_id', 'N/A')}: {str(e)}", exc_info=True)
        raise

# New function to publish order execution notification
async def publish_order_execution_notification(redis_client: Redis, user_id: str, order_id: str, symbol: str, order_type: str, execution_price: Decimal):
    """
    Publishes a notification that a pending order has been executed to the Redis pub/sub channel.
    This allows websocket clients to be notified of order execution in real-time.
    """
    try:
        from app.core.logging_config import orders_logger
        
        # Create the notification payload
        notification = {
            "event": "pending_order_executed",
            "user_id": user_id,
            "order_id": order_id,
            "symbol": symbol,
            "order_type": order_type,
            "execution_price": str(execution_price),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to the user's channel
        channel = f"user:{user_id}:notifications"
        await redis_client.publish(channel, json.dumps(notification))
        orders_logger.info(f"[PENDING_ORDER_EXECUTION_DEBUG] Published order execution notification for order {order_id} to channel {channel}")
    
    except Exception as e:
        from app.core.logging_config import orders_logger
        orders_logger.error(f"[PENDING_ORDER_EXECUTION_DEBUG] Error publishing order execution notification: {str(e)}", exc_info=True)

async def check_and_trigger_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis
) -> None:
    """
    Continuously checks all open orders to see if stop loss or take profit conditions are met.
    Also checks for pending orders to be triggered based on price conditions.
    This function should be run in a background task or scheduled job.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for SL/TP check")
            return
            
        # Check for pending orders in Redis
        try:
            # List all keys matching the pending_orders pattern
            pending_keys = await redis_client.keys(f"{REDIS_PENDING_ORDERS_PREFIX}:*")
            logger.info(f"Found {len(pending_keys)} pending order keys in Redis")
            
            # Process each pending order key
            for key in pending_keys:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = key  # Already a string
                
                # Get all user IDs for this key
                user_ids = await redis_client.hkeys(key_str)
                logger.info(f"Key: {key_str}, Users: {len(user_ids)}")
                
                # For each user, process their pending orders
                for user_id in user_ids:
                    # Handle both bytes and string user IDs
                    if isinstance(user_id, bytes):
                        user_id_str = user_id.decode('utf-8')
                    else:
                        user_id_str = user_id  # Already a string
                    
                    orders_json = await redis_client.hget(key_str, user_id_str)
                    if orders_json:
                        # Handle both bytes and string JSON
                        if isinstance(orders_json, bytes):
                            orders_json = orders_json.decode('utf-8')
                            
                        orders = json.loads(orders_json)
                        logger.info(f"  User {user_id_str} has {len(orders)} pending orders for {key_str}")
                        
                        # Process each order
                        for order in orders:
                            order_id = order.get('order_id', 'unknown')
                            order_type = order.get('order_type', 'unknown')
                            user_type = order.get('user_type', 'unknown')
                            symbol = order.get('order_company_name', 'unknown')
                            price = order.get('order_price', 'unknown')
                            logger.info(f"    Order {order_id}: {symbol} {order_type}, price={price}, user_type={user_type}")
                            try:
                                # Get current price for the symbol
                                current_price = await get_last_known_price(redis_client, symbol)
                                if current_price:
                                    # Trigger the order if conditions are met
                                    await trigger_pending_order(db, redis_client, order, current_price)
                            except Exception as e:
                                logger.error(f"Error processing pending order {order_id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error checking pending orders in Redis: {e}", exc_info=True)
            
        # Get all open orders from both user and demo user tables
        from app.database.models import UserOrder, DemoUserOrder
        from sqlalchemy.future import select
        from app.crud import crud_order
        
        # First get all live user orders with SL/TP
        live_orders_stmt = select(UserOrder).where(
            (UserOrder.order_status == "OPEN") & 
            ((UserOrder.stop_loss != None) | (UserOrder.take_profit != None))
        )
        live_result = await db.execute(live_orders_stmt)
        live_orders = live_result.scalars().all()
        
        # Then get all demo user orders with SL/TP
        demo_orders_stmt = select(DemoUserOrder).where(
            (DemoUserOrder.order_status == "OPEN") & 
            ((DemoUserOrder.stop_loss != None) | (DemoUserOrder.take_profit != None))
        )
        demo_result = await db.execute(demo_orders_stmt)
        demo_orders = demo_result.scalars().all()
        
        # Process live user orders
        for order in live_orders:
            try:
                # Process the order for SL/TP
                await process_order_stoploss_takeprofit(db, redis_client, order, 'live')
            except Exception as e:
                logger.error(f"Error processing live order {order.order_id} for SL/TP: {e}", exc_info=True)
                continue
        
        # Process demo user orders
        for order in demo_orders:
            try:
                await process_order_stoploss_takeprofit(db, redis_client, order, 'demo')
            except Exception as e:
                logger.error(f"Error processing demo order {order.order_id} for SL/TP: {e}", exc_info=True)
                continue
            
        logger.debug(f"Completed SL/TP check cycle for {len(live_orders)} live orders and {len(demo_orders)} demo orders")
        
    except Exception as e:
        logger.error(f"Error in check_and_trigger_stoploss_takeprofit: {e}", exc_info=True)

async def process_order_stoploss_takeprofit(
    db: AsyncSession,
    redis_client: Redis,
    order,
    user_type: str
) -> None:
    """
    Process a single order to check if stop loss or take profit conditions are met.
    If conditions are met, close the order.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for processing order {order.order_id}")
            return
            
        symbol = order.order_company_name
        order_type = order.order_type
        stop_loss = order.stop_loss
        take_profit = order.take_profit
        
        if not stop_loss and not take_profit:
            return  # No SL/TP to check

        # Get user's group name for adjusted price lookup
        user_data = await get_user_data_cache(redis_client, order.order_user_id, db, user_type)
        if not user_data:
            logger.warning(f"User data not found for user {order.order_user_id}")
            return

        user_group_name = user_data.get('group_name', 'default')
        
        # Get adjusted market prices from Redis cache
        adjusted_prices = await get_adjusted_market_price_cache(redis_client, user_group_name, symbol)
        if not adjusted_prices:
            logger.warning(f"No adjusted prices found for {symbol} when checking SL/TP for order {order.order_id}")
            return

        # Get adjusted buy and sell prices
        adjusted_buy_price = Decimal(str(adjusted_prices.get('buy', '0')))
        adjusted_sell_price = Decimal(str(adjusted_prices.get('sell', '0')))

        if adjusted_buy_price <= 0 or adjusted_sell_price <= 0:
            logger.warning(f"Invalid adjusted price values for {symbol}: buy={adjusted_buy_price}, sell={adjusted_sell_price}")
            return

        # Check stop loss and take profit conditions based on order type
        sl_triggered = False
        tp_triggered = False
        close_price = None
        close_message = None

        if order_type == "BUY":
            # For BUY orders:
            # - Stop loss triggers when adjusted_sell_price matches or goes below stop loss price
            # - Take profit triggers when adjusted_sell_price matches or goes above take profit price
            if stop_loss and adjusted_sell_price <= Decimal(str(stop_loss)):
                sl_triggered = True
                close_price = adjusted_sell_price
                close_message = "Stop loss triggered"
                logger.info(f"Stop loss triggered for BUY order {order.order_id}: adjusted_sell_price {adjusted_sell_price} <= SL {stop_loss}")
            elif take_profit and adjusted_sell_price >= Decimal(str(take_profit)):
                tp_triggered = True
                close_price = adjusted_sell_price
                close_message = "Take profit triggered"
                logger.info(f"Take profit triggered for BUY order {order.order_id}: adjusted_sell_price {adjusted_sell_price} >= TP {take_profit}")
        else:  # SELL order
            # For SELL orders:
            # - Stop loss triggers when adjusted_buy_price matches or goes above stop loss price
            # - Take profit triggers when adjusted_buy_price matches or goes below take profit price
            if stop_loss and adjusted_buy_price >= Decimal(str(stop_loss)):
                sl_triggered = True
                close_price = adjusted_buy_price
                close_message = "Stop loss triggered"
                logger.info(f"Stop loss triggered for SELL order {order.order_id}: adjusted_buy_price {adjusted_buy_price} >= SL {stop_loss}")
            elif take_profit and adjusted_buy_price <= Decimal(str(take_profit)):
                tp_triggered = True
                close_price = adjusted_buy_price
                close_message = "Take profit triggered"
                logger.info(f"Take profit triggered for SELL order {order.order_id}: adjusted_buy_price {adjusted_buy_price} <= TP {take_profit}")

        # If either condition is triggered, close the order
        if sl_triggered or tp_triggered and close_price:
            try:
                # Generate a close_id
                from app.services.order_processing import generate_unique_10_digit_id
                order_model = get_order_model(user_type)
                close_id = await generate_unique_10_digit_id(db, order_model, 'close_id')

                # Calculate profit/loss including commission
                entry_price = Decimal(str(order.order_price))
                quantity = Decimal(str(order.order_quantity))
                commission = Decimal(str(order.commission or '0'))

                # Get group symbol settings for contract size
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, symbol)
                contract_size = Decimal(str(group_symbol_settings.get('contract_size', 100000)))
                profit_currency = group_symbol_settings.get('profit_currency', 'USD')

                # Calculate net profit
                if order_type == "BUY":
                    profit = (close_price - entry_price) * quantity * contract_size - commission
                else:  # SELL
                    profit = (entry_price - close_price) * quantity * contract_size - commission

                # Convert profit to USD if needed
                if profit_currency != 'USD':
                    net_profit = await _convert_to_usd(
                        amount=profit,
                        from_currency=profit_currency,
                        user_id=order.order_user_id,
                        position_id=order.order_id,
                        value_description=f"SL/TP profit conversion for order {order.order_id}",
                        db=db,
                        redis_client=redis_client
                    )
                    logger.info(f"Converted profit from {profit} {profit_currency} to {net_profit} USD for order {order.order_id}")
                else:
                    net_profit = profit

                # Update order status and related fields
                from app.crud.crud_order import update_order_with_tracking
                update_fields = {
                    "order_status": "CLOSED",
                    "close_price": close_price,
                    "close_id": close_id,
                    "net_profit": net_profit,
                    "close_message": close_message
                }

                # Update the order with tracking
                updated_order = await update_order_with_tracking(
                    db=db,
                    db_order=order,
                    update_fields=update_fields,
                    user_id=order.order_user_id,
                    user_type=user_type,
                    action_type=f"AUTO_{'STOPLOSS' if sl_triggered else 'TAKEPROFIT'}_CLOSE"
                )

                # Get the user object
                if user_type == 'live':
                    from app.database.models import User
                    user = await User.by_id(db, order.order_user_id)
                else:
                    from app.database.models import DemoUser
                    user = await DemoUser.by_id(db, order.order_user_id)

                if not user:
                    logger.error(f"User {order.order_user_id} not found when closing order {order.order_id}")
                    return

                try:
                    # Get all open orders for the symbol to recalculate margin
                    all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                        db=db, user_id=order.order_user_id, symbol=symbol, order_model=order_model
                    )
                    
                    # Calculate margin before closing this order
                    margin_before_recalc_dict = await calculate_total_symbol_margin_contribution(
                        db=db,
                        redis_client=redis_client,
                        user_id=order.order_user_id,
                        symbol=symbol,
                        open_positions_for_symbol=all_open_orders_for_symbol,
                        order_model=order_model
                    )
                    margin_before_recalc = margin_before_recalc_dict["total_margin"]
                    logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Margin before recalc: {margin_before_recalc}")
                    
                    # Calculate current overall margin and non-symbol margin
                    current_overall_margin = Decimal(str(user.margin))
                    non_symbol_margin = current_overall_margin - margin_before_recalc
                    logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Current overall margin: {current_overall_margin}, Non-symbol margin: {non_symbol_margin}")
                    
                    # Calculate remaining orders after closing this one
                    remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order.order_id]
                    logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Remaining orders count: {len(remaining_orders_for_symbol_after_close)}")
                    
                    margin_after_symbol_recalc_dict = await calculate_total_symbol_margin_contribution(
                        db=db,
                        redis_client=redis_client,
                        user_id=order.order_user_id,
                        symbol=symbol,
                        open_positions_for_symbol=remaining_orders_for_symbol_after_close,
                        order_model=order_model
                    )
                    margin_after_symbol_recalc = margin_after_symbol_recalc_dict["total_margin"]
                    logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Margin after symbol recalc: {margin_after_symbol_recalc}")
                    
                    # Start a nested transaction to ensure atomic updates
                    async with db.begin_nested() as nested:
                        try:
                            # Update user's margin considering hedging
                            old_margin = user.margin
                            old_wallet_balance = user.wallet_balance
                            
                            # Calculate new margin - this is the key change
                            # We need to set the margin to the recalculated margin for remaining orders
                            user.margin = max(Decimal('0'), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                            
                            # Update user's wallet balance with net profit
                            user.wallet_balance = (old_wallet_balance + net_profit - (order.swap or Decimal('0'))).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
                            
                            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Updated user margin from {old_margin} to {user.margin}")
                            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Updated wallet balance from {old_wallet_balance} to {user.wallet_balance}")
                            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Net profit: {net_profit}, Swap: {order.swap or Decimal('0')}")
                            
                            # Update order status and related fields
                            update_fields = {
                                "order_status": "CLOSED",
                                "close_price": close_price,
                                "close_id": close_id,
                                "net_profit": net_profit,
                                "close_message": close_message,
                                "commission": order.commission,  # Preserve the commission
                                "swap": order.swap  # Preserve the swap
                            }

                            # Update the order with tracking
                            updated_order = await crud_order.update_order_with_tracking(
                                db=db,
                                db_order=order,
                                update_fields=update_fields,
                                user_id=order.order_user_id,
                                user_type=user_type,
                                action_type=f"AUTO_{'STOPLOSS' if sl_triggered else 'TAKEPROFIT'}_CLOSE"
                            )

                            # Create wallet transactions
                            from app.crud.wallet import create_wallet_transaction
                            
                            # Common data for wallet transactions
                            transaction_time = datetime.now(timezone.utc)
                            wallet_common_data = {
                                "symbol": symbol,
                                "order_quantity": order.order_quantity,
                                "is_approved": 1,
                                "order_type": order.order_type,
                                "transaction_time": transaction_time,
                                "order_id": order.order_id
                            }
                            
                            # Add user ID based on type
                            if user_type == 'demo':
                                wallet_common_data["demo_user_id"] = order.order_user_id
                            else:
                                wallet_common_data["user_id"] = order.order_user_id

                            # Create P/L transaction if there's profit/loss
                            if net_profit != Decimal("0.0"):
                                await create_wallet_transaction(
                                    db=db,
                                    user_id=order.order_user_id,
                                    amount=net_profit,
                                    transaction_type="Profit/Loss",
                                    order_id=order.order_id,
                                    description=f"P/L for closing order {order.order_id} - {close_message}",
                                    user_type=user_type,
                                    **wallet_common_data
                                )

                            # Create commission transaction if there's commission
                            if order.commission and order.commission > Decimal("0.0"):
                                await create_wallet_transaction(
                                    db=db,
                                    user_id=order.order_user_id,
                                    amount=-order.commission,
                                    transaction_type="Commission",
                                    order_id=order.order_id,
                                    description=f"Commission for closing order {order.order_id}",
                                    user_type=user_type,
                                    **wallet_common_data
                                )

                            # Create swap transaction if there's swap
                            if order.swap and order.swap != Decimal("0.0"):
                                await create_wallet_transaction(
                                    db=db,
                                    user_id=order.order_user_id,
                                    amount=-order.swap,
                                    transaction_type="Swap",
                                    order_id=order.order_id,
                                    description=f"Swap for closing order {order.order_id}",
                                    user_type=user_type,
                                    **wallet_common_data
                                )
                            
                            # If all operations succeed, commit the nested transaction
                            await nested.commit()
                            logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Successfully committed all changes in nested transaction")
                        except Exception as transaction_error:
                            # If any operation fails, roll back the nested transaction
                            await nested.rollback()
                            logger.error(f"[MARGIN_DEBUG] Order {order.order_id}: Error in nested transaction, rolling back: {str(transaction_error)}", exc_info=True)
                            raise

                    # Commit the outer transaction
                    await db.commit()
                    logger.info(f"[MARGIN_DEBUG] Order {order.order_id}: Successfully committed outer transaction")

                    # Update caches and publish events
                    await update_user_static_orders(order.order_user_id, db, redis_client, user_type)
                    await publish_order_update(redis_client, order.order_user_id)
                    await publish_user_data_update(redis_client, order.order_user_id)

                    logger.info(f"Successfully closed order {order.order_id} due to {close_message}")

                except Exception as close_error:
                    logger.error(f"Error closing order {order.order_id}: {str(close_error)}", exc_info=True)

            except Exception as e:
                logger.error(f"Error processing SL/TP for order {order.order_id}: {str(e)}", exc_info=True)

    except Exception as e:
        logger.error(f"Error processing SL/TP for order {order.order_id}: {str(e)}", exc_info=True)

async def get_last_known_price(redis_client: Redis, symbol: str) -> dict:
    """
    Get the last known price for a symbol from Redis.
    Returns a dictionary with bid (b) and ask (a) prices.
    """
    try:
        if not redis_client:
            logger.warning("Redis client not available for getting last known price")
            return None
            
        # Try to get the price from Redis
        price_key = f"market_data:{symbol}"
        price_data = await redis_client.get(price_key)
        
        if not price_data:
            logger.warning(f"No price data found for {symbol}")
            return None
            
        try:
            price_dict = json.loads(price_data)
            return price_dict
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in price data for {symbol}: {price_data}")
            return None
    except Exception as e:
        logger.error(f"Error getting last known price for {symbol}: {e}", exc_info=True)
        return None

async def get_user_data_cache(redis_client: Redis, user_id: int, db: AsyncSession, user_type: str = 'live') -> dict:
    """
    Get user data from cache or database.
    """
    try:
        if not redis_client:
            logger.warning(f"Redis client not available for getting user data for user {user_id}")
            # Fallback to database
            from app.crud.user import get_user_by_id, get_demo_user_by_id
            
            if user_type == 'live':
                user = await get_user_by_id(db, user_id, user_type=user_type)
            else:
                user = await get_demo_user_by_id(db, user_id)
                
            if not user:
                return {}
                
            return {
                "id": user.id,
                "group_name": getattr(user, 'group_name', None),
                "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
                "margin": str(user.margin) if hasattr(user, 'margin') else "0"
            }
        
        # Try to get from cache
        user_key = f"user_data:{user_type}:{user_id}"
        user_data = await redis_client.get(user_key)
        
        if user_data:
            try:
                return json.loads(user_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in user data for user {user_id}: {user_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.user import get_user_by_id, get_demo_user_by_id
        
        if user_type == 'live':
            user = await get_user_by_id(db, user_id, user_type=user_type)
        else:
            user = await get_demo_user_by_id(db, user_id)
            
        if not user:
            return {}
            
        user_data = {
            "id": user.id,
            "group_name": getattr(user, 'group_name', None),
            "wallet_balance": str(user.wallet_balance) if hasattr(user, 'wallet_balance') else "0",
            "margin": str(user.margin) if hasattr(user, 'margin') else "0"
        }
        
        # Cache the user data
        try:
            await redis_client.set(user_key, json.dumps(user_data), ex=300)  # 5 minutes expiry
        except Exception as e:
            logger.error(f"Error caching user data for user {user_id}: {e}", exc_info=True)
        
        return user_data
    except Exception as e:
        logger.error(f"Error getting user data for user {user_id}: {e}", exc_info=True)
        return {}

async def get_group_settings_cache(redis_client: Redis, group_name: str) -> dict:
    """
    Get group settings from cache or database.
    """
    try:
        if not group_name:
            return {}
            
        if not redis_client:
            logger.warning(f"Redis client not available for getting group settings for group {group_name}")
            # Fallback to database
            from app.crud.group import get_group_by_name
            from sqlalchemy.ext.asyncio import AsyncSession
            from app.database.session import AsyncSessionLocal
            
            async with AsyncSessionLocal() as db:
                group = await get_group_by_name(db, group_name)
                if not group:
                    return {}
                
                # Handle case where get_group_by_name returns a list
                group_obj = group[0] if isinstance(group, list) else group
                    
                return {
                    "id": group_obj.id,
                    "name": group_obj.name,
                    "sending_orders": getattr(group_obj, 'sending_orders', None)
                }
        
        # Try to get from cache
        group_key = f"group_settings:{group_name}"
        group_data = await redis_client.get(group_key)
        
        if group_data:
            try:
                return json.loads(group_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in group data for group {group_name}: {group_data}")
                return {}
        
        # Fallback to database if not in cache
        from app.crud.group import get_group_by_name
        from sqlalchemy.ext.asyncio import AsyncSession
        from app.database.session import AsyncSessionLocal
        
        async with AsyncSessionLocal() as db:
            group = await get_group_by_name(db, group_name)
            if not group:
                return {}
            
            # Handle case where get_group_by_name returns a list
            group_obj = group[0] if isinstance(group, list) else group
                
            group_data = {
                "id": group_obj.id,
                "name": group_obj.name,
                "sending_orders": getattr(group_obj, 'sending_orders', None)
            }
            
            # Cache the group data
            try:
                await redis_client.set(group_key, json.dumps(group_data), ex=300)  # 5 minutes expiry
            except Exception as e:
                logger.error(f"Error caching group data for group {group_name}: {e}", exc_info=True)
            
            return group_data
    except Exception as e:
        logger.error(f"Error getting group settings for group {group_name}: {e}", exc_info=True)
        return {}

async def update_user_static_orders(user_id: int, db: AsyncSession, redis_client: Redis, user_type: str):
    """
    Update the static orders cache for a user after order changes.
    This includes both open and pending orders.
    Always fetches fresh data from the database to ensure the cache is up-to-date.
    """
    try:
        logger.info(f"Starting update_user_static_orders for user {user_id}, user_type {user_type}")
        order_model = get_order_model(user_type)
        logger.info(f"Using order model: {order_model.__name__}")
        
        # Get open orders - always fetch from database to ensure fresh data
        open_orders_orm = await crud_order.get_all_open_orders_by_user_id(db, user_id, order_model)
        logger.info(f"Fetched {len(open_orders_orm)} open orders for user {user_id}")
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
        logger.info(f"Fetched {len(pending_orders_orm)} pending orders for user {user_id}")
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
            "updated_at": datetime.now().isoformat()
        }
        await set_user_static_orders_cache(redis_client, user_id, static_orders_data)
        logger.info(f"Updated static orders cache for user {user_id} with {len(open_orders_data)} open orders and {len(pending_orders_data)} pending orders")
        
        return static_orders_data
    except Exception as e:
        logger.error(f"Error updating static orders cache for user {user_id}: {e}", exc_info=True)
        return {"open_orders": [], "pending_orders": [], "updated_at": datetime.now().isoformat()}

        