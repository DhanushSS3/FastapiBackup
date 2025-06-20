# app/services/margin_calculator.py

# IMPORTANT: Correct formulas for margin calculation:
# 1. Contract value = contract_size * order_quantity (without price)
# 2. Margin = (contract_value * order_price) / user_leverage
# 3. Convert margin to USD if profit_currency != "USD"
#
# The contract_size and profit_currency are obtained from ExternalSymbolInfo table

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import logging
from typing import Optional, Tuple, Dict, Any, List
import json

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis.asyncio import Redis
from app.core.cache import get_adjusted_market_price_cache

from app.database.models import User, Group, ExternalSymbolInfo
from app.core.cache import (
    get_user_data_cache,
    set_user_data_cache,
    get_group_symbol_settings_cache,
    set_group_symbol_settings_cache,
    DecimalEncoder,
    get_live_adjusted_buy_price_for_pair,
    get_live_adjusted_sell_price_for_pair,
    get_adjusted_market_price_cache
)
from app.firebase_stream import get_latest_market_data
from app.crud.crud_symbol import get_symbol_type
from app.services.portfolio_calculator import _convert_to_usd, _calculate_adjusted_prices_from_raw
from app.core.logging_config import orders_logger

logger = logging.getLogger(__name__)

# --- HELPER FUNCTION: Calculate Base Margin Per Lot (Used in Hedging) ---
# This helper calculates a per-lot value based on Group margin setting, price, and leverage.
# This is used in the hedging logic in order_processing.py for comparison.
async def calculate_base_margin_per_lot(
    redis_client: Redis,
    user_id: int,
    symbol: str,
    price: Decimal,
    db: AsyncSession = None,
    user_type: str = 'live'
) -> Optional[Decimal]:
    """
    Calculates a base margin value per standard lot for a given symbol and price,
    considering user's group settings and leverage.
    This value is used for comparison in hedging calculations.
    Returns the base margin value per lot or None if calculation fails.
    """
    # Retrieve user data from cache to get group_name and leverage
    user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
    if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
        orders_logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
        return None

    group_name = user_data['group_name']
    # Ensure user_leverage is Decimal
    user_leverage_raw = user_data.get('leverage', 1)
    user_leverage = Decimal(str(user_leverage_raw)) if user_leverage_raw is not None else Decimal(1)


    # Retrieve group-symbol settings from cache
    # Need settings for the specific symbol
    group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
    # We need the 'margin' setting from the group for this calculation
    if not group_symbol_settings or 'margin' not in group_symbol_settings:
        orders_logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
        return None

    # Ensure margin_setting is Decimal
    margin_setting_raw = group_symbol_settings.get('margin', 0)
    margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


    if user_leverage <= 0:
         orders_logger.error(f"User leverage is zero or negative for user {user_id}.")
         return None

    # Calculation based on Group Base Margin Setting, Price, and Leverage
    # This formula seems to be the one needed for the per-lot comparison in hedging.
    try:
        # Ensure price is Decimal
        price_decimal = Decimal(str(price))
        base_margin_per_lot = (margin_setting * price_decimal) / user_leverage
        orders_logger.debug(f"Calculated base margin per lot (for hedging) for user {user_id}, symbol {symbol}, price {price}: {base_margin_per_lot}")
        return base_margin_per_lot
    except Exception as e:
        orders_logger.error(f"Error calculating base margin per lot (for hedging) for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None

async def calculate_single_order_margin(
    redis_client: Redis,
    symbol: str,
    order_type: str,
    quantity: Decimal,
    user_leverage: Decimal,
    group_settings: Dict[str, Any],
    external_symbol_info: Dict[str, Any],
    raw_market_data: Dict[str, Any],
    db: AsyncSession = None,
    user_id: int = None,
    order_price: Decimal = None
) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    """
    Calculate margin for a single order based on order details and market data.
    
    Args:
        redis_client: Redis client
        symbol: Trading symbol
        order_type: Type of order (BUY/SELL)
        quantity: Order quantity
        user_leverage: User's leverage
        group_settings: Group settings for the symbol
        external_symbol_info: External symbol information
        raw_market_data: Raw market data
        db: Database session (optional, needed for currency conversion)
        user_id: User ID (optional, needed for currency conversion)
        order_price: Optional order price to use for margin calculation
        
    Returns:
        Tuple of (margin, price, contract_value, commission)
    """
    try:
        orders_logger.info(f"[MARGIN_CALC] Starting margin calculation for {symbol} {order_type} order, quantity: {quantity}")
        
        # Set default group_name if it's not in the group_settings
        if 'group_name' not in group_settings and user_id is not None:
            # Try to get user data to find group_name
            orders_logger.info(f"[MARGIN_CALC] No group_name in settings, fetching for user {user_id}")
            try:
                from app.crud.user import get_user_by_id
                user = await get_user_by_id(db, user_id, user_type='live')
                if user and user.group_name:
                    group_settings['group_name'] = user.group_name
                    orders_logger.info(f"[MARGIN_CALC] Got group_name from database: {user.group_name}")
            except Exception as e:
                orders_logger.error(f"[MARGIN_CALC] Error getting user group name: {e}")
        
        # Get contract size from external symbol info
        contract_size_raw = external_symbol_info.get('contract_size', 100000)
        contract_size = Decimal(str(contract_size_raw))
        orders_logger.info(f"[MARGIN_CALC] Contract size for {symbol}: {contract_size} (raw: {contract_size_raw}, type: {type(contract_size_raw)})")
        
        # Use order_price if provided, otherwise get appropriate price based on order type
        price = None
        if order_price is not None:
            price = order_price
            orders_logger.info(f"[MARGIN_CALC] Using provided order price for {symbol}: {price}")
        else:
            # Get user_group_name from the group_settings
            user_group_name = group_settings.get('group_name', 'default')
            
            if order_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                # For buy orders, use the ask price
                try:
                    price_data = await get_live_adjusted_buy_price_for_pair(redis_client, symbol, user_group_name)
                    if price_data:
                        price = Decimal(str(price_data))
                        orders_logger.info(f"[MARGIN_CALC] Got BUY price for {symbol} from cache: {price}")
                except Exception as e:
                    orders_logger.error(f"[MARGIN_CALC] Error getting BUY price from cache: {e}")
            elif order_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
                # For sell orders, use the bid price
                try:
                    price_data = await get_live_adjusted_sell_price_for_pair(redis_client, symbol, user_group_name)
                    if price_data:
                        price = Decimal(str(price_data))
                        orders_logger.info(f"[MARGIN_CALC] Got SELL price for {symbol} from cache: {price}")
                except Exception as e:
                    orders_logger.error(f"[MARGIN_CALC] Error getting SELL price from cache: {e}")
            
            # If we couldn't get a price from the cache, try to get it from raw market data
            if price is None:
                orders_logger.info(f"[MARGIN_CALC] No price in cache for {symbol}, checking raw market data")
                if symbol in raw_market_data:
                    symbol_data = raw_market_data[symbol]
                    orders_logger.info(f"[MARGIN_CALC] Raw market data for {symbol}: {symbol_data}")
                    if order_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                        price_raw = symbol_data.get('ask', '0')
                        price = Decimal(str(price_raw))
                        orders_logger.info(f"[MARGIN_CALC] Using raw ask price for {symbol}: {price} (raw: {price_raw})")
                    else:
                        price_raw = symbol_data.get('bid', '0')
                        price = Decimal(str(price_raw))
                        orders_logger.info(f"[MARGIN_CALC] Using raw bid price for {symbol}: {price} (raw: {price_raw})")
                
                # If we still don't have a price, try last known price from cache
                if price is None or price == Decimal('0'):
                    try:
                        orders_logger.warning(f"[MARGIN_CALC] No price from market data for {symbol}, trying last known price")
                        last_price = await get_last_known_price(redis_client, symbol)
                        if last_price:
                            if order_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                                price_raw = last_price.get('o', last_price.get('ask', '0'))
                            else:
                                price_raw = last_price.get('b', last_price.get('bid', '0'))
                            
                            price = Decimal(str(price_raw))
                            orders_logger.info(f"[MARGIN_CALC] Using last known price for {symbol}: {price}")
                    except Exception as e:
                        orders_logger.error(f"[MARGIN_CALC] Error getting last known price: {e}")
                
                # If we still don't have a price, log an error and return zeros
                if price is None or price == Decimal('0'):
                    orders_logger.error(f"[MARGIN_CALC] Could not get any price for {symbol} {order_type} order")
                    return None, None, None, None
        
        # Calculate contract value using the CORRECT formula
        # Contract value = contract_size * quantity (without price)
        contract_value = contract_size * quantity
        
        orders_logger.info(f"[MARGIN_CALC] Contract value calculation for {symbol}:")
        orders_logger.info(f"[MARGIN_CALC] Contract value = contract_size * quantity = {contract_size} * {quantity} = {contract_value}")
        
        # Calculate margin using the CORRECT formula
        # Margin = (contract_value * price) / user_leverage
        # Ensure user_leverage is at least 1 to avoid division by zero
        effective_leverage = max(user_leverage, Decimal('1'))
        # For live users, leverage is typically 100 or 200
        if user_leverage <= Decimal('1') and user_id is not None:
            # Try to get user data to find proper leverage
            try:
                from app.crud.user import get_user_by_id
                user = await get_user_by_id(db, user_id, user_type='live')
                if user and user.leverage and user.leverage > Decimal('1'):
                    effective_leverage = user.leverage
                    orders_logger.info(f"[MARGIN_CALC] Using user's actual leverage from DB: {effective_leverage}")
                else:
                    # Default to 100 if we can't find a proper leverage
                    effective_leverage = Decimal('100')
                    orders_logger.info(f"[MARGIN_CALC] Using default leverage: {effective_leverage}")
            except Exception as e:
                orders_logger.error(f"[MARGIN_CALC] Error getting user leverage: {e}")
                effective_leverage = Decimal('100')  # Default to 100
                
        margin_raw = (contract_value * price) / effective_leverage
        margin = margin_raw.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        orders_logger.info(f"[MARGIN_CALC] Margin calculation: (contract_value * price) / effective_leverage = ({contract_value} * {price}) / {effective_leverage} = {margin_raw} (rounded to {margin})")
        
        # Calculate commission
        commission = Decimal('0.0')
        commission_type = int(group_settings.get('commision_type', 0))
        commission_value_type = int(group_settings.get('commision_value_type', 0))
        commission_rate = Decimal(str(group_settings.get('commision', '0.0')))
        
        orders_logger.info(f"[MARGIN_CALC] Commission settings for {symbol}: type={commission_type}, value_type={commission_value_type}, rate={commission_rate}")
        
        if commission_type in [0, 1]:  # "Every Trade" or "In"
            if commission_value_type == 0:  # Per lot
                commission = quantity * commission_rate
                orders_logger.info(f"[MARGIN_CALC] Per lot commission: {quantity} * {commission_rate} = {commission}")
            elif commission_value_type == 1:  # Percent of price
                commission = (commission_rate / Decimal('100')) * contract_value * price
                orders_logger.info(f"[MARGIN_CALC] Percent commission: ({commission_rate}/100) * {contract_value} * {price} = {commission}")
        
        commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        # Convert margin to USD if profit_currency is not USD
        profit_currency = external_symbol_info.get('profit_currency', 'USD')  # Get profit currency from external_symbol_info
        orders_logger.info(f"[MARGIN_CALC] Profit currency for {symbol}: {profit_currency}")
        
        if profit_currency != 'USD' and db is not None and user_id is not None:
            # Use position_id as empty string since we don't have one yet
            position_id = ""
            value_description = f"margin for {symbol} {order_type} order"
            
            orders_logger.info(f"[MARGIN_CALC] Converting margin from {profit_currency} to USD: {margin} {profit_currency}")
            
            # Convert margin to USD
            margin_usd = await _convert_to_usd(
                margin, 
                profit_currency, 
                user_id, 
                position_id, 
                value_description, 
                db, 
                redis_client
            )
            
            orders_logger.info(f"[MARGIN_CALC] Margin after USD conversion: {margin} {profit_currency} -> {margin_usd} USD")
            margin = margin_usd

        orders_logger.info(f"[MARGIN_CALC] Final results for {symbol} {order_type}: margin={margin}, price={price}, contract_value={contract_value}, commission={commission}")
        return margin, price, contract_value, commission
    
    except Exception as e:
        orders_logger.error(f"[MARGIN_CALC] Error calculating margin for {symbol}: {e}", exc_info=True)
        return None, None, None, None

async def get_external_symbol_info(db: AsyncSession, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Get external symbol info from the database.
    """
    try:
        from sqlalchemy.future import select
        from app.database.models import ExternalSymbolInfo
        
        stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(symbol))
        result = await db.execute(stmt)
        symbol_info = result.scalars().first()
        
        if symbol_info:
            orders_logger.info(f"[SYMBOL_INFO] Retrieved external symbol info for {symbol}: contract_size={symbol_info.contract_size}, profit_currency={symbol_info.profit}, digit={symbol_info.digit}")
            return {
                'contract_size': symbol_info.contract_size,
                'profit_currency': symbol_info.profit,
                'digit': symbol_info.digit
            }
        orders_logger.error(f"[SYMBOL_INFO] No external symbol info found for {symbol}")
        return None
    except Exception as e:
        orders_logger.error(f"[SYMBOL_INFO] Error getting external symbol info for {symbol}: {e}", exc_info=True)
        return None

from app.core.cache import get_last_known_price
from app.firebase_stream import get_latest_market_data

def calculate_pending_order_margin(
    order_type: str,
    order_quantity: Decimal,
    order_price: Decimal,
    symbol_settings: Dict[str, Any],
    user_leverage: Decimal = None
) -> Decimal:
    """
    Calculate margin for a pending order based on order details and symbol settings.
    This simplified version is used for pending order processing.
    Returns the calculated margin.
    """
    try:
        # Get required settings from symbol_settings
        contract_size_raw = symbol_settings.get('contract_size', 100000)
        contract_size = Decimal(str(contract_size_raw))
        
        # Get leverage - use explicitly provided user_leverage if available,
        # otherwise get from symbol_settings with a safer default
        if user_leverage is not None:
            leverage = Decimal(str(user_leverage))
            orders_logger.info(f"[PENDING_MARGIN_CALC] Using provided user leverage: {leverage}")
        else:
            leverage_raw = symbol_settings.get('leverage', 100)  # Use more reasonable default leverage
            leverage = Decimal(str(leverage_raw))
            orders_logger.info(f"[PENDING_MARGIN_CALC] Using symbol settings leverage: {leverage} (raw: {leverage_raw})")
        
        # Make sure leverage is reasonable (typically 100-500 for forex)
        # If it's too small (like 1), it would make margin requirements excessive
        if leverage < Decimal('10'):
            orders_logger.warning(f"[PENDING_MARGIN_CALC] Leverage very low ({leverage}), using default 100")
            leverage = Decimal('100')
        
        orders_logger.info(f"[PENDING_MARGIN_CALC] Calculating margin for pending {order_type} order: quantity={order_quantity}, price={order_price}")
        orders_logger.info(f"[PENDING_MARGIN_CALC] Settings: contract_size={contract_size} (raw: {contract_size_raw}), leverage={leverage}")
        
        # Calculate contract value using the CORRECT formula
        contract_value = contract_size * order_quantity
        orders_logger.info(f"[PENDING_MARGIN_CALC] Contract value = contract_size * order_quantity = {contract_size} * {order_quantity} = {contract_value}")
        
        # Calculate margin using the CORRECT formula
        margin_raw = (contract_value * order_price) / leverage
        margin = margin_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        orders_logger.info(f"[PENDING_MARGIN_CALC] Margin = (contract_value * order_price) / leverage = ({contract_value} * {order_price}) / {leverage} = {margin_raw} (rounded to {margin})")
        
        return margin
    except Exception as e:
        orders_logger.error(f"[PENDING_MARGIN_CALC] Error calculating margin for pending order: {e}", exc_info=True)
        return Decimal('0')

async def calculate_total_symbol_margin_contribution(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_positions_for_symbol: list,
    user_type: str,
    order_model=None
) -> Dict[str, Any]:
    """
    Calculate total margin contribution for a symbol considering hedged positions.
    Returns a dictionary with total_margin and other details.
    """
    try:
        total_buy_quantity = Decimal('0.0')
        total_sell_quantity = Decimal('0.0')
        all_margins_per_lot: List[Decimal] = []

        orders_logger.info(f"[MARGIN_CONTRIB] Calculating total margin contribution for user {user_id}, symbol {symbol}, positions: {len(open_positions_for_symbol)}")

        # Get user data for leverage
        user_data = await get_user_data_cache(redis_client, user_id, db, user_type)
        if not user_data:
            orders_logger.error(f"[MARGIN_CONTRIB] User data not found for user {user_id}")
            return {"total_margin": Decimal('0.0')}

        user_leverage_raw = user_data.get('leverage', '1.0')
        user_leverage = Decimal(str(user_leverage_raw))
        orders_logger.info(f"[MARGIN_CONTRIB] User leverage: {user_leverage} (raw: {user_leverage_raw})")
        
        if user_leverage <= 0:
            orders_logger.error(f"[MARGIN_CONTRIB] Invalid leverage for user {user_id}: {user_leverage}")
            return {"total_margin": Decimal('0.0')}

        # Get group settings for margin calculation
        group_name = user_data.get('group_name')
        group_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
        if not group_settings:
            orders_logger.error(f"[MARGIN_CONTRIB] Group settings not found for symbol {symbol}")
            return {"total_margin": Decimal('0.0')}

        # Get external symbol info
        external_symbol_info = await get_external_symbol_info(db, symbol)
        if not external_symbol_info:
            orders_logger.error(f"[MARGIN_CONTRIB] External symbol info not found for {symbol}")
            return {"total_margin": Decimal('0.0')}

        # Get raw market data for price calculations
        raw_market_data = get_latest_market_data()
        if not raw_market_data:
            orders_logger.error("[MARGIN_CONTRIB] Failed to get market data")
            return {"total_margin": Decimal('0.0')}

        # Process each position
        for i, position in enumerate(open_positions_for_symbol):
            try:
                position_quantity_raw = position.order_quantity
                position_quantity = Decimal(str(position_quantity_raw))
                position_type = position.order_type.upper()
                position_margin_raw = position.margin
                position_margin = Decimal(str(position_margin_raw))

                orders_logger.info(f"[MARGIN_CONTRIB] Position {i+1}: type={position_type}, quantity={position_quantity} (raw: {position_quantity_raw}), margin={position_margin} (raw: {position_margin_raw})")

                if position_quantity > 0:
                    # Calculate margin per lot for this position
                    margin_per_lot_raw = position_margin / position_quantity
                    margin_per_lot = margin_per_lot_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
                    all_margins_per_lot.append(margin_per_lot)
                    orders_logger.info(f"[MARGIN_CONTRIB] Position {i+1} margin per lot: {position_margin} / {position_quantity} = {margin_per_lot_raw} (rounded to {margin_per_lot})")

                    # Add to total quantities
                    if position_type in ['BUY', 'BUY_LIMIT', 'BUY_STOP']:
                        total_buy_quantity += position_quantity
                    elif position_type in ['SELL', 'SELL_LIMIT', 'SELL_STOP']:
                        total_sell_quantity += position_quantity
            except Exception as e:
                orders_logger.error(f"[MARGIN_CONTRIB] Error processing position {i+1}: {e}", exc_info=True)
                continue

        # Calculate net quantity (for hedged positions)
        net_quantity = max(total_buy_quantity, total_sell_quantity)
        orders_logger.info(f"[MARGIN_CONTRIB] Total buy quantity: {total_buy_quantity}, Total sell quantity: {total_sell_quantity}, Net quantity: {net_quantity}")
        
        # Get the highest margin per lot (for hedged positions)
        highest_margin_per_lot = max(all_margins_per_lot) if all_margins_per_lot else Decimal('0.0')
        orders_logger.info(f"[MARGIN_CONTRIB] All margins per lot: {all_margins_per_lot}, Highest margin per lot: {highest_margin_per_lot}")

        # Calculate total margin contribution
        total_margin_raw = highest_margin_per_lot * net_quantity
        total_margin = total_margin_raw.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        orders_logger.info(f"[MARGIN_CONTRIB] Total margin calculation: {highest_margin_per_lot} * {net_quantity} = {total_margin_raw} (rounded to {total_margin})")

        # Return the result
        return {"total_margin": total_margin, "net_quantity": net_quantity}

    except Exception as e:
        orders_logger.error(f"[MARGIN_CONTRIB] Error calculating total symbol margin contribution: {e}", exc_info=True)
        return {"total_margin": Decimal('0.0')}