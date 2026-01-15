
import logging
from typing import Dict, Optional, Tuple

from config.trading_config import (
    MAX_TRADE_SIZE_USD, MAX_POSITION_USD, 
    MAX_ACCOUNT_DRAWDOWN, INITIAL_CAPITAL, MAX_SPREAD_USD
)

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Enforces trading limits and safety checks.
    """
    
    def __init__(self, exchange_instance):
        self.exchange = exchange_instance
        self.max_drawdown = MAX_ACCOUNT_DRAWDOWN
        
    def validate_order(self, market_id: str, side: str, size_usd: float, snapshot: Dict) -> Tuple[bool, Optional[str]]:
        """
        Checks if an order violates any risk rules.
        Returns (True, None) if valid, (False, Reason) if rejected.
        """
        state = self.exchange.get_portfolio_state()
        equity = state['equity']
        
        # 1. Circuit Breaker: Max Drawdown
        # If equity drops below (1 - Limit) * Start
        drawdown_threshold = INITIAL_CAPITAL * (1 - self.max_drawdown)
        if equity < drawdown_threshold:
            return False, f"Account Drawdown Triggered (${equity:.2f} < ${drawdown_threshold:.2f})"
            
        # 2. Max Trade Size
        if size_usd > MAX_TRADE_SIZE_USD:
            return False, f"Size ${size_usd} > Max ${MAX_TRADE_SIZE_USD}"
            
        # 3. Max Position Exposure
        # If we are adding to a position, check new total
        current_pos = self.exchange.positions.get(market_id, {}).get('current_value', 0.0)
        
        # Assuming we are adding exposure (Long buy or Short sell)
        # Approximate new exposure
        new_exposure = abs(current_pos) + size_usd
        if new_exposure > MAX_POSITION_USD:
            return False, f"New Exposure ${new_exposure:.2f} > Limit ${MAX_POSITION_USD}"
            
        # 4. Spread Check (Liquidity Quality)
        ask = snapshot.get('ask_price_1')
        bid = snapshot.get('bid_price_1')
        if ask and bid:
            spread = ask - bid
            if spread > MAX_SPREAD_USD:
                return False, f"Spread ${spread:.2f} too wide (Max ${MAX_SPREAD_USD})"
        else:
             # If pricing missing, reject safety
             return False, "Market Data Incomplete (Missing Bid/Ask)"

        # 5. Cash Check (only for buys)
        if side == 'BUY' and state['cash'] < size_usd:
            return False, f"Insufficient Cash (${state['cash']:.2f})"
            
        return True, None
