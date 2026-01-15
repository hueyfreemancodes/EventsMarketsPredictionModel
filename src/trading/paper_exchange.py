
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config.trading_config import INITIAL_CAPITAL, MAX_POSITION_USD

logger = logging.getLogger(__name__)

import csv
import os

class PaperExchange:
    """
    Simulates a matching engine and portfolio tracker.
    Executes orders against supplied Market Snapshots.
    """
    
    def __init__(self, initial_capital: float = INITIAL_CAPITAL):
        self.cash = initial_capital
        self.positions: Dict[str, Dict] = {} # market_id -> {shares, avg_entry, current_value}
        self.trades: List[Dict] = []
        self.equity_history: List[Dict] = []
        
        # Persistence
        self.trade_log_path = "data/paper_trades.csv"
        self._init_trade_log()
        
    def _init_trade_log(self):
        """Initialize CSV with headers if not exists."""
        os.makedirs(os.path.dirname(self.trade_log_path), exist_ok=True)
        if not os.path.exists(self.trade_log_path):
            with open(self.trade_log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'market_id', 'side', 'price', 'shares', 'value', 'limit_price', 'liquidity_avail'])

    def _persist_trade(self, trade: Dict):
        """Append trade to CSV."""
        try:
            with open(self.trade_log_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    trade['timestamp'], trade['market_id'], trade['side'], 
                    trade['price'], trade['shares'], trade['value'], 
                    trade.get('limit_price'), trade.get('liquidity_avail')
                ])
        except Exception as e:
            logger.error(f"Failed to persist trade: {e}")
        
    def get_portfolio_state(self) -> Dict:
        """Returns current Equity, Cash, and Open Positions."""
        positions_value = sum(p['current_value'] for p in self.positions.values())
        equity = self.cash + positions_value
        return {
            'equity': equity,
            'cash': self.cash,
            'positions_value': positions_value,
            'open_positions': len(self.positions)
        }
        
    def submit_order(self, order: Dict, market_snapshot: Dict) -> Dict:
        """
        Attempts to execute an order against the provided snapshot.
        
        Args:
            order: {market_id, side (BUY/SELL), price, size_usd}
            market_snapshot: {ask_price_1, ask_size_1, bid_price_1, bid_size_1, ...}
            
        Returns:
            ExecutionReport: {status, avg_price, filled_shares, ...}
        """
        market_id = order['market_id']
        side = order['side'].upper()
        size_usd = order['size_usd']
        limit_price = order.get('price') # Optional limit
        
        # 1. Validation
        if size_usd <= 0:
            return {'status': 'REJECTED', 'reason': 'Invalid size'}
            
        # 2. Liquidity Check
        # BUY -> consume ASK
        # SELL -> consume BID (hit bid)
        
        if side == 'BUY':
            best_price = market_snapshot.get('ask_price_1')
            available_shares = market_snapshot.get('ask_size_1', 0)
        else:
            best_price = market_snapshot.get('bid_price_1')
            available_shares = market_snapshot.get('bid_size_1', 0)
            
        # Check if price exists (market might be closed or empty)
        if not best_price or best_price <= 0:
            return {'status': 'REJECTED', 'reason': 'No Liquidity / Null Price'}
            
        # Limit Check:
        # If BUY, Limit must be >= Best Ask (Marketable)
        # If SELL, Limit must be <= Best Bid (Marketable)
        if limit_price:
            if side == 'BUY' and limit_price < best_price:
                return {'status': 'REJECTED', 'reason': f'Limit {limit_price} below Best Ask {best_price}'}
            if side == 'SELL' and limit_price > best_price:
                return {'status': 'REJECTED', 'reason': f'Limit {limit_price} above Best Bid {best_price}'}
        
        # 3. Execution (Fill at Best Price)
        # Calculate shares we can buy with $Size
        # BUT shares are capped by `available_shares`
        
        desired_shares = size_usd / best_price
        filled_shares = min(desired_shares, available_shares)
        
        # Did we get a fill?
        if filled_shares <= 0:
             return {'status': 'REJECTED', 'reason': 'Zero fill size'}
             
        # Determine Fill Cost/Proceeds
        fill_value = filled_shares * best_price
        
        # 4. Update Portfolio
        self._update_balance(side, fill_value, filled_shares, market_id, best_price)
        
        # Log Trade
        trade_record = {
            'timestamp': datetime.now(),
            'market_id': market_id,
            'side': side,
            'price': best_price,
            'shares': filled_shares,
            'value': fill_value,
            'limit_price': limit_price,
            'liquidity_avail': available_shares
        }
        self.trades.append(trade_record)
        self._persist_trade(trade_record)
        
        msg = "Partial Fill" if filled_shares < desired_shares else "Filled"
        return {
            'status': msg,
            'filled_shares': filled_shares,
            'avg_price': best_price,
            'value': fill_value
        }

    def _update_balance(self, side: str, value: float, shares: float, market_id: str, price: float):
        if side == 'BUY':
            self.cash -= value
            
            # Update Position
            pos = self.positions.get(market_id, {'shares': 0.0, 'avg_entry': 0.0, 'cost_basis': 0.0})
            new_shares = pos['shares'] + shares
            new_cost = pos['cost_basis'] + value
            
            self.positions[market_id] = {
                'shares': new_shares,
                'cost_basis': new_cost,
                'avg_entry': new_cost / new_shares if new_shares > 0 else 0.0,
                'current_value': new_shares * price # Mark to market immediately
            }
            
        elif side == 'SELL':
            self.cash += value
            
            # Reduce Position
            pos = self.positions.get(market_id)
            if not pos:
                # Short selling? Allowed?
                # For now assume simple Long strategy (Buy Low / Sell High)
                # If shorting is required (Simulation did support Shorts), we need simpler logic:
                # Shorting = Negative Shares? 
                # Let's support Shorting for Polymarket (Yes/No tokens are technically separate assets).
                # But here we trade the outcome "Yes".
                # Simplify: Negative shares = Short Yes.
                
                # Logic for shorting:
                # Cash proceeds added.
                # Shares become negative.
                pos = {'shares': 0.0, 'avg_entry': 0.0, 'cost_basis': 0.0}
            
            new_shares = pos['shares'] - shares
            # Cost basis reduction?
            # FIFO / LIFO or just AVG.
            # If we go flat, cost basis is 0.
            
            self.positions[market_id] = {
                'shares': new_shares,
                'cost_basis': pos['cost_basis'] * (new_shares / pos['shares']) if pos['shares'] != 0 else 0,
                'avg_entry': pos['avg_entry'], # Entry price doesn't change on partial exit
                'current_value': new_shares * price
            }
            
            # Clean up empty positions
            if abs(new_shares) < 0.0001:
                del self.positions[market_id]
    
    def mark_to_market(self, market_prices: Dict[str, float]):
        """Updates the 'current_value' of all positions based on latest mid prices."""
        for m_id, price in market_prices.items():
            if m_id in self.positions:
                shares = self.positions[m_id]['shares']
                self.positions[m_id]['current_value'] = shares * price
