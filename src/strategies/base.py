
from abc import ABC, abstractmethod
from typing import Dict, Optional

class BaseStrategy(ABC):
    def __init__(self, name: str, config: Dict):
        self.name = name
        self.config = config
        self.pnl = 0.0
        self.trades = []
        self.positions = {}

    @abstractmethod
    def generate_signal(self, market_id: str, snapshot: Dict, prediction: float, features: Dict) -> Optional[Dict]:
        """
        Returns a trade signal or None.
        Signal format: {'side': 'BUY'/'SELL', 'confidence': float, 'limit_price': float}
        """
        pass

    def on_fill(self, market_id: str, side: str, price: float, shares: float):
        """Virtual fill handler for Shadow Mode"""
        # Simple FIFO tracking
        self.positions[market_id] = self.positions.get(market_id, {'shares': 0, 'cost_basis': 0})
        pos = self.positions[market_id]
        
        if side == 'BUY':
            total_cost = (pos['shares'] * pos['cost_basis']) + (shares * price)
            total_shares = pos['shares'] + shares
            pos['shares'] = total_shares
            pos['cost_basis'] = total_cost / total_shares if total_shares > 0 else 0
        else:
            # Realize PnL
            profit = (price - pos['cost_basis']) * shares
            self.pnl += profit
            self.trades.append({'market': market_id, 'pnl': profit})
            pos['shares'] -= shares
            if pos['shares'] <= 0:
                del self.positions[market_id]

    def get_stats(self):
        return {
            'name': self.name,
            'pnl': self.pnl,
            'trades': len(self.trades),
            'open_positions': len(self.positions)
        }
