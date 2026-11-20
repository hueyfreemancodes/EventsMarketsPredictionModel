
from .base import BaseStrategy

class SniperStrategy(BaseStrategy):
    """
    Original strict strategy.
    Max Spread: 5 cents
    Signal Threshold: 6%
    """
    def generate_signal(self, market_id, snapshots, prediction, features):
        snapshot = snapshots[-1]
        ask = snapshot.get('ask_price_1', 1.0)
        bid = snapshot.get('bid_price_1', 0.0)
        spread = ask - bid
        mid = (ask + bid) / 2
        
        # 1. Spread Filter
        if spread > self.config.get('max_spread', 0.05):
            return None
            
        # 2. Alpha Threshold
        if mid <= 0: return None
        required = (spread / mid) + 0.02
        threshold = max(required, self.config.get('min_confidence', 0.06))
        
        if abs(prediction) > threshold:
             side = 'BUY' if prediction > 0 else 'SELL'
             price = bid + 0.01 if side == 'BUY' else ask - 0.01
             return {'side': side, 'confidence': abs(prediction), 'limit_price': price}
        return None

class LiquidityProviderStrategy(BaseStrategy):
    """
    Looser strategy.
    Max Spread: 15 cents
    Signal Threshold: 2%
    Goal: Capture spread + small alpha
    """
    def generate_signal(self, market_id, snapshots, prediction, features):
        snapshot = snapshots[-1]
        ask = snapshot.get('ask_price_1', 1.0)
        bid = snapshot.get('bid_price_1', 0.0)
        spread = ask - bid
        mid = (ask + bid) / 2
        
        # 1. Loose Spread Filter
        if spread > self.config.get('max_spread', 0.15):
            return None
            
        # 2. Low Threshold
        if mid <= 0: return None
        # Just need to cover spread + 0.5%
        required = (spread / mid) + 0.005
        threshold = max(required, self.config.get('min_confidence', 0.02))
        
        if abs(prediction) > threshold:
             side = 'BUY' if prediction > 0 else 'SELL'
             # Market Make: Join the Best Bid
             price = bid if side == 'BUY' else ask 
             return {'side': side, 'confidence': abs(prediction), 'limit_price': price}
        return None

class MomentumStrategy(BaseStrategy):
    """
    Follows Kalshi Arb heavily.
    """
    def generate_signal(self, market_id, snapshots, prediction, features):
        snapshot = snapshots[-1]
        arb = features.get('arb_spread', 0.0) # Poly - Kalshi
        
        # If Poly is 5c cheaper than Kalshi -> BUY Poly
        if arb < -0.05: 
            return {'side': 'BUY', 'confidence': abs(arb), 'limit_price': snapshot.get('ask_price_1', 0.99)}
        # If Poly is 5c more expensive -> SELL Poly
        elif arb > 0.05:
            return {'side': 'SELL', 'confidence': abs(arb), 'limit_price': snapshot.get('bid_price_1', 0.01)}
            
        return None

class FadeStrategy(BaseStrategy):
    """
    The Elastic Band: Fades price velocity > 2%.
    """
    def generate_signal(self, market_id, snapshots, prediction, features):
        if len(snapshots) < 12:
            return None
            
        latest = snapshots[-1]
        oldest = snapshots[0] # assuming window ~20 snapshots (approx 1-2 mins depending on sampling)
        
        p1 = latest.get('mid_price')
        p0 = oldest.get('mid_price')
        
        if not p1 or not p0: return None
        
        velocity = p1 - p0
        
        # If Velocity > +0.02 (Price Spiked Up) -> SELL
        if velocity > 0.02:
            return {'side': 'SELL', 'confidence': velocity, 'limit_price': latest.get('bid_price_1', 0.0)}
            
        # If Velocity < -0.02 (Price Spiked Down) -> BUY
        elif velocity < -0.02:
            return {'side': 'BUY', 'confidence': abs(velocity), 'limit_price': latest.get('ask_price_1', 1.0)}
            
        return None
