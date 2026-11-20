
import sys
import os
import time
import pandas as pd
import logging
from datetime import datetime

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategies.implementations import SniperStrategy, LiquidityProviderStrategy, MomentumStrategy, FadeStrategy
from src.trading.paper_exchange import PaperExchange
from scripts.run_paper_bot import PaperTradingBot
from config import trading_config as cfg

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - SHADOW - %(message)s')
logger = logging.getLogger("ShadowRunner")

class ShadowRunner(PaperTradingBot):
    """
    Overrides the main Bot to run multiple strategies virtually.
    """
    def __init__(self):
        # Force Localhost for external script
        cfg.DB_HOST = 'localhost'
        cfg.DB_PORT = 8812
        
        super().__init__()
        
        # Initialize Components
        self.setup() 
        
        # 1. Define Strategies
        self.strategies = [
            SniperStrategy("Sniper (Live)", {'max_spread': 0.05, 'min_confidence': 0.06}),
            LiquidityProviderStrategy("Challenger: LP", {'max_spread': 0.15, 'min_confidence': 0.02}),
            MomentumStrategy("Challenger: Momentum", {'arb_threshold': 0.05}),
            FadeStrategy("The Fade (Mean Reversion)", {'velocity_threshold': 0.02})
        ]
        
        self.virtual_fills = {s.name: [] for s in self.strategies}
        self.stats = {s.name: {'pnl': 0.0, 'trades': 0} for s in self.strategies}
        # Track active virtual positions: {strat_name: {market_id: {side, entry_price, shares}}}
        self.active_positions = {s.name: {} for s in self.strategies}

    def run_cycle(self):
        """Run one iteration of data fetching and strategy processing."""
        # 1. Load Market List
        import json
        if not os.path.exists('scripts/nba_game_markets.json'):
             return

        with open('scripts/nba_game_markets.json', 'r') as f:
             markets = json.load(f)
             
        # 2. Iterate Markets
        for m in markets:
            mid = str(m.get('clob_token_id'))
            if not mid: continue
            
            # Fetch Data
            snapshots = self.fetch_recent_snapshots(mid)
            if not snapshots or len(snapshots) < 10: continue
            
            latest = snapshots[-1]
            features = self.prepare_features(snapshots)
            if features is None: continue
            
            # Predict
            import xgboost as xgb
            dmatrix = xgb.DMatrix(features)
            pred = self.model.predict(dmatrix)[0]
            
            # Evaluate Each Strategy
            for strat in self.strategies:
                signal = strat.generate_signal(mid, snapshots, pred, features.iloc[0].to_dict())
                
                # Check Existing Position
                active_pos = self.active_positions[strat.name].get(mid)
                
                if active_pos:
                    # WE HAVE A POSITION
                    # Check for Exit Signal (Opposite Side)
                    # Implementation Note: Strategies currently return 'BUY' or 'SELL' signal.
                    # If I am Long (BUY), I need a SELL signal to close.
                    
                    if signal and signal['side'] != active_pos['side']:
                        # CLOSE POSITION
                        exit_price = signal['limit_price']
                        self.close_virtual_position(strat, mid, exit_price)
                        
                    # Also check Stop Loss / Take Profit?
                    # For Shadow Mode comparison, let's keep it simple: Strategy Signal Exit Only.
                    # Or maybe Time Limit? 
                    # Let's stick to Signal Exit for now to test "Alpha".
                    
                else:
                    # NO POSITION
                    if signal:
                        # OPEN POSITION
                        entry_price = signal['limit_price']
                        self.open_virtual_position(strat, mid, signal['side'], entry_price)

    def open_virtual_position(self, strategy, market, side, price):
        if price <= 0 or price >= 1.0: return
        
        # Fixed Bet Size: $10
        shares = 10.0 / price
        
        self.active_positions[strategy.name][market] = {
            'side': side,
            'entry_price': price,
            'shares': shares,
            'timestamp': datetime.now()
        }
        logger.info(f"[{strategy.name}] OPEN {side} {market[:8]} @ {price:.2f} ({shares:.1f} shares)")
        self.save_shadow_report()

    def close_virtual_position(self, strategy, market, price):
        pos = self.active_positions[strategy.name].get(market)
        if not pos: return
        
        # Calculate PnL
        shares = pos['shares']
        entry = pos['entry_price']
        
        if pos['side'] == 'BUY':
            # Long: Profit = (Exit - Entry) * Shares
            pnl = (price - entry) * shares
        else:
            # Short: Profit = (Entry - Exit) * Shares
            pnl = (entry - price) * shares
            
        # Log Trade
        self.stats[strategy.name]['trades'] += 1 # Count completed trades
        self.stats[strategy.name]['pnl'] += pnl
        
        logger.info(f"[{strategy.name}] CLOSE {pos['side']} {market[:8]} @ {price:.2f} | PnL: ${pnl:.2f}")
        
        # Remove Position
        del self.active_positions[strategy.name][market]
        
        # Update Dashboard
        self.save_shadow_report()

    def save_shadow_report(self):
        import json
        
        # Inject Active Count into stats before saving
        for s in self.strategies:
            self.stats[s.name]['active'] = len(self.active_positions[s.name])
            
        with open('data/shadow_report.json', 'w') as f:
            json.dump(self.stats, f, indent=2)

if __name__ == "__main__":
    runner = ShadowRunner()
    logger.info("Starting Shadow Mode Runner...")
    while True:
        try:
            runner.run_cycle()
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)
