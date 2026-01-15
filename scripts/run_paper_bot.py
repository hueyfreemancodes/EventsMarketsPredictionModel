
import os
import sys
import time
import json
import logging
import asyncio
import pandas as pd
import numpy as np
import xgboost as xgb
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# Path Hack
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.trading.paper_exchange import PaperExchange
from src.trading.risk_manager import RiskManager
from src.feature_engineering.microstructure_features import MicrostructureFeaturesCalculator
import config.trading_config as cfg

# Logging
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL),
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("PaperBot")

class PaperTradingBot:
    def __init__(self):
        self.exchange = PaperExchange(cfg.INITIAL_CAPITAL)
        self.risk = RiskManager(self.exchange)
        self.calc = MicrostructureFeaturesCalculator(window_size=20)
        
        self.model = None
        self.markets = [] # List of {id, token, question}
        self.conn = None
        self.active_trades = {} # market_id -> {entry_time: datetime, side: str}
        self.fundamentals_cache = {} # market_id -> {team1_win_pct, ...}
        
    def setup(self):
        """Initialize DB, Model, and Markets."""
        logger.info("Setting up Paper Trading Bot...")
        
        # 1. Connect DB
        try:
            self.conn = psycopg2.connect(
                host=cfg.DB_HOST, port=cfg.DB_PORT, 
                user='admin', password='quest', database='qdb'
            )
            logger.info("Connected to QuestDB.")
        except Exception as e:
            logger.error(f"DB Connection failed: {e}")
            sys.exit(1)
            
        # 2. Load Model
        try:
            self.model = xgb.Booster()
            self.model.load_model('xgb_model.json')
            logger.info("XGBoost Model loaded.")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            sys.exit(1)
            
        # 3. Load Targets (Metadata)
        try:
            with open('nba_game_markets.json', 'r') as f:
                raw_markets = json.load(f)
            
            # Convert to internal format needed (Hex IDs)
            self.markets = []
            for m in raw_markets:
                tok = m.get('clob_token_id')
                if tok:
                    # Use raw Token ID (Asset ID) to match WSS
                    self.markets.append({
                        'id': tok,
                        'token': tok,
                        'title': m.get('title', 'Unknown'),
                        'question': m.get('question', 'Unknown')
                    })
                        
            logger.info(f"Loaded {len(self.markets)} potential market targets.")
            
            # 5. Load Fundamentals & Linkages
            self.load_fundamentals_cache()
            self.load_linkages()
            
        except Exception as e:
            logger.error(f"Failed to load markets: {e}")
            sys.exit(1)

    def load_linkages(self):
        """Load Polymarket -> Kalshi mappings."""
        try:
            query = "SELECT market_id, series_ticker FROM market_linkages WHERE source = 'kalshi'"
            # We need to map Poly ID -> Kalshi ID.
            # actually market_linkages has:
            # - Poly Row: market_id=0x..., team1=TOR, date=...
            # - Kalshi Row: market_id=KX..., team1=TOR, date=...
            # We need to join them. For simplicity in this bot, let's just fetch the PAIRS if we can.
            # Or simpler: load all market_linkages, match by (date, sorted_teams).
            
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM market_linkages")
                rows = cur.fetchall()
            
            # Build Match Keys
            poly_map = {}
            kalshi_map = {}
            
            for r in rows:
                if not r['game_date'] or not r['team1'] or not r['team2']: continue
                
                # Normalize Team Names? Assuming they are already abbr from ingestion?
                # Ingestion puts "TOR", "IND" etc.
                teams = sorted([str(r['team1']).upper(), str(r['team2']).upper()])
                
                date_val = r['game_date']
                if isinstance(date_val, datetime): date_val = date_val.date()
                if isinstance(date_val, str): 
                    try: date_val = datetime.strptime(date_val, "%Y-%m-%d").date()
                    except: continue
                    
                key = (date_val, teams[0], teams[1])
                
                mid = r['market_id']
                if r['source'] == 'polymarket':
                    poly_map[key] = mid
                elif r['source'] == 'kalshi':
                    kalshi_map[key] = mid
            
            # Create Direct Map
            self.kalshi_map = {} # PolyID -> KalshiID
            count = 0
            for key, pid in poly_map.items():
                if key in kalshi_map:
                    self.kalshi_map[pid] = kalshi_map[key]
                    count += 1
                    
            logger.info(f"Loaded {count} Polymarket->Kalshi linkages.")
            
        except Exception as e:
            logger.error(f"Failed to load linkages: {e}")

    def fetch_kalshi_snapshot(self, kalshi_id: str):
        """Fetch latest Kalshi snapshot."""
        if not kalshi_id: return None
        query = f"""
            SELECT * FROM order_book_snapshots 
            WHERE market_id = '{kalshi_id}'
            ORDER BY timestamp DESC 
            LIMIT 1
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                row = cur.fetchone()
            return row
        except:
            return None

    def load_fundamentals_cache(self):
        """Load latest fundamentals for all active markets from DB."""
        logger.info("Refreshing Fundamentals Cache...")
        if not self.markets: return
        
        try:
            # 1. Fetch recent stats
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM sports_fundamentals WHERE game_date >= '{datetime.now().date()}'")
                all_stats = cur.fetchall()
                
            stats_map = {}
            for row in all_stats:
                d = row['game_date'].date() if isinstance(row['game_date'], datetime) else row['game_date']
                t = row['home_team'] # Abbr
                stats_map[(d, t)] = row
                stats_map[(d, row['away_team'])] = row
                
            # 2. Map Markets -> Stats
            with open('nba_game_markets.json', 'r') as f:
                full_meta = json.load(f)
            token_to_meta = {m.get('clob_token_id'): m for m in full_meta}
            
            hits = 0
            for m in self.markets:
                mid = m['id']
                meta = token_to_meta.get(mid)
                if not meta: continue
                
                # Robust: Parse Slug for Team Abbr & Date
                # slug: "nba-tor-ind-2026-01-14"
                slug = meta.get('slug', '')
                parts = slug.split('-')
                
                t1, gdate = None, None
                
                if len(parts) >= 4 and parts[0] == 'nba':
                    try:
                        t1 = parts[1].upper()
                        # Date is the rest
                        date_str = "-".join(parts[3:])
                        gdate = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except: pass
                
                # Fallback to metadata fields if slug parsing fails
                if not t1:
                     t1 = meta.get('team1') 
                     
                if t1 and gdate:
                    stats = stats_map.get((gdate, t1))
                    if stats:
                        # Determine if T1 is Home or Away in the Stats record
                        is_home = (stats['home_team'] == t1)
                        
                        self.fundamentals_cache[mid] = {
                            'team1_win_pct': stats['home_win_pct'] if is_home else stats['away_win_pct'],
                            'team2_win_pct': stats['away_win_pct'] if is_home else stats['home_win_pct'],
                            'spread_vegas': 0.0
                        }
                        hits += 1
            
            logger.info(f"Loaded Fundamentals for {hits}/{len(self.markets)} markets.")

        except Exception as e:
            logger.error(f"Failed to load fundamentals: {e}")

    def to_hex(self, val):
        if not val: return None
        if str(val).startswith("0x"): return val
        try: return hex(int(val))
        except: return None

    def fetch_recent_snapshots(self, market_id: str, limit: int = 20) -> list:
        """Fetch raw snapshots for feature calculation."""
        query = f"""
            SELECT * FROM order_book_snapshots 
            WHERE market_id = '{market_id}' 
            ORDER BY timestamp DESC 
            LIMIT {limit}
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
        # Sort ASC for calculation (Oldest -> Newest)
        return sorted(rows, key=lambda x: x['timestamp'])

    def prepare_features(self, snapshots: list) -> pd.DataFrame:
        """Convert snapshots to model-ready features."""
        if len(snapshots) < 10:
            return None
            
        # Calc Feats
        feats_list = self.calc.calculate_all_features(snapshots)
        if not feats_list:
            return None
            
        # Take the NEWEST row (last one)
        latest = feats_list[-1]
        df = pd.DataFrame([latest])
        
        # Add Raw Snapshot Features (Required by V4 Model)
        latest_snap = snapshots[-1]
        df['ask_price_1'] = latest_snap.get('ask_price_1', 0.0)
        df['bid_price_1'] = latest_snap.get('bid_price_1', 0.0)
        df['spread'] = df['ask_price_1'] - df['bid_price_1']
        
        # Inject Fundamentals
        market_id = snapshots[0].get('market_id')
        fund = self.fundamentals_cache.get(market_id, {})
        
        # Inject Kalshi Data
        kal_id = self.kalshi_map.get(market_id)
        k_snap = self.fetch_kalshi_snapshot(kal_id) if kal_id else None
        
        k_micro = np.nan
        k_vamp = np.nan
        k_ofi = np.nan
        k_volatility = np.nan
        arb_spread = np.nan
        
        if k_snap:
            # Simple features from raw snapshot
            bid = k_snap.get('bid_price_1') or 0
            ask = k_snap.get('ask_price_1') or 0
            if bid > 0 and ask > 0:
                k_micro = (bid + ask) / 2 # Approx micro price as Mid
                k_vamp = k_micro # Aproxx
                
                # Arb Spread
                poly_micro = df['micro_price'].iloc[0]
                if not pd.isna(poly_micro):
                    arb_spread = poly_micro - k_micro
        
        # Patch Missing / Artifacts
        defaults = {
            'team1_win_pct': fund.get('team1_win_pct', 0.5), 
            'team2_win_pct': fund.get('team2_win_pct', 0.5), 
            'spread_vegas': fund.get('spread_vegas', 0.0),
            'k_micro_price': k_micro, 
            'k_vamp': k_vamp, 
            'k_ofi': k_ofi, 
            'k_volatility': k_volatility, 
            'arb_spread': arb_spread, 
            'feed_latency': 0 # Real-time latency hard to meas here
        }
        for c, v in defaults.items():
            if c not in df.columns:
                df[c] = v
                
        # Strict Column Ordering (V4 Model)
        model_cols = [
            'ofi_1s', 'vamp', 'micro_price', 'spread_volatility', 'ofi_ema_05',
            'ask_price_1', 'bid_price_1', 'spread',
            'k_micro_price', 'k_vamp', 'k_ofi', 'k_volatility', 
            'arb_spread', 'feed_latency', 'team1_win_pct',
            'team2_win_pct', 'spread_vegas'
        ]
        
        # Verify all cols exist
        for c in model_cols:
            if c not in df.columns:
                df[c] = np.nan # Use NaN for missing features instead of 0.0
                
        return df[model_cols]

    def run(self):
        """Main Loop."""
        logger.info("Bot Started. Press Ctrl+C to stop.")
        
        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                logger.info("Bot Stopped.")
                self.print_summary()
                break
            except Exception as e:
                logger.error(f"CRITICAL ERROR in tick: {e}", exc_info=True)
                # Auto-Reconnect logic
                if "connection already closed" in str(e) or "closed" in str(e):
                    logger.warning("♻️ DB Connection lost. Attempting Reconnect...")
                    try:
                        if self.conn: self.conn.close()
                        self.conn = psycopg2.connect(
                            host=cfg.DB_HOST, port=cfg.DB_PORT, 
                            user='admin', password='quest', database='qdb'
                        )
                        logger.info("✅ Reconnected to DB.")
                    except Exception as rec_e:
                        logger.error(f"Reconnect failed: {rec_e}")
                
            time.sleep(5)

    def tick(self):
        """Single iteration."""
        # Refresh Portfolio Valuation
        self.exchange.mark_to_market({}) # TODO: Pass map of live prices
        
        # Check active trades for Exits (Time or Stop/Profit)
        self.manage_positions()
        
        # Scan Markets for Entry
        for market in self.markets:
            mid = market['id']
            snapshots = self.fetch_recent_snapshots(mid)
            if not snapshots:
                continue
                
            # Latest Snapshot for Price/Liquidity
            latest_snap = snapshots[-1]
            current_price = latest_snap.get('mid_price') or 0
            
            # --- OPTIMIZATION: Fail Fast on Illiquid Markets ---
            # Don't run Feature Eng or AI if the spread is garbage.
            ask = latest_snap.get('ask_price_1', 1.0)
            bid = latest_snap.get('bid_price_1', 0.0)
            spread = ask - bid
            
            if spread > 0.10:
                # Silent Skip
                continue
            
            # Predict
            df = self.prepare_features(snapshots)
            if df is None: continue
            
            dmatrix = xgb.DMatrix(df)
            pred = self.model.predict(dmatrix)[0]
            
            # DEBUG: Log every 50th prediction or if > 0.005
            # import random
            # if random.random() < 0.01 or abs(pred) > 0.005:
            #      logger.info(f"DEBUG: {mid[:8]} Snapshots={len(snapshots)} Pred={pred:.4f}")
            
            # LOGIC
            # Only enter if flat
            if mid not in self.exchange.positions:
                if abs(pred) > cfg.SIGNAL_THRESHOLD:
                    side = 'BUY' if pred > 0 else 'SELL'
                    logger.info(f"SIGNAL {side} {mid[:8]} Pred: {pred:.4f}")
                    
                    self.execute_entry(mid, side, current_price, latest_snap)
                    
        # Report
        state = self.exchange.get_portfolio_state()
        print(f"\rEquity: ${state['equity']:.2f} | Cash: ${state['cash']:.2f} | Pos: {state['open_positions']}", end="")

    def execute_entry(self, market_id, side, price, snapshot):
        # Risk Check
        valid, reason = self.risk.validate_order(market_id, side, cfg.MAX_TRADE_SIZE_USD, snapshot)
        if not valid:
            logger.warning(f"Rejected {side} {market_id}: {reason}")
            return
            
        # Submit
        order = {
            'market_id': market_id,
            'side': side,
            'price': price, # Limit Price = Current Mid (Aggressive?) No, use Best Ask/Bid
            'size_usd': cfg.MAX_TRADE_SIZE_USD
        }
        
        # Wait, for BUY we want to buy at ASK.
        limit_price = snapshot.get('ask_price_1') if side == 'BUY' else snapshot.get('bid_price_1')
        order['price'] = limit_price
        
        result = self.exchange.submit_order(order, snapshot)
        
        if result['status'] == 'Filled' or result['status'] == 'Partial Fill':
            logger.info(f"FILLED {side} {market_id} @ ${result['avg_price']:.3f}")
            # Track time
            self.active_trades[market_id] = {
                'entry_time': datetime.now(),
                'side': side
            }
        else:
            logger.warning(f"Failed to fill: {result.get('reason')}")

    def manage_positions(self):
        """Check time limits and stops."""
        to_close = []
        for mid, pos in self.exchange.positions.items():
            trade_info = self.active_trades.get(mid)
            if not trade_info: continue
            
            # 1. Time Exit
            duration = (datetime.now() - trade_info['entry_time']).total_seconds()
            if duration >= cfg.TIME_LIMIT_SECONDS:
                to_close.append((mid, "Time Limit"))
                continue
                
            # 2. ROI (Stop/Profit) - Need current price
            # We need to fetch price again? Or optimize?
            # For prototype, we fetch just for managed positions
            snaps = self.fetch_recent_snapshots(mid, 1)
            if not snaps: continue
            curr_price = snaps[-1].get('mid_price')
            if not curr_price: continue
            
            # Update Mark to Market for Exchange
            self.exchange.mark_to_market({mid: curr_price})
            
            # Check PnL %
            avg_entry = pos['avg_entry']
            if avg_entry > 0:
                pnl_pct = (curr_price - avg_entry) / avg_entry
                if trade_info['side'] == 'SELL': pnl_pct = -pnl_pct
                
                if pnl_pct >= cfg.TAKE_PROFIT_ROI:
                    to_close.append((mid, f"Take Profit (+{pnl_pct:.1%})"))
                elif pnl_pct <= cfg.STOP_LOSS_ROI:
                    to_close.append((mid, f"Stop Loss ({pnl_pct:.1%})"))

        for mid, reason in to_close:
            self.execute_exit(mid, reason)

    def execute_exit(self, market_id, reason):
        pos = self.exchange.positions.get(market_id)
        if not pos: return
        
        # Generate opposing order
        # For simplicity, we just liquidate.
        # We need snapshot to determine exit price
        snaps = self.fetch_recent_snapshots(market_id, 1)
        if not snaps: return
        snapshot = snaps[-1]
        
        # Closing Long -> Sell
        # Closing Short -> Buy
        # Assuming we only have Longs based on PaperExchange implementation
        # Wait, PaperExchange supported 'SELL'
        
        # Wait, if we are Long (shares > 0), we SELL.
        # If we are Short (implementation pending), we BUY.
        
        side = 'SELL' # Assume close long for now
        
        # Calculate full value
        size_usd = pos['current_value'] 
        
        logger.info(f"Closing {market_id}: {reason}")
        
        order = {
            'market_id': market_id,
            'side': side,
            'price': snapshot.get('bid_price_1'), # Hit Bid
            'size_usd': size_usd
        }
        
        # Submit
        self.exchange.submit_order(order, snapshot)
        self.active_trades.pop(market_id, None)

    def print_summary(self):
        state = self.exchange.get_portfolio_state()
        print("\n--- Session Summary ---")
        print(f"Final Equity: ${state['equity']:.2f}")
        print(f"Total Trades: {len(self.exchange.trades)}")
        print("-----------------------")

if __name__ == "__main__":
    bot = PaperTradingBot()
    bot.setup()
    bot.run()
