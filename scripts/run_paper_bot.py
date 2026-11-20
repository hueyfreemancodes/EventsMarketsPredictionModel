
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
# Ensure logs directory
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log")
    ]
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
            
        # Initialize Execution State
        self.active_orders = {} # mid -> {side, price, size_usd, timestamp}
        self.last_fill_check_time = datetime.now() - timedelta(minutes=5) # Look back 5m on startup
            
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
            # Load markets from shared scripts/ directory for container sync
            market_file = os.path.join(os.path.dirname(__file__), 'nba_game_markets.json')
            logger.info(f"Loading markets from {market_file}")
            with open(market_file, 'r') as f:
                raw_markets = json.load(f)
            
            # Convert to internal format needed (Hex IDs)
            self.markets = []
            for m in raw_markets:
                tok = m.get('clob_token_id')
                if tok:
                    self.markets.append({
                        'id': tok,
                        'clob_token_id': tok, # Explicitly include for tick loop
                        'token': tok,
                        'title': m.get('title', 'Unknown'),
                        'question': m.get('question', 'Unknown')
                    })
                        
            logger.info(f"Loaded {len(self.markets)} potential market targets.")
            
            # 5. Load Fundamentals & Linkages
            self.load_fundamentals_cache()
            self.load_linkages()

            # 6. Reconcile Active Trades (Fix Management Zombie Bug)
            logger.info("Reconciling Risk Manager with Portfolio...")
            restored_count = 0
            for mid, pos in self.exchange.positions.items():
                if mid not in self.active_trades:
                    # Find latest trade timestamp for this market
                    last_trade = None
                    for t in reversed(self.exchange.trades):
                        if t['market_id'] == mid:
                            last_trade = t
                            break
                    
                    entry_time = last_trade['timestamp'] if last_trade else datetime.now()
                    if isinstance(entry_time, str):
                        try: entry_time = datetime.fromisoformat(entry_time)
                        except: entry_time = datetime.now()

                    # Determine Side (if shares < 0 => SELL/Short)
                    # PaperExchange currently uses shares > 0 for Long, shares < 0 for Short
                    side = 'SELL' if pos['shares'] < 0 else 'BUY'

                    self.active_trades[mid] = {
                        'entry_time': entry_time,
                        'side': side
                    }
                    restored_count += 1
            
            logger.info(f"Restored Risk Management for {restored_count} active positions.")
            
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
                # Normalize Team Names using shared utility
                from src.utils.nba_mapping import get_team_abbr
                t1 = get_team_abbr(str(r['team1']))
                t2 = get_team_abbr(str(r['team2']))
                # Sort to canonicalize
                teams = sorted([t1, t2])
                
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
        # Use RAW market_id (Decimal String) - Do NOT convert to Hex
        # hex_id = self.to_hex(market_id) 
        
        # logger.info(f"DEBUG DB: Querying {market_id}")
        
        query = f"""
            SELECT * FROM order_book_snapshots 
            WHERE market_id = '{market_id}' 
            ORDER BY timestamp DESC 
            LIMIT {limit}
        """ 

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                if not rows:
                   # logger.info(f"DEBUG DB: No rows for {market_id}")
                   pass
        except Exception as e:
            logger.error(f"Snapshot fetch failed: {e}")
            raise e # Trigger Reconnect Logic in Main Loop
            
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


    def check_fills(self):
        """Match active orders against recent public trades."""
        if not self.active_orders: return
        
        now = datetime.now()
        # Fetch trades since last check
        # Note: QuestDB timestamps are microsecond precision usually.
        # Use simple string format for SQL
        query_time = self.last_fill_check_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        # Build strict market ID filter to optimize query
        mids = ",".join([f"'{mid}'" for mid in self.active_orders.keys()])
        if not mids: return
        
        query = f"""
            SELECT * FROM trades 
            WHERE timestamp >= '{query_time}' 
            AND market_id IN ({mids})
            ORDER BY timestamp ASC
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                recent_trades = cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch trades for matching: {e}")
            return

        fill_count = 0
        for trade in recent_trades:
            mid = trade['market_id']
            if mid not in self.active_orders: continue
            
            order = self.active_orders[mid]
            trade_price = trade['price']
            
            # MATCHING LOGIC (Maker Strategy)
            # If I am buying at $0.30, and a trade happens at $0.29 (or $0.30), I am filled.
            is_fill = False
            if order['side'] == 'BUY' and trade_price <= order['price']:
                is_fill = True
            elif order['side'] == 'SELL' and trade_price >= order['price']:
                is_fill = True
                
            if is_fill:
                logger.info(f"MATCHED {mid} Order:{order['price']} Trade:{trade_price}")
                
                # EXECUTE FILL via PaperExchange
                # Create Synthetic Snapshot to guarantee fill check passes
                syn_snap = {
                    'ask_price_1': order['price'] if order['side'] == 'BUY' else 999.0,
                    'ask_size_1': 999999.0,
                    'bid_price_1': order['price'] if order['side'] == 'SELL' else 0.0,
                    'bid_size_1': 999999.0
                }
                
                fill_order = {
                    'market_id': mid,
                    'side': order['side'],
                    'price': order['price'],
                    'size_usd': order['size_usd']
                }
                
                res = self.exchange.submit_order(fill_order, syn_snap)
                if res['status'] in ['Filled', 'Partial Fill']:
                     del self.active_orders[mid]
                     self.active_trades[mid] = {'entry_time': now, 'side': order['side']}
                     fill_count += 1
                     
        if fill_count > 0:
            logger.info(f"Matched and Filled {fill_count} orders via Trade Stream.")
            
        self.last_fill_check_time = now

    def cancel_stale_orders(self):
        """Cancel orders that have been open too long."""
        now = datetime.now()
        to_remove = []
        for mid, order in self.active_orders.items():
            duration = (now - order['timestamp']).total_seconds()
            if duration > 180: # 3 minutes
                 logger.info(f"CANCEL {mid} - Stale ({duration:.0f}s)")
                 to_remove.append(mid)
        
        for mid in to_remove:
            del self.active_orders[mid]

    def save_active_orders(self):
        """Dump active orders to JSON for Dashboard visibility."""
        try:
            # Convert to serializable format
            export_data = {}
            for mid, order in self.active_orders.items():
                export_data[mid] = {
                    'side': order['side'],
                    'price': order['price'],
                    'size_usd': order['size_usd'],
                    'timestamp': order['timestamp'].isoformat()
                }
            
            # Atomic write (write to temp then rename) not strictly needed for this scale
            with open("data/active_orders.json", "w") as f:
                json.dump(export_data, f)
                
        except Exception as e:
            logger.error(f"Failed to save active orders: {e}")

    def tick(self):
        """Single iteration."""
        # logger.info("DEBUG: Entering Tick")
        # 0. Check for Fills and Cancellations (Maker Strategy)
        self.check_fills()
        self.cancel_stale_orders()
        self.save_active_orders() # Export state
        
        # Refresh Portfolio Valuation
        self.exchange.mark_to_market({}) # TODO: Pass map of live prices
        
        # Check active trades for Exits (Time or Stop/Profit)
        self.manage_positions()
        
        # Scan Markets for Entry
        count = 0
        for market in self.markets:
            count += 1
            # use CLOB Token ID as the primary key
            mid = market.get('clob_token_id')
            
            # DEBUG: Trace first few markets
            if count <= 3:
                logger.info(f"DEBUG LOOP: Checking mid={mid}")
                
            if not mid: mid = market.get('market_id') # Fallback
            if not mid: continue
            
            # Skip if we already have a position OR an active order
            if mid in self.exchange.positions: continue
            if mid in self.active_orders: continue
            
            snapshots = self.fetch_recent_snapshots(mid)
            if not snapshots:
                # logger.info(f"DEBUG: No Snaps {mid}")
                continue
                
            # Latest Snapshot for Price/Liquidity
            latest_snap = snapshots[-1]
            current_price = latest_snap.get('mid_price') or 0
            
            # --- OPTIMIZATION: Fail Fast on Illiquid Markets ---
            # Don't run Feature Eng or AI if the spread is garbage.
            ask = latest_snap.get('ask_price_1')
            bid = latest_snap.get('bid_price_1')
            
            # Handle None (explicitly set to bad values if missing)
            if ask is None: ask = 1.0
            if bid is None: bid = 0.0
            
            spread = ask - bid
            
            if spread > 0.99:
                # Silent Skip - DEBUG: Log occasionally to verify
                # import random
                # if random.random() < 0.01:
                logger.info(f"DEBUG SKIP {mid[:8]} Spread:{spread:.2f} > 0.99")
                continue
            
            # Predict
            df = self.prepare_features(snapshots)
            if df is None: 
                 logger.info(f"DEBUG SKIP {mid[:8]} No Features (Rows={len(snapshots)})")
                 continue
            
            dmatrix = xgb.DMatrix(df)
            pred = self.model.predict(dmatrix)[0]
            
            # DEBUG: Log every prediction
            logger.info(f"DEBUG EVAL {mid[:8]} Pred:{pred:.4f} Spread:{spread:.2f}")
            
            # LOGIC
            # Only enter if flat
            if mid not in self.exchange.positions:
                # Dynamic Threshold: Pred must beat Spread Cost + Min Profit (2%)
                mid_price = current_price if current_price > 0 else 0.50
                spread_pct = spread / mid_price
                
                required_alpha = spread_pct + 0.02 # Cost + 2% Edge
                
                # Minimum floor from config
                required_alpha = max(required_alpha, cfg.SIGNAL_THRESHOLD)
                
                if abs(pred) > required_alpha:
                    side = 'BUY' if pred > 0 else 'SELL'
                    logger.info(f"SIGNAL {side} {mid[:8]} Pred:{pred:.4f} > Req:{required_alpha:.4f} (Spr:{spread_pct:.1%})")
                    
                    self.execute_entry(mid, side, current_price, latest_snap)
                else:
                    # Optional: Log 'close calls' to debug signal strength
                    # if abs(pred) > 0.02:
                    #    logger.info(f"Skipped {mid[:8]} Pred:{pred:.4f} < Cost:{required_alpha:.4f}")
                    pass

                    
        # Report
        state = self.exchange.get_portfolio_state()
        logger.info(f"STATUS REPORT | Equity: ${state['equity']:.2f} | Cash: ${state['cash']:.2f} | Pos: {state['open_positions']}")

    def execute_entry(self, market_id, side, price, snapshot):
        # Risk Check
        # valid, reason = self.risk.validate_order(market_id, side, cfg.MAX_TRADE_SIZE_USD, snapshot)
        # if not valid:
        #    logger.warning(f"Rejected {side} {market_id}: {reason}")
        #    return
            
        # Determine Limit Price (Maker Strategy)
        tick_size = 0.01
        limit_price = price # Default to Mid if something fails
        
        if side == 'BUY':
             best_bid = snapshot.get('bid_price_1', 0) or 0
             best_ask = snapshot.get('ask_price_1', 1) or 1
             
             # Start at Best Bid
             limit_price = best_bid
             # If spread is wide (> 2 ticks), front-run by 1 tick
             if (best_ask - best_bid) > (1.5 * tick_size):
                 limit_price += tick_size
        else:
             best_bid = snapshot.get('bid_price_1', 0) or 0
             best_ask = snapshot.get('ask_price_1', 1) or 1
             
             limit_price = best_ask
             if (best_ask - best_bid) > (1.5 * tick_size):
                 limit_price -= tick_size
                 
        # Submit to Active Orders (Pending Fill)
        logger.info(f"PLACING LIMIT {side} {market_id[:8]} @ ${limit_price:.2f} (Mid: ${price:.3f})")
        
        self.active_orders[market_id] = {
            'market_id': market_id,
            'side': side,
            'price': limit_price,
            'size_usd': cfg.MAX_TRADE_SIZE_USD,
            'timestamp': datetime.now()
        }

    def manage_positions(self):
        """Check time limits and stops."""
        to_close = []
        for mid, pos in self.exchange.positions.items():
            trade_info = self.active_trades.get(mid)
            if not trade_info: continue
            
            # Fetch Price First (for both Time and TP/SL checks)
            snaps = self.fetch_recent_snapshots(mid, 1)
            if not snaps: continue
            curr_price = snaps[-1].get('mid_price')
            if not curr_price: continue
            
            # Update Mark to Market for Exchange
            self.exchange.mark_to_market({mid: curr_price})
            
            # Calculate PnL % using Mid Price (Unrealized)
            avg_entry = pos['avg_entry']
            pnl_pct = 0.0
            if avg_entry > 0:
                pnl_pct = (curr_price - avg_entry) / avg_entry
                if trade_info['side'] == 'SELL': pnl_pct = -pnl_pct

            # 1. Time Exit (Smart Hold)
            duration = (datetime.now() - trade_info['entry_time']).total_seconds()
            if duration >= cfg.TIME_LIMIT_SECONDS:
                # ONLY Exit on Time Limit if we are showing a profit (Mid-to-Mid)
                # This prevents churning flat markets just to pay the spread again.
                if pnl_pct > 0:
                    to_close.append((mid, f"Time Limit (Profit {pnl_pct:.1%})"))
                else:
                    # Log occasionally? 
                    # logger.info(f"Smart Hold {mid}: Time Limit reached but PnL {pnl_pct:.1%} <= 0")
                    pass 
                continue

            # 2. ROI (Stop/Profit)
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
        
        # Determine Side to Close
        # If shares > 0 (LONG), we SELL.
        # If shares < 0 (SHORT), we BUY.
        if pos['shares'] > 0:
            side = 'SELL'
        else:
            side = 'BUY'
        
        # Calculate full value (Magnitude)
        size_usd = abs(pos['current_value'])
        
        # Validation: If size is really small, just clear it locally
        if size_usd < 0.01:
             logger.info(f"Auto-Clearing dust {market_id}: {reason} (Val=${size_usd:.4f})")
             self.active_trades.pop(market_id, None)
             if market_id in self.exchange.positions:
                 del self.exchange.positions[market_id]
             return

        # Determine Execution Price
        exec_price = snapshot.get('bid_price_1') if side == 'SELL' else snapshot.get('ask_price_1')
        if not exec_price:
            logger.warning(f"Cannot close {market_id}: No liquidity on {side} side")
            return

        # Explicitly close by shares to avoid dust
        shares_to_close = abs(pos['shares'])
        # Nominal value for logging/legacy checks
        size_usd = shares_to_close * exec_price
        
        logger.info(f"Closing {market_id}: {reason} (Qty: {shares_to_close:.4f} @ ${exec_price:.2f})")
        
        order = {
            'market_id': market_id,
            'side': side,
            'price': exec_price, 
            'size_usd': size_usd,
            'shares': shares_to_close  # Explicit share quantity
        }
        
        # Submit
        result = self.exchange.submit_order(order, snapshot)
        if result['status'] in ['Filled', 'Partial Fill']:
            logger.info(f"FILLED CLOSE {side} {market_id} @ ${result['avg_price']:.3f} | PnL Realized")
            self.active_trades.pop(market_id, None)
        else:
            logger.warning(f"Failed to CLOSE {market_id}: {result.get('reason')}")

    def print_summary(self):
        state = self.exchange.get_portfolio_state()
        print("\n--- Session Summary ---")
        print(f"Final Equity: ${state['equity']:.2f}")
        print(f"Total Trades: {len(self.exchange.trades)}")
        print("-----------------------")

if __name__ == "__main__":
    from src.utils import ServiceSupervisor
    
    def main_loop():
        bot = PaperTradingBot()
        bot.setup()
        bot.run()

    supervisor = ServiceSupervisor("PaperBot")
    supervisor.run(main_loop)
