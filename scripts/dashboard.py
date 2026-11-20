
import streamlit as st
import pandas as pd
import time
import os
import re
import sys
# Add parent dir to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.trading_config import INITIAL_CAPITAL

st.set_page_config(page_title="PredEx Dashboard", layout="wide", page_icon="🦅")

# --- Constants ---

# --- Constants ---
# --- Constants ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # /app/
TRADES_PATH = os.path.join(BASE_DIR, "data", "paper_trades.csv")
active_orders_path = os.path.join(BASE_DIR, "data", "active_orders.json") # used below
BOT_LOG_PATH = os.path.join(BASE_DIR, "logs", "bot.log")
METADATA_PATH = os.path.join(BASE_DIR, "scripts", "nba_game_markets.json")
RAW_METADATA_PATH = os.path.join(BASE_DIR, "polymarket_nba_markets_100639.json") # Fallback
REFRESH_RATE = 2

# --- Helper Functions ---


def load_metadata():
    """Load Map of Market IDs to Metadata (Primary + Fallback)"""
    import json
    mapping = {}
    
    # 1. Load Raw Fallback (Broadest Coverage)
    if os.path.exists(RAW_METADATA_PATH):
        try:
            with open(RAW_METADATA_PATH, 'r') as f:
                raw_data = json.load(f)
            for m in raw_data:
                mid = str(m.get('clob_token_id'))
                title = m.get('title', 'Unknown')
                outcome = m.get('outcome_label', 'Unknown')
                mapping[mid] = {'game': title, 'selection': outcome}
        except:
            pass

    # 2. Load Active Filtered (Highest Accuracy, Overwrites Raw)
    if os.path.exists(METADATA_PATH):
        try:
            with open(METADATA_PATH, 'r') as f:
                data = json.load(f)
            for m in data:
                mid = str(m.get('clob_token_id'))
                title = m.get('title', 'Unknown')
                outcome = m.get('outcome_label', 'Unknown')
                mapping[mid] = {'game': title, 'selection': outcome}
        except:
            pass
            
    return mapping

# ... (omitted load_trades and others unchanged)

# ... inside main loop ...


import psycopg2
from psycopg2.extras import RealDictCursor

# --- Database Config ---
DB_CONFIG = {
    "host": "localhost", # Default to localhost if running outside docker, or use env var
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "database": "qdb"
}

def load_trades():
    """Load trades from Csv."""
    if not os.path.exists(TRADES_PATH):
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(TRADES_PATH)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        # st.error(f"Error loading trades: {e}")
        return pd.DataFrame()

def parse_bot_log():
    """Extracts latest Equity/Cash from bot log."""
    if not os.path.exists(BOT_LOG_PATH):
        return None
        
    # Read last chunk to find the status line
    try:
        with open(BOT_LOG_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            filesize = f.tell()
            seek_dist = min(filesize, 50000) # Read last 50KB to catch status through log spam
            f.seek(-seek_dist, os.SEEK_END)
            content = f.read().decode('utf-8', errors='ignore')
            
            lines = content.splitlines()
            # Look for Equity line in reverse
            for line in reversed(lines):
                if "Equity:" in line and "Cash:" in line:
                    return line.strip()
    except Exception as e:
        return None
    return None

def extract_metrics(status_line):
    # Regex to grab numbers
    # Equity: $2451.55 | Cash: $2568.64 | Pos: 48
    if not status_line: return {}
    
    eq_match = re.search(r"Equity:\s*\$?([\d\.,]+)", status_line)
    cash_match = re.search(r"Cash:\s*\$?([\d\.,]+)", status_line)
    pos_match = re.search(r"Pos:\s*(\d+)", status_line)
    
    def parse_float(s):
        return float(s.replace(',', '')) if s else 0.0

    return {
        "equity": parse_float(eq_match.group(1)) if eq_match else 0.0,
        "cash": parse_float(cash_match.group(1)) if cash_match else 0.0,
        "open_pos": int(pos_match.group(1)) if pos_match else 0
    }

def calculate_stats(df):
    if df.empty: return {}
    
    # 1. Total Volume
    total_vol = df['value'].sum()
    
    # 2. Reconstruct PnL by Market
    # Group by Market ID
    market_grps = df.groupby('market_id')
    
    realized_pnl = 0.0
    wins = 0
    losses = 0
    
    for mid, group in market_grps:
        # FIFO PnL Calculation to handle multiple entries/exits
        fifo_buy_queue = [] # List of (price, shares)
        # Sort by timestamp
        group = group.sort_values('timestamp')
        
        filtered_pnl = 0.0
        
        for idx, row in group.iterrows():
            side = row['side']
            shares = row['shares']
            price = row['price']
            
            if side == 'BUY':
                fifo_buy_queue.append([price, shares])
            elif side == 'SELL':
                shares_to_close = shares
                
                # Match against buy queue
                while shares_to_close > 0.0001 and len(fifo_buy_queue) > 0:
                     buy_price, buy_shares = fifo_buy_queue[0]
                     
                     matched_shares = min(shares_to_close, buy_shares)
                     
                     # PnL for this chunk
                     trade_pnl = (price - buy_price) * matched_shares
                     realized_pnl += trade_pnl
                     filtered_pnl += trade_pnl
                     
                     # Update queue
                     fifo_buy_queue[0][1] -= matched_shares
                     shares_to_close -= matched_shares
                     
                     if fifo_buy_queue[0][1] < 0.0001:
                         fifo_buy_queue.pop(0)
                         
        if filtered_pnl > 0.01: wins += 1
        elif filtered_pnl < -0.01: losses += 1

    total_closed = wins + losses
    win_rate = (wins / total_closed * 100) if total_closed > 0 else 0
    
    return {
        "realized_pnl": realized_pnl,
        "total_volume": total_vol,
        "trades_count": len(df),
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses
    }

# --- Layout ---

st.title("🦅 PredEx Live Monitor")

# Metrics Container
status_placeholder = st.empty()
metrics_placeholder = st.empty()
charts_placeholder = st.empty()
trades_placeholder = st.empty()


while True:
    # 0. Refresh Metadata
    meta_map = load_metadata()

    # 1. Load Data
    df = load_trades()
    status_line = parse_bot_log()
    live_metrics = extract_metrics(status_line)
    hist_stats = calculate_stats(df)
    
    with status_placeholder.container():
        # Clean status bar
        if status_line:
            now_et = pd.Timestamp.now(tz='US/Eastern').strftime('%H:%M:%S')
            st.success(f"🟢 System Active | {now_et} ET")
        else:
            st.warning("⚠️ Waiting for Bot Status...")


    
    if os.path.exists(active_orders_path):
        try:
            import json
            with open(active_orders_path, "r") as f:
                orders = json.load(f)
                pending_count = len(orders)
                pending_data = [] # <--- Initialize here
                # Parse for display
                for mid, o in orders.items():
                    meta = meta_map.get(str(mid), {})
                    game_title = meta.get('game', mid[:15])
                    selection = meta.get('selection', '')
                    
                    # Formatting: "Knicks vs Pistons - Pistons ML"
                    market_lbl = f"{game_title} - {selection}" if selection else game_title
                    
                    pending_data.append({
                        "Game": game_title,
                        "Market": market_lbl,
                        "Side": o['side'],
                        "Price": o['price'],
                        "Time": o['timestamp']
                    })
        except Exception as e:
            st.error(f"Error loading pending orders from {active_orders_path}: {e}")
            pending_count = 0
            pending_data = []
    else:
        pending_count = 0
        pending_data = [] # Ensure defined if file missing

    with metrics_placeholder.container():
        # Display Metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        
        # Calculate Positions from DataFrame (True State)
        real_pos_count = 0
        st_pos_df = pd.DataFrame()
        
        if not df.empty:
            # Group by Market and Side
            # ... (Existing logic unchanged, just keeping it here conceptually or relying on prev implementation)
            # Actually I need to re-implement the block since I am replacing the container block
            try:
                pos_df = df.pivot_table(index='market_id', columns='side', values='shares', aggfunc='sum', fill_value=0)
                if 'BUY' not in pos_df.columns: pos_df['BUY'] = 0
                if 'SELL' not in pos_df.columns: pos_df['SELL'] = 0
                
                pos_df['Net Position'] = pos_df['BUY'] - pos_df['SELL']
                open_positions = pos_df[pos_df['Net Position'].abs() > 0.0001].copy() # Use abs for net position
                real_pos_count = len(open_positions)
                
                display_data = []
                for mid, row in open_positions.iterrows():
                    meta = meta_map.get(str(mid), {})
                    game_lbl = meta.get('game', str(mid)[:15])
                    sel_lbl = meta.get('selection', 'Unknown')
                    
                    market_lbl = f"{game_lbl} - {sel_lbl}"
                    
                    display_data.append({
                        "Game": game_lbl,
                        "Market": market_lbl,
                        "Selection": sel_lbl,
                        "Shares": row['Net Position'],
                        "Market ID": mid
                    })
                st_pos_df = pd.DataFrame(display_data)
            except Exception as e:
                # st.error(f"Error calculating positions: {e}")
                pass

        equity = live_metrics.get("equity", 0)
        cash = live_metrics.get("cash", 0)
        real_pnl = hist_stats.get("realized_pnl", 0)
        eq_delta = equity - INITIAL_CAPITAL 
        pnl_delta = real_pnl

        col1.metric("Equity", f"${equity:,.2f}", f"{eq_delta:.2f}")
        col2.metric("Cash", f"${cash:,.2f}")
        col3.metric("Positions", f"{real_pos_count}")
        col4.metric("Pending", f"{pending_count}", delta=f"{pending_count} Active" if pending_count > 0 else "0", delta_color="off")
        col5.metric("PnL", f"${real_pnl:.2f}", f"{pnl_delta:.2f}")

        # Show Positions Table
        if not st_pos_df.empty:
            with st.expander("Active Positions Details", expanded=True):
                st.dataframe(st_pos_df, use_container_width=True)
                
        # Show Pending Orders
        if pending_count > 0:
            with st.expander("Pending Limit Orders", expanded=False):
                 st.dataframe(pd.DataFrame(pending_data), use_container_width=True)

    with charts_placeholder.container():
        st.divider()
        col_main, col_stat = st.columns([2, 1])
        
        with col_main:
             st.subheader("Recent Activity")
             if not df.empty:
                 # Resample 1 min
                 vol_series = df.set_index('timestamp')['value'].resample('1T').sum()
                 st.bar_chart(vol_series[-30:]) # Last 30 mins
             else:
                 st.info("No trades yet.")

        with col_stat:
             st.subheader("Performance")
             wins = hist_stats.get("wins", 0)
             losses = hist_stats.get("losses", 0)
             total = wins + losses
             if total > 0:
                 st.write(f"Win Rate: **{hist_stats.get('win_rate', 0):.1f}%**")
                 st.progress(wins/total)
                 st.write(f"Wins: {wins} | Losses: {losses}")
             else:
                 st.write("No Closed Trades")

    with trades_placeholder.container():
        st.divider()
        st.subheader("Trade Log (Latest 10)")
        if not df.empty:
             # Enrich Data using meta_map
             def get_meta(mid, field):
                 val = meta_map.get(mid)
                 if isinstance(val, dict):
                     return val.get(field, mid)
                 return mid

             df['Game'] = df['market_id'].apply(lambda x: get_meta(x, 'game'))
             df['Selection'] = df['market_id'].apply(lambda x: get_meta(x, 'selection'))
             
             display_cols = ['timestamp', 'side', 'Game', 'Selection', 'price', 'shares', 'value']
             
             latest = df.sort_values('timestamp', ascending=False).head(10)
             # Shorten Game Title
             latest['Game'] = latest['Game'].astype(str).apply(lambda x: x[:37] + "..." if len(x) > 40 else x)
             
             st.dataframe(latest[display_cols], use_container_width=True, hide_index=True)
        else:
             st.write("Log empty.")

    time.sleep(REFRESH_RATE)
