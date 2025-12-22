import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Add source root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Project imports
from src.utils.nba_mapping import get_team_abbr

# Configuration
DB_HOST = os.getenv('QUESTDB_HOST', 'localhost')
DB_CONFIG = {
    "host": DB_HOST,
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "database": "qdb"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_frame(query: str, conn) -> pd.DataFrame:
    """Execute SQL and return DataFrame."""
    return pd.read_sql(query, conn)


def _normalize_linkages(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares linkage table with canonical match keys."""
    # Convert dates and normalize teams
    df['game_date'] = pd.to_datetime(df['game_date'])
    df['team1'] = df['team1'].apply(get_team_abbr)
    df['team2'] = df['team2'].apply(get_team_abbr)
    
    # Drop valid rows
    df = df.dropna(subset=['team1', 'team2', 'game_date'])

    # Create canonical key: YYYY-MM-DD|TEAM_A|TEAM_B (Sorted alphabetical)
    def _make_key(row):
        teams = sorted([row['team1'], row['team2']])
        date_str = row['game_date'].date().isoformat()
        return f"{date_str}|{teams[0]}|{teams[1]}"

    df['match_key'] = df.apply(_make_key, axis=1)
    return df


def get_v2_training_set(outfile: str = 'final_training_set_v2.csv'):
    """
    Builds the V2 training set by merging Polymarket and Kalshi data
    via timestamp alignment (asof merge) and joining fundamental stats.
    """
    logger.info("Building Unified V2 Dataset...")

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            
            # --- 1. Load & Map Linkages ---
            logger.info("Fetching market linkages...")
            raw_links = fetch_frame(
                "SELECT market_id, source, team1, team2, game_date FROM market_linkages", 
                conn
            )
            links = _normalize_linkages(raw_links)
            
            # Create mapping dicts
            poly_map = links[links['source'] == 'polymarket'].set_index('match_key')['market_id'].to_dict()
            kalshi_map = links[links['source'] == 'kalshi'].set_index('match_key')['market_id'].to_dict()
            
            # Direct PolyID -> KalshiID lookup
            poly_to_kalshi = {pid: kalshi_map[key] for key, pid in poly_map.items() if key in kalshi_map}
            
            logger.info(f"Found {len(poly_map)} Polymarket games ({len(poly_to_kalshi)} with Kalshi overlap).")

            # --- 2. Microstructure Data ---
            logger.info("Loading order book features...")
            micro_df = fetch_frame("""
                SELECT timestamp, market_id, 
                       ofi_1s, vamp, micro_price, spread_volatility, ofi_ema_05
                FROM microstructure_features
            """, conn)
            
            micro_df['timestamp'] = pd.to_datetime(micro_df['timestamp'])
            
            # Infer platform (Polymarket IDs start with 0x)
            micro_df['platform'] = micro_df['market_id'].apply(
                lambda x: 'polymarket' if str(x).startswith('0x') else 'kalshi'
            )

            # Split by platform
            poly_data = micro_df[micro_df['platform'] == 'polymarket'].copy()
            kalshi_data = micro_df[micro_df['platform'] == 'kalshi'].rename(columns={
                'micro_price': 'k_micro_price',
                'vamp': 'k_vamp',
                'ofi_ema_05': 'k_ofi',
                'spread_volatility': 'k_volatility'
            }).copy()
            
            # --- 3. Strict Time Alignment (Merge AsOf) ---
            logger.info("Aligning cross-exchange feeds...")
            
            merged_chunks = []
            
            for pid, group in poly_data.groupby('market_id'):
                group = group.sort_values('timestamp')
                kalshi_id = poly_to_kalshi.get(pid)
                
                # Default to Polymarket data only if no Kalshi match found
                market_merged = group
                
                if kalshi_id and not kalshi_data.empty:
                    k_subset = kalshi_data[kalshi_data['market_id'] == kalshi_id].sort_values('timestamp')
                    
                    if not k_subset.empty:
                        # Find closest Kalshi update within 5 minutes BEFORE the Polymarket update
                        market_merged = pd.merge_asof(
                            group, 
                            k_subset, 
                            on='timestamp', 
                            direction='backward', 
                            tolerance=pd.Timedelta('5m')
                        )
                        
                        # Calculate Arbitrage Signal
                        market_merged['arb_spread'] = market_merged['micro_price'] - market_merged['k_micro_price']
                        market_merged['feed_latency'] = (market_merged['timestamp'] - market_merged['timestamp_right']).dt.total_seconds()
                        
                        # Cleanup merge artifacts
                        market_merged = market_merged.drop(columns=['market_id_y', 'timestamp_right'], errors='ignore')
                        market_merged = market_merged.rename(columns={'market_id_x': 'market_id'})

                merged_chunks.append(market_merged)
            
            combined_df = pd.concat(merged_chunks, ignore_index=True) if merged_chunks else poly_data
            
            # --- 4. Enforce Schema ---
            # Ensure critical columns exist even if no merges succeeded
            expected_cols = ['k_micro_price', 'k_vamp', 'k_ofi', 'k_volatility', 'arb_spread', 'feed_latency']
            for col in expected_cols:
                if col not in combined_df.columns:
                    combined_df[col] = pd.NA

            # --- 5. Slow Alpha (Fundamentals) ---
            logger.info("Attaching fundamental stats...")
            fundamentals = fetch_frame("SELECT * FROM sports_fundamentals", conn)
            fundamentals['game_date'] = pd.to_datetime(fundamentals['game_date'])
            
            # Map fundamentals to market IDs using the Linkage DataFrame
            # This is cleaner than looping. We join `links` to `fundamentals` then to `combined_df`.
            
            # Prepare Poly-Link-Fund metadata table
            poly_links_df = links[links['source'] == 'polymarket'].copy()
            
            # Join fundamentals to the linkage table first
            # We need to match on Date + Team
            # Simpler approach: Create a unique match key in fundamentals too?
            
            fundamentals['home_abbr'] = fundamentals['home_team'] # Already normalized in DB? Assuming name match.
            # Actually, `links` uses abbreviations. `fundamentals` might use full names or abbr.
            # Safety: The previous script did a loop. Let's do a smarter loop or merge.
            
            meta_records = []
            for _, link_row in poly_links_df.iterrows():
                # Find matching game in fundamentals
                # Logic: Same date, and team1 is either home or away
                match = fundamentals[
                    (fundamentals['game_date'] == link_row['game_date']) & 
                    ((fundamentals['home_team'] == link_row['team1']) | (fundamentals['away_team'] == link_row['team1']))
                ]
                
                if not match.empty:
                    stats = match.iloc[0]
                    is_home = (stats['home_team'] == link_row['team1'])
                    
                    meta_records.append({
                        'market_id': link_row['market_id'],
                        'team1_win_pct': stats['home_win_pct'] if is_home else stats['away_win_pct'],
                        'team2_win_pct': stats['away_win_pct'] if is_home else stats['home_win_pct'],
                        'spread_vegas': stats.get('spread', 0)
                    })
            
            meta_df = pd.DataFrame(meta_records)
            final_df = pd.merge(combined_df, meta_df, on='market_id', how='inner')
            
            # --- 6. Target Engineering ---
            # Sort for valid shifting
            final_df = final_df.sort_values(['market_id', 'timestamp'])
            
            # Target: 60s future return (approx 12 periods @ 5s)
            final_df['target_return_60s'] = final_df.groupby('market_id')['micro_price'].shift(-12) - final_df['micro_price']
            final_df = final_df.dropna(subset=['target_return_60s'])

            # Export
            final_df.to_csv(outfile, index=False)
            
            overlap_count = final_df['k_micro_price'].notna().sum()
            logger.info(f"✅ Saved {len(final_df)} rows to {outfile}")
            logger.info(f"   -> Cross-Exchange Overlap: {overlap_count} rows ({overlap_count/len(final_df):.1%})")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    get_v2_training_set()
