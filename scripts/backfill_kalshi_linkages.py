import os
import sys
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Set

import psycopg2

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_collection.nba_team_abbreviations import get_team_abbreviation

# Logging Config
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# DB Config
DB_CONFIG = {
    "host": os.getenv('QUESTDB_HOST', 'localhost'),
    "port": int(os.getenv('QUESTDB_PORT', 8812)),
    "user": "admin",
    "password": "quest",
    "database": "qdb"
}

@dataclass
class GameInfo:
    team1: str
    team2: str
    date: datetime.date
    raw_data: dict

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_kalshi_ids(cursor) -> List[str]:
    """Fetch distinct Kalshi market IDs from snapshots."""
    cursor.execute("SELECT DISTINCT market_id FROM order_book_snapshots WHERE platform='kalshi'")
    return [r[0] for r in cursor.fetchall()]

def fetch_polymarket_games(cursor) -> List[GameInfo]:
    """Fetch known Polymarket games and normalize team codes."""
    cursor.execute("SELECT team1, team2, game_date, series_ticker FROM market_linkages WHERE source='polymarket'")
    rows = cursor.fetchall()
    
    games = []
    for r in rows:
        # r: (team1, team2, game_date, series_ticker)
        # Normalize teams to codes
        t1_code = get_team_abbreviation(r[0]) or r[0]
        t2_code = get_team_abbreviation(r[1]) or r[1]
        
        # Ensure codes are 3 chars
        if len(str(t1_code)) != 3 or len(str(t2_code)) != 3:
            continue
            
        game_dt = r[2]
        if isinstance(game_dt, str):
            try:
                game_dt = datetime.fromisoformat(game_dt)
            except ValueError:
                continue
                
        games.append(GameInfo(
            team1=t1_code,
            team2=t2_code,
            date=game_dt.date(),
            raw_data={'original_t1': r[0], 'original_t2': r[1], 'ticker': r[3]}
        ))
    return games

def parse_kalshi_ticker(ticker: str) -> Optional[GameInfo]:
    """
    Parses a Kalshi ticker like 'KXNBAGAME-25DEC25LALGSW-GSW'
    Returns a GameInfo object or None.
    """
    # Expected: KXNBAGAME-25DEC25LALGSW-GSW
    parts = ticker.split('-')
    if len(parts) < 3:
        return None
        
    core = parts[1] # 25DEC25LALGSW
    if len(core) < 13: 
        return None
        
    date_part = core[:7]  # 25DEC25
    teams_part = core[7:] # LALGSW
    
    try:
        dt = datetime.strptime(date_part, "%y%b%d").date()
    except ValueError:
        return None
        
    t1 = teams_part[:3]
    t2 = teams_part[3:]
    
    return GameInfo(team1=t1, team2=t2, date=dt, raw_data={'ticker': ticker})

def backfill_linkages():
    logger.info("Starting Kalshi Linkage Backfill...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Get Data
                k_ids = fetch_kalshi_ids(cur)
                poly_games = fetch_polymarket_games(cur)
                
                logger.info(f"Loaded {len(k_ids)} Kalshi IDs and {len(poly_games)} Polymarket games.")
                
                matches = []
                
                for kid in k_ids:
                    k_game = parse_kalshi_ticker(kid)
                    if not k_game:
                        continue
                        
                    # Find Match
                    # Logic: Same date, same set of teams
                    k_teams = {k_game.team1, k_game.team2}
                    
                    found_game = None
                    for pg in poly_games:
                        if pg.date == k_game.date:
                            if {pg.team1, pg.team2} == k_teams:
                                found_game = pg
                                break
                    
                    if found_game:
                        # Prepare Insert
                        matches.append((
                            kid,
                            'kalshi',
                            k_game.team1,
                            k_game.team2,
                            k_game.date, # game_date
                            f"Match from Ticker {kid}",
                            kid.split('-')[0], # series_ticker
                            datetime.utcnow()
                        ))
                
                logger.info(f"found {len(matches)} matching Kalshi markets.")
                
                if matches:
                    insert_sql = """
                    INSERT INTO market_linkages (
                        market_id, source, team1, team2,
                        game_date, original_title, series_ticker, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(market_id) DO NOTHING;
                    """
                    # Note: Conflict clause assumes market_id is unique/PK.
                    # If QuestDB doesn't support ON CONFLICT, we rely on the script being idempotent via query check?
                    # QuestDB does NOT support ON CONFLICT.
                    # We should filter existing IDs first. Or just insert and ignore dupe failure?
                    # Better: Filter against existing Linkages
                    
                    cur.execute("SELECT market_id FROM market_linkages WHERE source='kalshi'")
                    existing_ids = {r[0] for r in cur.fetchall()}
                    
                    new_matches = [m for m in matches if m[0] not in existing_ids]
                    
                    if new_matches:
                        cur.executemany(insert_sql.split("ON CONFLICT")[0], new_matches)
                        conn.commit()
                        logger.info(f"✅ Successfully inserted {len(new_matches)} new linkages.")
                    else:
                        logger.info("⚠️ All matches already exist in DB.")

    except Exception as e:
        logger.error(f"Backfill Error: {e}", exc_info=True)

if __name__ == "__main__":
    backfill_linkages()
