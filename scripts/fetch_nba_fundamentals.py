#!/usr/bin/env python3
"""
NBA Fundamentals Fetcher
------------------------
Fetches game headers and team stats for a date range (defaults to upcoming week).
Ingests data into QuestDB 'sports_fundamentals' table.
"""
import sys
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_collection.nba_api_collector import NBADataCollector
from src.data_collection.ingester import QuestDBIngester
from nba_api.stats.endpoints import scoreboardv2

# Config
DAYS_TO_FETCH = 7
START_DATE_OFFSET = -1 # Start from yesterday

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NBAFundamentals")

def get_team_abbr_by_id(team_id: int, collector: NBADataCollector) -> Optional[str]:
    """Reverse lookup team ID to abbreviation using collector's map."""
    if not collector._team_id_map:
        return None
        
    for abbr, info in collector._team_id_map.items():
        if info.get('id') == team_id:
            return abbr
    return None

def build_game_record(game_row: pd.Series, home_stats: Dict, away_stats: Dict, date_obj: datetime) -> Dict:
    """Constructs the fundamental feature record."""
    home_abbr = home_stats.get('abbr') 
    away_abbr = away_stats.get('abbr')
    
    # Defaults for extensive schema
    return {
        'timestamp': datetime.now(),
        'event_id': f"NBA_{home_abbr}_{away_abbr}_{date_obj.strftime('%Y%m%d')}",
        'sport': 'NBA',
        'league': 'NBA',
        'home_team': home_abbr,
        'away_team': away_abbr,
        'game_date': date_obj,
        
        # Home Stats
        'home_win_pct': home_stats.get('win_pct', 0),
        'home_avg_score': home_stats.get('avg_points_scored', 0),
        'home_avg_points_allowed': home_stats.get('avg_points_allowed', 0),
        'home_avg_point_diff': home_stats.get('avg_point_diff', 0),
        'home_home_win_pct': home_stats.get('home_win_pct', 0),
        'home_last_3_wins': home_stats.get('last_3_wins', 0),
        'home_last_5_wins': home_stats.get('last_5_wins', 0),
        
        # Away Stats
        'away_win_pct': away_stats.get('win_pct', 0),
        'away_avg_score': away_stats.get('avg_points_scored', 0),
        'away_avg_points_allowed': away_stats.get('avg_points_allowed', 0),
        'away_avg_point_diff': away_stats.get('avg_point_diff', 0),
        'away_away_win_pct': away_stats.get('away_win_pct', 0),
        'away_last_3_wins': away_stats.get('last_3_wins', 0),
        'away_last_5_wins': away_stats.get('last_5_wins', 0),
        
        # Fillers (Schema requirements)
        'home_point_diff_std': 0.0,
        'away_point_diff_std': 0.0,
        'is_home_back2back': False,
        'is_away_back2back': False,
        'travel_distance': 0.0,
        'rest_days_home': 0,
        'rest_days_away': 0,
        'altitude_diff': 0.0,
        'injuries_home': '[]',
        'injuries_away': '[]',
        'lineup_home': '[]',
        'lineup_away': '[]'
    }

def process_date(target_date: datetime, collector: NBADataCollector, ingester: QuestDBIngester):
    """Fetches and ingests games for a single date."""
    date_str = target_date.strftime('%Y-%m-%d')
    logger.info(f"Checking games for {date_str}...")
    
    try:
        board = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=30)
        games_df = board.game_header.get_data_frame()
        
        if games_df.empty:
            logger.info(f"No games found for {date_str}.")
            return

        logger.info(f"Found {len(games_df)} games.")
        
        batch = []
        for _, game in games_df.iterrows():
            home_id = game['HOME_TEAM_ID']
            away_id = game['VISITOR_TEAM_ID']
            
            home_abbr = get_team_abbr_by_id(home_id, collector)
            away_abbr = get_team_abbr_by_id(away_id, collector)
            
            if not home_abbr or not away_abbr:
                logger.warning(f"Could not map IDs: {home_id} vs {away_id}")
                continue
                
            # Fetch Stats
            h_stats = collector.fetch_team_stats(home_abbr) or {}
            a_stats = collector.fetch_team_stats(away_abbr) or {}
            
            # Inject Abbr for record builder
            h_stats['abbr'] = home_abbr
            a_stats['abbr'] = away_abbr
            
            if not h_stats or not a_stats:
                logger.warning(f"Skipping {home_abbr} vs {away_abbr} due to missing stats.")
                continue

            record = build_game_record(game, h_stats, a_stats, target_date)
            batch.append(record)
            
            logger.info(f"  Prepared: {home_abbr} vs {away_abbr}")
            time.sleep(0.5) # Throttle API calls
            
        if batch:
            ingester.ingest_sports_fundamentals_batch(batch)
            logger.info(f"✅ Ingested {len(batch)} games for {date_str}")
            
    except Exception as e:
        logger.error(f"Failed to process {date_str}: {e}")

def main():
    logger.info("Initializing NBA Fundamentals Fetcher...")
    
    collector = NBADataCollector()
    ingester = QuestDBIngester()
    
    start_date = datetime.now() + timedelta(days=START_DATE_OFFSET)
    
    try:
        for i in range(DAYS_TO_FETCH):
            target_date = start_date + timedelta(days=i)
            process_date(target_date, collector, ingester)
            time.sleep(1) # Gap between days
            
        logger.info("Update Complete.")
        
    finally:
        ingester.close()

if __name__ == "__main__":
    main()
