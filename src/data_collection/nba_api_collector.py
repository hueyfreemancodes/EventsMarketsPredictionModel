"""
NBA Data Collector using nba_api
Replaces sportsipy with nba_api for more reliable NBA data collection
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.data_collection.logger import logger

# Try importing nba_api
NBA_API_AVAILABLE = False
try:
    from nba_api.stats.endpoints import (
        teamgamelog,
        playergamelog,
        commonteamroster,
        TeamDashboardByGeneralSplits
    )
    from nba_api.stats.static import teams, players
    from nba_api.live.nba.endpoints import scoreboard
    NBA_API_AVAILABLE = True
except ImportError as e:
    NBA_API_AVAILABLE = False
    # Don't log here - logger might not be initialized
    pass


class NBADataCollector:
    """Collects NBA data using nba_api library"""
    
    def __init__(self, lookback_games: int = 10):
        """
        Initialize NBA data collector
        
        Args:
            lookback_games: Number of recent games to analyze
        """
        self.lookback_games = lookback_games
        self.team_stats_cache = {}
        self.player_stats_cache = {}
        self._team_id_map = None  # Cache team ID mappings
        
        if not NBA_API_AVAILABLE:
            logger.warning("nba_api not available. Install with: pip install nba-api")
            self.enabled = False
        else:
            self.enabled = True
            self._build_team_id_map()
            logger.info("NBA data collector initialized (using nba_api)")
    
    def _build_team_id_map(self):
        """Build mapping of abbreviations to team IDs"""
        if not NBA_API_AVAILABLE:
            return
        
        try:
            nba_teams = teams.get_teams()
            self._team_id_map = {}
            for team in nba_teams:
                abbrev = team.get('abbreviation')
                if abbrev:
                    self._team_id_map[abbrev] = {
                        'id': team.get('id'),
                        'full_name': team.get('full_name'),
                        'city': team.get('city'),
                        'nickname': team.get('nickname', ''),
                    }
            logger.debug(f"Built team ID map with {len(self._team_id_map)} teams")
        except Exception as e:
            logger.error(f"Error building team ID map: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            self._team_id_map = {}
    
    def get_team_id(self, team_abbrev: str) -> Optional[int]:
        """Get team ID from abbreviation"""
        if not self._team_id_map:
            return None
        team_info = self._team_id_map.get(team_abbrev.upper())
        return team_info['id'] if team_info else None
    
    def fetch_team_stats(self, team_abbrev: str, season: str = None) -> Optional[Dict]:
        """
        Fetch recent team performance statistics
        
        Args:
            team_abbrev: Team abbreviation (e.g., 'LAL')
            season: Season in format 'YYYY-YY' (e.g., '2023-24'). If None, uses current season
        
        Returns:
            Dictionary with team statistics
        """
        if not self.enabled:
            return None
        
        # Get team ID
        team_id = self.get_team_id(team_abbrev)
        if not team_id:
            logger.warning(f"Team {team_abbrev} not found")
            return None
        
        # Use current season if not specified
        if season is None:
            current_date = datetime.now()
            if current_date.month >= 10:  # Season starts in October
                season = f"{current_date.year}-{str(current_date.year + 1)[-2:]}"
            else:
                season = f"{current_date.year - 1}-{str(current_date.year)[-2:]}"
        
        # Check cache
        cache_key = f"{team_abbrev}_{season}_{self.lookback_games}"
        if cache_key in self.team_stats_cache:
            cache_time, cached_data = self.team_stats_cache[cache_key]
            if (datetime.now() - cache_time).seconds < 3600:  # 1 hour cache
                logger.debug(f"Using cached team stats for {team_abbrev}")
                return cached_data
        
        try:
            logger.info(f"Fetching NBA team stats for {team_abbrev} (season {season})...")
            
            # Use TeamDashboardByGeneralSplits - this endpoint actually returns data!
            # Add delay to avoid rate limiting
            time.sleep(0.6)
            
            try:
                if season:
                    dashboard = TeamDashboardByGeneralSplits(team_id=team_id, season=season)
                else:
                    dashboard = TeamDashboardByGeneralSplits(team_id=team_id)
            except Exception as e:
                logger.warning(f"Error with season {season}, trying current season: {e}")
                dashboard = TeamDashboardByGeneralSplits(team_id=team_id)
            
            # Get overall stats (first dataframe)
            df = dashboard.get_data_frames()[0]
            
            if len(df) == 0:
                logger.warning(f"No stats found for {team_abbrev} in season {season}")
                return {}
            
            # Also try to get game log for recent games
            time.sleep(0.6)
            try:
                game_log = teamgamelog.TeamGameLog(team_id=team_id, season=season if season else None)
                recent_games_df = game_log.get_data_frames()[0]
            except:
                recent_games_df = None
            
            # Get overall stats from dashboard
            overall_stats = df.iloc[0] if len(df) > 0 else None
            
            # Calculate statistics from dashboard data
            stats = {}
            
            if overall_stats is not None:
                stats['win_pct'] = overall_stats.get('W_PCT', 0.0) if 'W_PCT' in overall_stats else 0.0
                stats['avg_points_scored'] = overall_stats.get('PTS', 0.0) / overall_stats.get('GP', 1) if 'PTS' in overall_stats and 'GP' in overall_stats else 0.0
                stats['avg_points_allowed'] = overall_stats.get('OPP_PTS', 0.0) / overall_stats.get('GP', 1) if 'OPP_PTS' in overall_stats and 'GP' in overall_stats else 0.0
                stats['avg_point_diff'] = stats['avg_points_scored'] - stats['avg_points_allowed']
            
            # Get recent game-by-game stats if available
            if recent_games_df is not None and len(recent_games_df) > 0:
                recent_games = recent_games_df.head(self.lookback_games)
                
                if 'WL' in recent_games.columns:
                    stats['last_3_wins'] = int((recent_games.head(3)['WL'] == 'W').sum())
                    stats['last_5_wins'] = int((recent_games.head(5)['WL'] == 'W').sum())
                else:
                    stats['last_3_wins'] = 0
                    stats['last_5_wins'] = 0
                
                # Home/away splits
                if 'MATCHUP' in recent_games.columns:
                    home_games = recent_games[recent_games['MATCHUP'].str.contains('vs.')]
                    away_games = recent_games[recent_games['MATCHUP'].str.contains('@')]
                    
                    stats['home_win_pct'] = (home_games['WL'] == 'W').mean() if len(home_games) > 0 else 0.0
                    stats['away_win_pct'] = (away_games['WL'] == 'W').mean() if len(away_games) > 0 else 0.0
                else:
                    stats['home_win_pct'] = 0.0
                    stats['away_win_pct'] = 0.0
            else:
                # Use dashboard splits if available
                splits_df = dashboard.get_data_frames()[1] if len(dashboard.get_data_frames()) > 1 else None
                if splits_df is not None and len(splits_df) > 0:
                    home_row = splits_df[splits_df['GROUP_VALUE'] == 'Home']
                    away_row = splits_df[splits_df['GROUP_VALUE'] == 'Away']
                    
                    stats['home_win_pct'] = home_row['W_PCT'].iloc[0] if len(home_row) > 0 else 0.0
                    stats['away_win_pct'] = away_row['W_PCT'].iloc[0] if len(away_row) > 0 else 0.0
                else:
                    stats['home_win_pct'] = 0.0
                    stats['away_win_pct'] = 0.0
                    stats['last_3_wins'] = 0
                    stats['last_5_wins'] = 0
            
            # Cache results
            self.team_stats_cache[cache_key] = (datetime.now(), stats)
            
            num_games = len(recent_games_df) if recent_games_df is not None else 0
            logger.info(f"Fetched stats for {team_abbrev}: {stats.get('win_pct', 0):.2%} win rate")
            return stats
            
        except Exception as e:
            logger.error(f"Error fetching team stats for {team_abbrev}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def get_todays_games(self) -> List[Dict]:
        """Get today's NBA games"""
        if not self.enabled:
            return []
        
        try:
            scoreboard_data = scoreboard.ScoreBoard()
            games = scoreboard_data.get_dict()['scoreboard']['games']
            
            result = []
            for game in games:
                result.append({
                    'game_id': game.get('gameId'),
                    'home_team': game.get('homeTeam', {}).get('teamName'),
                    'away_team': game.get('awayTeam', {}).get('teamName'),
                    'home_abbrev': game.get('homeTeam', {}).get('teamTricode'),
                    'away_abbrev': game.get('awayTeam', {}).get('teamTricode'),
                    'game_time': game.get('gameTimeUTC'),
                    'status': game.get('gameStatusText'),
                })
            
            logger.info(f"Found {len(result)} games today")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching today's games: {e}")
            return []

