"""
NBA Data Collector using nba_api
Replaces sportsipy with nba_api for more reliable NBA data collection
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import time
import pandas as pd

# Standardize non-standard abbreviations from odds providers
TEAM_ABBREV_MAP = {
    'BRK': 'BKN',  # Brooklyn
    'PHO': 'PHX',  # Phoenix
    'CHO': 'CHA',  # Charlotte
    'NOP': 'NOP',  # No change but sometimes confused with NO
    'GS':  'GSW',  # Golden State
    'NY':  'NYK',  # New York
    'SA':  'SAS',  # San Antonio
}

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
        TeamDashboardByGeneralSplits,
        LeagueDashTeamStats,
        scoreboardv2
    )
    from nba_api.stats.static import teams, players
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
        self._team_id_map = None  # Cache team ID mappings
        self.cache_file = os.path.join(os.path.dirname(__file__), '../../data/nba_stats_cache.json')
        self._load_cache()
        
        if not NBA_API_AVAILABLE:
            logger.warning("nba_api not available. Install with: pip install nba-api")
            self.enabled = False
        else:
            self.enabled = True
            self._build_team_id_map()
            logger.info("NBA data collector initialized (using nba_api)")

    def _load_cache(self):
        """Load stats from local JSON cache"""
        self.team_stats_cache = {}
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    # Convert string timestamps back to datetime
                    for k, v in data.items():
                        ts = datetime.fromisoformat(v[0])
                        self.team_stats_cache[k] = (ts, v[1])
                logger.info(f"Loaded {len(self.team_stats_cache)} items from NBA cache")
            except Exception as e:
                logger.error(f"Error loading NBA cache: {e}")

    def _save_cache(self):
        """Save stats to local JSON cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            serializable = {}
            for k, v in self.team_stats_cache.items():
                serializable[k] = (v[0].isoformat(), v[1])
            with open(self.cache_file, 'w') as f:
                json.dump(serializable, f)
        except Exception as e:
            logger.error(f"Error saving NBA cache: {e}")
    
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
        
        # Standardize abbreviation
        std_abbrev = team_abbrev.upper()
        if std_abbrev in TEAM_ABBREV_MAP:
            std_abbrev = TEAM_ABBREV_MAP[std_abbrev]
            
        team_info = self._team_id_map.get(std_abbrev)
        return team_info['id'] if team_info else None
    
    def fetch_team_stats(self, team_abbrev: str, season: str = None, game_date_str: str = None) -> Optional[Dict]:
        """
        Fetch recent team performance statistics
        
        Args:
            team_abbrev: Team abbreviation (e.g., 'LAL')
            season: Season in format 'YYYY-YY' (e.g., '2023-24'). If None, uses current season
            game_date_str: Target game date in format 'YYYY-MM-DD'. If provided, stats are limited to games before this date.
        
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
        cache_key = f"{team_abbrev}_{season}_{self.lookback_games}_{game_date_str}"
        if cache_key in self.team_stats_cache:
            cache_time, cached_data = self.team_stats_cache[cache_key]
            if (datetime.now() - cache_time).seconds < 3600:  # 1 hour cache
                logger.debug(f"Using cached team stats for {team_abbrev}")
                return cached_data
        
        try:
            logger.info(f"Fetching NBA team stats for {team_abbrev} (season {season})...")
            
            # Format date_to_nullable for API (MM/DD/YYYY)
            date_to = None
            if game_date_str:
                dt = datetime.strptime(game_date_str, "%Y-%m-%d")
                yesterday = dt - timedelta(days=1)
                date_to = yesterday.strftime("%m/%d/%Y")
            
            # Use TeamDashboardByGeneralSplits - this endpoint actually returns data!
            # Add delay to avoid rate limiting
            time.sleep(1.0)
            
            try:
                # Set a longer timeout for the request
                dashboard = TeamDashboardByGeneralSplits(
                    team_id=team_id, 
                    season=season,
                    date_to_nullable=date_to,
                    timeout=60
                )
            except Exception as e:
                logger.warning(f"Error with season {season}, trying current season: {e}")
                dashboard = TeamDashboardByGeneralSplits(team_id=team_id, date_to_nullable=date_to, timeout=60)
            
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
                
                # Filter game log if date is provided
                if game_date_str:
                    target_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                    recent_games_df['parsed_date'] = pd.to_datetime(recent_games_df['GAME_DATE']).dt.date
                    recent_games_df = recent_games_df[recent_games_df['parsed_date'] < target_date]
            except:
                recent_games_df = None
            
            # Get overall stats from dashboard
            overall_stats = df.iloc[0] if len(df) > 0 else None
            
            # Calculate statistics from dashboard data
            stats = {}
            
            if overall_stats is not None:
                stats['win_pct'] = overall_stats.get('W_PCT', 0.0) if 'W_PCT' in overall_stats else 0.0
                stats['avg_points_scored'] = overall_stats.get('PTS', 0.0) / overall_stats.get('GP', 1) if 'PTS' in overall_stats and 'GP' in overall_stats else 0.0
                
                # OPP_PTS is often missing in TeamDashboardByGeneralSplits, calculate from PLUS_MINUS
                if 'OPP_PTS' in overall_stats:
                    stats['avg_points_allowed'] = overall_stats.get('OPP_PTS', 0.0) / overall_stats.get('GP', 1)
                elif 'PLUS_MINUS' in overall_stats:
                    # PTS - PLUS_MINUS = OPP_PTS
                    opp_pts = overall_stats.get('PTS', 0.0) - overall_stats.get('PLUS_MINUS', 0.0)
                    stats['avg_points_allowed'] = opp_pts / overall_stats.get('GP', 1)
                else:
                    stats['avg_points_allowed'] = 114.5 # League baseline fallback
                
                stats['avg_point_diff'] = stats['avg_points_scored'] - stats['avg_points_allowed']
                
                # Include pace if available (it might be in some variants of this endpoint)
                if 'PACE' in overall_stats:
                    stats['pace'] = overall_stats.get('PACE')
                else:
                    stats['pace'] = 98.5 # League baseline fallback
            
            # Get recent game-by-game stats if available
            if recent_games_df is not None and len(recent_games_df) > 0:
                # Apply Garbage Time Filter (Tier 3.4)
                # Removes extreme blowouts (>25pts) to keep efficiency metrics clean
                recent_games_df = self.filter_garbage_time(recent_games_df)
                
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
            
            # Cache results and save to disk
            self.team_stats_cache[cache_key] = (datetime.now(), stats)
            self._save_cache()
            
            num_games = len(recent_games_df) if recent_games_df is not None else 0
            logger.info(f"Fetched stats for {team_abbrev}: {stats.get('win_pct', 0):.2%} win rate")
            return stats
            
        except Exception as e:
            logger.error(f"Error fetching team stats for {team_abbrev}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def fetch_player_id(self, player_name: str) -> Optional[int]:
        """Get player ID from name"""
        if not NBA_API_AVAILABLE:
            return None
        
        try:
            # Search for player
            player_list = players.find_players_by_full_name(player_name)
            if not player_list:
                # Try case-insensitive or partial match if full name fails
                player_list = [p for p in players.get_players() if player_name.lower() in p['full_name'].lower()]
            
            if player_list:
                # Return first active player if possible, else first match
                active_players = [p for p in player_list if p.get('is_active')]
                return active_players[0]['id'] if active_players else player_list[0]['id']
            
            return None
        except Exception as e:
            logger.error(f"Error fetching player ID for {player_name}: {e}")
            return None

    def get_team_roster(self, team_abbrev: str) -> List[Dict]:
        """Get current roster for a team"""
        if not self.enabled:
            return []
        
        team_id = self.get_team_id(team_abbrev)
        if not team_id:
            return []
            
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id)
            df = roster.get_data_frames()[0]
            
            players_list = []
            for _, row in df.iterrows():
                players_list.append({
                    'player_id': row.get('PLAYER_ID'),
                    'name': row.get('PLAYER'),
                    'number': row.get('NUM'),
                    'position': row.get('POSITION'),
                    'height': row.get('HEIGHT'),
                    'weight': row.get('WEIGHT')
                })
            return players_list
        except Exception as e:
            logger.error(f"Error fetching roster for {team_abbrev}: {e}")
            return []

    def fetch_player_stats(self, player_name: str, season: str = None, game_date_str: str = None) -> Optional[Dict]:
        """
        Fetch player's season averages and last-N game stats
        """
        if not self.enabled:
            return None
            
        player_id = self.fetch_player_id(player_name)
        if not player_id:
            logger.warning(f"Player {player_name} not found")
            return None

        # Use current season if not specified
        if season is None:
            current_date = datetime.now()
            if current_date.month >= 10:
                season = f"{current_date.year}-{str(current_date.year + 1)[-2:]}"
            else:
                season = f"{current_date.year - 1}-{str(current_date.year)[-2:]}"

        try:
            time.sleep(0.6) # Rate limit protection
            
            # Fetch game log for recent form
            log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            df = log.get_data_frames()[0]
            
            # Ensure we filter by date to avoid look-ahead bias
            if game_date_str:
                target_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                df['parsed_date'] = pd.to_datetime(df['GAME_DATE']).dt.date
                df = df[df['parsed_date'] < target_date]
            
            if df.empty:
                logger.warning(f"No game log found for {player_name} in {season}")
                return None
                
            recent_games = df.head(self.lookback_games)
            
            stats = {
                'player_id': player_id,
                'name': player_name,
                'team_abbr': df.iloc[0]['MATCHUP'][:3] if (not df.empty and 'MATCHUP' in df.columns) else None,
                'games_played': len(df),
                # Season Averages (from whole log)
                'season_avg_pts': df['PTS'].mean(),
                'season_avg_reb': df['REB'].mean(),
                'season_avg_ast': df['AST'].mean(),
                'season_avg_fg3m': df['FG3M'].mean(),
                'season_avg_min': df['MIN'].mean(),
                'season_avg_fga': df['FGA'].mean(),
                'season_avg_fg3a': df['FG3A'].mean(),
                'season_avg_fta': df['FTA'].mean(),
                'season_fg_pct': df['FG_PCT'].mean(),
                'season_fg3_pct': df['FG3_PCT'].mean(),
                'season_ft_pct': df['FT_PCT'].mean(),
                # Recent Form (Lookback games)
                'recent_avg_pts': recent_games['PTS'].mean(),
                'recent_avg_reb': recent_games['REB'].mean(),
                'recent_avg_ast': recent_games['AST'].mean(),
                'recent_avg_fg3m': recent_games['FG3M'].mean(),
                'recent_avg_min': recent_games['MIN'].mean(),
                'recent_avg_fga': recent_games['FGA'].mean(),
                'recent_avg_fg3a': recent_games['FG3A'].mean(),
                'recent_avg_fta': recent_games['FTA'].mean(),
                'recent_fg_pct': recent_games['FG_PCT'].mean(),
                'recent_fg3_pct': recent_games['FG3_PCT'].mean(),
                'recent_ft_pct': recent_games['FT_PCT'].mean(),
                # Consistency (Standard Deviation)
                'std_pts': recent_games['PTS'].std() if len(recent_games) > 1 else 0,
                'std_reb': recent_games['REB'].std() if len(recent_games) > 1 else 0,
                'std_ast': recent_games['AST'].std() if len(recent_games) > 1 else 0,
                'std_fg3m': recent_games['FG3M'].std() if len(recent_games) > 1 else 0,
            }
            
            logger.info(f"Fetched stats for {player_name}: {stats['season_avg_pts']:.1f} PPG")
            return stats
            
        except Exception as e:
            logger.error(f"Error fetching stats for player {player_name}: {e}")
            return None

    def get_team_context(self, team_abbrev: str, game_date_str: str = None) -> Dict:
        """
        Get contextual variables like Rest Days (B2B) and Altitude for a team.
        Returns {'rest_days': int, 'altitude': bool}
        """
        if not self.enabled: return {'rest_days': 2, 'altitude': False}
        
        # Altitude check
        is_altitude = team_abbrev.upper() in ['DEN', 'UTA']
        
        try:
            team_id = self.get_team_id(team_abbrev)
            if not team_id: return {'rest_days': 2, 'altitude': is_altitude}
            
            # Rate limit protection
            time.sleep(0.6)
            
            log = teamgamelog.TeamGameLog(team_id=team_id)
            df = log.get_data_frames()[0]
            
            if game_date_str:
                target_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
            else:
                target_date = datetime.now().date()
            
            # The 'GAME_DATE' format is usually 'MMM DD, YYYY'
            df['parsed_date'] = pd.to_datetime(df['GAME_DATE']).dt.date
            
            past_games = df[df['parsed_date'] < target_date]
            if not past_games.empty:
                last_game_date = past_games.iloc[0]['parsed_date']
                rest_days = (target_date - last_game_date).days - 1
            else:
                rest_days = 3 # Default rest
            
            return {
                'rest_days': max(0, rest_days),
                'altitude': is_altitude
            }
            
        except Exception as e:
            logger.error(f"Error fetching team context for {team_abbrev}: {e}")
            return {'rest_days': 2, 'altitude': is_altitude}

    def get_todays_games(self, date_str: str = None) -> List[Dict]:
        """Get today's NBA games"""
        if not self.enabled:
            return []
        
        try:
            if not date_str:
                # Default to today's local date
                date_str = datetime.now().strftime("%Y-%m-%d")
                
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=60)
            sb_dict = sb.get_dict()
            games = sb_dict['resultSets'][0]['rowSet']
            headers = sb_dict['resultSets'][0]['headers']
            
            def get_val(row, field):
                return row[headers.index(field)]
            
            result = []
            for game in games:
                game_id = get_val(game, 'GAME_ID')
                home_id = get_val(game, 'HOME_TEAM_ID')
                away_id = get_val(game, 'VISITOR_TEAM_ID')
                
                home_team_info = teams.find_team_name_by_id(home_id)
                away_team_info = teams.find_team_name_by_id(away_id)
                
                result.append({
                    'game_id': game_id,
                    'home_team': home_team_info['nickname'] if home_team_info else str(home_id),
                    'away_team': away_team_info['nickname'] if away_team_info else str(away_id),
                    'home_abbrev': home_team_info['abbreviation'] if home_team_info else str(home_id),
                    'away_abbrev': away_team_info['abbreviation'] if away_team_info else str(away_id),
                    'game_time': get_val(game, 'GAME_STATUS_TEXT'),
                    'status': get_val(game, 'GAME_STATUS_TEXT'),
                })
            
            logger.info(f"Found {len(result)} games for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching today's games: {e}")
            return []

    def get_team_advanced_stats(self, game_date_str: str = None) -> Dict[int, Dict[str, float]]:
        """
        Fetch advanced team metrics (ORtg, DRtg, Pace) for all teams.
        Returns a dictionary mapping team_id to their stats.
        """
        if not self.enabled: return {}
        try:
            from nba_api.stats.endpoints import LeagueDashTeamStats
            # Using 'Advanced' measure type to get ORtg, DRtg, Pace
            logger.info("Fetching advanced team metrics for all teams")
            
            # If date is provided, format it as MM/DD/YYYY for the API
            date_to = None
            if game_date_str:
                dt = datetime.strptime(game_date_str, "%Y-%m-%d")
                # We want stats BEFORE the game date
                yesterday = dt - timedelta(days=1)
                date_to = yesterday.strftime("%m/%d/%Y")
            
            stats = LeagueDashTeamStats(measure_type_detailed_defense='Advanced', date_to_nullable=date_to)
            df = stats.get_data_frames()[0]
            
            team_stats = {}
            for _, row in df.iterrows():
                team_stats[row['TEAM_ID']] = {
                    'team_name': row['TEAM_NAME'],
                    'ortg': row['OFF_RATING'],
                    'drtg': row['DEF_RATING'],
                    'pace': row['PACE'],
                    'efg_pct': row.get('EFG_PCT', 0.5),
                    'tov_pct': row.get('TM_TOV_PCT', 0.15)
                }
            return team_stats
        except Exception as e:
            logger.error(f"Error fetching team advanced stats: {e}")
            return {}

    def get_game_results(self, date_str: str) -> Dict[str, Any]:
        """
        Fetch game results for a specific date (YYYY-MM-DD)
        Returns: { 'TEAM_ABBR': { 'win': True/False, 'score': X, 'opp_score': Y } }
        """
        if not self.enabled: return {}
        
        from nba_api.stats.endpoints import leaguegamefinder
        try:
            # Format date for API
            game_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
            finder = leaguegamefinder.LeagueGameFinder(date_from_nullable=game_date, date_to_nullable=game_date)
            games = finder.get_data_frames()[0]
            
            results = {}
            for _, row in games.iterrows():
                abbr = row['TEAM_ABBREVIATION']
                wl = row['WL']
                results[abbr] = {
                    'win': wl == 'W',
                    'score': row['PTS'],
                    'opp_abbr': row['MATCHUP'].split()[-1],
                    'date': date_str
                }
            return results
        except Exception as e:
            logger.error(f"Error fetching game results for {date_str}: {e}")
            return {}

    def fetch_player_actuals(self, date_str: str) -> Dict[str, Dict[str, float]]:
        """
        Fetch actual player stats for a specific date
        Returns: { 'Player Name': { 'pts': X, 'reb': Y, 'ast': Z, 'fg3m': W } }
        """
        if not self.enabled: return {}
        
        from nba_api.stats.endpoints import leaguegamefinder
        try:
            game_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
            # player_or_team='P' for players
            finder = leaguegamefinder.LeagueGameFinder(date_from_nullable=game_date, date_to_nullable=game_date, player_or_team_abbreviation='P')
            player_stats = finder.get_data_frames()[0]
            
            actuals = {}
            for _, row in player_stats.iterrows():
                name = row['PLAYER_NAME']
                actuals[name] = {
                    'pts': float(row['PTS']),
                    'reb': float(row['REB']),
                    'ast': float(row['AST']),
                    'fg3m': float(row['FG3M'])
                }
            return actuals
        except Exception as e:
            logger.error(f"Error fetching player actuals for {date_str}: {e}")
            return {}

    def filter_garbage_time(self, game_log_df: 'pd.DataFrame', 
                            margin_threshold: int = 25,
                            minutes_remaining_threshold: float = 6.0) -> 'pd.DataFrame':
        """
        Filter out garbage time games/rows from a team game log.
        
        Garbage time is defined as when a team won or lost by more than
        `margin_threshold` points. In these games, the final margin suggests
        that bench players got significant run in the 4th quarter, polluting
        per-game efficiency metrics.
        
        This is a simplified heuristic — true garbage time filtering would 
        require play-by-play data with win probability tracking. This approximation
        removes the most extreme blowouts from the rolling window.
        
        Args:
            game_log_df: DataFrame with at least 'PTS' and 'PLUS_MINUS' columns
            margin_threshold: Absolute margin to flag as garbage time (default 25)
            minutes_remaining_threshold: Not used in this heuristic (for future PBP)
            
        Returns:
            Filtered DataFrame with garbage time games removed
        """
        if game_log_df is None or game_log_df.empty:
            return game_log_df
        
        if 'PLUS_MINUS' not in game_log_df.columns:
            logger.debug("No PLUS_MINUS column found, skipping garbage time filter")
            return game_log_df
        
        original_len = len(game_log_df)
        filtered = game_log_df[game_log_df['PLUS_MINUS'].abs() <= margin_threshold]
        removed = original_len - len(filtered)
        
        if removed > 0:
            logger.info(f"  Garbage time filter: removed {removed} blowout games (>{margin_threshold}pt margin)")
        
        # Safety: don't filter out too many games (keep at least 60% of the data)
        if len(filtered) < original_len * 0.6:
            logger.warning(f"  Garbage time filter would remove too many games ({removed}/{original_len}). Keeping all.")
            return game_log_df
        
        return filtered

if __name__ == "__main__":
    collector = NBADataCollector()
    # Test with a recent date
    test_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    results = collector.get_game_results(test_date)
    print(f"Results for {test_date}: {list(results.keys())}")
