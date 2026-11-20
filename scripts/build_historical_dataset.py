import os
import sys
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime, timedelta

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.data_collection.nba_api_collector import NBADataCollector
from src.models.player_bayesian_model import PlayerBayesianModel
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playergamelog, teamgamelog

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HistoricalIngestion")

class HistoricalDatasetBuilder:
    def __init__(self):
        self.nba = NBADataCollector()
        self.player_model = PlayerBayesianModel()
        self.seasons = ["2024-25", "2025-26"] 
        
    def get_top_players(self, limit=100):
        """Get high-usage players by PPG to build a high-quality training set"""
        logger.info(f"Fetching top {limit} players by PPG for {self.seasons[0]}...")
        from nba_api.stats.endpoints import leaguedashplayerstats
        try:
            # Fetch stats for the most recent completed season to find 'relevant' players
            dash = leaguedashplayerstats.LeagueDashPlayerStats(season=self.seasons[0], timeout=60)
            df = dash.get_data_frames()[0]
            # Sort by PTS DESC and take top limit
            top_df = df.sort_values(by='PTS', ascending=False).head(limit)
            
            top_players = []
            for _, row in top_df.iterrows():
                top_players.append({
                    'id': int(row['PLAYER_ID']),
                    'full_name': row['PLAYER_NAME']
                })
            return top_players
        except Exception as e:
            logger.error(f"Error fetching top players: {e}. Falling back to default list.")
            all_players = players.get_active_players()
            return all_players[:limit]

    def fetch_all_team_logs(self):
        """Fetch and cache all team logs to get opponent stats quickly"""
        logger.info("Fetching all team game logs...")
        team_logs = {}
        all_teams = teams.get_teams()
        
        for team in all_teams:
            try:
                log = teamgamelog.TeamGameLog(team_id=team['id'], season=self.season, timeout=60)
                df = log.get_data_frames()[0]
                # Convert GAME_DATE to datetime objects
                df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
                team_logs[team['id']] = df
                time.sleep(0.6) # Rate limit
            except Exception as e:
                logger.error(f"Error fetching log for team {team['abbreviation']}: {e}")
                
        return team_logs

    def calculate_rolling_team_metrics(self, team_id, game_date, team_logs):
        """Calculate ORtg/DRtg for a team BEFORE a specific date"""
        log = team_logs.get(team_id)
        if log is None or log.empty:
            return 110.0, 110.0 # Default league average
            
        # Get games before the target date
        past_games = log[log['GAME_DATE'] < game_date]
        if past_games.empty:
            return 110.0, 110.0
            
        # Simplified ORtg/DRtg from raw points for the sake of speed
        # In production, we'd use advanced stats endpoints
        avg_pts = past_games['PTS'].mean()
        avg_opp_pts = past_games.get('OPP_PTS', pd.Series([110]*len(past_games))).mean()
        
        return avg_pts, avg_opp_pts

    def build_dataset(self, player_limit=20):
        """Build the historical dataset"""
        top_players = self.get_top_players(limit=player_limit)
        all_rows = []

        for season in self.seasons:
            logger.info(f"=== Starting Ingestion for Season {season} ===")
            
            for p in top_players:
                logger.info(f"Processing historical data for {p['full_name']} ({season})...")
                try:
                    # 1. Get full season game log
                    log_call = playergamelog.PlayerGameLog(player_id=p['id'], season=season, timeout=60)
                    df = log_call.get_data_frames()[0]
                    if df.empty: continue
                    
                    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
                    # Sort by date ascending to process chronologically
                    df = df.sort_values('GAME_DATE')
                    
                    # 2. Iterate through games (starting from game 5 to have a baseline)
                    for i in range(5, len(df)):
                        game_row = df.iloc[i]
                        game_date = game_row['GAME_DATE']
                        past_games = df.iloc[:i] # All games before this one
                        
                        # Target Values (What actually happened)
                        actual_pts = game_row['PTS']
                        
                        # Feature: Rolling Averages
                        recent_10 = past_games.tail(10)
                        
                        # Calculate Rest Days
                        last_game_date = past_games.iloc[-1]['GAME_DATE']
                        rest_days = (game_date - last_game_date).days
                        rest_days = min(rest_days, 4) # Cap at 4+ days
                        
                        # Home/Away
                        is_home = 0 if '@' in game_row['MATCHUP'] else 1
                        
                        features = {
                            'player_id': p['id'],
                            'player_name': p['full_name'],
                            'season': season,
                            'game_date': game_date.strftime('%Y-%m-%d'),
                            'is_home': is_home,
                            'rest_days': rest_days,
                            'season_avg_pts': past_games['PTS'].mean(),
                            'season_avg_reb': past_games['REB'].mean(),
                            'season_avg_ast': past_games['AST'].mean(),
                            'season_avg_fg3m': past_games['FG3M'].mean(),
                            'recent_avg_pts': recent_10['PTS'].mean(),
                            'recent_avg_reb': recent_10['REB'].mean(),
                            'recent_avg_ast': recent_10['AST'].mean(),
                            'recent_avg_fg3m': recent_10['FG3M'].mean(),
                            'recent_avg_min': recent_10['MIN'].mean(),
                            'season_avg_min': past_games['MIN'].mean(),
                            'std_pts': past_games['PTS'].std() if len(past_games) > 1 else 0,
                            'true_usage': (past_games['FGA'].mean() + 0.44 * past_games['FTA'].mean()) / past_games['MIN'].mean() if past_games['MIN'].mean() > 0 else 0,
                            'prop_line': past_games['PTS'].mean(), 
                        }
                        
                        features['target_pts'] = 1 if actual_pts > features['prop_line'] else 0
                        features['actual_pts'] = actual_pts
                        
                        prediction = self.player_model.predict_prop(
                            stats=features,
                            line=features['prop_line'],
                            metric='player_points'
                        )
                        features['bayesian_prob'] = prediction.prob_over
                        all_rows.append(features)
                        
                    # Save incrementally after each player
                    if all_rows:
                        temp_df = pd.DataFrame(all_rows)
                        output_path = os.path.join(project_root, 'data/historical_training_set.csv')
                        if not os.path.exists(output_path):
                            temp_df.to_csv(output_path, index=False)
                        else:
                            temp_df.to_csv(output_path, mode='a', header=False, index=False)
                        all_rows = [] # Clear the buffer
                        
                    time.sleep(0.6) # Rate limit
                except Exception as e:
                    logger.error(f"Error processing {p['full_name']} in {season}: {e}")
                
        return True

if __name__ == "__main__":
    builder = HistoricalDatasetBuilder()
    # Scale up to 100 players for a robust training set
    builder.build_dataset(player_limit=100)
