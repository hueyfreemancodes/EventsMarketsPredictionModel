"""
Opponent-Adjusted Efficiency Model

Uses Ridge Regression to adjust team ORtg/DRtg for schedule strength.
A team's raw 120 ORtg might look elite, but if they played the Wizards
and Hornets back-to-back, their true strength is lower.

This module builds a strength-of-schedule matrix from game logs and
solves for each team's "true" offensive and defensive rating.
"""

import numpy as np
import logging
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger("OpponentAdjustment")

try:
    from sklearn.linear_model import Ridge
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


class OpponentAdjustedEfficiency:
    """
    Calculates opponent-adjusted ORtg/DRtg using a simple ridge regression
    approach to control for schedule strength.
    
    The key insight: in a zero-sum game, every team's offense plays against
    another team's defense. By solving the system simultaneously, we can
    separate a team's true strength from the quality of opponents they faced.
    """
    
    def __init__(self, alpha: float = 100.0):
        """
        Args:
            alpha: Ridge regression regularization strength.
                   Higher = more regression toward league average.
                   100.0 is a reasonable default for 82-game NBA seasons.
        """
        self.alpha = alpha
        self.enabled = SKLEARN_AVAILABLE
        self._adjusted_ratings: Dict[int, Dict[str, float]] = {}
    
    def fit_from_game_logs(self, game_logs: List[Dict]) -> Dict[int, Dict[str, float]]:
        """
        Fit opponent-adjusted ratings from a list of game results.
        
        Args:
            game_logs: List of dicts, each with:
                - 'home_id': int (team ID)
                - 'away_id': int (team ID)  
                - 'home_ortg': float (home team's offensive rating that game)
                - 'away_ortg': float (away team's offensive rating that game)
                - 'pace': float (game pace)
                
        Returns:
            Dict mapping team_id -> {'adj_ortg': float, 'adj_drtg': float}
        """
        if not self.enabled:
            logger.warning("sklearn not available. Returning empty ratings.")
            return {}
        
        if not game_logs:
            return {}
        
        # Extract unique team IDs
        team_ids = sorted(set(
            [g['home_id'] for g in game_logs] + [g['away_id'] for g in game_logs]
        ))
        team_idx = {tid: i for i, tid in enumerate(team_ids)}
        n_teams = len(team_ids)
        
        # Build the design matrix for offensive ratings
        # Each game creates two observations:
        #   1. Home team's ORtg = f(home_offense, away_defense)
        #   2. Away team's ORtg = f(away_offense, home_defense)
        n_games = len(game_logs)
        n_obs = n_games * 2
        
        # Feature matrix: [n_obs x (2 * n_teams)]
        # First n_teams columns = offensive strength indicators
        # Next n_teams columns = defensive weakness indicators
        X = np.zeros((n_obs, 2 * n_teams))
        y = np.zeros(n_obs)
        
        for i, game in enumerate(game_logs):
            h_idx = team_idx[game['home_id']]
            a_idx = team_idx[game['away_id']]
            
            # Observation 1: Home offense vs Away defense
            X[2*i, h_idx] = 1.0            # Home team offensive indicator
            X[2*i, n_teams + a_idx] = 1.0   # Away team defensive indicator
            y[2*i] = game.get('home_ortg', 115.0)
            
            # Observation 2: Away offense vs Home defense
            X[2*i+1, a_idx] = 1.0           # Away team offensive indicator
            X[2*i+1, n_teams + h_idx] = 1.0  # Home team defensive indicator
            y[2*i+1] = game.get('away_ortg', 115.0)
        
        # Fit ridge regression
        model = Ridge(alpha=self.alpha, fit_intercept=True)
        model.fit(X, y)
        
        # Extract coefficients
        # Intercept ≈ league average ORtg
        # First n_teams coefficients = offensive adjustments (positive = better offense)
        # Next n_teams coefficients = defensive adjustments (positive = worse defense)
        league_avg = model.intercept_
        off_coeffs = model.coef_[:n_teams]
        def_coeffs = model.coef_[n_teams:]
        
        results = {}
        for tid, idx in team_idx.items():
            adj_ortg = league_avg + off_coeffs[idx]
            adj_drtg = league_avg + def_coeffs[idx]
            results[tid] = {
                'adj_ortg': float(adj_ortg),
                'adj_drtg': float(adj_drtg),
                'adj_net': float(adj_ortg - adj_drtg),
                'raw_off_adj': float(off_coeffs[idx]),
                'raw_def_adj': float(def_coeffs[idx])
            }
        
        self._adjusted_ratings = results
        logger.info(f"Fitted opponent-adjusted ratings for {n_teams} teams from {n_games} games")
        return results
    
    def get_adjusted_rating(self, team_id: int) -> Optional[Dict[str, float]]:
        """
        Get the opponent-adjusted ratings for a specific team.
        Must call fit_from_game_logs first.
        """
        return self._adjusted_ratings.get(team_id)
    
    def blend_with_raw(self, 
                       raw_ortg: float, 
                       raw_drtg: float, 
                       adj_ortg: float, 
                       adj_drtg: float,
                       blend_weight: float = 0.6) -> Tuple[float, float]:
        """
        Blend raw and adjusted ratings. Use adjusted more heavily as the
        season progresses and the regression model has more data.
        
        Args:
            raw_ortg: Raw offensive rating
            raw_drtg: Raw defensive rating
            adj_ortg: Opponent-adjusted offensive rating
            adj_drtg: Opponent-adjusted defensive rating
            blend_weight: Weight given to adjusted ratings (0-1)
            
        Returns:
            Tuple of (blended_ortg, blended_drtg)
        """
        blended_ortg = (raw_ortg * (1 - blend_weight)) + (adj_ortg * blend_weight)
        blended_drtg = (raw_drtg * (1 - blend_weight)) + (adj_drtg * blend_weight)
        return blended_ortg, blended_drtg


def build_game_logs_from_api(nba_collector, game_date_str: str = None) -> List[Dict]:
    """
    Helper function to build game logs from the NBA API for the opponent adjustment model.
    This pulls team game logs and structures them for the ridge regression.
    
    Note: This is a simplified implementation for Phase 1.
    Phase 2 will build a proper historical dataset with per-game efficiency data.
    """
    game_logs = []
    
    try:
        from nba_api.stats.endpoints import leaguegamelog
        import time
        
        time.sleep(1.0)
        log = leaguegamelog.LeagueGameLog(
            season='2025-26',
            season_type_all_star='Regular Season',
            timeout=60
        )
        df = log.get_data_frames()[0]
        
        if df.empty:
            return game_logs
        
        # Filter to games before the target date if specified
        if game_date_str:
            from datetime import datetime
            import pandas as pd
            target_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
            df['parsed_date'] = pd.to_datetime(df['GAME_DATE']).dt.date
            df = df[df['parsed_date'] < target_date]
        
        # Group by game ID to pair home/away
        for game_id, group in df.groupby('GAME_ID'):
            if len(group) != 2:
                continue
            
            # Determine home/away by matchup string (contains "vs." for home, "@" for away)
            home_row = group[group['MATCHUP'].str.contains('vs.', na=False)]
            away_row = group[group['MATCHUP'].str.contains('@', na=False)]
            
            if home_row.empty or away_row.empty:
                continue
            
            home_row = home_row.iloc[0]
            away_row = away_row.iloc[0]
            
            # Estimate possessions for each team
            # Formula: FGA + 0.44 * FTA - OREB + TOV
            h_poss = home_row['FGA'] + 0.44 * home_row['FTA'] - home_row['OREB'] + home_row['TOV']
            a_poss = away_row['FGA'] + 0.44 * away_row['FTA'] - away_row['OREB'] + away_row['TOV']
            
            # Average pace for the game
            game_poss = (h_poss + a_poss) / 2
            
            if game_poss > 0:
                # Calculate true ORtg (Points per 100 possessions)
                home_ortg = (home_row['PTS'] / h_poss) * 100
                away_ortg = (away_row['PTS'] / a_poss) * 100
                
                game_logs.append({
                    'home_id': int(home_row['TEAM_ID']),
                    'away_id': int(away_row['TEAM_ID']),
                    'home_ortg': home_ortg,
                    'away_ortg': away_ortg,
                    'pace': game_poss
                })
        
        logger.info(f"Built {len(game_logs)} game logs for opponent adjustment model")
        
    except Exception as e:
        logger.error(f"Error building game logs: {e}")
    
    return game_logs
