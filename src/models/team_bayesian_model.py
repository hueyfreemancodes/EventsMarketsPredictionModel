import numpy as np
from scipy.stats import norm
import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger("TeamParametricModel")

class TeamParametricModel:
    """
    Parametric model for projecting NBA Game Lines (Spreads, Moneylines, Totals).
    Uses team advanced metrics (Offensive Rating, Defensive Rating, Pace) to estimate 
    score differentials and probability distributions.
    
    Note: This is a deterministic parametric model (point estimate → normal CDF),
    not a Bayesian model with priors/posteriors. Named accordingly.
    """
    
    # Baseline constants anchored to a "typical" 220-point NBA game
    BASE_SPREAD_STD = 13.0
    BASE_TOTAL_STD = 16.5
    BASE_TOTAL_ANCHOR = 220.0
    
    def __init__(self, league_avg_ortg: float = 115.0, league_avg_pace: float = 99.0):
        self.league_avg_ortg = league_avg_ortg
        self.league_avg_pace = league_avg_pace

    def _dynamic_std(self, projected_total: float, base_std: float) -> float:
        """
        Scale standard deviation dynamically based on projected total.
        Higher-paced, higher-scoring games have wider outcome distributions.
        Uses square-root scaling to avoid over-correction.
        """
        return base_std * (projected_total / self.BASE_TOTAL_ANCHOR) ** 0.5

    def project_score(self, team_ortg: float, opp_drtg: float, expected_pace: float) -> float:
        """
        Calculate expected points for a team in a matchup.
        Formula: ((Team ORtg + Opp DRtg) / 2) * (Pace / 100)
        """
        # Blend the team's offensive efficiency with the opponent's defensive efficiency
        true_efficiency = (team_ortg + opp_drtg) / 2
        return true_efficiency * (expected_pace / 100)

    def calculate_game_projections(self, 
                                   home_stats: Dict[str, float], 
                                   away_stats: Dict[str, float],
                                   home_rest_days: int = 2,
                                   away_rest_days: int = 2,
                                   is_high_altitude: bool = False) -> Dict[str, float]:
        """
        Projects the final score for both teams.
        Includes adjustments for home-court advantage, fatigue, and altitude.
        """
        
        home_ortg = home_stats.get('ortg', self.league_avg_ortg)
        home_drtg = home_stats.get('drtg', self.league_avg_ortg)
        home_pace = home_stats.get('pace', self.league_avg_pace)
        
        away_ortg = away_stats.get('ortg', self.league_avg_ortg)
        away_drtg = away_stats.get('drtg', self.league_avg_ortg)
        away_pace = away_stats.get('pace', self.league_avg_pace)

        # Industry-standard pace projection:
        # Fast teams dictate pace more than slow teams resist it.
        # Formula: (Team_Pace * Opp_Pace) / League_Avg_Pace
        expected_pace = (home_pace * away_pace) / self.league_avg_pace

        # Home Court Advantage (modern NBA ≈ 2.0 - 2.5 point margin swing)
        # We apply this as an asymmetrical boost to account for both spread and total increase.
        home_ortg += 1.25
        away_ortg -= 0.75

        # Fatigue adjustments
        if home_rest_days == 0: # Back-to-back
            home_ortg -= 1.5
            home_drtg += 1.5 # Defense gets worse
        if away_rest_days == 0:
            away_ortg -= 1.5
            away_drtg += 1.5

        # Altitude adjustment (Denver/Utah) - penalizes away team more
        if is_high_altitude:
            away_ortg -= 1.0
            away_drtg += 1.0

        # Project scores
        home_proj_pts = self.project_score(home_ortg, away_drtg, expected_pace)
        away_proj_pts = self.project_score(away_ortg, home_drtg, expected_pace)

        home_margin = home_proj_pts - away_proj_pts
        projected_total = home_proj_pts + away_proj_pts

        return {
            'home_proj_pts': home_proj_pts,
            'away_proj_pts': away_proj_pts,
            'home_margin': home_margin,       # Positive = Home wins
            'projected_total': projected_total,
            'expected_pace': expected_pace
        }

    def evaluate_spread(self, projected_margin: float, line: float, side: str,
                        projected_total: float = None) -> float:
        """
        Evaluates a point spread bet using a normal distribution.
        Standard deviation scales dynamically with the projected total.
        """
        spread_std = self._dynamic_std(
            projected_total or self.BASE_TOTAL_ANCHOR, self.BASE_SPREAD_STD
        )
        
        target_margin = -line
        prob_cover = norm.sf(target_margin, loc=projected_margin, scale=spread_std)
        return prob_cover

    def evaluate_moneyline(self, projected_margin: float, 
                           projected_total: float = None) -> float:
        """
        Calculates the probability of winning the game straight up (Margin > 0).
        """
        spread_std = self._dynamic_std(
            projected_total or self.BASE_TOTAL_ANCHOR, self.BASE_SPREAD_STD
        )
        prob_win = norm.sf(0, loc=projected_margin, scale=spread_std)
        return prob_win

    def evaluate_total(self, projected_total: float, line: float) -> Tuple[float, float]:
        """
        Evaluates an Over/Under total points bet.
        """
        total_std = self._dynamic_std(projected_total, self.BASE_TOTAL_STD)
        prob_over = norm.sf(line, loc=projected_total, scale=total_std)
        prob_under = norm.cdf(line, loc=projected_total, scale=total_std)
        return prob_over, prob_under

    def regress_to_market(self, model_value: float, market_value: float, 
                          regression_weight: float = 0.40) -> float:
        """
        Blend our model's projection toward the market (Vegas) line.
        
        Even the best models in the world regress toward the market because:
        - Vegas lines incorporate private information (injuries, sharp money, matchup coaching)
        - The market is extremely efficient (closing lines are ~97% accurate)
        - Our model's systematic biases are reduced by anchoring to reality
        
        Args:
            model_value: Our raw projection (e.g., projected total or margin)
            market_value: The Vegas line (e.g., total of 215.5 or spread of -7.5)
            regression_weight: How much to pull toward market (0.40 = 40% market, 60% model)
        
        Returns:
            Blended projection
        """
        return model_value * (1 - regression_weight) + market_value * regression_weight

    def apply_playoff_adjustment(self, projections: dict, is_playoff: bool = True) -> dict:
        """
        Adjust projections for playoff context.
        
        Historical data shows playoff games average:
        - ~3-5 fewer possessions per game (tighter rotations, more half-court sets)
        - ~4-8 fewer total points (better defensive effort, no load management)
        - Tighter margins (better teams, more competitive series)
        
        We model this as a 4% pace reduction and 1.5-point DRtg improvement.
        """
        if not is_playoff:
            return projections
        
        # Reduce projected total by ~4% to account for playoff pace/defense
        playoff_factor = 0.96
        projections['home_proj_pts'] *= playoff_factor
        projections['away_proj_pts'] *= playoff_factor
        projections['projected_total'] = projections['home_proj_pts'] + projections['away_proj_pts']
        projections['home_margin'] = projections['home_proj_pts'] - projections['away_proj_pts']
        
        return projections

