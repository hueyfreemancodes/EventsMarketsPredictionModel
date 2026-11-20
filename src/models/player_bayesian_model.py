import numpy as np
from scipy.stats import norm, nbinom, binom
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class PlayerPrediction:
    player_name: str
    metric: str
    projected_mean: float
    projected_std: float
    prob_over: float
    prob_under: float
    factors: Dict[str, float]

class PlayerBayesianModel:
    """
    Upgraded Bayesian model for predicting player prop probabilities.
    Uses Normal distribution with observed standard deviation for better variance modeling.
    """
    
    def __init__(self, weights: Optional[Dict] = None):
        self.weights = weights or {
            "season_avg": 0.25,    # Reduced (Regular season is less relevant in playoffs)
            "recent_avg": 0.55,    # Increased (Playoff form matters most)
            "market_adj": 0.20
        }
        self.PLAYOFF_DEFLATION = 1.0  # Neutral (Removed 4% deflation)

    def predict_prop(self, 
                    stats: Dict, 
                    line: float, 
                    metric: str,
                    market_prob: Optional[float] = None,
                    opponent_stats: Optional[Dict] = None,
                    team_context: Optional[Dict] = None) -> PlayerPrediction:
        """
        Predict probability of over/under for a specific prop line.
        """
        m_map = {
            'player_points': 'pts',
            'player_rebounds': 'reb',
            'player_assists': 'ast',
            'player_threes': 'fg3m'
        }
        key = m_map.get(metric, metric)
        
        s_avg = stats.get(f'season_avg_{key}', 0)
        r_avg = stats.get(f'recent_avg_{key}', 0)
        std = stats.get(f'std_{key}', s_avg * 0.25) # Default to 25% CV if std missing
        if std == 0: std = s_avg * 0.25
        
        # 1. Base Projection (Dynamic Weighted Mean)
        # Weight of "season average" decays as games played increases:
        # - Early season (10 games): season_weight ≈ 0.40 (lean on full-season avg)
        # - Late season (70+ games): season_weight ≈ 0.15 (lean on recent form)
        games_played = stats.get('games_played', 30)
        # Normalize weights so they sum to (1.0 - market_adj)
        market_w = self.weights.get('market_adj', 0.20)
        remaining_w = 1.0 - market_w
        
        # season_weight decays from 0.40 to 0.15 relative to the remaining weight
        season_ratio = max(0.15, 0.45 - (games_played / 200))
        season_weight = season_ratio * remaining_w
        recent_weight = remaining_w - season_weight
        
        # If tracking data is available, blend it with the outcome average
        if metric == 'player_assists' and stats.get('potential_ast', 0) > 0:
            # Assist conversion rate is typically ~55%
            expected_ast_from_tracking = stats['potential_ast'] * 0.55
            base_projection = (s_avg * 0.2 + expected_ast_from_tracking * 0.8)
        elif metric == 'player_rebounds' and stats.get('reb_chances', 0) > 0:
            # Rebound conversion rate is typically ~60%
            expected_reb_from_tracking = stats['reb_chances'] * 0.60
            base_projection = (s_avg * 0.2 + expected_reb_from_tracking * 0.8)
        else:
            if r_avg == 0:
                base_projection = s_avg
            else:
                base_projection = (s_avg * season_weight) + (r_avg * recent_weight)
                
        # 1b. Minutes-Gated Usage Adjustment
        # Only apply usage boost if the player's minutes also increased,
        # preventing false inflation for spot-up shooters replacing injured ball-handlers.
        # Usage Rate measures play-finishing (shots/turnovers), NOT total involvement.
        true_usg = stats.get('true_usage', 0)
        recent_min = stats.get('recent_avg_min', 0)
        season_min = stats.get('season_avg_min', 0)
        min_ratio = recent_min / season_min if season_min > 0 else 1.0
        
        if true_usg > 0 and metric in ['player_points', 'player_threes'] and min_ratio > 1.03:
            usg_multiplier = true_usg / 0.20 # Relative to league average 20%
            base_projection *= (1.0 + (usg_multiplier - 1.0) * 0.5) # Dampen effect
            
        # 2. Matchup Adjustment (Defensive Strength & Pace)
        matchup_adj = 1.0
        if opponent_stats:
            league_avg_pts = 114.5
            opp_allowed = opponent_stats.get('avg_points_allowed', league_avg_pts)
            def_adj = opp_allowed / league_avg_pts
            
            league_avg_pace = 98.5
            opp_pace = opponent_stats.get('pace', league_avg_pace)
            pace_adj = opp_pace / league_avg_pace
            
            # Blend defense and pace (e.g. fast pace gives more opportunities even if defense is good)
            matchup_adj = (def_adj + pace_adj) / 2.0
            
            # Floor/Ceiling to prevent massive probability crashes
            matchup_adj = max(0.85, min(1.15, matchup_adj))
            
        # 2b. Contextual Adjustment (Fatigue, Altitude, Home Court)
        context_adj = 1.0
        if team_context:
            rest_days = team_context.get('rest_days', 2)
            altitude = team_context.get('altitude', False)
            is_home = team_context.get('is_home', False)
            
            if rest_days == 0:
                context_adj *= 0.96 # 4% penalty for back-to-back
            if altitude:
                context_adj *= 0.98 # 2% penalty for high altitude
            if is_home:
                context_adj *= 1.02 # 2% boost for home court
            
            # Game Script Bridging: Blowout Risk Penalty
            # If the game model projects a large spread, star players will sit
            # in the 4th quarter (~6-8 min lost), shifting props toward Under.
            blowout_risk = team_context.get('blowout_risk', 0)
            if blowout_risk > 0:
                context_adj *= (1.0 - blowout_risk * 0.12)
                
        # 3. Apply Adjustments
        projected_mean = base_projection * matchup_adj * context_adj * self.PLAYOFF_DEFLATION
        # CALIBRATION FIX (2026-04-25): Historical audit showed the raw std is far too
        # narrow, pushing Normal CDF probabilities to extremes (87% model → 54% actual).
        # Multiplying by 1.8x produces softer, well-calibrated probabilities that match
        # real NBA outcome variance. This accounts for game-to-game randomness that
        # rolling averages fundamentally cannot capture (foul trouble, hot/cold shooting,
        # rotation changes, garbage time, etc.).
        projected_std = std * 1.8
        
        # 4. Market Signal Adjustment (Continuous Shift)
        if market_prob is not None:
            current_prob_over = 1 - norm.cdf(line, projected_mean, projected_std)
            diff = market_prob - current_prob_over
            projected_mean += diff * projected_std * self.weights['market_adj']

        # 5. Calculate Final Probabilities based on metric type
        prob_over, prob_under = 0.0, 0.0
        dist_used = "Normal"
        
        if metric in ['player_rebounds', 'player_assists']:
            # Negative Binomial Distribution for over-dispersed count data
            var = projected_std ** 2
            if var > projected_mean and projected_mean > 0:
                p = projected_mean / var
                n = (projected_mean ** 2) / (var - projected_mean)
                prob_under = nbinom.cdf(line, n, p)
                prob_over = 1 - prob_under
                dist_used = "NegativeBinomial"
            else:
                prob_under = norm.cdf(line, projected_mean, projected_std)
                prob_over = 1 - prob_under
                
        elif metric == 'player_points' and 'season_avg_fga' in stats:
            # Monte Carlo Binomial Simulation for Points
            w_s, w_r = self.weights['season_avg'], self.weights['recent_avg']
            
            fga = (stats.get('season_avg_fga', 0)*w_s + stats.get('recent_avg_fga', 0)*w_r) * matchup_adj
            fg3a = (stats.get('season_avg_fg3a', 0)*w_s + stats.get('recent_avg_fg3a', 0)*w_r) * matchup_adj
            fta = (stats.get('season_avg_fta', 0)*w_s + stats.get('recent_avg_fta', 0)*w_r) * matchup_adj
            
            fg_pct = stats.get('season_fg_pct', 0.45)*w_s + stats.get('recent_fg_pct', 0.45)*w_r
            fg3_pct = stats.get('season_fg3_pct', 0.35)*w_s + stats.get('recent_fg3_pct', 0.35)*w_r
            ft_pct = stats.get('season_ft_pct', 0.75)*w_s + stats.get('recent_ft_pct', 0.75)*w_r
            
            fg2a = max(0, fga - fg3a)
            # Impute FG2 PCT
            fg2_makes = max(0, (fga * fg_pct) - (fg3a * fg3_pct))
            fg2_pct = fg2_makes / fg2a if fg2a > 0 else 0.5
            
            if fga > 0:
                num_sims = 10000
                # Simulate shots as Poisson for number of attempts, then Binomial for makes
                sim_fg3a = np.random.poisson(max(0.1, fg3a), num_sims)
                sim_fg2a = np.random.poisson(max(0.1, fg2a), num_sims)
                sim_fta = np.random.poisson(max(0.1, fta), num_sims)
                
                sim_fg3m = np.random.binomial(sim_fg3a, min(1.0, max(0.0, fg3_pct)))
                sim_fg2m = np.random.binomial(sim_fg2a, min(1.0, max(0.0, fg2_pct)))
                sim_ftm = np.random.binomial(sim_fta, min(1.0, max(0.0, ft_pct)))
                
                sim_points = 3*sim_fg3m + 2*sim_fg2m + sim_ftm
                
                # Apply market shift to simulated points directly by offsetting
                shift = projected_mean - np.mean(sim_points)
                sim_points = sim_points + shift
                
                prob_over = np.mean(sim_points > line)
                prob_under = np.mean(sim_points <= line)
                projected_std = float(np.std(sim_points))
                dist_used = "MonteCarloBinomial"
            else:
                prob_under = norm.cdf(line, projected_mean, projected_std)
                prob_over = 1 - prob_under
        else:
            # Fallback to Normal for Threes or missing data
            prob_under = norm.cdf(line, projected_mean, projected_std)
            prob_over = 1 - prob_under
        
        return PlayerPrediction(
            player_name=stats.get('name', 'Unknown'),
            metric=metric,
            projected_mean=projected_mean,
            projected_std=projected_std,
            prob_over=prob_over,
            prob_under=prob_under,
            factors={
                "base_projection": base_projection,
                "matchup_adj": matchup_adj,
                "playoff_deflation": self.PLAYOFF_DEFLATION,
                "distribution": dist_used
            }
        )

if __name__ == "__main__":
    model = PlayerBayesianModel()
    test_stats = {
        'name': 'LeBron James',
        'season_avg_pts': 25.0,
        'recent_avg_pts': 28.0
    }
    
    # Prediction for Over 24.5 Points
    prediction = model.predict_prop(test_stats, 24.5, 'player_points', market_prob=0.60)
    print(f"Player: {prediction.player_name}")
    print(f"Projected {prediction.metric}: {prediction.projected_mean:.2f}")
    print(f"Prob Over: {prediction.prob_over:.1%}")
    print(f"Prob Under: {prediction.prob_under:.1%}")
