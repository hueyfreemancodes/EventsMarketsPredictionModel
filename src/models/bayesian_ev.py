"""
Bayesian EV Model — Phase 1: Game-Level

Estimates true win probability for NBA game outcomes using a Bayesian approach:
  1. Prior: home-court advantage (~60% historical base rate)
  2. Likelihood factors: win% differential, recent form, home/away splits, point differential
  3. Posterior: logistic combination → calibrated probability
"""

import math
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------- historical priors ----------
HOME_WIN_RATE = 0.588          # NBA home-court advantage 2015-2024
LEAGUE_AVG_WIN_PCT = 0.500


@dataclass
class ModelOutput:
    """Result from the Bayesian probability model."""
    home_prob: float            # calibrated P(home wins)
    away_prob: float            # 1 - home_prob
    confidence: float           # 0-1 how much data informed the estimate
    factors: Dict[str, float]   # individual factor contributions for transparency


class BayesianEVModel:
    """
    Estimates true game-outcome probabilities from team fundamentals.

    Every factor produces a log-odds shift from the prior.  The shifts are
    summed and passed through the logistic function to produce a calibrated
    probability.

    Weights were hand-tuned against 2018-2024 NBA regular-season results
    and can be overridden via ``config``.
    """

    DEFAULT_WEIGHTS = {
        "home_court":       0.30,   # log-odds shift for home court
        "win_pct_diff":     1.20,   # reduced from 2.50
        "recent_form":      0.50,   # reduced from 1.00
        "home_away_split":  0.50,   # reduced from 0.80
        "point_diff":       0.10,   # reduced from 0.15
        "market_signal":    0.20,   # reduced from 0.75
    }

    def __init__(self, config: Optional[Dict] = None):
        cfg = config or {}
        self.weights = {k: cfg.get(f"w_{k}", v)
                        for k, v in self.DEFAULT_WEIGHTS.items()}
        self.min_confidence = cfg.get("min_confidence", 0.0)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def predict(
        self,
        home_stats: Optional[Dict],
        away_stats: Optional[Dict],
        market_prob: Optional[float] = None,
    ) -> ModelOutput:
        """
        Predict P(home wins).

        ``home_stats`` / ``away_stats`` dicts should match the shape
        returned by ``NBADataCollector.fetch_team_stats()``:
            win_pct, avg_points_scored, avg_points_allowed, avg_point_diff,
            last_3_wins, last_5_wins, home_win_pct, away_win_pct
        """
        log_odds = 0.0
        factors: Dict[str, float] = {}
        data_points = 0
        max_data_points = 5   # one per factor

        # ---- 1. Home-court prior ----
        hc_shift = self.weights["home_court"]
        log_odds += hc_shift
        factors["home_court"] = hc_shift
        data_points += 1

        hs = home_stats or {}
        aws = away_stats or {}

        # ---- 2. Season win% differential ----
        h_wp = hs.get("win_pct")
        a_wp = aws.get("win_pct")
        if h_wp is not None and a_wp is not None:
            diff = float(h_wp) - float(a_wp)           # +1 if home is perfect, away is 0-82
            shift = diff * self.weights["win_pct_diff"]
            log_odds += shift
            factors["win_pct_diff"] = shift
            data_points += 1

        # ---- 3. Recent form (last 5 games) ----
        h_l5 = hs.get("last_5_wins")
        a_l5 = aws.get("last_5_wins")
        if h_l5 is not None and a_l5 is not None:
            # normalise to [-1, 1]:  5-0 → +1  0-5 → -1
            form_diff = (int(h_l5) - int(a_l5)) / 5.0
            shift = form_diff * self.weights["recent_form"]
            log_odds += shift
            factors["recent_form"] = shift
            data_points += 1

        # ---- 4. Home/away venue splits ----
        h_home_wp = hs.get("home_win_pct")
        a_away_wp = aws.get("away_win_pct")
        if h_home_wp is not None and a_away_wp is not None:
            # home team's home record vs away team's road record
            venue_diff = float(h_home_wp) - float(a_away_wp)
            shift = venue_diff * self.weights["home_away_split"]
            log_odds += shift
            factors["home_away_split"] = shift
            data_points += 1

        # ---- 5. Point differential ----
        h_pd = hs.get("avg_point_diff")
        a_pd = aws.get("avg_point_diff")
        if h_pd is not None and a_pd is not None:
            diff = float(h_pd) - float(a_pd)
            shift = diff * self.weights["point_diff"]
            log_odds += shift
            factors["point_diff"] = shift
            data_points += 1

        # ---- 6. Market Signal (Polymarket) ----
        if market_prob is not None:
            # We treat the current model's output (before this factor) 
            # as our prior, and shift towards the market.
            # Convert market_prob to log-odds
            m_p = max(0.01, min(0.99, market_prob))
            market_log_odds = math.log(m_p / (1.0 - m_p))
            
            # The shift is proportional to the difference between 
            # model-so-far and market-price
            model_so_far_p = self._logistic(log_odds)
            model_so_far_log_odds = log_odds
            
            # Weighted average shift
            shift = (market_log_odds - model_so_far_log_odds) * self.weights["market_signal"]
            
            # Dampen/Cap the market shift to prevent 0/1 swings from noisy signals
            max_shift = 2.0 # Allow at most ~2.0 log-odds shift (~40% prob change)
            shift = max(-max_shift, min(max_shift, shift))
            
            log_odds += shift
            factors["market_signal"] = shift
            data_points += 1
            max_data_points = 6
        else:
            max_data_points = 5

        # ---- logistic squash ----
        home_prob = self._logistic(log_odds)

        # clamp to avoid 0/1
        home_prob = max(0.02, min(0.98, home_prob))

        confidence = data_points / max_data_points

        return ModelOutput(
            home_prob=round(home_prob, 4),
            away_prob=round(1.0 - home_prob, 4),
            confidence=round(confidence, 2),
            factors=factors,
        )

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    @staticmethod
    def _logistic(x: float) -> float:
        """Standard logistic sigmoid."""
        try:
            return 1.0 / (1.0 + math.exp(-x))
        except OverflowError:
            return 0.0 if x < 0 else 1.0
