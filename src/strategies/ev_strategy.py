"""
EV Strategy — generates trade signals from fundamentals-based edge.

Unlike the other strategies that trade on price microstructure (spread,
velocity, arb), EVStrategy trades on *game-outcome mispricing*:
  Model says 60% → Market says 50% → Buy YES.
"""

from typing import Dict, Optional
from ..models.bayesian_ev import BayesianEVModel
from ..ev.calculator import EVCalculator
from .base import BaseStrategy


class EVStrategy(BaseStrategy):
    """
    Fundamentals-alpha strategy.

    Uses BayesianEVModel to estimate true win probability, compares to
    the market mid price, and generates a signal when edge exceeds threshold.

    Config keys:
        min_edge (float): minimum edge to generate signal (default 0.05)
        kelly_mult (float): Kelly fraction multiplier (default 0.25)
        max_kelly (float): max fraction of bankroll per bet (default 0.10)
    """

    def __init__(self, name: str = "ev_fundamentals", config: Optional[Dict] = None):
        super().__init__(name, config or {})
        self.model = BayesianEVModel(config)
        self.ev_calc = EVCalculator(config)

    def generate_signal(
        self,
        market_id: str,
        snapshots,           # list of recent snapshots (or single snapshot)
        prediction: float,   # unused — we compute our own prediction from fundamentals
        features: Dict,
    ) -> Optional[Dict]:
        """
        Generate signal from fundamentals edge.

        ``features`` must contain:
            home_stats (dict): from NBADataCollector.fetch_team_stats()
            away_stats (dict): same for the away team
            home_label (str): e.g. "LAL"
            away_label (str): e.g. "BOS"
            game_label (str): e.g. "LAL vs BOS"

        Market price is extracted from the latest snapshot's mid.
        """
        # ---- extract snapshot ----
        snapshot = snapshots[-1] if isinstance(snapshots, list) else snapshots
        ask = snapshot.get("ask_price_1", 1.0)
        bid = snapshot.get("bid_price_1", 0.0)
        mid = (ask + bid) / 2
        if mid <= 0 or mid >= 1:
            return None

        # Build order book for calculator
        order_book = {
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "bid_size": snapshot.get("bid_size_1", 0.0),
            "ask_size": snapshot.get("ask_size_1", 0.0)
        }

        # ---- run model ----
        home_stats = features.get("home_stats")
        away_stats = features.get("away_stats")
        if not home_stats and not away_stats:
            return None

        output = self.model.predict(home_stats, away_stats)

        # ---- compute edge (for the YES / home side) ----
        ev_result = self.ev_calc.compute(
            game_label=features.get("game_label", market_id),
            side_label=features.get("home_label", "Home"),
            model_prob=output.home_prob,
            market_price=mid,
            order_book=order_book
        )

        if ev_result.recommended_action == "PASS":
            return None

        side = "BUY" if ev_result.recommended_action == "BUY YES" else "SELL"
        # Maker price (still front-running or matching)
        # but now based on a better EV signal
        price = bid + 0.01 if side == "BUY" else ask - 0.01

        return {
            "side": side,
            "confidence": abs(ev_result.edge),
            "limit_price": price,
            "edge": ev_result.edge,
            "realizable_ev": ev_result.realizable_ev,
            "model_prob": ev_result.model_prob,
            "market_prob": ev_result.market_prob,
            "kelly": ev_result.kelly_half,
            "factors": output.factors,
        }
