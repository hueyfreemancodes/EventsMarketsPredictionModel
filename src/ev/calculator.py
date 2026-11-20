"""
EV Calculator

Computes edge, expected value, and Kelly fraction from a model probability
and a market-implied probability (derived from the mid price on Polymarket/Kalshi).
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class EVResult:
    """One EV calculation result for a single market side."""
    game: str
    side: str                   # e.g. "LAL" or "Home"
    model_prob: float           # our probability
    market_prob: float          # market-implied probability (mid)
    edge: float                 # model_prob - market_prob
    ev_per_dollar: float        # theoretical expected profit per $1 risked (at mid)
    realizable_ev: float        # realizable expected profit (at best ask/bid)
    kelly_fraction: float       # full Kelly fraction
    kelly_half: float           # half-Kelly (practical sizing)
    best_bid: Optional[float]   # live best bid
    best_ask: Optional[float]   # live best ask
    recommended_action: str     # "BUY YES", "BUY NO", or "PASS"


class EVCalculator:
    """
    Turns (model_prob, order_book) into an actionable edge signal.

    Liquidity-Aware EV:
        Computes both theoretical EV (at mid-price) and realizable EV
        (crossing the spread to the best ask/bid).
    """

    def __init__(self, config: Optional[dict] = None):
        cfg = config or {}
        self.min_edge = cfg.get("min_edge", 0.05)              # 5% minimum edge
        self.kelly_fraction_mult = cfg.get("kelly_mult", 0.25)  # quarter-Kelly
        self.max_kelly = cfg.get("max_kelly", 0.10)             # cap at 10% of bankroll
        self.min_market_prob = cfg.get("min_market_prob", 0.05)  # ignore extreme lines

    def compute(
        self,
        game_label: str,
        side_label: str,
        model_prob: float,
        market_price: float = 0.5,  # fallback mid price
        order_book: Optional[dict] = None,
    ) -> EVResult:
        """
        Compute EV for a single side of a market.

        Args:
            game_label: human-readable game
            side_label: which outcome this price represents
            model_prob: our estimated probability (0-1)
            market_price: YES mid price (used if order_book is missing)
            order_book: Optional dict with 'mid', 'bid', 'ask', 'bid_size', 'ask_size'

        Returns:
            EVResult with theoretical and realizable metrics
        """
        if order_book:
            mid = order_book.get("mid", market_price)
            bid = order_book.get("bid")
            ask = order_book.get("ask")
        else:
            mid = market_price
            bid = None
            ask = None

        market_prob = max(mid, self.min_market_prob)
        market_prob = min(market_prob, 1.0 - self.min_market_prob)

        edge = model_prob - market_prob

        # Theoretical EV (at mid)
        ev_per_dollar = (model_prob * (1.0 - market_prob)) - ((1.0 - model_prob) * market_prob)

        # Realizable EV (taking the spread)
        if ask is not None and model_prob > market_prob:
            # Buying this side
            realizable_ev = (model_prob * (1.0 - ask)) - ((1.0 - model_prob) * ask)
        elif bid is not None and model_prob < market_prob:
            # Fading this side (buying NO)
            # Price of NO is (1 - bid)
            p_no = 1.0 - bid
            prob_no = 1.0 - model_prob
            realizable_ev = (prob_no * (1.0 - p_no)) - ((1.0 - prob_no) * p_no)
        else:
            realizable_ev = ev_per_dollar

        # Kelly: f* = (b*p - q) / b  where b = payout odds, p = model_prob
        payout_odds = (1.0 / market_prob) - 1.0 if market_prob > 0 else 0
        if payout_odds > 0:
            kelly_full = ((payout_odds * model_prob) - (1.0 - model_prob)) / payout_odds
        else:
            kelly_full = 0.0

        kelly_full = max(kelly_full, 0.0)
        kelly_half = min(kelly_full * self.kelly_fraction_mult, self.max_kelly)

        # Action (Use REALIZABLE EV for the signal)
        if realizable_ev >= (self.min_edge * 0.5) and edge >= self.min_edge:
            action = "BUY YES"
        elif realizable_ev >= (self.min_edge * 0.5) and edge <= -self.min_edge:
            action = "BUY NO"
        else:
            action = "PASS"

        return EVResult(
            game=game_label,
            side=side_label,
            model_prob=round(model_prob, 4),
            market_prob=round(market_prob, 4),
            edge=round(edge, 4),
            ev_per_dollar=round(ev_per_dollar, 4),
            realizable_ev=round(realizable_ev, 4),
            kelly_fraction=round(kelly_full, 4),
            kelly_half=round(kelly_half, 4),
            best_bid=bid,
            best_ask=ask,
            recommended_action=action,
        )
