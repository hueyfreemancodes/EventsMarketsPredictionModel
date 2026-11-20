#!/usr/bin/env python3
"""
find_ev_bets.py — +EV Bet Finder (Phase 1: game-level moneyline)

Pulls today's NBA markets from Polymarket, fetches team stats from nba_api,
runs the Bayesian model, and prints actionable +EV opportunities.

Usage:
    python -m scripts.find_ev_bets [--min-edge 0.05] [--json]
"""

import sys
import os
import json
import argparse
import logging
from datetime import datetime

# -- path bootstrap --
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.fetch_markets import fetch_active_nba_markets
from src.data_collection.market_prices import fetch_market_prices
from src.data_collection.nba_api_collector import NBADataCollector
from src.models.bayesian_ev import BayesianEVModel
from src.ev.calculator import EVCalculator, EVResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Polymarket team name → NBA abbreviation ──────────────────────────

TEAM_ABBREV_MAP = {
    # Full names (as they appear in Polymarket "vs." titles)
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "LA Clippers": "LAC", "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA", "Washington Wizards": "WAS",
    # Short names (some markets use just nicknames)
    "Hawks": "ATL", "Celtics": "BOS", "Nets": "BKN", "Hornets": "CHA",
    "Bulls": "CHI", "Cavaliers": "CLE", "Cavs": "CLE",
    "Mavericks": "DAL", "Mavs": "DAL", "Nuggets": "DEN", "Pistons": "DET",
    "Warriors": "GSW", "Rockets": "HOU", "Pacers": "IND", "Clippers": "LAC",
    "Lakers": "LAL", "Grizzlies": "MEM", "Heat": "MIA", "Bucks": "MIL",
    "Timberwolves": "MIN", "Wolves": "MIN",
    "Pelicans": "NOP", "Knicks": "NYK", "Thunder": "OKC",
    "Magic": "ORL", "76ers": "PHI", "Sixers": "PHI",
    "Suns": "PHX", "Trail Blazers": "POR", "Blazers": "POR",
    "Kings": "SAC", "Spurs": "SAS", "Raptors": "TOR",
    "Jazz": "UTA", "Wizards": "WAS",
}


def resolve_abbrev(name: str) -> str:
    """Resolve a team name/nickname to its NBA abbreviation."""
    name = name.strip()
    # exact match first
    if name in TEAM_ABBREV_MAP:
        return TEAM_ABBREV_MAP[name]
    # try nickname only (last word)
    last_word = name.split()[-1] if name else ""
    if last_word in TEAM_ABBREV_MAP:
        return TEAM_ABBREV_MAP[last_word]
    # already an abbreviation?
    if len(name) == 3 and name.isupper():
        return name
    return name  # fallback — will likely fail lookups gracefully


def run(min_edge: float = 0.05, output_json: bool = False):
    """Main runner."""

    # 1) Fetch active Polymarket moneyline markets
    logger.info("=== Finding +EV NBA Bets ===")
    all_markets = fetch_active_nba_markets()

    # Dedupe and keep only moneyline (token_index 0 is team1 YES side)
    game_markets = {}  # keyed by market_id
    for m in all_markets:
        mid = m["market_id"]
        if mid not in game_markets:
            game_markets[mid] = {"team1": m["team1"], "team2": m["team2"],
                                 "title": m["title"], "tokens": {}}
        game_markets[mid]["tokens"][m["token_index"]] = m

    logger.info(f"Found {len(game_markets)} unique game markets")

    # 2) Fetch live CLOB order books for all tokens
    all_token_ids = [m["clob_token_id"] for m in all_markets if m.get("clob_token_id")]
    from src.data_collection.market_prices import fetch_market_books
    live_books = fetch_market_books(all_token_ids) if all_token_ids else {}
    price_source = "live" if live_books else "fallback (0.50)"
    logger.info(f"Price source: {price_source} — got {len(live_books)} books")

    # 3) Init model + collector
    model = BayesianEVModel()
    ev_calc = EVCalculator({"min_edge": min_edge})
    collector = NBADataCollector(lookback_games=10)

    # 4) Loop over games
    results: list[EVResult] = []
    for mid, gm in game_markets.items():
        team1_name = gm["team1"]
        team2_name = gm["team2"]
        abbrev1 = resolve_abbrev(team1_name)
        abbrev2 = resolve_abbrev(team2_name)

        logger.info(f"Analysing: {team1_name} ({abbrev1}) vs {team2_name} ({abbrev2})")

        # Fetch NBA stats (rate-limited inside collector)
        stats1 = collector.fetch_team_stats(abbrev1)
        stats2 = collector.fetch_team_stats(abbrev2)

        if not stats1 and not stats2:
            logger.warning(f"  Skipping — no stats available for either team")
            continue

        # Convention: team1 in Polymarket title is listed first = "home" proxy
        # (Polymarket titles usually put the home team first)
        model_output = model.predict(home_stats=stats1, away_stats=stats2)

        # Look up live market books
        t0 = gm["tokens"].get(0, {})
        t1 = gm["tokens"].get(1, {})

        book0 = live_books.get(t0.get("clob_token_id", ""))
        book1 = live_books.get(t1.get("clob_token_id", ""))

        # Compute EV for Team 1
        ev1 = ev_calc.compute(
            game_label=f"{team1_name} vs {team2_name}",
            side_label=team1_name,
            model_prob=model_output.home_prob,
            market_price=book0["mid"] if book0 else 0.50,
            order_book=book0
        )
        
        # Compute EV for Team 2
        # For Team 2 (YES), we use book1 if available
        ev2 = ev_calc.compute(
            game_label=f"{team1_name} vs {team2_name}",
            side_label=team2_name,
            model_prob=model_output.away_prob,
            market_price=book1["mid"] if book1 else (1.0 - ev1.market_prob),
            order_book=book1
        )

        results.append(ev1)
        results.append(ev2)

    # 5) Filter and sort by edge
    actionable = [r for r in results if r.recommended_action != "PASS"]
    actionable.sort(key=lambda r: abs(r.edge), reverse=True)

    # 6) Output
    if output_json:
        import dataclasses
        print(json.dumps([dataclasses.asdict(r) for r in actionable], indent=2))
        return

    print("\n" + "=" * 80)
    print(f"  +EV BET FINDER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Min edge: {min_edge:.0%}  |  Markets scanned: {len(game_markets)}")
    print("=" * 80)

    if not actionable:
        print("\n  No +EV bets found above threshold.\n")
        print(f"  All model estimates (prices: {price_source}):\n")
        for r in sorted(results, key=lambda r: abs(r.edge), reverse=True):
            arrow = "▲" if r.edge > 0 else "▼"
            print(f"    {arrow} {r.game:40s}  {r.side:25s}"
                  f"  Model: {r.model_prob:.1%}  Edge: {r.edge:+.1%}")
    else:
        print(f"\n  Found {len(actionable)} actionable bets:\n")
        for r in actionable:
            spread = (r.best_ask - r.best_bid) if (r.best_ask and r.best_bid) else 0
            print(f"  {'🟢' if r.recommended_action == 'BUY YES' else '🔴'}"
                  f"  {r.recommended_action:8s}  {r.game}")
            print(f"       Side: {r.side}")
            print(f"       Model: {r.model_prob:.1%}  |  Market: {r.market_prob:.1%} (Spread: ${spread:.2f})")
            print(f"       Edge: {r.edge:+.1%}  |  Realizable EV/$: {r.realizable_ev:+.4f}")
            print(f"       Theoretical EV/$: {r.ev_per_dollar:+.4f}  |  Kelly: {r.kelly_half:.1%}")
            print()

    print("=" * 80)
    n_live = sum(1 for tid in all_token_ids if tid in live_books)
    n_fallback = len(all_token_ids) - n_live
    print(f"  Books: {n_live} live (CLOB orderbook) | {n_fallback} fallback (0.50)")
    if n_fallback > 0:
        print("  Fallback tokens had empty orderbooks or API errors.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="+EV NBA Bet Finder")
    parser.add_argument("--min-edge", type=float, default=0.05,
                        help="Minimum edge threshold (default 0.05 = 5%%)")
    parser.add_argument("--json", action="store_true",
                        help="Output actionable bets as JSON")
    args = parser.parse_args()
    run(min_edge=args.min_edge, output_json=args.json)
