#!/usr/bin/env python3
"""
backtest_ev.py — Historical backtest for the Bayesian EV model.

Since we don't have historical Polymarket orderbook data readily available,
this backtest uses:
  1. Known NBA game results (from nba_api game logs)
  2. Simulated market lines (based on historical closing lines / 50-50 baseline)
  3. The Bayesian model's predictions vs actual outcomes

The output: hit rate, ROI, simulated P/L on flat bets and Kelly bets.

Usage:
    python -m scripts.backtest_ev [--season 2024-25] [--num-games 100]
"""

import sys
import os
import time
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.models.bayesian_ev import BayesianEVModel
from src.ev.calculator import EVCalculator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from nba_api.stats.endpoints import leaguegamefinder
    from nba_api.stats.static import teams as nba_teams_static
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    logger.warning("nba_api not installed. Run: pip install nba-api")


def build_team_stats_snapshot(games_before: list, is_home: bool) -> Dict:
    """
    Build a stats snapshot from a team's game log entries *before* a given game.
    This simulates what we'd know heading into game day.
    """
    if not games_before:
        return {}

    wins = sum(1 for g in games_before if g.get("WL") == "W")
    total = len(games_before)
    win_pct = wins / total if total > 0 else 0.5

    pts_scored = [g.get("PTS", 100) for g in games_before]
    pts_allowed = [g.get("PTS", 100) - g.get("PLUS_MINUS", 0) for g in games_before]
    avg_scored = sum(pts_scored) / len(pts_scored)
    avg_allowed = sum(pts_allowed) / len(pts_allowed)

    last_5 = games_before[:5]
    last_5_wins = sum(1 for g in last_5 if g.get("WL") == "W")

    home_games = [g for g in games_before if "vs." in str(g.get("MATCHUP", ""))]
    away_games = [g for g in games_before if "@" in str(g.get("MATCHUP", ""))]

    home_wp = (sum(1 for g in home_games if g.get("WL") == "W") / len(home_games)
               if home_games else 0.5)
    away_wp = (sum(1 for g in away_games if g.get("WL") == "W") / len(away_games)
               if away_games else 0.5)

    return {
        "win_pct": win_pct,
        "avg_points_scored": avg_scored,
        "avg_points_allowed": avg_allowed,
        "avg_point_diff": avg_scored - avg_allowed,
        "last_5_wins": last_5_wins,
        "last_3_wins": sum(1 for g in games_before[:3] if g.get("WL") == "W"),
        "home_win_pct": home_wp,
        "away_win_pct": away_wp,
    }


def run_backtest(season: str = "2024-25", num_games: int = 200, lookback: int = 15):
    """
    Run historical backtest.

    For each game in the season:
      1. Grab all prior game logs for both teams
      2. Build stats snapshots (simulating pre-game knowledge)
      3. Run Bayesian model → get P(home wins)
      4. Check actual result
      5. Accumulate hit rate & simulated P/L
    """
    if not NBA_API_AVAILABLE:
        print("ERROR: nba_api required for backtest. Install with: pip install nba-api")
        return

    model = BayesianEVModel()
    ev_calc = EVCalculator({"min_edge": 0.03})

    # Fetch all games for the season
    logger.info(f"Fetching game data for season {season}...")
    time.sleep(1)
    try:
        finder = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable="00",
            season_type_nullable="Regular Season",
        )
        all_games_df = finder.get_data_frames()[0]
    except Exception as e:
        logger.error(f"Failed to fetch game data: {e}")
        return

    if all_games_df.empty:
        logger.error("No game data returned")
        return

    # Convert to dict list, sorted by date (oldest first)
    all_games = all_games_df.to_dict("records")
    all_games.sort(key=lambda g: g.get("GAME_DATE", ""))

    # Build per-team game logs (in chronological order)
    team_logs: Dict[int, list] = {}
    for g in all_games:
        tid = g.get("TEAM_ID")
        if tid not in team_logs:
            team_logs[tid] = []
        team_logs[tid].append(g)

    # Find unique game IDs (each game has two rows — one per team)
    game_pairs: Dict[str, list] = {}
    for g in all_games:
        gid = g.get("GAME_ID")
        if gid not in game_pairs:
            game_pairs[gid] = []
        game_pairs[gid].append(g)

    # Keep only games with exactly 2 teams
    complete_games = {gid: rows for gid, rows in game_pairs.items() if len(rows) == 2}

    # Sort by date and limit
    sorted_game_ids = sorted(complete_games.keys(),
                              key=lambda gid: complete_games[gid][0].get("GAME_DATE", ""))

    # Skip first `lookback` games so we have prior data
    test_games = sorted_game_ids[lookback * 2:]  # rough — each team plays ~2 games per 3 days
    if num_games:
        test_games = test_games[:num_games]

    logger.info(f"Backtesting {len(test_games)} games (lookback={lookback})...")

    # ---- Backtest loop ----
    correct = 0
    total = 0
    flat_pnl = 0.0
    kelly_pnl = 0.0
    bankroll = 1000.0
    ev_bets = 0

    for gid in test_games:
        rows = complete_games[gid]

        # Determine home/away
        home_row = None
        away_row = None
        for r in rows:
            matchup = str(r.get("MATCHUP", ""))
            if "vs." in matchup:
                home_row = r
            elif "@" in matchup:
                away_row = r

        if not home_row or not away_row:
            continue

        home_tid = home_row["TEAM_ID"]
        away_tid = away_row["TEAM_ID"]

        # Get all prior games for each team (before this game date)
        game_date = home_row.get("GAME_DATE", "")
        home_prior = [g for g in team_logs.get(home_tid, [])
                      if g.get("GAME_DATE", "") < game_date][-lookback:]
        away_prior = [g for g in team_logs.get(away_tid, [])
                      if g.get("GAME_DATE", "") < game_date][-lookback:]

        if len(home_prior) < 5 or len(away_prior) < 5:
            continue  # not enough history

        # Build snapshots
        home_stats = build_team_stats_snapshot(list(reversed(home_prior)), is_home=True)
        away_stats = build_team_stats_snapshot(list(reversed(away_prior)), is_home=False)

        # Run model
        output = model.predict(home_stats, away_stats)

        # Actual result
        home_won = home_row.get("WL") == "W"
        actual_home_prob = 1.0 if home_won else 0.0

        # Model prediction
        model_pick_home = output.home_prob > 0.50
        model_correct = model_pick_home == home_won

        if model_correct:
            correct += 1
        total += 1

        # Simulate flat bet ($10 on model's pick)
        if model_pick_home:
            market_price = 0.50  # baseline — no live market data
            ev = ev_calc.compute("", "Home", output.home_prob, market_price)
            if home_won:
                flat_pnl += 10 * (1 / market_price - 1)  # payout
            else:
                flat_pnl -= 10
        else:
            market_price = 0.50
            ev = ev_calc.compute("", "Away", output.away_prob, market_price)
            if not home_won:
                flat_pnl += 10 * (1 / market_price - 1)
            else:
                flat_pnl -= 10

        # Kelly sizing
        edge = abs(output.home_prob - 0.5)
        if edge > 0.03:
            ev_bets += 1
            kelly_size = min(ev.kelly_half * bankroll, bankroll * 0.10)
            if model_correct:
                kelly_pnl += kelly_size
            else:
                kelly_pnl -= kelly_size

    # ---- Results ----
    hit_rate = correct / total if total > 0 else 0

    print("\n" + "=" * 70)
    print(f"  BACKTEST RESULTS — Season {season}")
    print("=" * 70)
    print(f"  Games tested:       {total}")
    print(f"  Model hit rate:     {correct}/{total} = {hit_rate:.1%}")
    print(f"  Flat bet P/L:       ${flat_pnl:+,.2f}  (@ $10/bet vs 50/50 line)")
    print(f"  Kelly P/L:          ${kelly_pnl:+,.2f}  (starting bankroll $1000)")
    print(f"  +EV bets taken:     {ev_bets}/{total} ({ev_bets/total:.0%} of games)" if total else "")
    print(f"  Confidence:         {output.confidence:.0%} avg data coverage" if total else "")
    print("=" * 70)
    print("  NOTE: Market prices use 50/50 baseline. Real ROI depends on")
    print("  actual market pricing. This validates model accuracy, not P/L.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest Bayesian EV Model")
    parser.add_argument("--season", default="2024-25", help="NBA season (default: 2024-25)")
    parser.add_argument("--num-games", type=int, default=200, help="Max games to test")
    parser.add_argument("--lookback", type=int, default=15, help="Prior games for stats")
    args = parser.parse_args()
    run_backtest(season=args.season, num_games=args.num_games, lookback=args.lookback)
