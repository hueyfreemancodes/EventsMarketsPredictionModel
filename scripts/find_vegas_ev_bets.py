import os
import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

logger = logging.getLogger("VegasEVFinder")

from src.data_collection.nba_api_collector import NBADataCollector
from src.data_collection.odds_api_collector import OddsAPICollector
from src.models.bayesian_ev import BayesianEVModel
from src.utils.nba_mapping import get_team_abbr
from scripts.fetch_markets import fetch_active_nba_markets

def main():
    logger.info("=== Starting NBA Vegas +EV Discovery Pipeline ===")
    
    # 1. Fetch Today's Active Teams from Scoreboard
    nba = NBADataCollector()
    todays_games = nba.get_todays_games()
    if not todays_games:
        logger.warning("No games found today. Exiting.")
        return
        
    active_teams = set()
    for g in todays_games:
        active_teams.add(g['home_abbrev'])
        active_teams.add(g['away_abbrev'])
    logger.info(f"Today's active teams: {active_teams}")

    # 2. Fetch Polymarket Sentiment
    logger.info("Step 1: Fetching Polymarket implied probabilities...")
    poly_markets = fetch_active_nba_markets(mode='winner')
    poly_signals = {}
    for m in poly_markets:
        team_abbr = get_team_abbr(m['outcome_label'])
        if team_abbr:
            poly_signals[team_abbr] = m['price']
            
    # 3. Fetch Vegas Odds
    logger.info("Step 2: Fetching Vegas odds from The Odds API...")
    odds_collector = OddsAPICollector()
    raw_odds = odds_collector.fetch_live_odds()
    vegas_lines = odds_collector.get_game_lines(raw_odds)
    
    # Filter for today's games only
    game_lines = [g for g in vegas_lines if g['home_abbr'] in active_teams and g['away_abbr'] in active_teams]
    
    if not game_lines:
        logger.warning("No Vegas lines found for today's active teams yet.")
        return

    # 4. Analyze
    model = BayesianEVModel()
    results = []
    
    for game in game_lines:
        home_abbr = game['home_abbr']
        away_abbr = game['away_abbr']
        
        # 4.1. Fetch Stats
        h_stats = nba.fetch_team_stats(home_abbr)
        a_stats = nba.fetch_team_stats(away_abbr)
        
        # 4.2. Get market signal (use home prob if available)
        h_market = poly_signals.get(home_abbr)
        
        # Predict
        prediction = model.predict(h_stats, a_stats, h_market)
        if not prediction: continue
        
        def american_to_prob(american):
            if american > 0: return 100 / (american + 100)
            else: return abs(american) / (abs(american) + 100)

        # Apply each book
        for book_name, odds in game['books'].items():
            h_odds = odds['home_odds']
            a_odds = odds['away_odds']
            if h_odds is None or a_odds is None: continue
            
            # Calculate EV for Home
            h_implied = american_to_prob(h_odds)
            h_ev = (prediction.home_prob / h_implied) - 1
            
            # Calculate EV for Away
            a_implied = american_to_prob(a_odds)
            a_ev = (prediction.away_prob / a_implied) - 1
            
            results.append({
                "game": f"{away_abbr} @ {home_abbr}",
                "book": book_name,
                "team": home_abbr,
                "odds": h_odds,
                "true_prob": prediction.home_prob,
                "implied_prob": h_implied,
                "ev": h_ev
            })
            
            results.append({
                "game": f"{away_abbr} @ {home_abbr}",
                "book": book_name,
                "team": away_abbr,
                "odds": a_odds,
                "true_prob": prediction.away_prob,
                "implied_prob": a_implied,
                "ev": a_ev
            })

    # 5. Report & Log
    logger.info("Step 4: Generating Report & Logging Bets...")
    EV_THRESHOLD = 0.05
    WIN_PROB_THRESHOLD = 0.52
    
    top_bets = []
    bets_to_log = []
    
    for r in results:
        if r['ev'] > EV_THRESHOLD and r['true_prob'] > WIN_PROB_THRESHOLD:
            top_bets.append(r)
            log_entry = {
                "player": "TEAM",
                "game": r['game'],
                "metric": "moneyline",
                "line": 0,
                "book": r['book'],
                "odds": r['odds'],
                "prob": r['true_prob'],
                "ev": r['ev'],
                "side": r['team'],
                "timestamp": datetime.now().isoformat()
            }
            bets_to_log.append(log_entry)
            
    top_bets.sort(key=lambda x: x['ev'], reverse=True)
    
    if bets_to_log:
        os.makedirs("data", exist_ok=True)
        log_file = "data/logged_bets.json"
        existing_bets = []
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                try: existing_bets = json.load(f)
                except: existing_bets = []
        
        today = datetime.now().strftime("%Y-%m-%d")
        for b in bets_to_log:
            is_dup = any(eb['side'] == b['side'] and eb['game'] == b['game'] and eb['timestamp'].startswith(today) for eb in existing_bets)
            if not is_dup:
                existing_bets.append(b)
                
        with open(log_file, "w") as f:
            json.dump(existing_bets, f, indent=2)
        logger.info(f"Logged {len(bets_to_log)} high-confidence game lines.")

    print("\n" + "="*80)
    print(f" NBA VEGAS +EV REPORT (HIGH-CONFIDENCE) - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*80)
    
    if not top_bets:
        print("No high-confidence game bets found meeting thresholds.")
    else:
        print(f"{'GAME':<20} | {'TEAM':<10} | {'BOOK':<12} | {'ODDS':<6} | {'TRUE%':<6} | {'EV%'}")
        print("-" * 80)
        for bet in top_bets:
            print(f"{bet['game']:<20} | {bet['team']:<10} | {bet['book']:<12} | {bet['odds']:>6} | {bet['true_prob']:>6.1%} | {bet['ev']*100:>6.1f}%")
    print("\n" + "="*80)

if __name__ == "__main__":
    main()
