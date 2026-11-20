import os
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Validator")

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.data_collection.nba_api_collector import NBADataCollector

def main():
    logger.info("Starting Performance Validation Pipeline")
    
    prop_file = "data/logged_bets.json"
    game_file = "data/logged_game_bets.json"
    
    all_bets = []
    
    # Load Props
    if os.path.exists(prop_file):
        with open(prop_file, "r") as f:
            try:
                props = json.load(f)
                for p in props:
                    p['type'] = 'PROP'
                    all_bets.append(p)
            except: pass
    
    # Load Game Lines
    if os.path.exists(game_file):
        with open(game_file, "r") as f:
            try:
                games = json.load(f)
                for g in games:
                    g['type'] = 'GAME'
                    all_bets.append(g)
            except: pass

    if not all_bets:
        logger.info("No bets found to validate.")
        return

    nba = NBADataCollector()
    
    # Group by date
    bets_by_date = {}
    for bet in all_bets:
        # FILTER: Only high confidence picks from yesterday (EV > 15%)
        # Both Props and Game lines use decimal EV (0.15 = 15%)
        edge = bet.get('ev', bet.get('edge', 0))
        if edge < 0.15: continue
        
        date_str = bet['timestamp'].split('T')[0]
        if date_str not in bets_by_date: bets_by_date[date_str] = []
        bets_by_date[date_str].append(bet)

    summary = {"total": 0, "wins": 0, "losses": 0, "pending": 0}

    print("\n" + "="*110)
    print(f" NBA PERFORMANCE VALIDATION REPORT - {datetime.now().strftime('%Y-%m-%d')}")
    print("="*110)
    print(f"{'DATE':<12} | {'TYPE':<6} | {'MARKET':<12} | {'PICK':<20} | {'LINE':<6} | {'RESULT':<8} | {'W/L'}")
    print("-" * 110)

    for date_str, bets in sorted(bets_by_date.items(), reverse=True):
        game_results = nba.get_game_results(date_str)
        player_actuals = nba.fetch_player_actuals(date_str)
        
        if not game_results and date_str == datetime.now().strftime("%Y-%m-%d"):
            for b in bets:
                label = b.get('player', b.get('game'))
                print(f"{date_str:<12} | {b['type']:<6} | {b.get('metric', b.get('market')):<12} | {label:<20} | {b['line']:<6} | {'PENDING':<8} | -")
                summary['pending'] += 1
            continue

        for b in bets:
            summary['total'] += 1
            win = False
            found = False
            actual_desc = "N/A"
            
            if b['type'] == 'PROP':
                player = b['player']
                actual = player_actuals.get(player)
                if actual:
                    found = True
                    m_map = {'points': 'pts', 'rebounds': 'reb', 'assists': 'ast', 'threes': 'fg3m'}
                    val = actual.get(m_map.get(b['metric'], b['metric']))
                    actual_desc = str(val)
                    win = (val > b['line']) if b['side'] == "OVER" else (val < b['line'])
            
            elif b['type'] == 'GAME':
                # Game Line logic
                teams_split = b['game'].split(' @ ')
                if len(teams_split) == 2:
                    away_abbr, home_abbr = teams_split[0], teams_split[1]
                    away_res = game_results.get(away_abbr)
                    home_res = game_results.get(home_abbr)
                    
                    if away_res and home_res:
                        found = True
                        if b['market'] == 'Total':
                            total_score = away_res['score'] + home_res['score']
                            actual_desc = str(total_score)
                            win = (total_score > b['line']) if b['team'] == "OVER" else (total_score < b['line'])
                        elif b['market'] == 'Spread':
                            target_team = b['team']
                            is_home = (target_team == home_abbr)
                            t_score = home_res['score'] if is_home else away_res['score']
                            o_score = away_res['score'] if is_home else home_res['score']
                            actual_desc = f"{t_score}-{o_score}"
                            win = (t_score + b['line']) > o_score

            if found:
                status = "WIN" if win else "LOSS"
                if win: summary['wins'] += 1
                else: summary['losses'] += 1
                
                label = b.get('player', b.get('game'))
                market = b.get('metric', b.get('market'))
                print(f"{date_str:<12} | {b['type']:<6} | {market:<12} | {label:<20} | {b['line']:<6} | {actual_desc:<8} | {status}")
            else:
                summary['pending'] += 1

    print("-" * 110)
    wr = (summary['wins'] / summary['total'] * 100) if summary['total'] > 0 else 0
    print(f"SUMMARY: Total: {summary['total']} | Wins: {summary['wins']} | Losses: {summary['losses']} | Pending: {summary['pending']}")
    print(f"WIN RATE: {wr:.1f}%")
    print("=" * 110 + "\n")

if __name__ == "__main__":
    main()
