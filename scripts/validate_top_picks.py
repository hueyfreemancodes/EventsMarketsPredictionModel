import os
import sys
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.data_collection.nba_api_collector import NBADataCollector

def main():
    date_str = "2026-04-24"
    nba = NBADataCollector()
    game_results = nba.get_game_results(date_str)
    player_actuals = nba.fetch_player_actuals(date_str)

    game_picks = [
        {"game": "SAS @ POR", "market": "Total", "side": "OVER", "line": 219.5},
        {"game": "LAL @ HOU", "market": "Total", "side": "OVER", "line": 206.5},
        {"game": "LAL @ HOU", "market": "Spread", "team": "LAL", "line": 9.5},
        {"game": "BOS @ PHI", "market": "Spread", "team": "PHI", "line": 7.5},
        {"game": "BOS @ PHI", "market": "Total", "side": "OVER", "line": 215.5},
    ]

    prop_picks = [
        {"player": "Paul George", "metric": "points", "line": 18.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Kevin Durant", "metric": "points", "line": 23.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "LeBron James", "metric": "rebounds", "line": 6.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Tyrese Maxey", "metric": "threes", "line": 2.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Jayson Tatum", "metric": "rebounds", "line": 10.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Derrick White", "metric": "points", "line": 13.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Alperen Sengun", "metric": "rebounds", "line": 8.5, "side": "UNDER", "model": "XGBoost Boosted"},
        {"player": "Sam Hauser", "metric": "points", "line": 8.5, "side": "UNDER", "model": "Bayesian Only"},
        {"player": "Luke Kornet", "metric": "rebounds", "line": 7.5, "side": "UNDER", "model": "Bayesian Only"},
        {"player": "Toumani Camara", "metric": "rebounds", "line": 4.5, "side": "UNDER", "model": "Bayesian Only"},
    ]

    print("\n" + "="*100)
    print(f" 🎯 OFFICIAL TOP PICKS VALIDATION REPORT - {date_str}")
    print("="*100)
    print(f"{'TYPE':<6} | {'MARKET':<12} | {'TARGET':<20} | {'LINE/SIDE':<12} | {'RESULT':<8} | {'W/L'}")
    print("-" * 100)

    wins = 0
    losses = 0
    pending = 0

    # 1. Evaluate Game Lines
    for p in game_picks:
        win = False
        actual_desc = "N/A"
        teams = p['game'].split(' @ ')
        away, home = teams[0], teams[1]
        
        if p['market'] == 'Total':
            if away in game_results and home in game_results:
                total_score = game_results[away]['score'] + game_results[home]['score']
                actual_desc = str(total_score)
                win = (total_score > p['line']) if p['side'] == "OVER" else (total_score < p['line'])
                status = "WIN" if win else "LOSS"
            else:
                status = "PENDING"
            print(f"GAME   | Total        | {p['game']:<20} | {p['side']} {p['line']:<6} | {actual_desc:<8} | {status}")
            
        elif p['market'] == 'Spread':
            if away in game_results and home in game_results:
                target_team = p['team']
                is_home = (target_team == home)
                t_score = game_results[home]['score'] if is_home else game_results[away]['score']
                o_score = game_results[away]['score'] if is_home else game_results[home]['score']
                actual_desc = f"{t_score}-{o_score}"
                win = (t_score + p['line']) > o_score
                status = "WIN" if win else "LOSS"
            else:
                status = "PENDING"
            print(f"GAME   | Spread       | {p['game']:<20} | {p['team']} +{p['line']:<6} | {actual_desc:<8} | {status}")
            
        if status == "WIN": wins += 1
        elif status == "LOSS": losses += 1
        else: pending += 1

    print("-" * 100)

    # 2. Evaluate Props
    m_map = {'points': 'pts', 'rebounds': 'reb', 'assists': 'ast', 'threes': 'fg3m'}
    for p in prop_picks:
        actual = player_actuals.get(p['player'])
        actual_desc = "N/A"
        if actual:
            val = actual.get(m_map.get(p['metric'], p['metric']))
            if val is not None:
                actual_desc = str(val)
                win = (val > p['line']) if p['side'] == "OVER" else (val < p['line'])
                status = "WIN" if win else "LOSS"
            else:
                status = "PENDING"
        else:
            status = "DNP/PENDING"
            
        if status == "WIN": wins += 1
        elif status == "LOSS": losses += 1
        else: pending += 1
        
        print(f"PROP   | {p['metric']:<12} | {p['player']:<20} | {p['side']} {p['line']:<6} | {actual_desc:<8} | {status}")

    print("-" * 100)
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0
    print(f"SUMMARY: Total: {total} | Wins: {wins} | Losses: {losses} | Pending/DNP: {pending}")
    print(f"WIN RATE: {wr:.1f}%")
    print("=" * 100 + "\n")

if __name__ == "__main__":
    main()
