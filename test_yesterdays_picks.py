import json
import sys
from datetime import datetime
from src.data_collection.nba_api_collector import NBADataCollector

nba = NBADataCollector()
actuals = nba.fetch_player_actuals('2026-04-23')
game_results = nba.get_game_results('2026-04-23')

def normalize(name):
    return name.replace('ć', 'c').replace('.', '')

normalized_actuals = {normalize(k): v for k, v in actuals.items()}

top_player_props = [
    {"player": "Tim Hardaway Jr", "metric": "threes", "line": 1.5, "side": "UNDER"},
    {"player": "Naz Reid", "metric": "rebounds", "line": 4.5, "side": "UNDER"},
    {"player": "Ayo Dosunmu", "metric": "assists", "line": 2.5, "side": "UNDER"},
    {"player": "Nikola Jokic", "metric": "threes", "line": 1.5, "side": "UNDER"},
    {"player": "Ayo Dosunmu", "metric": "rebounds", "line": 2.5, "side": "UNDER"},
    {"player": "Anthony Edwards", "metric": "rebounds", "line": 5.5, "side": "UNDER"},
    {"player": "Cameron Johnson", "metric": "rebounds", "line": 3.5, "side": "UNDER"},
    {"player": "Nikola Jokic", "metric": "assists", "line": 9.5, "side": "UNDER"},
    {"player": "Jamal Murray", "metric": "rebounds", "line": 4.5, "side": "UNDER"},
    {"player": "Christian Braun", "metric": "rebounds", "line": 4.5, "side": "UNDER"},
]

print("🎯 YESTERDAY'S TOP PLAYER PROPS RESULTS")
print(f"{'PLAYER':<18} | {'METRIC':<10} | {'LINE':<5} | {'SIDE':<5} | {'ACTUAL':<6} | {'RESULT':<6}")
print("-" * 65)

wins = 0
losses = 0

m_map = {'points': 'pts', 'rebounds': 'reb', 'assists': 'ast', 'threes': 'fg3m'}

for prop in top_player_props:
    p_name = prop['player']
    actual = normalized_actuals.get(p_name)
    if not actual:
        print(f"{p_name:<18} | {prop['metric']:<10} | {prop['line']:<5} | {prop['side']:<5} | {'DNP':<6} | {'PUSH':<6}")
        continue
    
    val = actual.get(m_map.get(prop['metric'], prop['metric']))
    if val is None:
        continue
        
    win = False
    if prop['side'] == 'OVER':
        win = val > prop['line']
    else:
        win = val < prop['line']
        
    if win:
        wins += 1
        res = "WIN"
    else:
        losses += 1
        res = "LOSS"
        
    print(f"{p_name:<18} | {prop['metric']:<10} | {prop['line']:<5} | {prop['side']:<5} | {val:<6} | {res:<6}")


print("\n🏀 YESTERDAY'S TOP GAME LINES RESULTS")
print(f"{'GAME':<18} | {'MARKET':<10} | {'SIDE':<5} | {'LINE':<5} | {'ACTUAL':<6} | {'RESULT':<6}")
print("-" * 65)

cle_res = game_results.get('CLE')
tor_res = game_results.get('TOR')
if cle_res and tor_res:
    total_pts = cle_res['score'] + tor_res['score']
    win = total_pts > 221.5
    res = "WIN" if win else "LOSS"
    if win: wins += 1
    else: losses += 1
    print(f"{'CLE @ TOR':<18} | {'Total':<10} | {'OVER':<5} | {'221.5':<5} | {total_pts:<6} | {res:<6}")

print("-" * 65)
total_bets = wins + losses
win_pct = (wins / total_bets) * 100 if total_bets > 0 else 0
print(f"TOTAL WINS: {wins}")
print(f"TOTAL LOSSES: {losses}")
print(f"WIN PERCENTAGE: {win_pct:.1f}%")

