import json

g_bets=json.load(open('data/logged_game_bets.json'))
recent_games = set([b['game'] for b in g_bets if '2026-04-24T' in b['timestamp']])

print("\n🏀 TODAY'S TOP GAME LINES (FanDuel)")
print("GAME       | MARKET     | TEAM/SIDE  | LINE | ODDS | EV%")
print("-" * 65)
if recent_games:
    top_g=[b for b in g_bets if b['ev'] > 0 and b['book'] == 'FanDuel' and '2026-04-24T' in b['timestamp']]
    top_g.sort(key=lambda x: x['ev'], reverse=True)
    for b in top_g[:5]:
        print(f"{b['game']:10} | {b['market'][:10]:10} | {b['team'][:10]:10} | {b['line']:4} | {b['odds']:4} | {b['ev']*100:.1f}%")
else:
    print("No +EV Game Lines found for today.")

print("\n🎯 TODAY'S TOP PLAYER PROPS (FanDuel)")
print("PLAYER          | GAME      | METRIC     | LINE | SIDE  | ODDS | EV%")
print("-" * 70)
try:
    p_bets=json.load(open('data/logged_bets.json'))
    top_p=[b for b in p_bets if b['ev'] > 0 and b['book'] == 'FanDuel' and '2026-04-24T' in b['timestamp']]
    top_p.sort(key=lambda x: x['ev'], reverse=True)
    for b in top_p[:15]:
        print(f"{b['player'][:15]:15} | {b['game']} | {b['metric'][:10]:10} | {b['line']:4} | {b['side']:5} | {b['odds']:4} | {b['ev']*100:.1f}%")
except Exception as e:
    print('Error props:', e)

