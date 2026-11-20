import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.nba_api_collector import NBADataCollector
from src.data_collection.odds_api_collector import OddsAPICollector

nba = NBADataCollector(lookback_games=10)
odds = OddsAPICollector()

game_date_str = "2026-04-25"
todays_games = nba.get_todays_games(date_str=game_date_str)
active_teams = set()
for g in todays_games:
    active_teams.add(g['home_abbrev'])
    active_teams.add(g['away_abbrev'])
print("Today's teams:", active_teams)

live_odds = odds.fetch_live_odds()
all_game_lines = odds.get_game_lines(live_odds)
game_lines = [g for g in all_game_lines if g['home_abbr'] in active_teams and g['away_abbr'] in active_teams]

print(f"\nFound {len(game_lines)} games with lines\n")

for game in game_lines:
    event_id = game['event_id']
    home = game['home_abbr']
    away = game['away_abbr']
    print(f"\n=== {away} @ {home} (event_id: {event_id}) ===")
    
    raw_props = odds.fetch_player_props(event_id, ["player_rebounds"])
    clean = odds.process_props(raw_props)
    
    # Find KAT and ANT specifically
    for p in clean:
        if p['player'] in ['Karl-Anthony Towns', 'Anthony Edwards', 'Devin Booker', 'Shai Gilgeous-Alexander']:
            print(f"  {p['player']} | {p['market']} | line={p['line']} | over={p['over']} | under={p['under']} | book={p['book']}")
