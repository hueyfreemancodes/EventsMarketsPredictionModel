import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.nba_api_collector import NBADataCollector

nba = NBADataCollector(lookback_games=10)
stats = nba.fetch_player_stats("Devin Booker", game_date_str="2026-04-25")
if stats:
    print(f"Devin Booker team_abbr: {stats.get('team_abbr')}")

stats2 = nba.fetch_player_stats("Onyeka Okongwu", game_date_str="2026-04-25")
if stats2:
    print(f"Onyeka Okongwu team_abbr: {stats2.get('team_abbr')}")
