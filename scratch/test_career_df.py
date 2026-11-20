import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.nba_api_collector import NBADataCollector
from nba_api.stats.endpoints import playercareerstats

nba = NBADataCollector(lookback_games=10)
player_id = nba.fetch_player_id("Devin Booker")
career = playercareerstats.PlayerCareerStats(player_id=player_id)
df = career.get_data_frames()[0]
print(df.columns)
print(df.tail(1)['TEAM_ABBREVIATION'].values[0])
