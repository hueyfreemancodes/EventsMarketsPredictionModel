import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.nba_api_collector import NBADataCollector
from nba_api.stats.endpoints import playergamelog

nba = NBADataCollector(lookback_games=10)
player_id = nba.fetch_player_id("Devin Booker")
log = playergamelog.PlayerGameLog(player_id=player_id, season="2025-26")
df = log.get_data_frames()[0]
print(df.columns)
