import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.odds_api_collector import OddsAPICollector

odds = OddsAPICollector()
# NYK @ ATL event for KAT
raw = odds.fetch_player_props("184d31a9a32dffc9c91eac3766d6f12d", ["player_rebounds"])

for bm in raw.get("bookmakers", []):
    if bm["title"] != "FanDuel": continue
    for market in bm["markets"]:
        for outcome in market["outcomes"]:
            if "Towns" in str(outcome.get("description", "")):
                print(json.dumps(outcome, indent=2))
