import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.odds_api_collector import OddsAPICollector

odds = OddsAPICollector()
games = odds.fetch_live_odds(markets="h2h")
for g in games:
    print(f"Game: {g['away_team']} @ {g['home_team']} | ID: {g['id']}")
    props = odds.fetch_player_props(g['id'], ["player_points"])
    for b in props.get("bookmakers", []):
        for m in b.get("markets", []):
            for o in m.get("outcomes", []):
                name = o.get("description")
                if "Kuminga" in name or "Dosunmu" in name:
                    print(f"  Found {name} in game {g['away_team']} @ {g['home_team']}!")
