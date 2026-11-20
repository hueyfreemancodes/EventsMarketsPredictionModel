import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.odds_api_collector import OddsAPICollector

odds = OddsAPICollector()
raw = odds.fetch_player_props("184d31a9a32dffc9c91eac3766d6f12d", ["player_rebounds"])
clean = odds.process_props(raw)
for p in clean:
    if "Towns" in p["player"] or "Edwards" in p["player"]:
        print(json.dumps(p, indent=2))

# Now fetch ALL markets and check KAT across all of them
raw2 = odds.fetch_player_props("184d31a9a32dffc9c91eac3766d6f12d", ["player_rebounds", "player_points", "player_assists", "player_threes"])
clean2 = odds.process_props(raw2)
kat_rows = [p for p in clean2 if "Towns" in p["player"]]
print("\nAll KAT props after process_props:")
for r in kat_rows:
    print(f"  {r['market']} | line={r['line']}")
    
# Also check ANT from the DEN@MIN game
raw3 = odds.fetch_player_props("b24e970fd53cf667dd009e54d107d4b1", ["player_rebounds", "player_points"])
clean3 = odds.process_props(raw3)
ant_rows = [p for p in clean3 if "Edwards" in p["player"]]
print("\nAll ANT props after process_props:")
for r in ant_rows:
    print(f"  {r['market']} | line={r['line']}")
