import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from scripts.fetch_markets import fetch_active_nba_markets

markets = fetch_active_nba_markets()
print(f"Total valid markets: {len(markets)}")
for m in markets:
    print(f"DEBUG MARKET: {m}")
