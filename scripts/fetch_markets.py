import requests
import json
from datetime import datetime
import sys
import os

# Filter for active/upcoming games
NBA_TAG_ID = "100639"
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"

def fetch_active_nba_markets():
    print(f"Fetching active NBA markets (Tag: {NBA_TAG_ID})...")
    
    active_markets = []
    
    current_time = datetime.utcnow().isoformat()
    print(f"Current UTC Time: {current_time}")

    # Fetch both OPEN and CLOSED markets to ensure coverage for historical data
    for is_closed in ["false", "true"]:
        print(f"Fetching markets (closed={is_closed})...")
        offset = 0
        limit = 100
        has_more = True
        
        while has_more:
            print(f"Fetching offset {offset}...")
            params = {
                "closed": is_closed,
                "tag_id": NBA_TAG_ID,
                "limit": limit,
                "offset": offset
            }
            
            # Safety break for closed markets to save time
            if is_closed == "true" and offset >= 1000:
                print("Limit reached for closed markets.")
                break
        
        try:
            response = requests.get(GAMMA_API_URL, params=params)
            response.raise_for_status()
            events = response.json()
        except Exception as e:
            print(f"Error fetching from Gamma API: {e}")
            break
            
        if not events:
            has_more = False
            break
            
        for event in events:
            title = event.get('title', '')
            start_date = event.get('startDate')
            slug = event.get('slug', '').lower()
            
            # Skip old events (sanity check, though closed=false should handle most)
            # Simple string comparison works for ISO format
            if start_date and start_date < current_time:
                 # Optional: Allow games that started recently (e.g. today) but for now strict future
                 # Actually, we want LIVE games too, so let's check if it's within last 3 hours?
                 # Simplifying: Just check "nba-" slug filter primarily.
                 pass

            # STRICT FILTER: Ensure it's an NBA game via slug
            if "nba-" in slug and " vs. " in title:
                # print(f"MATCH: {title} ({start_date})")
                
                for market in event.get('markets', []):
                    if market.get('closed'):
                        continue
                        
                    # Parse clobTokenIds
                    raw_tokens = market.get('clobTokenIds', [])
                    token_id = None
                    if isinstance(raw_tokens, str):
                        try:
                            raw_tokens = json.loads(raw_tokens)
                        except:
                            raw_tokens = []
                            
                    if isinstance(raw_tokens, list) and len(raw_tokens) > 0:
                        token_id = raw_tokens[0]

                    market_record = {
                        "market_id": market.get('id'),
                        "title": title,
                        "question": market.get('question'),
                        "start_date": start_date,
                        "slug": event.get('slug'),
                        "group": market.get('groupItemTitle', title),
                        "clob_token_id": token_id
                    }
                    
                    teams = title.split(" vs. ")
                    if len(teams) == 2:
                        market_record['team1'] = teams[0].strip()
                        market_record['team2'] = teams[1].strip()
                    
                    active_markets.append(market_record)

        offset += limit
        # Safety break
        if offset > 2000:
            print("Reached safety limit of 2000 events.")
            break

    print(f"Found {len(active_markets)} active NBA markets.")
    return active_markets

if __name__ == "__main__":
    markets = fetch_active_nba_markets()
    
    if markets:
        # Deduplicate by market_id
        unique_markets = {m['market_id']: m for m in markets}.values()
        
        with open("nba_game_markets.json", "w") as f:
            json.dump(list(unique_markets), f, indent=2)
        print("Updated nba_game_markets.json")
    else:
        print("No active markets found.")
