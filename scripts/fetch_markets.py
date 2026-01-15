
import requests
import json
import time
from datetime import datetime, timedelta
import sys
import os

# Filter for active/upcoming games
NBA_TAG_ID = "100639"
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"

def fetch_active_nba_markets():
    print(f"Fetching active NBA markets (Tag: {NBA_TAG_ID})...", flush=True)
    
    active_markets = []
    current_time = datetime.utcnow().isoformat()

    # Fetch only OPEN markets for live collector efficiency
    for is_closed in ["false"]:
        print(f"--- Fetching markets (closed={is_closed}) ---", flush=True)
        offset = 0
        limit = 100
        has_more = True
        
        while has_more:
            print(f"Fetching offset {offset}...", flush=True)
            params = {
                "closed": is_closed,
                "tag_id": NBA_TAG_ID,
                "limit": limit,
                "offset": offset
            }
            
            # Safety break
            if offset >= 2000:
                print("Limit reached.", flush=True)
                break
        
            try:
                response = requests.get(GAMMA_API_URL, params=params, timeout=10)
                response.raise_for_status()
                events = response.json()
            except Exception as e:
                print(f"Error fetching API: {e}", flush=True)
                break
                
            if not events:
                print("No more events returned.", flush=True)
                has_more = False
                break
            
            # Process Events
            for event in events:
                title = event.get('title', '')
                start_date = event.get('startDate')
                slug = event.get('slug', '').lower()
                
                # Filter for NBA
                if "nba-" in slug and " vs. " in title:
                    # Parse Date from slug to filter out OLD games (even if API says active)
                    try:
                        # Slug format: nba-tm1-tm2-YYYY-MM-DD
                        date_str = slug[-10:]
                        game_date = datetime.strptime(date_str, "%Y-%m-%d")
                        # Allow games from yesterday (in case they went late) but discard older
                        cutoff = datetime.utcnow() - timedelta(hours=24)
                        if game_date < cutoff:
                            # print(f"Skipping old game: {slug}", flush=True)
                            continue
                    except:
                        pass

                    for market in event.get('markets', []):
                        if market.get('closed'): continue

                        question = market.get('question', '')
                        group = market.get('groupItemTitle', '')
                        
                        # --- FILTERS ---
                        is_main_line = False
                        
                        # 1. Moneyline
                        # strict: "Winner" or "Moneyline"
                        # loose: Question matches Event Title exactly (e.g. "Spurs vs. Thunder")
                        if "Winner" in question or "Moneyline" in str(group):
                            is_main_line = True
                        elif question.strip() == title.strip():
                            is_main_line = True
                        # 2. Spread
                        elif "Spread" in question or "Handicap" in question:
                             is_main_line = True
                        # 3. Totals
                        elif "Total" in question or "Over" in question or "Under" in question:
                            is_main_line = True
                        
                        # 4. Exclude Player Props
                        prop_keywords = ["Rebounds", "Assists", "Threes", "Points", "Double Double", "Steals", "Blocks"]
                        if "Total" in question:
                            pass
                        elif any(k in question for k in prop_keywords):
                            is_main_line = False
                                
                        if not is_main_line:
                            continue

                        # Check ID
                        raw_tokens = market.get('clobTokenIds', [])
                        if isinstance(raw_tokens, str):
                            try: raw_tokens = json.loads(raw_tokens)
                            except: raw_tokens = []
                        
                        if not raw_tokens or not isinstance(raw_tokens, list):
                            continue

                        # Create a record for EACH token (Asset)
                        # Token 0 = Team 1 (usually), Token 1 = Team 2 (usually)
                        teams = title.split(" vs. ")
                        team1 = teams[0].strip() if len(teams) == 2 else "Team1"
                        team2 = teams[1].strip() if len(teams) == 2 else "Team2"

                        for i, token_id in enumerate(raw_tokens):
                            # Guess the outcome name: Token 0 is usually Team 1, Token 1 is Team 2
                            # This is a heuristic, but good enough for logging
                            outcome_label = team1 if i == 0 else team2
                            if i > 1: outcome_label = f"Outcome_{i}"

                            market_record = {
                                "market_id": market.get('id'),
                                "title": title,
                                "question": question,
                                "start_date": start_date,
                                "slug": event.get('slug'),
                                "group": group,
                                "clob_token_id": token_id,
                                "token_index": i,
                                "outcome_label": outcome_label, 
                                "team1": team1,
                                "team2": team2
                            }
                            
                            active_markets.append(market_record)

            # INCREMENT OFFSET
            print(f"Processed batch. Incrementing offset from {offset} to {offset+limit}", flush=True)
            offset += limit

    print(f"Found {len(active_markets)} Active Main-Line NBA markets.", flush=True)
    return active_markets

if __name__ == "__main__":
    markets = fetch_active_nba_markets()
    if markets:
        # Dedupe
        unique_markets = list({m['clob_token_id']: m for m in markets}.values())
        print(f"Unique markets: {len(unique_markets)}", flush=True)
        
        with open("nba_game_markets.json", "w") as f:
            json.dump(unique_markets, f, indent=2)
        print("Updated nba_game_markets.json", flush=True)
    else:
        print("No active markets found.", flush=True)
