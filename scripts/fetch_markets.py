
import requests
import json
import time
from datetime import datetime, timedelta
import sys
import os

# Filter for active/upcoming games
NBA_TAG_ID = "745"
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"

def fetch_active_nba_markets(mode='winner'):
    """
    mode: 'winner' (Moneyline) or 'props' (Player Props)
    """
    print(f"Fetching active NBA {mode} markets (Tag: {NBA_TAG_ID})...", flush=True)
    
    active_markets = []
    
    # Fetch only OPEN markets
    for is_closed in ["false"]:
        offset = 0
        limit = 100
        has_more = True
        
        while has_more:
            params = {
                "closed": is_closed,
                "tag_id": NBA_TAG_ID,
                "limit": limit,
                "offset": offset
            }
            
            try:
                response = requests.get(GAMMA_API_URL, params=params, timeout=10)
                response.raise_for_status()
                events = response.json()
            except Exception as e:
                break
                
            if not events:
                has_more = False
                break
            
            for event in events:
                title = event.get('title', '')
                slug = event.get('slug', '').lower()
                
                # Filter for NBA
                if "nba-" not in slug: continue

                for market in event.get('markets', []):
                    if market.get('closed'): continue

                    question = market.get('question', '')
                    group = market.get('groupItemTitle', '')
                    
                    is_valid = False
                    player_name = None
                    
                    if mode == 'winner':
                        # STRICT KEYWORD FILTER (Exclude Props)
                        exclude_keywords = ["Points", "Rebounds", "Assists", "Threes", "Double Double", "Final Score"]
                        if any(k.lower() in question.lower() for k in exclude_keywords): continue

                        is_winner = any(k.lower() in question.lower() for k in ["winner", "win", "who will win"])
                        if is_winner or question.strip() == title.strip():
                            is_valid = True
                    
                    elif mode == 'props':
                        prop_keywords = ["Points", "Rebounds", "Assists", "Threes", "Double Double", "Steals", "Blocks"]
                        if any(k in question for k in prop_keywords):
                            is_valid = True
                            # Heuristic for player name: Look for common NBA player names or capitalized words
                            # Polymarket usually puts the player name in the question
                            # e.g. "How many points will LeBron James score?"
                            # We'll use a simple approach: find capitalized words not in title
                            words = question.split()
                            capitalized = [w.strip("?,.") for w in words if w[0].isupper() and w.lower() not in ["how", "many", "will", "score", "points", "rebounds", "assists"]]
                            if len(capitalized) >= 2:
                                player_name = " ".join(capitalized[:2])

                    if not is_valid: continue

                    # Liquidity & Spread Filters
                    try:
                        spread = float(market.get('spread', 1.0))
                        liquidity = float(market.get('liquidityNum', 0))
                        
                        # More lenient for props as they have lower volume
                        min_liq = 2000 if mode == 'winner' else 500
                        if liquidity < min_liq: continue
                        if spread > (0.08 if mode == 'winner' else 0.15): continue
                    except:
                        continue

                    raw_tokens = market.get('clobTokenIds', [])
                    if not raw_tokens: continue

                    for i, token_id in enumerate(json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens):
                        # Extract teams for winner mode
                        team1, team2 = "Team1", "Team2"
                        if " vs. " in title:
                            parts = title.split(" vs. ")
                            team1 = parts[0].split(":")[-1].strip().replace("NBA Playoffs", "").split("(")[0].strip()
                            team2 = parts[1].split("Total Games")[0].split("(")[0].strip()
                        
                        outcome_label = team1 if i == 0 else team2
                        if i > 1: outcome_label = f"Outcome_{i}"

                        market_record = {
                            "market_id": market.get('id'),
                            "title": title,
                            "question": question,
                            "slug": event.get('slug'),
                            "clob_token_id": token_id,
                            "token_index": i,
                            "player_name": player_name,
                            "outcome_label": outcome_label,
                            "team1": team1,
                            "team2": team2,
                            "price": market.get('lastTradePrice')
                        }
                        active_markets.append(market_record)

            offset += limit

    print(f"Found {len(active_markets)} Active {mode} markets.", flush=True)
    return active_markets

if __name__ == "__main__":
    winner_markets = fetch_active_nba_markets(mode='winner')
    prop_markets = fetch_active_nba_markets(mode='props')
    
    all_markets = winner_markets + prop_markets
    if all_markets:
        with open("nba_game_markets.json", "w") as f:
            json.dump(all_markets, f, indent=2)
        print("Updated nba_game_markets.json with Winners and Props", flush=True)
