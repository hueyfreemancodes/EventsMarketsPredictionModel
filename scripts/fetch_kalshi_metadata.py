
import sys
import os
import json
import asyncio
import logging
from datetime import datetime, timedelta

# Path hack
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_collection.kalshi_client import KalshiClient
from config.api_keys import get_kalshi_credentials
from src.data_collection.nba_team_abbreviations import get_team_abbreviation

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("KalshiFetcher")

def extract_teams_from_title(title: str):
    """
    Parses "Washington at Sacramento" -> (WAS, SAC)
    """
    try:
        if " at " in title:
            parts = title.lower().split(" at ")
            away_str = parts[0].strip()
            home_str = parts[1].split(" winner")[0].strip() # Remove "winner?"
            
            away = get_team_abbreviation(away_str)
            home = get_team_abbreviation(home_str)
            return away, home
    except Exception:
        pass
    return None, None

async def main():
    logger.info("Fetching Kalshi Metadata...")
    
    creds = get_kalshi_credentials()
    if not os.path.exists(creds['api_secret']):
         logger.error("No credentials.")
         return

    client = KalshiClient(
        api_key=creds['api_key'],
        api_secret=creds['api_secret']
    )
    
    # 1. Fetch Markets
    markets = client.discover_markets_by_event(series_ticker="KXNBAGAME", limit=100)
    logger.info(f"Found {len(markets)} markets.")
    
    output_records = []
    
    for m in markets:
        # Looking for Game Winner markets mostly
        # Ticker e.g., KXNBAGAME-26JAN14LALDAL-LAL
        # Actually proper Game Winner market tickers usually end in team?
        # But 'KXNBAGAME' returns options.
        # We need the EVENT level or the MARKET level?
        # The 'discover' returns markets. 
        
        # Valid Title: "Dallas at LA Lakers Winner?"
        if "Winner" in m['title']:
            away, home = extract_teams_from_title(m['title'])
            
            # Date Extraction from Ticker? 
            # KXNBAGAME-26JAN14... -> 2026-01-14
            # Ticker format: KXNBAGAME-YYMMMDD...
            # 26JAN14 -> 2026-01-14
            try:
                date_part = m['ticker'].split('-')[1][:7] # 26JAN14
                game_date = datetime.strptime(date_part, "%y%b%d")
            except:
                game_date = datetime.now() # Fallback

            if away and home:
                record = {
                    "source": "kalshi",
                    "id": m['ticker'],
                    "title": m['title'],
                    "original_title": m['title'],
                    "team1": away, # Linker expects team1/team2 (no home/away strictness, but good to be Consistent)
                    "team2": home,
                    "date": game_date.strftime("%Y-%m-%d"),
                    "series_ticker": "KXNBAGAME"
                }
                output_records.append(record)
    
    # Save
    outfile = "kalshi_verified_games.json"
    with open(outfile, 'w') as f:
        json.dump(output_records, f, indent=2)
        
    logger.info(f"Saved {len(output_records)} verified markets to {outfile}")

if __name__ == "__main__":
    asyncio.run(main())
