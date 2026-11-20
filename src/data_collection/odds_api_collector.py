import os
import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Add project root to sys.path if needed
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.nba_mapping import normalize_team_name, get_team_abbr

logger = logging.getLogger(__name__)
load_dotenv()

class OddsAPICollector:
    """
    Fetches real-time NBA odds from The Odds API.
    """
    
    BASE_URL = "https://api.the-odds-api.com/v4/sports"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("ODDS_API_KEY")
        if not self.api_key:
            raise ValueError("ODDS_API_KEY must be provided or set in environment.")

    def fetch_live_odds(self, sport: str = "basketball_nba", regions: str = "us", markets: str = "h2h") -> List[Dict[str, Any]]:
        """
        Fetches current moneyline odds for NBA games.
        """
        logger.info(f"Fetching {sport} odds from The Odds API...")
        
        url = f"{self.BASE_URL}/{sport}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": "american",
            "bookmakers": "fanduel"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            logger.error(f"Error fetching odds: {e}")
            return []

    def get_game_lines(self, data: List[Dict[str, Any]], target_books: List[str] = ["FanDuel"]) -> List[Dict[str, Any]]:
        """
        Processes raw Odds API data into a clean list of game lines.
        """
        game_lines = []
        
        for game in data:
            home_team = game.get("home_team")
            away_team = game.get("away_team")
            commence_time = game.get("commence_time")
            
            home_abbr = get_team_abbr(home_team)
            away_abbr = get_team_abbr(away_team)
            
            if not home_abbr or not away_abbr:
                logger.warning(f"Could not map teams: {home_team} vs {away_team}")
                continue
                
            game_id = f"{away_abbr}@{home_abbr}_{commence_time[:10]}"
            
            lines = {
                "event_id": game.get("id"),
                "game_id": game_id,
                "home_team": home_team,
                "away_team": away_team,
                "home_abbr": home_abbr,
                "away_abbr": away_abbr,
                "commence_time": commence_time,
                "books": {}
            }
            
            for bookmaker in game.get("bookmakers", []):
                book_name = bookmaker.get("title")
                if book_name in target_books:
                    books_data = {}
                    
                    h2h_market = next((m for m in bookmaker.get("markets", []) if m["key"] == "h2h"), None)
                    if h2h_market:
                        outcomes = h2h_market.get("outcomes", [])
                        books_data["home_ml"] = next((o["price"] for o in outcomes if o["name"] == home_team), None)
                        books_data["away_ml"] = next((o["price"] for o in outcomes if o["name"] == away_team), None)
                        
                    spreads_market = next((m for m in bookmaker.get("markets", []) if m["key"] == "spreads"), None)
                    if spreads_market:
                        outcomes = spreads_market.get("outcomes", [])
                        home_spread_obj = next((o for o in outcomes if o["name"] == home_team), None)
                        away_spread_obj = next((o for o in outcomes if o["name"] == away_team), None)
                        if home_spread_obj and away_spread_obj:
                            books_data["home_spread"] = home_spread_obj.get("point")
                            books_data["home_spread_odds"] = home_spread_obj.get("price")
                            books_data["away_spread"] = away_spread_obj.get("point")
                            books_data["away_spread_odds"] = away_spread_obj.get("price")
                            
                    totals_market = next((m for m in bookmaker.get("markets", []) if m["key"] == "totals"), None)
                    if totals_market:
                        outcomes = totals_market.get("outcomes", [])
                        over_obj = next((o for o in outcomes if o["name"] == "Over"), None)
                        under_obj = next((o for o in outcomes if o["name"] == "Under"), None)
                        if over_obj and under_obj:
                            books_data["total"] = over_obj.get("point")
                            books_data["over_odds"] = over_obj.get("price")
                            books_data["under_odds"] = under_obj.get("price")

                    if books_data:
                        lines["books"][book_name] = books_data
            
            if lines["books"]:
                game_lines.append(lines)
                
        return game_lines

    def fetch_player_props(self, event_id: str, markets: List[str], sport: str = "basketball_nba", regions: str = "us") -> Dict[str, Any]:
        """
        Fetches player props for a specific event.
        """
        logger.info(f"Fetching player props for event {event_id}...")
        
        url = f"{self.BASE_URL}/{sport}/events/{event_id}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": "american",
            "bookmakers": "fanduel"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching props for {event_id}: {e}")
            return {}

    def process_props(self, data: Dict[str, Any], target_books: List[str] = ["FanDuel"]) -> List[Dict[str, Any]]:
        """
        Processes raw event props into a clean list of player prop lines.
        """
        prop_lines = []
        if not data or "bookmakers" not in data:
            return []

        for bookmaker in data.get("bookmakers", []):
            book_name = bookmaker.get("title")
            if book_name not in target_books:
                continue

            for market in bookmaker.get("markets", []):
                market_key = market.get("key")
                
                # Group by player
                player_outcomes = {}
                for outcome in market.get("outcomes", []):
                    player_name = outcome.get("description") # Player name is in description for props
                    if player_name not in player_outcomes:
                        player_outcomes[player_name] = {"over": None, "under": None, "line": None}
                    
                    if outcome.get("name") == "Over":
                        player_outcomes[player_name]["over"] = outcome.get("price")
                        player_outcomes[player_name]["line"] = outcome.get("point")
                    elif outcome.get("name") == "Under":
                        player_outcomes[player_name]["under"] = outcome.get("price")
                        player_outcomes[player_name]["line"] = outcome.get("point")

                for player, odds in player_outcomes.items():
                    if odds["over"] and odds["under"]:
                        prop_lines.append({
                            "player": player,
                            "book": book_name,
                            "market": market_key,
                            "line": odds["line"],
                            "over": odds["over"],
                            "under": odds["under"]
                        })
        
        return prop_lines

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = OddsAPICollector()
    raw_data = collector.fetch_live_odds()
    clean_lines = collector.get_game_lines(raw_data)
    
    if clean_lines:
        first_game = clean_lines[0]
        print(f"Fetching props for {first_game['game_id']}...")
        props_data = collector.fetch_player_props(first_game['event_id'], ["player_points", "player_rebounds"])
        clean_props = collector.process_props(props_data)
        
        import json
        print(json.dumps(clean_props[:5], indent=2))
