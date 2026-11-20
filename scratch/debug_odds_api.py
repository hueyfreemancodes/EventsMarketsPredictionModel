import os
import httpx
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")

url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?apiKey={API_KEY}&regions=us&markets=h2h&bookmakers=draftkings,fanduel"
response = httpx.get(url)
data = response.json()

print(f"Total games returned: {len(data)}")
for game in data:
    print(f"{game['home_team']} vs {game['away_team']} | Start: {game['commence_time']}")
