import requests
import json

def check_market(id):
    try:
        url = f"https://gamma-api.polymarket.com/markets/{id}"
        r = requests.get(url).json()
        print(f"--- Market {id} ---")
        print(f"Title: {r.get('title')}")
        print(f"Group: {r.get('groupItemTitle')}")
        print(f"Spread: {r.get('spread')}")
        print(f"Liquidity: {r.get('liquidityNum')}")
        print(f"Volume: {r.get('volumeNum')}")
        print(f"Active: {r.get('active')}")
        print(f"Closed: {r.get('closed')}")
    except Exception as e:
        print(f"Error fetching {id}: {e}")

# Zombie Market (from report)
check_market("1184041") 

# Potentially Active Market (maybe duplicate?)
check_market("1193944")
