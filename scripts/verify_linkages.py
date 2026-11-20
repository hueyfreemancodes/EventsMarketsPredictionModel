import psycopg2
import pandas as pd
from tabulate import tabulate

DB_CONFIG = {
    "host": "localhost",
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "database": "qdb"
}

def check_linkages():
    print("Connecting to QuestDB...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    # 1. Get Linkages for Today
    query_links = """
    SELECT market_id, source, team1, team2, original_title 
    FROM market_linkages 
    WHERE game_date >= '2026-01-15T00:00:00.000000Z' 
      AND game_date < '2026-01-16T00:00:00.000000Z'
      AND (
          team1 IN ('LAL','CHA','NYK','GSW','MEM','ORL', 'Lakers', 'Hornets', 'Knicks', 'Warriors', 'Grizzlies', 'Magic') 
       OR team2 IN ('LAL','CHA','NYK','GSW','MEM','ORL', 'Lakers', 'Hornets', 'Knicks', 'Warriors', 'Grizzlies', 'Magic')
      )
    """
    links = pd.read_sql(query_links, conn)
    
    # Just iterate all unique relevant markets found by SQL
    if links.empty:
        print("No linkages found matching query.")
        return

    report = []
    
    # Get Price Data for ALL found links
    ids = tuple(links['market_id'].unique())
    id_list = "', '".join(ids)
    
    query_prices = f"""
    SELECT market_id, bid_price_1, ask_price_1, timestamp 
    FROM order_book_snapshots 
    WHERE market_id IN ('{id_list}') 
    ORDER BY timestamp DESC
    """
    prices = pd.read_sql(query_prices, conn).drop_duplicates('market_id')
    
    # Merge
    merged = pd.merge(links, prices, on='market_id', how='left')
    
    for _, row in merged.iterrows():
        # Clean team selection for display
        team_display = row['team1']
        if 'Winner' in str(row['original_title']): # Kalshi style ?
             team_display = f"{row['team1']} vs {row['team2']}"
             
        report.append({
            'Game': row['original_title'],
            'Source': row['source'],
            'Market ID': row['market_id'][:15] + "...",
            'Team1': row['team1'],
            'Team2': row['team2'],
            'Bid': row['bid_price_1'],
            'Ask': row['ask_price_1'],
            'Last Update': row['timestamp']
        })

    # Output
    print("\n### Market Linkage Report (Jan 15, 2026)")
    if report:
        print(tabulate(pd.DataFrame(report), headers="keys", tablefmt="github"))
    else:
        print("No linkage data found.")

    # Fundamentals
    # Fundamentals
    print("\nfetching fundamentals...")
    fund_query = "SELECT * FROM sports_fundamentals WHERE game_date >= '2026-01-15'"
    fund = pd.read_sql(fund_query, conn)

    print("\n### Fundamentals (Jan 15)")
    teams = ['MEM', 'ORL', 'LAL', 'CHA', 'GSW', 'NYK']
    fund_filtered = fund[fund['home_team'].isin(teams) & fund['away_team'].isin(teams)]
    print(tabulate(fund_filtered[['home_team', 'away_team', 'home_win_pct', 'away_win_pct', 'home_avg_score', 'away_avg_score']], headers="keys", tablefmt="github"))


if __name__ == "__main__":
    check_linkages()
