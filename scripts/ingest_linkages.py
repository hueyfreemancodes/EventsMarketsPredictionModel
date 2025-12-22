import json
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_collection.ingester import QuestDBIngester

def ingest_data():
    ingester = QuestDBIngester()
    
    # 1. Ensure table exists
    ingester.create_market_linkages_table()
    
    # 2. Clear existing data (for clean re-ingest)
    print("Clearing existing data...")
    try:
        cur = ingester.conn.cursor()
        cur.execute("TRUNCATE TABLE market_linkages")
        ingester.conn.commit()
        cur.close()
    except Exception as e:
        print(f"Truncate failed (might be empty): {e}")
        ingester.conn.rollback()
    
    # Files to ingest
    files = [
        'nba_game_markets.json',       # Polymarket (Tag 100639)
        'kalshi_verified_games.json'   # Kalshi (KXNBAGAME)
    ]
    
    total_ingested = 0
    
    for filename in files:
        if not os.path.exists(filename):
            print(f"Skipping {filename} - not found")
            continue
            
        print(f"Ingesting {filename}...")
        with open(filename, 'r') as f:
            data = json.load(f)
            
        for record in data:
            # Prepare record for QuestDB
            source = record.get('source', 'polymarket') # Default to polymarket if missing
            series_ticker = record.get('series_ticker', None)
            
            market_id = None
            game_date = None
            original_title = None
            team1 = record.get('team1')
            team2 = record.get('team2')
            
            if source == 'polymarket':
                market_id = record.get('market_id')
                original_title = record.get('title')
                
                # Parse date from slug if possible, e.g. "nba-mem-min-2025-12-17"
                slug = record.get('slug', '')
                try:
                    # simplistic extraction: take last 10 chars if they look like date
                    if len(slug) >= 10:
                        date_part = slug[-10:]
                        game_date = datetime.strptime(date_part, "%Y-%m-%d")
                except:
                    # Fallback to start_date (which might be market start, not game)
                    s_date = record.get('start_date')
                    if s_date:
                        try:
                            # Handle ISO format
                            game_date = datetime.fromisoformat(s_date.replace('Z', '+00:00'))
                        except:
                            pass
                            
            elif source == 'kalshi':
                market_id = record.get('id')
                original_title = record.get('original_title')
                date_str = record.get('date')
                if date_str:
                    try:
                        game_date = datetime.strptime(date_str, "%Y-%m-%d")
                    except:
                        pass
            
            db_record = {
                'market_id': market_id,
                'source': source,
                'team1': team1,
                'team2': team2,
                'game_date': game_date,
                'original_title': original_title,
                'series_ticker': series_ticker,
                'created_at': datetime.utcnow()
            }
            
            try:
                ingester.ingest_market_linkage(db_record)
                total_ingested += 1
            except Exception as e:
                print(f"Failed to ingest {market_id}: {e}")
                
    print(f"✅ Ingestion complete. {total_ingested} records added to market_linkages.")
    ingester.close()

if __name__ == "__main__":
    ingest_data()
