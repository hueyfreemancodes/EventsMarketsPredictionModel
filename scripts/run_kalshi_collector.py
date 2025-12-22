#!/usr/bin/env python3
"""
Run Kalshi Collector
Targeted collection of NBA Spreads and Totals.
Discovery Strategy:
1. Scan Series: ['KXNBAGAME', 'KXNBATOTAL']
2. Fetch Active Events -> Markets
3. Filter for Liquidity (Bid > 0) to remove empty Total strikes.
"""
import sys
import os
import asyncio
import json
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.data_collection.kalshi_client import KalshiClient
from src.data_collection.ingester import QuestDBIngester
from config.api_keys import get_kalshi_credentials

async def main():
    logger.info("🚀 Starting Kalshi NBA Collector")
    
    # Init DB Ingester
    try:
        ingester = QuestDBIngester()
        logger.info("✅ Database Connected.")
    except Exception as e:
        logger.error(f"❌ Database Init Failed: {e}")
        return

    creds = get_kalshi_credentials()
    client = KalshiClient(
        api_key=creds['api_key'],
        api_secret=creds['api_secret']
    )
    
    if not client.enabled:
        logger.error("Client init failed.")
        return

    # 1. Target Series
    target_series = ['KXNBAGAME', 'KXNBATOTAL']
    
    # Storage setup
    os.makedirs('data/kalshi_live', exist_ok=True)
    csv_path = f"data/kalshi_live/nba_snapshots_{datetime.now().strftime('%Y%m%d')}.csv"
    if not os.path.exists(csv_path):
        with open(csv_path, 'w') as f:
            f.write("timestamp,ticker,title,series,yes_bid,yes_ask,volume,status\n")

    logger.info(f"💾 Saving snapshots to: {csv_path}")

    # Polling Loop
    try:
        while True:
            logger.info("--- Polling Cycle ---")
            loop_start = datetime.now()
            
            # Discovery (Refreshing list to catch new markets or updates)
            # In a real daemon, we might cache this for 10 mins, but for robustness we fetch fresh.
            active_markets = []
            for series in target_series:
                found = client.discover_markets_by_event(series_ticker=series, limit=100)
                for m in found:
                     # Filter: Active + Liquid
                    if m.get('status') == 'active' and (m.get('yes_bid') or 0) > 0:
                        active_markets.append(m)
            
            logger.info(f"   Collected {len(active_markets)} active markets.")
            
            # Save Snapshots (CSV + QuestDB)
            with open(csv_path, 'a') as f:
                ts_iso = datetime.utcnow().isoformat()
                for m in active_markets:
                    # CSV Backup
                    row = [
                        ts_iso,
                        m['ticker'],
                        f"\"{m['title']}\"", 
                        m.get('series_ticker', ''),
                        str(m.get('yes_bid', '')),
                        str(m.get('yes_ask', '')),
                        str(m.get('volume', 0)),
                        m.get('status', '')
                    ]
                    f.write(",".join(row) + "\n")
                    
                    # QuestDB Ingestion
                    try:
                        bid_p = m.get('yes_bid')
                        ask_p = m.get('yes_ask')
                        # Calculate mid/spread
                        mid = None
                        spread = None
                        if bid_p and ask_p:
                             mid = (bid_p + ask_p) / 2
                             spread = ask_p - bid_p
                             
                        db_row = {
                            'timestamp': ts_iso,
                            'market_id': m['ticker'],
                            'outcome': 'YES',
                            'platform': 'kalshi',
                            # Level 1
                            'bid_price_1': bid_p,
                            'bid_size_1': m.get('yes_bid_count', 0), # Using count as proxy size if 'volume' not detailed
                            'ask_price_1': ask_p,
                            'ask_size_1': m.get('yes_ask_count', 0),
                            # Levels 2-3 (Empty)
                            'bid_price_2': None, 'bid_size_2': None,
                            'bid_price_3': None, 'bid_size_3': None,
                            'ask_price_2': None, 'ask_size_2': None,
                            'ask_price_3': None, 'ask_size_3': None,
                            # Meta
                            'mid_price': mid,
                            'spread': spread,
                            'total_bid_volume': None,
                            'total_ask_volume': None 
                        }
                        ingester.ingest_order_book_snapshot(db_row)
                    except Exception as ie:
                        logger.error(f"Ingest Error for {m['ticker']}: {ie}")

            logger.info("   ✅ Snapshots saved to CSV and QuestDB.")
            
            # Sleep
            await asyncio.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Stopped by user.")

if __name__ == "__main__":
    asyncio.run(main())
