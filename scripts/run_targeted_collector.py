#!/usr/bin/env python3
"""
Targeted Data Collector
Fetches linked market IDs from the database and begins high-frequency order book collection.
"""
import sys
import os
import asyncio
import os
import signal
import sys
import json
from datetime import datetime
import psycopg2
from typing import List, Dict

# Path hack to allow direct script execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_collection.polymarket_client import PolymarketClient
from src.data_collection.logger import logger
from scripts.fetch_markets import fetch_active_nba_markets
from scripts.ingest_linkages import ingest_data
import time

# Configuration Defaults (Environment variables take precedence)
DB_HOST = os.getenv('QUESTDB_HOST', 'localhost')
DB_PORT = int(os.getenv('QUESTDB_PORT', 8812))
DB_USER = "admin"
DB_PASS = "quest"
DB_NAME = "qdb"
DURATION = int(os.getenv('COLLECTION_DURATION_SECONDS', 60))


def fetch_target_market_ids() -> List[str]:
    """Retrieve active market IDs that have been linked to NBA games."""
    try:
        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT market_id FROM market_linkages WHERE source='polymarket'")
                return [row[0] for row in cur.fetchall() if row[0]]
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return []


def load_market_metadata(filepath: str) -> Dict:
    try:
        with open(filepath, 'r') as f:
            raw_data = json.load(f)
            # Convert List to Dict keyed by market_id
            if isinstance(raw_data, list):
                return {str(m.get('market_id')): m for m in raw_data}
            return raw_data
    except FileNotFoundError:
        logger.error(f"Metadata file not found: {filepath}")
        return {}


async def main():
    logger.info("Starting Smart Targeted Data Collector...")
    
    # Initialize Client
    api_key = api_secret = api_pass = None
    try:
        from config.api_keys import get_polymarket_credentials, has_polymarket_credentials
        if has_polymarket_credentials():
            creds = get_polymarket_credentials()
            api_key = creds.get('api_key')
            api_secret = creds.get('api_secret')
            api_pass = creds.get('api_passphrase')
            logger.info("Authenticated Mode: Enabled ✅")
    except ImportError:
        logger.info("Authenticated Mode: Disabled (Public API Only)")

    client = PolymarketClient(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_pass,
        mode="websocket", # Switch to WSS
        polling_interval=5
    )

    if not client.enabled:
        return
        
    # Connect WSS
    if client.mode == "websocket":
        logger.info("Connecting to WSS...")
        await client.connect(channel_type="market")
        # Start listener task
        asyncio.create_task(client._listen())

    # Loop Variables
    last_refresh_time = 0
    REFRESH_INTERVAL = 900  # 15 minutes
    subscribed_tokens = set()
    
    logger.info("Entering Continuous Smart Loop (Ctrl+C to stop)")
    client.running = True

    while True:
        try:
            current_time = time.time()
            
            # --- 1. REFRESH PHASE ---
            if current_time - last_refresh_time > REFRESH_INTERVAL:
                logger.info("🔄 Refreshing Active Markets List...")
                
                # A. Fetch Active Markets (Gamma API)
                loop = asyncio.get_running_loop()
                markets = await loop.run_in_executor(None, fetch_active_nba_markets)
                
                if markets:
                    # B. Update JSON
                    with open('nba_game_markets.json', 'w') as f:
                        json.dump(markets, f, indent=2)
                        
                    # Dedupe
                    unique_markets = list({m['clob_token_id']: m for m in markets}.values())
                    full_metadata = {str(m.get('clob_token_id')): m for m in unique_markets}
                    
                    # C. Ingest to DB
                    await loop.run_in_executor(None, ingest_data)
                    
                    # D. Identify New Tokens to Subscribe
                    new_tokens_to_sub = []
                    current_tokens = set()
                    
                    for token_id, mdata in full_metadata.items():
                        # We are keyed by Token ID, so this IS the asset ID
                        if token_id:
                            current_tokens.add(token_id)
                            if token_id not in subscribed_tokens:
                                new_tokens_to_sub.append(token_id)
                    
                    # E. Subscribe to New Tokens
                    if new_tokens_to_sub and client.connected:
                        logger.info(f"Subscribing to {len(new_tokens_to_sub)} new tokens...")
                        await client.subscribe_to_markets(new_tokens_to_sub, channel_type="market")
                        subscribed_tokens.update(new_tokens_to_sub)
                    
                    logger.info(f"✅ Targets Updated: Tracking {len(current_tokens)} Active Tokens")
                    last_refresh_time = current_time
                else:
                    logger.warning("No active markets found during refresh.")
            
            # --- 2. REST FALLBACK / KEEP ALIVE ---
            if client.mode == "rest":
                 # Fallback polling logic (removed for brevity/focus on WSS)
                 pass
            
            # --- 3. RATE LIMIT / PACE ---
            await asyncio.sleep(5)
            
        except asyncio.CancelledError:
            logger.info("Stopping...")
            break
        except Exception as e:
            logger.error(f"Error in Smart Loop: {e}")
            await asyncio.sleep(10)

    # Cleanup
    await client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
