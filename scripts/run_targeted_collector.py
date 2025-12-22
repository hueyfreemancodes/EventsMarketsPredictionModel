#!/usr/bin/env python3
"""
Targeted Data Collector
Fetches linked market IDs from the database and begins high-frequency order book collection.
"""
import sys
import os
import asyncio
import json
import psycopg2
from typing import List, Dict

# Path hack to allow direct script execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_collection.polymarket_client import PolymarketClient
from src.data_collection.logger import logger

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
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Metadata file not found: {filepath}")
        return {}


async def main():
    logger.info("Starting Targeted Data Collector...")

    # 1. Fetch Targets
    target_ids = fetch_target_market_ids()
    if not target_ids:
        logger.warning("No linked markets found in DB. Exiting.")
        return

    logger.info(f"Loaded {len(target_ids)} market targets from QuestDB.")

    # 2. Correlate with Asset IDs (Token IDs)
    # TODO: Move this filename to a config variable
    metadata = load_market_metadata('polymarket_nba_markets_100639.json')
    
    subscription_targets = []
    for market_id in target_ids:
        market_data = metadata.get(market_id)
        if not market_data:
            continue
            
        asset_ids = market_data.get('clobTokenIds', [])
        if asset_ids:
            subscription_targets.append({
                'condition_id': market_id,
                'asset_ids': asset_ids
            })

    if not subscription_targets:
        logger.error("No valid asset IDs found for targets. Aborting.")
        return

    logger.info(f"Initializing collection for {len(subscription_targets)} markets.")

    # 3. Initialize Client
    # Attempt to load API keys if present, otherwise default to public
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
        mode="rest",  # REST allows for more reliable snapshot pacing than WS
        polling_interval=5
    )

    if not client.enabled:
        return

    # 4. Run Collection Loop
    if DURATION < 0:
        logger.info("Mode: Continuous Collection (Press Ctrl+C to stop)")
        try:
            await client.start_polling(markets=subscription_targets)
        except asyncio.CancelledError:
            logger.info("Process interrupted.")
    else:
        logger.info(f"Mode: Batch Collection ({DURATION}s)")
        try:
            task = asyncio.create_task(client.start_polling(markets=subscription_targets))
            await asyncio.sleep(DURATION)
            client.running = False
            await task
        except Exception as e:
            logger.error(f"Collection error: {e}")

    # 5. Report
    stats = client.get_stats()
    logger.info(f"Session Complete. Snapshots captured: {stats['snapshots_stored']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
