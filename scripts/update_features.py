#!/usr/bin/env python3
"""
Update Features Service
-----------------------
Calculates high-frequency microstructure features (OFI, VAMP, etc.) 
from raw order book snapshots and stores them in QuestDB.

Usage:
    python scripts/update_features.py
"""
import sys
import os
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.data_collection.ingester import QuestDBIngester
from src.feature_engineering.microstructure_features import MicrostructureFeaturesCalculator

# Logging Config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("FeatureUpdater")

def fetch_snapshots(conn, market_id: str) -> List[Dict[str, Any]]:
    """Fetches raw order book snapshots for a given market."""
    query = """
    SELECT 
        timestamp, market_id, outcome, 
        bid_price_1, bid_size_1, bid_price_2, bid_size_2, bid_price_3, bid_size_3,
        ask_price_1, ask_size_1, ask_price_2, ask_size_2, ask_price_3, ask_size_3,
        mid_price, spread, total_bid_volume, total_ask_volume
    FROM order_book_snapshots
    WHERE market_id = %s
    ORDER BY timestamp ASC
    """
    with conn.cursor() as cur:
        cur.execute(query, (market_id,))
        if not cur.description:
            return []
            
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        # Determine platform to handle nulls appropriately if needed?
        # Standard cleaning: zero out None values for numeric fields
        cleaned_snaps = []
        for row in rows:
            record = dict(zip(cols, row))
            # Clean numeric nulls
            for k, v in record.items():
                if v is None and k not in ['timestamp', 'market_id', 'outcome']:
                    record[k] = 0.0
            cleaned_snaps.append(record)
            
        return cleaned_snaps

def update_features():
    logger.info("Initializing Feature Update...")
    
    ingester = QuestDBIngester()
    ingester.create_microstructure_features_table()
    
    # Initialize Calculator (Window size = 20 snapshots)
    calculator = MicrostructureFeaturesCalculator(window_size=20)
    
    try:
        # 1. Get List of Markets to Process
        # Ideally only active linked markets, but for now all linked markets
        with ingester.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT market_id FROM market_linkages")
            market_ids = [row[0] for row in cur.fetchall() if row[0]]
            
        logger.info(f"Found {len(market_ids)} linked markets to process.")
        
        for idx, market_id in enumerate(market_ids):
            # logger.info(f"[{idx+1}/{len(market_ids)}] Processing {market_id}...")
            
            # 2. Fetch Data
            snapshots = fetch_snapshots(ingester.conn, market_id)
            if not snapshots:
                # logger.warning(f"No snapshots found for {market_id}. Skipping.")
                continue
                
            # 3. Calculate Features
            features = calculator.calculate_all_features(snapshots, market_id=market_id)
            
            # 4. Ingest Results
            if features:
                # Ingest in batch? Method expects single?
                # The existing class has `ingest_microstructure_features` (singular).
                # We can loop. Transactions are handled per insert in the current class?
                # Actually Ingester methods usually commit per call or batch.
                # Let's inspect `ingest_microstructure_features`: It commits every call.
                # Optimization: Could add batch ingestion later. For now, loop is fine for offline update.
                for feat in features:
                    ingester.ingest_microstructure_features(feat)
                
            logger.info(f"[{idx+1}/{len(market_ids)}] {market_id}: Updated {len(features)} records.")
            
        logger.info("✅ Feature update complete.")
        
    except Exception as e:
        logger.error(f"Update failed: {e}", exc_info=True)
    finally:
        ingester.close()

if __name__ == "__main__":
    update_features()
