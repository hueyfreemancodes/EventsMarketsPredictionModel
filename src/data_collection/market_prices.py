"""
Market Price Fetcher

Lightweight module that fetches live order book data from Polymarket's CLOB REST API.
Designed for one-shot batch lookups (like find_ev_bets.py), not continuous streaming.

The CLOB endpoint is: GET https://clob.polymarket.com/book?token_id=<token_id>
Response: {"bids": [{"price": "0.55", "size": "1200"}, ...],
           "asks": [{"price": "0.57", "size": "800"}, ...]}
"""

import logging
import time
from typing import Dict, List, Optional, Any

import requests

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"
RATE_LIMIT_DELAY = 0.25  # 250ms between requests to avoid 429s


def fetch_order_book(token_id: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
    """
    Fetch the full order book for a single CLOB token.

    Returns:
        Dict with 'mid', 'bid', 'ask', 'bid_size', 'ask_size', or None if unavailable.
    """
    try:
        url = f"{CLOB_BASE_URL}/book"
        response = requests.get(url, params={"token_id": token_id}, timeout=timeout)

        if response.status_code != 200:
            logger.warning(f"CLOB returned {response.status_code} for token {token_id[:12]}...")
            return None

        data = response.json()
        if not isinstance(data, dict):
            return None

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        best_bid = _extract_price(bids[0]) if bids else None
        best_ask = _extract_price(asks[0]) if asks else None
        bid_size = _extract_size(bids[0]) if bids else 0.0
        ask_size = _extract_size(asks[0]) if asks else 0.0

        if best_bid is None and best_ask is None:
            return None

        # Calculate mid
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        else:
            mid = best_ask

        return {
            "mid": mid,
            "bid": best_bid,
            "ask": best_ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "spread": (best_ask - best_bid) if (best_ask is not None and best_bid is not None) else 1.0
        }

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching book for token {token_id[:12]}...")
        return None
    except Exception as e:
        logger.error(f"Error fetching order book for {token_id[:12]}...: {e}")
        return None


def fetch_market_prices(token_ids: List[str]) -> Dict[str, float]:
    """
    LEGACY: Batch-fetch mid prices for a list of CLOB token IDs.
    Maintained for backward compatibility.
    """
    books = fetch_market_books(token_ids)
    return {tid: book["mid"] for tid, book in books.items()}


def fetch_market_books(token_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Batch-fetch order books for a list of CLOB token IDs.

    Returns:
        Dict mapping token_id -> order_book dict.
    """
    results: Dict[str, Dict[str, Any]] = {}
    unique_ids = list(set(token_ids))

    for i, tid in enumerate(unique_ids):
        book = fetch_order_book(tid)
        if book:
            results[tid] = book
        if i < len(unique_ids) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    return results


def _extract_price(level) -> Optional[float]:
    """Extract price from a bid/ask level (dict or list format)."""
    if isinstance(level, dict):
        p = level.get("price")
        return float(p) if p else None
    elif isinstance(level, (list, tuple)) and len(level) >= 1:
        return float(level[0])
    return None


def _extract_size(level) -> float:
    """Extract size from a bid/ask level (dict or list format)."""
    if isinstance(level, dict):
        s = level.get("size")
        return float(s) if s else 0.0
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        return float(level[1])
    return 0.0
