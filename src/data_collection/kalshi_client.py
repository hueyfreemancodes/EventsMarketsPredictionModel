"""
Kalshi API Client
Connects to Kalshi API for order book data
Supports market discovery and order book polling
Works without API keys for public market data (limited)
"""

import sys
import os
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

try:
    from kalshi_python import Configuration, KalshiClient as SDKKalshiClient
    KALSHI_AVAILABLE = True
except ImportError as e:
    KALSHI_AVAILABLE = False
    KALSHI_IMPORT_ERROR = str(e)
except Exception as e:
    # Some versions may have different import structure
    KALSHI_AVAILABLE = False
    KALSHI_IMPORT_ERROR = str(e)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from .logger import logger
from .ingester import QuestDBIngester


class KalshiClient:
    """Kalshi API client for order book data"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_url: str = "https://api.elections.kalshi.com/trade-api/v2",
        rate_limit_delay: float = 0.5,  # 2 requests/second max
        polling_interval: int = 300  # 5 minutes default
    ):
        """
        Initialize Kalshi client
        
        Args:
            api_key: API key ID (optional for public data)
            api_secret: Private key PEM string or path (optional)
            api_url: Kalshi API base URL
            rate_limit_delay: Seconds between requests (0.5 = 2 req/sec max)
            polling_interval: Seconds between polling cycles
        """
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.api_secret = api_secret
        self.rate_limit_delay = rate_limit_delay
        self.polling_interval = polling_interval
        
        self.client = None
        self.ingester = None
        self.running = False
        self.subscribed_markets = []
        
        # Statistics
        self.stats = {
            'snapshots_stored': 0,
            'trades_stored': 0,
            'api_calls': 0,
            'errors': 0,
            'markets_found': 0
        }
        
        # Initialize client if credentials provided
        if KALSHI_AVAILABLE and api_key and api_secret:
            try:
                # Load private key if it's a file path
                private_key = api_secret
                if os.path.isfile(str(api_secret)):
                    with open(api_secret, 'r') as f:
                        private_key = f.read()
                
                # Kalshi Configuration expects api_key as a dict
                # The library handles RSA-PSS signing internally
                config = Configuration(
                    host=self.api_url,
                    api_key={"api_key": api_key, "private_key": private_key}
                )
                self.client = SDKKalshiClient(config)
                self.enabled = True
                logger.info("Kalshi client initialized (authenticated)")
            except Exception as e:
                logger.warning(f"Failed to initialize authenticated Kalshi client: {e}")
                logger.info("Falling back to unauthenticated mode (limited access)")
                self.enabled = True  # Still enable for market discovery
        elif KALSHI_AVAILABLE:
            self.enabled = True
            logger.info("Kalshi client initialized (unauthenticated - limited access)")
            logger.info("Note: Full functionality requires API key ID and private key")
        else:
            self.enabled = False
            error_msg = f"kalshi-python not available. Install with: pip install kalshi-python"
            if 'KALSHI_IMPORT_ERROR' in globals():
                error_msg += f" (Error: {KALSHI_IMPORT_ERROR})"
            logger.error(error_msg)
    
    def discover_sports_markets(
        self,
        sport: str = "NBA",
        limit: int = 100
    ) -> List[Dict]:
        """
        Discover sports-related markets on Kalshi
        
        Args:
            sport: Sport to filter (NBA, NFL, etc.)
            limit: Maximum number of markets to return
        
        Returns:
            List of market dictionaries
        """
        if not self.enabled:
            logger.error("Client not enabled")
            return []
        
        markets = []
        
        try:
            if self.client:
                # Use authenticated client with low-level Access
                # This helps bypass loose schema validation issues (e.g. empty 'category')
                try:
                    # Access the underlying ApiClient (kalshi-python wrapping)
                    api_client = self.client.api_client
                    
                    # Construct URL manually to ensure params are correct
                    host = self.client.configuration.host
                    url = f"{host}/markets"
                    
                    # Removing 'status' filter because API behavior is inconsistent (returns 400 or 0 matches)
                    params = {
                        'limit': '100' # Reverting to safe limit
                    }
                    import urllib.parse
                    import json
                    query_string = urllib.parse.urlencode(params)
                    full_url = f"{url}?{query_string}"
                    
                    response = api_client.call_api(
                        'GET', 
                        full_url, 
                        header_params={'Accept': 'application/json'}
                        # Removed response_type arg
                    )
                    
                    # Parse raw response
                    market_list = []
                    
                    # Handle response data (might be stream or preloaded)
                    raw_data = getattr(response, 'data', None)
                    if not raw_data and hasattr(response, 'read'):
                        raw_data = response.read()

                    if raw_data:
                         data = json.loads(raw_data.decode('utf-8'))
                         market_list = data.get('markets', [])
                         logger.info(f"[DEBUG] Fetch returned {len(market_list)} raw markets.")
                    
                    self.stats['api_calls'] += 1
                    
                    # Client-side filtering
                    for market_dict in market_list:
                        # Extract fields
                        ticker = market_dict.get('ticker', '')
                        title = str(market_dict.get('title', market_dict.get('subtitle', ''))).lower()
                        category = str(market_dict.get('category', market_dict.get('series_ticker', ''))).lower()
                        series = str(market_dict.get('series_ticker', '')).lower()
                        
                        # Flexible Matching Logic
                        is_match = False
                        
                        # 1. Sport Name or Series Ticker in Text
                        if sport.lower() in title or sport.lower() in category or sport.lower() in series:
                            is_match = True
                        if 'kxnbagame' in series.lower() or 'kxnbagame' in ticker.lower():
                            is_match = True
                        
                        # 2. Ticker Prefix (e.g. "KXMVE NBA" -> "KXMVE" + "NBA")
                        if f"{sport.lower()}" in ticker.lower():
                            is_match = True

                        # STAGE 2: Quality Filtering (User Request)
                        # Filter out bundles/parlays and illiquid markets
                        if is_match:
                            # 1. Exclude Bundles (Titles with commas are usually multi-leg parlays)
                            if ',' in title:
                                is_match = False
                            
                            # 2. Require Liquidity (Bid > 0)
                            # User asked for "Bid/Asks that make sense"
                            # We allow slight flexibility for testing, but ideally yes_bid > 0
                            yes_bid = market_dict.get('yes_bid')
                            if yes_bid is None or yes_bid == 0:
                                is_match = False
                                
                            # 3. Target Spreads and Totals specifically (if requested, or generally for quality)
                            # We keep "Winner" (Moneyline), "Spread", "Total", "Over", "Under"
                            valid_types = ['winner', 'spread', 'total', 'over', 'under', 'points']
                            if not any(t in title.lower() for t in valid_types):
                                # If it's just a player prop like "Jalen Brunson", we might want to keep it detailed
                                # but usually "Over 20.5" is in the title.
                                pass 
                            
                        # 3. Allow all if sport is empty
                        if not sport:
                            is_match = True
                        
                        if is_match:
                            markets.append({
                                'ticker': ticker,
                                'event_ticker': market_dict.get('event_ticker'),
                                'title': market_dict.get('title') or market_dict.get('subtitle'),
                                'category': market_dict.get('category'),
                                'status': market_dict.get('status'),
                                'yes_bid': market_dict.get('yes_bid'),
                                'yes_ask': market_dict.get('yes_ask'),
                                'no_bid': market_dict.get('no_bid'),
                                'no_ask': market_dict.get('no_ask'),
                                'volume': market_dict.get('volume'),
                            })
                            
                            if len(markets) >= limit:
                                break
                    
                    logger.info(f"Found {len(markets)} {sport} markets via authenticated API (Raw Request)")
                    
                except Exception as e:
                    logger.warning(f"Error using authenticated API Raw Request: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    # Fall back to unauthenticated discovery
                    pass
            
            # Fallback: Use public API or web scraping
            if not markets and REQUESTS_AVAILABLE:
                markets = self._discover_markets_public(sport, limit)
            
        except Exception as e:
            logger.error(f"Error discovering markets: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        self.stats['markets_found'] = len(markets)
        return markets
    
    def discover_markets_by_event(self, series_ticker: str = "KXNBAGAME", limit: int = 100) -> list:
        """
        Discover markets by first fetching Events, then fetching markets for each event.
        This bypasses the flooding of 'Bundle' markets in the main feed.
        """
        if not self.enabled:
            return []
            
        import urllib.parse
        import json
        
        filtered_markets = []
        api_client = self.client.api_client
        host = self.client.configuration.host
        
        # 1. Fetch Active Events
        # Removing status filter to ensure we get events (then filter/check later)
        e_url = f"{host}/events"
        e_params = {
            'limit': str(limit),
            'series_ticker': series_ticker
        }
        e_query = urllib.parse.urlencode(e_params)
        e_full_url = f"{e_url}?{e_query}"
        
        logger.info(f"Fetching Events from: {e_full_url}")
        
        try:
            resp = api_client.call_api('GET', e_full_url, header_params={'Accept': 'application/json'})
            raw = getattr(resp, 'data', None) or resp.read()
            
            if not raw:
                return []
                
            events_data = json.loads(raw.decode('utf-8'))
            events = events_data.get('events', [])
            logger.info(f"Found {len(events)} active events.")
            
            # 2. For each event, fetch markets
            for event in events:
                e_ticker = event['event_ticker']
                
                m_url = f"{host}/markets"
                m_params = {'event_ticker': e_ticker, 'limit': '100'} # Fetch all markets for this game
                m_query = urllib.parse.urlencode(m_params)
                m_full_url = f"{m_url}?{m_query}"
                
                m_resp = api_client.call_api('GET', m_full_url, header_params={'Accept': 'application/json'})
                m_raw = getattr(m_resp, 'data', None) or m_resp.read()
                
                if m_raw:
                    m_data = json.loads(m_raw.decode('utf-8'))
                    markets = m_data.get('markets', [])
                    
                    # 3. Filter for Spreads/Totals/Liquidity
                    for m in markets:
                        title = m.get('title', '')
                        # Exclude Parlays (commas)
                        if ',' in title: continue
                        
                        # Keywords
                        allowed = ['winner', 'spread', 'total', 'over', 'under', 'points']
                        if any(k in title.lower() for k in allowed):
                             # Optional: Check Liquidity if strict
                             # if (m.get('yes_bid') or 0) > 0:
                             filtered_markets.append(m)
                             
        except Exception as e:
            logger.error(f"Error in event-based discovery: {e}")
            
        return filtered_markets

    def _discover_markets_public(self, sport: str, limit: int) -> List[Dict]:
        """Discover markets using public API (if available)"""
        # Kalshi may have a public API endpoint
        # This is a placeholder - actual endpoint may vary
        try:
            # Try public markets endpoint
            url = f"{self.api_url}/markets"
            params = {
                'limit': limit,
                'status': 'active' # Try active
            }
            
            response = requests.get(url, params=params, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                markets = data.get('markets', [])
                
                result = []
                for market in markets:
                    result.append({
                        'ticker': market.get('ticker'),
                        'event_ticker': market.get('event_ticker'),
                        'title': market.get('title'),
                        'category': market.get('category'),
                        'status': market.get('status'),
                        'yes_bid': market.get('yes_bid'),
                        'yes_ask': market.get('yes_ask'),
                        'no_bid': market.get('no_bid'),
                        'no_ask': market.get('no_ask'),
                    })
                
                logger.info(f"Found {len(result)} markets via public API")
                return result
            else:
                logger.debug(f"Public API returned {response.status_code}")
                return []
                
        except Exception as e:
            logger.debug(f"Public API discovery failed: {e}")
            return []
    
    def get_market_order_book(self, ticker: str) -> Optional[Dict]:
        """
        Get order book for a specific market
        
        Args:
            ticker: Market ticker (e.g., "NBA-WINNER-2024-12-25")
        
        Returns:
            Order book snapshot dict or None
        """
        if not self.enabled:
            return None
        
        try:
            if self.client:
                # Use authenticated client (Low Level)
                try:
                    api_client = self.client.api_client
                    host = self.client.configuration.host
                    url = f"{host}/markets/{ticker}"
                    
                    response = api_client.call_api(
                        'GET', 
                        url, 
                        header_params={'Accept': 'application/json'}
                    )
                    
                    self.stats['api_calls'] += 1
                    
                    raw_data = getattr(response, 'data', None)
                    if not raw_data and hasattr(response, 'read'):
                        raw_data = response.read()

                    if raw_data:
                         import json
                         data = json.loads(raw_data.decode('utf-8'))
                         # The response for single market might be nested or direct
                         # Usually { "market": { ... } }
                         market_data = data.get('market', data)
                         
                         return self._parse_market_data(market_data, ticker)
                    
                except Exception as e:
                    logger.warning(f"Error getting market via authenticated API: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
            
            # Fallback: Try public API
            if REQUESTS_AVAILABLE:
                return self._get_order_book_public(ticker)
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching order book for {ticker}: {e}")
            self.stats['errors'] += 1
            return None
    
    def _get_order_book_public(self, ticker: str) -> Optional[Dict]:
        """Get order book via public API"""
        try:
            url = f"{self.api_url}/markets/{ticker}"
            response = requests.get(url, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_market_data(data, ticker)
            else:
                logger.debug(f"Public API returned {response.status_code} for {ticker}")
                return None
        except Exception as e:
            logger.debug(f"Public API fetch failed: {e}")
            return None
    
    def _parse_market_data(self, market_data: Dict, ticker: str) -> Optional[Dict]:
        """Parse market data into order book snapshot format"""
        try:
            # Kalshi market data structure
            # Markets have YES/NO outcomes with bid/ask prices
            
            # Extract YES outcome prices
            yes_bid = market_data.get('yes_bid') or market_data.get('yesBid')
            yes_ask = market_data.get('yes_ask') or market_data.get('yesAsk')
            
            # Extract NO outcome prices (inverse of YES for binary markets)
            no_bid = market_data.get('no_bid') or market_data.get('noBid')
            no_ask = market_data.get('no_ask') or market_data.get('noAsk')
            
            # Convert to decimal if needed (Kalshi may use cents)
            def to_decimal(value):
                if value is None:
                    return None
                # If value > 1, assume it's in cents, divide by 100
                if isinstance(value, (int, float)) and value > 1:
                    return float(value) / 100.0
                return float(value)
            
            yes_bid_price = to_decimal(yes_bid)
            yes_ask_price = to_decimal(yes_ask)
            no_bid_price = to_decimal(no_bid)
            no_ask_price = to_decimal(no_ask)
            
            # Calculate mid prices
            yes_mid = (yes_bid_price + yes_ask_price) / 2 if (yes_bid_price and yes_ask_price) else None
            no_mid = (no_bid_price + no_ask_price) / 2 if (no_bid_price and no_ask_price) else None
            
            # Calculate spreads
            yes_spread = yes_ask_price - yes_bid_price if (yes_bid_price and yes_ask_price) else None
            no_spread = no_ask_price - no_bid_price if (no_bid_price and no_ask_price) else None
            
            # For YES outcome
            yes_snapshot = {
                'market_id': ticker,
                'outcome': 'YES',
                'bid_price_1': yes_bid_price,
                'bid_size_1': None,  # Kalshi may not provide size in public API
                'bid_price_2': None,
                'bid_size_2': None,
                'bid_price_3': None,
                'bid_size_3': None,
                'ask_price_1': yes_ask_price,
                'ask_size_1': None,
                'ask_price_2': None,
                'ask_size_2': None,
                'ask_price_3': None,
                'ask_size_3': None,
                'mid_price': yes_mid,
                'spread': yes_spread,
                'total_bid_volume': None,
                'total_ask_volume': None,
            }
            
            # For NO outcome
            no_snapshot = {
                'market_id': ticker,
                'outcome': 'NO',
                'bid_price_1': no_bid_price,
                'bid_size_1': None,
                'bid_price_2': None,
                'bid_size_2': None,
                'bid_price_3': None,
                'bid_size_3': None,
                'ask_price_1': no_ask_price,
                'ask_size_1': None,
                'ask_price_2': None,
                'ask_size_2': None,
                'ask_price_3': None,
                'ask_size_3': None,
                'mid_price': no_mid,
                'spread': no_spread,
                'total_bid_volume': None,
                'total_ask_volume': None,
            }
            
            # Return both outcomes
            return {
                'yes': yes_snapshot,
                'no': no_snapshot,
                'ticker': ticker,
                'title': market_data.get('title'),
                'category': market_data.get('category'),
            }
            
        except Exception as e:
            logger.error(f"Error parsing market data: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    async def poll_markets(self, markets: List[Dict]):
        """
        Poll markets and store order book data
        
        Args:
            markets: List of market dicts with ticker
        """
        if self.ingester is None:
            self.ingester = QuestDBIngester()
        
        for market in markets:
            ticker = market.get('ticker') or market.get('event_ticker')
            
            if not ticker:
                continue
            
            try:
                # Get order book
                order_book = self.get_market_order_book(ticker)
                
                if order_book:
                    # Store YES outcome
                    if 'yes' in order_book:
                        yes_data = order_book['yes'].copy()
                        yes_data['timestamp'] = datetime.now()
                        yes_data['platform'] = 'Kalshi'
                        
                        try:
                            self.ingester.ingest_order_book_snapshot(yes_data)
                            self.stats['snapshots_stored'] += 1
                        except Exception as e:
                            logger.error(f"Error storing YES snapshot: {e}")
                    
                    # Store NO outcome
                    if 'no' in order_book:
                        no_data = order_book['no'].copy()
                        no_data['timestamp'] = datetime.now()
                        no_data['platform'] = 'Kalshi'
                        
                        try:
                            self.ingester.ingest_order_book_snapshot(no_data)
                            self.stats['snapshots_stored'] += 1
                        except Exception as e:
                            logger.error(f"Error storing NO snapshot: {e}")
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error polling market {ticker}: {e}")
                self.stats['errors'] += 1
                continue
    
    async def start_polling(self, markets: List[Dict] = None, sport: str = "NBA"):
        """
        Start polling markets
        
        Args:
            markets: List of market dicts. If None, discovers markets
            sport: Sport to discover if markets not provided
        """
        if not self.enabled:
            logger.error("Client not enabled")
            return
        
        # Discover markets if not provided
        if markets is None:
            logger.info(f"Discovering {sport} markets...")
            markets = self.discover_sports_markets(sport=sport)
            
            if not markets:
                logger.warning(f"No {sport} markets found")
                return
        
        self.running = True
        self.subscribed_markets = markets
        
        logger.info(f"Starting Kalshi polling for {len(markets)} markets (interval: {self.polling_interval}s)")
        
        while self.running:
            try:
                await self.poll_markets(markets)
                await asyncio.sleep(self.polling_interval)
            except KeyboardInterrupt:
                logger.info("Polling stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(5)  # Wait before retry
    
    async def stop(self):
        """Stop the client"""
        self.running = False
        if self.ingester:
            self.ingester.close()
        logger.info("Kalshi client stopped")
    
    def get_stats(self) -> Dict:
        """Get client statistics"""
        return {
            **self.stats,
            'running': self.running,
            'subscribed_markets': len(self.subscribed_markets),
        }

