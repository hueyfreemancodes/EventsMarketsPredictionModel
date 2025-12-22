import sys
import os
import asyncio
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from collections import deque

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from .logger import logger
from .ingester import QuestDBIngester

# Constants
REST_API_URL = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com"

class PolymarketClient:
    """
    Polymarket CLOB Client.
    Supports REST polling and WebSocket streaming for market data.
    """
    
    def __init__(
        self,
        api_url: str = REST_API_URL,
        websocket_url: str = WS_URL,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        mode: str = "rest",
        reconnect_delay: int = 5,
        max_reconnect_attempts: int = 10,
        polling_interval: int = 5
    ):
        self.api_url = api_url.rstrip('/')
        self.websocket_url = websocket_url
        self.creds = {
            'key': api_key,
            'secret': api_secret,
            'passphrase': api_passphrase
        }
        
        self.mode = mode.lower()
        self.reconnect_delay = reconnect_delay
        self.max_retries = max_reconnect_attempts
        self.polling_interval = polling_interval
        
        self.websocket = None
        self.connected = False
        self.targets = set() # Market IDs
        self.retry_count = 0
        self.ingester = None
        self.running = False
        
        # Cache
        self.order_book_cache = {} 
        
        # Telemetry
        self.stats = {
            'msgs_recv': 0,
            'snaps_saved': 0,
            'errors': 0,
            'api_calls': 0
        }
        
        # Dependency Check
        if self.mode == "websocket" and not WEBSOCKETS_AVAILABLE:
            logger.warning("websockets lib missing. Degrading to REST mode.")
            self.mode = "rest"
            
        if self.mode == "rest" and not REQUESTS_AVAILABLE:
            logger.error("requests lib missing. Client disabled.")
            self.running = False
            return

        self.ingester = QuestDBIngester()
        
        if self.mode == "rest":
            if not REQUESTS_AVAILABLE:
                logger.error("requests library not available. Install with: pip install requests")
                self.enabled = False
                return
            self.enabled = True
            logger.info("Polymarket client initialized (REST API mode)")
        else:
            self.enabled = True
            logger.info("Polymarket client initialized (WebSocket mode)")
    
    async def connect(self, channel_type: str = "market"):
        """
        Connect to Polymarket WebSocket
        
        Args:
            channel_type: "market" or "user"
        """
        if not self.enabled:
            logger.error("Client not enabled. Install websockets library.")
            return False
        
        try:
            # Correct WebSocket URL format
            ws_url = f"{self.websocket_url}/ws/{channel_type}"
            logger.info(f"Connecting to Polymarket WebSocket: {ws_url}")
            
            self.websocket = await websockets.connect(
                ws_url,
                ping_interval=None,  # We'll handle ping ourselves
                ping_timeout=None
            )
            self.connected = True
            self.reconnect_attempts = 0
            logger.info("✅ Connected to Polymarket WebSocket")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Polymarket WebSocket: {e}")
            self.connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from WebSocket"""
        if self.websocket:
            await self.websocket.close()
            self.connected = False
            logger.info("Disconnected from Polymarket WebSocket")
    
    async def subscribe_to_markets(self, asset_ids: List[str], channel_type: str = "market"):
        """
        Subscribe to markets via WebSocket
        
        Args:
            asset_ids: List of asset IDs (token IDs/clobTokenIds) for MARKET channel
                      or condition IDs for USER channel
            channel_type: "market" or "user"
        """
        if not self.connected or not self.websocket:
            logger.warning("Not connected. Call connect() first.")
            return
        
        try:
            if channel_type == "market":
                # MARKET channel uses asset_ids
                subscribe_msg = {
                    "type": "MARKET",
                    "assets_ids": asset_ids
                }
            elif channel_type == "user":
                # USER channel uses markets (condition IDs) and requires auth
                if not self.api_key:
                    logger.error("USER channel requires API authentication")
                    return
                
                auth = {
                    "apiKey": self.api_key,
                    "secret": self.api_secret,
                    "passphrase": self.api_passphrase or ""
                }
                
                subscribe_msg = {
                    "type": "USER",
                    "markets": asset_ids,  # condition IDs for user channel
                    "auth": auth
                }
            else:
                logger.error(f"Invalid channel type: {channel_type}")
                return
            
            await self.websocket.send(json.dumps(subscribe_msg))
            self.subscribed_markets.update(asset_ids)
            logger.info(f"Subscribed to {len(asset_ids)} {channel_type} channels")
        except Exception as e:
            logger.error(f"Error subscribing: {e}")
    
    async def unsubscribe_from_market(self, market_id: str):
        """Unsubscribe from a market"""
        if not self.connected or not self.websocket:
            return
        
        try:
            unsubscribe_msg = {
                "type": "unsubscribe",
                "channel": "level2",
                "market": market_id
            }
            await self.websocket.send(json.dumps(unsubscribe_msg))
            self.subscribed_markets.discard(market_id)
            logger.info(f"Unsubscribed from market: {market_id}")
        except Exception as e:
            logger.error(f"Error unsubscribing from market {market_id}: {e}")
    
    def _parse_order_book_message(self, message: Dict) -> Optional[Dict]:
        """
        Parse order book message from Polymarket WebSocket
        
        Args:
            message: Raw message from WebSocket
        
        Returns:
            Parsed order book snapshot dict or None
        """
        try:
            # Polymarket WebSocket message format
            # Messages can be: order book updates, trades, etc.
            
            # Get asset/market identifier
            asset_id = message.get('asset_id') or message.get('token_id') or message.get('market')
            condition_id = message.get('condition_id') or message.get('market_id')
            
            if not asset_id and not condition_id:
                return None
            
            # Use condition_id as market_id for database
            market_id = condition_id or asset_id
            
            # Get message type
            msg_type = message.get('type', '').lower()
            
            # Handle snapshot (full order book)
            if msg_type in ['snapshot', 'l2snapshot', 'book']:
                bids = message.get('bids', [])
                asks = message.get('asks', [])
                
                # Extract top 3 levels
                bid_price_1 = float(bids[0][0]) if len(bids) > 0 else None
                bid_size_1 = float(bids[0][1]) if len(bids) > 0 else None
                bid_price_2 = float(bids[1][0]) if len(bids) > 1 else None
                bid_size_2 = float(bids[1][1]) if len(bids) > 1 else None
                bid_price_3 = float(bids[2][0]) if len(bids) > 2 else None
                bid_size_3 = float(bids[2][1]) if len(bids) > 2 else None
                
                ask_price_1 = float(asks[0][0]) if len(asks) > 0 else None
                ask_size_1 = float(asks[0][1]) if len(asks) > 0 else None
                ask_price_2 = float(asks[1][0]) if len(asks) > 1 else None
                ask_size_2 = float(asks[1][1]) if len(asks) > 1 else None
                ask_price_3 = float(asks[2][0]) if len(asks) > 2 else None
                ask_size_3 = float(asks[2][1]) if len(asks) > 2 else None
                
                # Calculate mid price and spread
                if bid_price_1 and ask_price_1:
                    mid_price = (bid_price_1 + ask_price_1) / 2
                    spread = ask_price_1 - bid_price_1
                else:
                    mid_price = None
                    spread = None
                
                # Calculate total volumes
                total_bid_volume = sum(float(b[1]) for b in bids) if bids else 0.0
                total_ask_volume = sum(float(a[1]) for a in asks) if asks else 0.0
                
                # Determine outcome (YES/NO) - may need market info
                outcome = message.get('outcome', 'YES')  # Default to YES
                
                return {
                    'market_id': market_id,
                    'outcome': outcome,
                    'bid_price_1': bid_price_1,
                    'bid_size_1': bid_size_1,
                    'bid_price_2': bid_price_2,
                    'bid_size_2': bid_size_2,
                    'bid_price_3': bid_price_3,
                    'bid_size_3': bid_size_3,
                    'ask_price_1': ask_price_1,
                    'ask_size_1': ask_size_1,
                    'ask_price_2': ask_price_2,
                    'ask_size_2': ask_size_2,
                    'ask_price_3': ask_price_3,
                    'ask_size_3': ask_size_3,
                    'mid_price': mid_price,
                    'spread': spread,
                    'total_bid_volume': total_bid_volume,
                    'total_ask_volume': total_ask_volume,
                }
            
            # Handle update (delta)
            elif msg_type in ['update', 'l2update', 'delta']:
                # Update existing order book state
                # This would maintain state and apply deltas
                # For now, we'll log it
                logger.debug(f"Received update for market {market_id}")
                return None
            
            # Handle trade
            elif msg_type in ['trade', 'match', 'fill']:
                # Parse trade message
                price = float(message.get('price', 0))
                size = float(message.get('size', 0))
                side = message.get('side', 'BUY')
                trade_id = message.get('trade_id') or message.get('id')
                
                return {
                    'type': 'trade',
                    'market_id': market_id,
                    'outcome': message.get('outcome', 'YES'),
                    'price': price,
                    'size': size,
                    'side': side,
                    'trade_id': trade_id,
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing order book message: {e}")
            logger.debug(f"Message: {message}")
            return None
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            self.stats['messages_received'] += 1
            
            # Parse message
            parsed = self._parse_order_book_message(data)
            
            if not parsed:
                return
            
            # Store in database
            if self.ingester is None:
                self.ingester = QuestDBIngester()
            
            if parsed.get('type') == 'trade':
                # Store trade
                trade_data = {
                    'timestamp': datetime.now(),
                    'market_id': parsed['market_id'],
                    'outcome': parsed['outcome'],
                    'platform': 'Polymarket',
                    'price': parsed['price'],
                    'size': parsed['size'],
                    'side': parsed['side'],
                    'trade_id': parsed.get('trade_id', 0),
                }
                try:
                    self.ingester.ingest_trade(trade_data)
                    self.stats['trades_stored'] += 1
                except Exception as e:
                    logger.error(f"Error storing trade: {e}")
            else:
                # Store order book snapshot
                snapshot_data = {
                    'timestamp': datetime.now(),
                    'market_id': parsed['market_id'],
                    'outcome': parsed['outcome'],
                    'platform': 'Polymarket',
                    'bid_price_1': parsed.get('bid_price_1'),
                    'bid_size_1': parsed.get('bid_size_1'),
                    'bid_price_2': parsed.get('bid_price_2'),
                    'bid_size_2': parsed.get('bid_size_2'),
                    'bid_price_3': parsed.get('bid_price_3'),
                    'bid_size_3': parsed.get('bid_size_3'),
                    'ask_price_1': parsed.get('ask_price_1'),
                    'ask_size_1': parsed.get('ask_size_1'),
                    'ask_price_2': parsed.get('ask_price_2'),
                    'ask_size_2': parsed.get('ask_size_2'),
                    'ask_price_3': parsed.get('ask_price_3'),
                    'ask_size_3': parsed.get('ask_size_3'),
                    'mid_price': parsed.get('mid_price'),
                    'spread': parsed.get('spread'),
                    'total_bid_volume': parsed.get('total_bid_volume', 0.0),
                    'total_ask_volume': parsed.get('total_ask_volume', 0.0),
                }
                try:
                    self.ingester.ingest_order_book_snapshot(snapshot_data)
                    self.stats['snapshots_stored'] += 1
                except Exception as e:
                    logger.error(f"Error storing snapshot: {e}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON message: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            self.stats['errors'] += 1
    
    async def _listen(self):
        """Listen for WebSocket messages"""
        if not self.websocket:
            return
        
        # Start ping thread
        ping_task = asyncio.create_task(self._ping_loop())
        
        try:
            async for message in self.websocket:
                # Handle PONG responses
                if message == "PONG":
                    continue
                
                await self._handle_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.connected = False
        except Exception as e:
            logger.error(f"Error in WebSocket listener: {e}")
            self.connected = False
        finally:
            ping_task.cancel()
    
    async def _ping_loop(self):
        """Send PING messages every 10 seconds to keep connection alive"""
        while self.connected and self.websocket:
            try:
                await asyncio.sleep(10)
                if self.websocket:
                    await self.websocket.send("PING")
            except Exception as e:
                logger.debug(f"Error in ping loop: {e}")
                break
    
    async def _reconnect(self, channel_type: str = "market"):
        """Attempt to reconnect to WebSocket"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached")
            return False
        
        self.reconnect_attempts += 1
        logger.info(f"Reconnecting... (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})")
        
        await asyncio.sleep(self.reconnect_delay)
        
        if await self.connect(channel_type=channel_type):
            # Resubscribe to markets (need to reconstruct asset_ids from subscribed_markets)
            # This is simplified - in production you'd store the original asset_ids
            asset_ids = list(self.subscribed_markets)
            if asset_ids:
                await self.subscribe_to_markets(asset_ids, channel_type=channel_type)
            self.stats['reconnects'] += 1
            return True
        
        return False
    
    async def start(self, market_ids: List[str] = None):
        """
        Start client (REST or WebSocket mode)
        
        Args:
            market_ids: List of market IDs. If None, attempts to discover sports markets
        """
        if not self.enabled:
            logger.error("Client not enabled")
            return
        
        # Discover markets if not provided
        if market_ids is None:
            logger.info("Discovering sports markets...")
            markets = self.discover_sports_markets()
            
            if not markets:
                logger.warning("No markets found. Provide market_ids manually.")
                return
        else:
            # Convert market_ids to market dicts format
            markets = [{'condition_id': mid, 'asset_ids': []} for mid in market_ids]
        
        # Start in appropriate mode
        if self.mode == "rest":
            await self.start_polling(markets)
        else:
            # WebSocket mode
            self.running = True
            
            # Connect to MARKET channel
            if not await self.connect(channel_type="market"):
                logger.error("Failed to connect. Cannot start.")
                return
            
            # Extract asset IDs from markets
            all_asset_ids = []
            for market in markets:
                asset_ids = market.get('asset_ids', [])
                all_asset_ids.extend(asset_ids)
            
            if all_asset_ids:
                await self.subscribe_to_markets(all_asset_ids, channel_type="market")
            else:
                logger.warning("No asset IDs found. Cannot subscribe to WebSocket.")
                return
            
            # Listen for messages
            while self.running:
                try:
                    await self._listen()
                except Exception as e:
                    logger.error(f"Error in listen loop: {e}")
                
                # Attempt reconnection if disconnected
                if not self.connected and self.running:
                    if not await self._reconnect(channel_type="market"):
                        logger.error("Failed to reconnect. Stopping.")
                        break
    
    async def stop(self):
        """Stop the client"""
        self.running = False
        await self.disconnect()
        if self.ingester:
            self.ingester.close()
        logger.info("Polymarket client stopped")
    
    def get_stats(self) -> Dict:
        """Get client statistics"""
        return {
            **self.stats,
            'connected': self.connected,
            'subscribed_markets': len(self.subscribed_markets),
        }
    
    def _get_order_book_rest(self, condition_id: str, asset_ids: List[str] = None) -> Optional[Dict]:
        """
        Get order book via CLOB REST API
        
        Args:
            condition_id: Market condition ID
            asset_ids: List of asset IDs (token IDs) for this market
        
        Returns:
            Order book snapshot dict or None
        """
        if not REQUESTS_AVAILABLE:
            return None
        
        try:
            # Use CLOB API to get order book
            # Need to query for each asset ID (outcome)
            if not asset_ids:
                # Try to get asset IDs from Gamma API
                market_data = self.get_market_order_book(condition_id)
                if market_data and isinstance(market_data, dict):
                    clob_token_ids = market_data.get('clobTokenIds', [])
                    if isinstance(clob_token_ids, str):
                        import json
                        clob_token_ids = json.loads(clob_token_ids)
                    asset_ids = clob_token_ids
            
            if not asset_ids:
                logger.warning(f"No asset IDs found for condition {condition_id}")
                return None
            
            # Get order book for first asset (YES outcome typically)
            # In production, you'd want to get both YES and NO
            asset_id = asset_ids[0]
            # CLOB API endpoint format: /book?token_id=<asset_id>
            url = f"{self.api_url}/book"
            params = {'token_id': asset_id}
            
            # Alternative: Try /markets/<condition_id>/book if above doesn't work
            # url = f"{self.api_url}/markets/{condition_id}/book"
            
            headers = {}
            if self.api_key:
                headers['Authorization'] = f"Bearer {self.api_key}"
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_order_book_rest(data, condition_id, asset_id)
            else:
                logger.warning(f"CLOB API returned {response.status_code} for asset {asset_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching order book via REST: {e}")
            self.stats['errors'] += 1
            return None
    
    def _parse_order_book_rest(self, data: Dict, condition_id: str, asset_id: str) -> Optional[Dict]:
        """Parse order book data from CLOB REST API response"""
        try:
            # Handle case where data might be empty or error response
            if not data or not isinstance(data, dict):
                logger.debug(f"Invalid data format: {type(data)}")
                return None
            
            # CLOB API returns bids and asks
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            # Check if we have valid data
            if not bids and not asks:
                logger.debug("No bids or asks in order book (market may be closed)")
                return None
            
            # Extract top 3 levels
            # CLOB API returns bids/asks as dicts with 'price' and 'size' keys
            # Handle both dict format and array format for compatibility
            def get_price_size(level, index):
                """Extract price and size from bid/ask level"""
                if not level:
                    return None, None
                
                if isinstance(level, dict):
                    # Dict format: {'price': '0.001', 'size': '7609.22'}
                    price = level.get('price')
                    size = level.get('size')
                    return float(price) if price else None, float(size) if size else None
                elif isinstance(level, (list, tuple)) and len(level) >= 2:
                    # Array format: ['0.001', '7609.22']
                    return float(level[0]), float(level[1])
                else:
                    return None, None
            
            bid_price_1, bid_size_1 = get_price_size(bids[0] if len(bids) > 0 else None, 0)
            bid_price_2, bid_size_2 = get_price_size(bids[1] if len(bids) > 1 else None, 1)
            bid_price_3, bid_size_3 = get_price_size(bids[2] if len(bids) > 2 else None, 2)
            
            ask_price_1, ask_size_1 = get_price_size(asks[0] if len(asks) > 0 else None, 0)
            ask_price_2, ask_size_2 = get_price_size(asks[1] if len(asks) > 1 else None, 1)
            ask_price_3, ask_size_3 = get_price_size(asks[2] if len(asks) > 2 else None, 2)
            
            # Calculate mid price and spread
            if bid_price_1 and ask_price_1:
                mid_price = (bid_price_1 + ask_price_1) / 2
                spread = ask_price_1 - bid_price_1
            else:
                mid_price = None
                spread = None
            
            # Calculate total volumes
            def get_volume(level):
                """Extract volume from bid/ask level"""
                if isinstance(level, dict):
                    return float(level.get('size', 0))
                elif isinstance(level, (list, tuple)) and len(level) > 1:
                    return float(level[1])
                return 0.0
            
            total_bid_volume = sum(get_volume(b) for b in bids) if bids else 0.0
            total_ask_volume = sum(get_volume(a) for a in asks) if asks else 0.0
            
            return {
                'market_id': condition_id,
                'asset_id': asset_id,
                'outcome': 'YES' if asset_id else 'NO',  # First asset is typically YES
                'bid_price_1': bid_price_1,
                'bid_size_1': bid_size_1,
                'bid_price_2': bid_price_2,
                'bid_size_2': bid_size_2,
                'bid_price_3': bid_price_3,
                'bid_size_3': bid_size_3,
                'ask_price_1': ask_price_1,
                'ask_size_1': ask_size_1,
                'ask_price_2': ask_price_2,
                'ask_size_2': ask_size_2,
                'ask_price_3': ask_price_3,
                'ask_size_3': ask_size_3,
                'mid_price': mid_price,
                'spread': spread,
                'total_bid_volume': total_bid_volume,
                'total_ask_volume': total_ask_volume,
            }
        except Exception as e:
            logger.error(f"Error parsing REST order book: {e}")
            logger.debug(f"Data structure: {type(data)}, keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            if isinstance(data, dict):
                logger.debug(f"Bids type: {type(data.get('bids'))}, length: {len(data.get('bids', []))}")
                logger.debug(f"Asks type: {type(data.get('asks'))}, length: {len(data.get('asks', []))}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    async def poll_markets(self, markets: List[Dict]):
        """
        Poll markets via REST API (for REST mode)
        
        Args:
            markets: List of market dicts with condition_id and asset_ids
        """
        if self.ingester is None:
            self.ingester = QuestDBIngester()
        
        for market in markets:
            condition_id = market.get('condition_id')
            asset_ids = market.get('asset_ids', [])
            
            if not condition_id:
                continue
            
            snapshot = self._get_order_book_rest(condition_id, asset_ids)
            
            if snapshot:
                snapshot_data = {
                    'timestamp': datetime.now(),
                    'market_id': snapshot['market_id'],
                    'outcome': snapshot['outcome'],
                    'platform': 'Polymarket',
                    'bid_price_1': snapshot.get('bid_price_1'),
                    'bid_size_1': snapshot.get('bid_size_1'),
                    'bid_price_2': snapshot.get('bid_price_2'),
                    'bid_size_2': snapshot.get('bid_size_2'),
                    'bid_price_3': snapshot.get('bid_price_3'),
                    'bid_size_3': snapshot.get('bid_size_3'),
                    'ask_price_1': snapshot.get('ask_price_1'),
                    'ask_size_1': snapshot.get('ask_size_1'),
                    'ask_price_2': snapshot.get('ask_price_2'),
                    'ask_size_2': snapshot.get('ask_size_2'),
                    'ask_price_3': snapshot.get('ask_price_3'),
                    'ask_size_3': snapshot.get('ask_size_3'),
                    'mid_price': snapshot.get('mid_price'),
                    'spread': snapshot.get('spread'),
                    'total_bid_volume': snapshot.get('total_bid_volume', 0.0),
                    'total_ask_volume': snapshot.get('total_ask_volume', 0.0),
                }
                
                try:
                    self.ingester.ingest_order_book_snapshot(snapshot_data)
                    self.stats['snapshots_stored'] += 1
                except Exception as e:
                    logger.error(f"Error storing snapshot: {e}")
            
            # Rate limiting
            await asyncio.sleep(0.5)
    
    async def start_polling(self, markets: List[Dict] = None):
        """
        Start polling markets via REST API
        
        Args:
            markets: List of market dicts. If None, discovers sports markets
        """
        if not self.enabled:
            logger.error("Client not enabled")
            return
        
        # Discover markets if not provided
        if markets is None:
            logger.info("Discovering sports markets...")
            markets = self.discover_sports_markets()
            
            if not markets:
                logger.warning("No markets found")
                return
        
        self.running = True
        
        logger.info(f"Starting REST polling for {len(markets)} markets (interval: {self.polling_interval}s)")
        
        while self.running:
            try:
                await self.poll_markets(markets)
                logger.info(f"--- Polling Cycle Complete ({len(markets)} markets) ---")
                await asyncio.sleep(self.polling_interval)
            except KeyboardInterrupt:
                logger.info("Polling stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(self.reconnect_delay)
    
    def discover_sports_markets(self, category: str = "Sports", limit: int = 100) -> List[Dict]:
        """
        Discover sports-related markets on Polymarket via Gamma API
        
        Args:
            category: Category to filter (default: "Sports")
            limit: Maximum number of events to return
        
        Returns:
            List of market dictionaries with condition_id, asset_ids, etc.
        """
        if not REQUESTS_AVAILABLE:
            logger.error("requests library not available")
            return []
        
        try:
            # Use Gamma API for market discovery
            url = f"{self.gamma_api_url}/events"
            
            params = {
                'category': category,
                'active': 'true',
                'closed': 'false',  # Only get active markets
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                events = response.json()
                
                # Extract markets from events
                markets = []
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    
                    # Filter for NBA/sports (or all if no filter specified)
                    title = event.get('title', '').lower()
                    ticker = event.get('ticker', '').lower()
                    
                    # If category is Sports, include all sports markets
                    # Otherwise filter for NBA/basketball
                    if category.lower() == 'sports':
                        # Include all sports markets
                        pass
                    elif not any(keyword in (title + ' ' + ticker) for keyword in ['nba', 'basketball']):
                        continue
                    
                    # Get markets from event
                    event_markets = event.get('markets', [])
                    for market in event_markets:
                        if not isinstance(market, dict):
                            continue
                        
                        # Extract condition ID and asset IDs
                        condition_id = market.get('conditionId')
                        clob_token_ids = market.get('clobTokenIds', [])
                        
                        if condition_id and clob_token_ids:
                            # Parse JSON string if needed
                            if isinstance(clob_token_ids, str):
                                import json
                                clob_token_ids = json.loads(clob_token_ids)
                            
                            markets.append({
                                'condition_id': condition_id,
                                'asset_ids': clob_token_ids,
                                'market_id': market.get('id'),
                                'question': market.get('question'),
                                'title': event.get('title'),
                                'category': market.get('category', 'Sports'),
                            })
                
                logger.info(f"Found {len(markets)} NBA/sports markets")
                return markets
            else:
                logger.warning(f"Gamma API returned {response.status_code}: {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Error discovering markets: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def get_market_order_book(self, condition_id: str) -> Optional[Dict]:
        """
        Get order book for a specific market via Gamma API
        
        Args:
            condition_id: Market condition ID
        
        Returns:
            Market data with order book info
        """
        if not REQUESTS_AVAILABLE:
            return None
        
        try:
            # Use Gamma API markets endpoint
            url = f"{self.gamma_api_url}/markets"
            params = {'condition_id': condition_id}
            
            response = requests.get(url, params=params, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.warning(f"Gamma API returned {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching market order book: {e}")
            return None

