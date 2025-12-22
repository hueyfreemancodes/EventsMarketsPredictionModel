"""
QuestDB Data Ingester
Handles writing data to QuestDB database
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

try:
    import psycopg2
    from psycopg2.extras import execute_values
    from config import QUESTDB_HOST
    from .logger import logger
except ImportError as e:
    print(f"Import error: {e}")
    raise

# QuestDB PostgreSQL wire protocol port
QUESTDB_PORT = 8812
QUESTDB_USER = "admin"
QUESTDB_PASSWORD = "quest"
QUESTDB_DATABASE = "qdb"


class QuestDBIngester:
    """Handles data ingestion to QuestDB"""
    
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish connection to QuestDB"""
        try:
            self.conn = psycopg2.connect(
                host=os.environ.get('QUESTDB_HOST', QUESTDB_HOST),
                port=int(os.environ.get('QUESTDB_PORT', QUESTDB_PORT)),
                user=QUESTDB_USER,
                password=QUESTDB_PASSWORD,
                database=QUESTDB_DATABASE
            )
            logger.info(f"Connected to QuestDB at {QUESTDB_HOST}:{QUESTDB_PORT}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            raise
    
    def _ensure_connected(self):
        """Ensure connection is active, reconnect if needed"""
        try:
            self.conn.cursor().execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Connection lost, reconnecting...")
            self._connect()
    
    def ingest_order_book_snapshot(self, data: Dict):
        """
        Ingest order book snapshot data
        
        Args:
            data: Dictionary with fields matching order_book_snapshots table
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            insert_sql = """
            INSERT INTO order_book_snapshots (
                timestamp, market_id, outcome, platform,
                bid_price_1, bid_size_1, bid_price_2, bid_size_2, bid_price_3, bid_size_3,
                ask_price_1, ask_size_1, ask_price_2, ask_size_2, ask_price_3, ask_size_3,
                mid_price, spread, total_bid_volume, total_ask_volume
            ) VALUES (
                %(timestamp)s, %(market_id)s, %(outcome)s, %(platform)s,
                %(bid_price_1)s, %(bid_size_1)s, %(bid_price_2)s, %(bid_size_2)s,
                %(bid_price_3)s, %(bid_size_3)s,
                %(ask_price_1)s, %(ask_size_1)s, %(ask_price_2)s, %(ask_size_2)s,
                %(ask_price_3)s, %(ask_size_3)s,
                %(mid_price)s, %(spread)s, %(total_bid_volume)s, %(total_ask_volume)s
            )
            """
            
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested order book snapshot for {data.get('market_id')}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting order book snapshot: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_trade(self, data: Dict):
        """
        Ingest trade data
        
        Args:
            data: Dictionary with fields matching trades table
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            insert_sql = """
            INSERT INTO trades (
                timestamp, market_id, outcome, platform,
                price, size, side, trade_id
            ) VALUES (
                %(timestamp)s, %(market_id)s, %(outcome)s, %(platform)s,
                %(price)s, %(size)s, %(side)s, %(trade_id)s
            )
            """
            
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested trade for {data.get('market_id')}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting trade: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_sports_fundamentals(self, data: Dict):
        """Ingest sports fundamentals data (single record)"""
        self.ingest_sports_fundamentals_batch([data])
    
    def _convert_numpy_types(self, data: Dict) -> Dict:
        """Convert numpy/pandas types to native Python types for QuestDB"""
        import numpy as np
        converted = {}
        for key, value in data.items():
            if value is None:
                converted[key] = None
            elif isinstance(value, (np.integer, np.int64, np.int32)):
                converted[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                converted[key] = float(value)
            elif isinstance(value, np.bool_):
                converted[key] = bool(value)
            elif isinstance(value, np.ndarray):
                # Convert arrays to lists
                converted[key] = value.tolist()
            else:
                converted[key] = value
        return converted
    
    def ingest_sports_fundamentals_batch(self, data_list: List[Dict]):
        """Ingest multiple sports fundamentals records (batch)"""
        if not data_list:
            return
        
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            # Use first record to determine fields
            fields = list(data_list[0].keys())
            placeholders = ', '.join([f'%({f})s' for f in fields])
            field_names = ', '.join(fields)
            
            insert_sql = f"""
            INSERT INTO sports_fundamentals ({field_names})
            VALUES ({placeholders})
            """
            
            for data in data_list:
                # Convert numpy types to native Python types
                converted_data = self._convert_numpy_types(data)
                cursor.execute(insert_sql, converted_data)
            
            self.conn.commit()
            logger.debug(f"Ingested {len(data_list)} sports fundamentals records")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting sports fundamentals: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_player_stats(self, data: Dict):
        """Ingest player statistics data"""
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            fields = list(data.keys())
            placeholders = ', '.join([f'%({f})s' for f in fields])
            field_names = ', '.join(fields)
            
            insert_sql = f"""
            INSERT INTO player_stats ({field_names})
            VALUES ({placeholders})
            """
            
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested player stats for {data.get('player_id')}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting player stats: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_game_schedule(self, data: Dict):
        """Ingest game schedule data (single record)"""
        self.ingest_game_schedules([data])
    
    def ingest_game_schedules(self, data_list: List[Dict]):
        """Ingest multiple game schedule records (batch)"""
        if not data_list:
            return
        
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            # Use first record to determine fields
            fields = list(data_list[0].keys())
            placeholders = ', '.join([f'%({f})s' for f in fields])
            field_names = ', '.join(fields)
            
            insert_sql = f"""
            INSERT INTO game_schedules ({field_names})
            VALUES ({placeholders})
            """
            
            for data in data_list:
                cursor.execute(insert_sql, data)
            
            self.conn.commit()
            logger.debug(f"Ingested {len(data_list)} game schedules")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting game schedules: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_sportsbook_odds(self, data: Dict):
        """Ingest sportsbook odds data"""
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            fields = list(data.keys())
            placeholders = ', '.join([f'%({f})s' for f in fields])
            field_names = ', '.join(fields)
            
            insert_sql = f"""
            INSERT INTO sportsbook_odds ({field_names})
            VALUES ({placeholders})
            """
            
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested sportsbook odds for {data.get('event_id')} from {data.get('sportsbook')}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting sportsbook odds: {e}")
            raise
        finally:
            cursor.close()
    
    def ingest_microstructure_features(self, data: Dict):
        """
        Ingest microstructure features data
        
        Args:
            data: Dictionary with fields matching microstructure_features table
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        
        try:
            # Convert numpy types to native Python types
            data = self._convert_numpy_types(data)
            
            insert_sql = """
            INSERT INTO microstructure_features (
                timestamp, market_id, outcome,
                ofi_1s, ofi_5s, ofi_15s, ofi_60s,
                vamp, micro_price, obi_weighted,
                kyle_lambda, pin_score,
                volume_imbalance, depth_ratio, spread_volatility,
                ofi_ema_01, ofi_ema_03, ofi_ema_05
            ) VALUES (
                %(timestamp)s, %(market_id)s, %(outcome)s,
                %(ofi_1s)s, %(ofi_5s)s, %(ofi_15s)s, %(ofi_60s)s,
                %(vamp)s, %(micro_price)s, %(obi_weighted)s,
                %(kyle_lambda)s, %(pin_score)s,
                %(volume_imbalance)s, %(depth_ratio)s, %(spread_volatility)s,
                %(ofi_ema_01)s, %(ofi_ema_03)s, %(ofi_ema_05)s
            )
            """
            
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested microstructure features for {data.get('market_id')}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting microstructure features: {e}")
            raise
        finally:
            cursor.close()
    
    def create_market_linkages_table(self):
        """Create the market_linkages table if it doesn't exist"""
        self._ensure_connected()
        cursor = self.conn.cursor()
        try:
            create_sql = """
            CREATE TABLE IF NOT EXISTS market_linkages (
                market_id SYMBOL,
                source SYMBOL,
                team1 SYMBOL,
                team2 SYMBOL,
                game_date TIMESTAMP,
                original_title STRING,
                series_ticker SYMBOL,
                created_at TIMESTAMP
            ) timestamp(created_at) PARTITION BY MONTH;
            """
            cursor.execute(create_sql)
            self.conn.commit()
            logger.info("Ensured market_linkages table exists")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating market_linkages table: {e}")
            raise
        finally:
            cursor.close()

    def ingest_market_linkage(self, data: Dict):
        """
        Ingest market linkage data
        Args:
            data: Dictionary with fields matching market_linkages table
        """
        self._ensure_connected()
        cursor = self.conn.cursor()
        try:
            insert_sql = """
            INSERT INTO market_linkages (
                market_id, source, team1, team2,
                game_date, original_title, series_ticker, created_at
            ) VALUES (
                %(market_id)s, %(source)s, %(team1)s, %(team2)s,
                %(game_date)s, %(original_title)s, %(series_ticker)s, %(created_at)s
            )
            """
            cursor.execute(insert_sql, data)
            self.conn.commit()
            logger.debug(f"Ingested linkage for {data.get('market_id')}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ingesting market linkage: {e}")
            raise
        finally:
            cursor.close()

    def create_microstructure_features_table(self):
        """Create the microstructure_features table if it doesn't exist"""
        self._ensure_connected()
        cursor = self.conn.cursor()
        try:
            create_sql = """
            CREATE TABLE IF NOT EXISTS microstructure_features (
                timestamp TIMESTAMP,
                market_id SYMBOL,
                outcome SYMBOL,
                ofi_1s DOUBLE,
                ofi_5s DOUBLE,
                ofi_15s DOUBLE,
                ofi_60s DOUBLE,
                vamp DOUBLE,
                micro_price DOUBLE,
                obi_weighted DOUBLE,
                kyle_lambda DOUBLE,
                pin_score DOUBLE,
                volume_imbalance DOUBLE,
                depth_ratio DOUBLE,
                spread_volatility DOUBLE,
                ofi_ema_01 DOUBLE,
                ofi_ema_03 DOUBLE,
                ofi_ema_05 DOUBLE
            ) timestamp(timestamp) PARTITION BY DAY;
            """
            cursor.execute(create_sql)
            self.conn.commit()
            logger.info("Ensured microstructure_features table exists")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating microstructure_features table: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed QuestDB connection")

