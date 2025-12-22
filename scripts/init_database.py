#!/usr/bin/env python3
"""
Initialize QuestDB Database Schema
Creates all required tables for the sports prediction model.
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    import psycopg2
    from config import QUESTDB_HOST
except ImportError:
    print("Installing required dependencies...")
    os.system(f"{sys.executable} -m pip install psycopg2-binary")
    import psycopg2
    from config import QUESTDB_HOST

# QuestDB PostgreSQL wire protocol port (not ILP port)
QUESTDB_PORT = 8812

# QuestDB uses PostgreSQL wire protocol
QUESTDB_USER = "admin"
QUESTDB_PASSWORD = "quest"
QUESTDB_DATABASE = "qdb"

def create_connection():
    """Create connection to QuestDB via PostgreSQL wire protocol"""
    try:
        conn = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,  # 8812 for PostgreSQL wire protocol
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DATABASE
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Error connecting to QuestDB: {e}")
        print(f"\nMake sure QuestDB is running:")
        print(f"  docker-compose up -d")
        print(f"  or")
        print(f"  ./scripts/start_questdb.sh")
        print(f"\nWaiting a few seconds for QuestDB to be fully ready...")
        print(f"Then try again.")
        sys.exit(1)

def create_tables(conn):
    """Create all required tables"""
    cursor = conn.cursor()
    
    tables = {
        "order_book_snapshots": """
        CREATE TABLE IF NOT EXISTS order_book_snapshots (
            timestamp TIMESTAMP,
            market_id SYMBOL,
            outcome SYMBOL,
            platform SYMBOL,
            bid_price_1 DOUBLE,
            bid_size_1 DOUBLE,
            bid_price_2 DOUBLE,
            bid_size_2 DOUBLE,
            bid_price_3 DOUBLE,
            bid_size_3 DOUBLE,
            ask_price_1 DOUBLE,
            ask_size_1 DOUBLE,
            ask_price_2 DOUBLE,
            ask_size_2 DOUBLE,
            ask_price_3 DOUBLE,
            ask_size_3 DOUBLE,
            mid_price DOUBLE,
            spread DOUBLE,
            total_bid_volume DOUBLE,
            total_ask_volume DOUBLE
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        
        "trades": """
        CREATE TABLE IF NOT EXISTS trades (
            timestamp TIMESTAMP,
            market_id SYMBOL,
            outcome SYMBOL,
            platform SYMBOL,
            price DOUBLE,
            size DOUBLE,
            side SYMBOL,
            trade_id LONG
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        
        "microstructure_features": """
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
            spread_volatility DOUBLE
        ) TIMESTAMP(timestamp) PARTITION BY HOUR;
        """,
        
        "sports_fundamentals": """
        CREATE TABLE IF NOT EXISTS sports_fundamentals (
            timestamp TIMESTAMP,
            event_id SYMBOL,
            sport SYMBOL,
            league SYMBOL,
            home_team SYMBOL,
            away_team SYMBOL,
            game_date TIMESTAMP,
            home_win_pct DOUBLE,
            away_win_pct DOUBLE,
            home_avg_score DOUBLE,
            away_avg_score DOUBLE,
            home_avg_points_allowed DOUBLE,
            away_avg_points_allowed DOUBLE,
            home_avg_point_diff DOUBLE,
            away_avg_point_diff DOUBLE,
            home_home_win_pct DOUBLE,
            away_away_win_pct DOUBLE,
            home_last_3_wins INT,
            home_last_5_wins INT,
            away_last_3_wins INT,
            away_last_5_wins INT,
            home_point_diff_std DOUBLE,
            away_point_diff_std DOUBLE,
            is_home_back2back BOOLEAN,
            is_away_back2back BOOLEAN,
            travel_distance DOUBLE,
            rest_days_home INT,
            rest_days_away INT,
            altitude_diff DOUBLE,
            injuries_home STRING,
            injuries_away STRING,
            lineup_home STRING,
            lineup_away STRING
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        
        "player_stats": """
        CREATE TABLE IF NOT EXISTS player_stats (
            timestamp TIMESTAMP,
            player_id SYMBOL,
            player_name SYMBOL,
            sport SYMBOL,
            team SYMBOL,
            stat_type SYMBOL,
            stat_avg DOUBLE,
            stat_std DOUBLE,
            stat_median DOUBLE,
            stat_75th DOUBLE,
            stat_25th DOUBLE,
            stat_last_3_avg DOUBLE,
            stat_last_5_avg DOUBLE,
            stat_last_10_avg DOUBLE,
            games_played INT,
            last_game_date TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        
        "game_schedules": """
        CREATE TABLE IF NOT EXISTS game_schedules (
            timestamp TIMESTAMP,
            event_id SYMBOL,
            sport SYMBOL,
            home_team SYMBOL,
            away_team SYMBOL,
            game_date TIMESTAMP,
            game_time TIMESTAMP,
            venue SYMBOL,
            venue_location STRING,
            venue_latitude DOUBLE,
            venue_longitude DOUBLE,
            is_playoff BOOLEAN,
            is_neutral_site BOOLEAN
        ) TIMESTAMP(timestamp) PARTITION BY MONTH;
        """,
        
        "sportsbook_odds": """
        CREATE TABLE IF NOT EXISTS sportsbook_odds (
            timestamp TIMESTAMP,
            event_id SYMBOL,
            sport SYMBOL,
            sportsbook SYMBOL,
            market_type SYMBOL,
            home_team SYMBOL,
            away_team SYMBOL,
            -- Odds in different formats
            american_odds_home INT,
            american_odds_away INT,
            decimal_odds_home DOUBLE,
            decimal_odds_away DOUBLE,
            -- Implied probabilities (with vig)
            implied_prob_home DOUBLE,
            implied_prob_away DOUBLE,
            -- True probabilities (after Shin's method / vig removal)
            true_prob_home DOUBLE,
            true_prob_away DOUBLE,
            -- For spreads/totals
            line DOUBLE,
            over_odds INT,
            under_odds INT,
            -- Metadata
            last_updated TIMESTAMP,
            is_main_line BOOLEAN
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
    }
    
    print("Creating database tables...")
    for table_name, create_sql in tables.items():
        try:
            cursor.execute(create_sql)
            print(f"  ✅ Created table: {table_name}")
        except Exception as e:
            print(f"  ⚠️  Table {table_name}: {e}")
            # Continue even if table already exists
    
    conn.commit()
    cursor.close()
    print("\n✅ Database schema initialized successfully!")

def verify_tables(conn):
    """Verify all tables were created"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM tables() 
        WHERE table_name IN (
            'order_book_snapshots',
            'trades',
            'microstructure_features',
            'sports_fundamentals',
            'player_stats',
            'game_schedules',
            'sportsbook_odds'
        )
        ORDER BY table_name;
    """)
    
    tables = cursor.fetchall()
    cursor.close()
    
    expected_tables = {
        'order_book_snapshots',
        'trades',
        'microstructure_features',
        'sports_fundamentals',
        'player_stats',
        'game_schedules',
        'sportsbook_odds'
    }
    
    found_tables = {row[0] for row in tables}
    
    print("\n📊 Database Tables:")
    for table in sorted(expected_tables):
        status = "✅" if table in found_tables else "❌"
        print(f"  {status} {table}")
    
    if found_tables == expected_tables:
        print("\n✅ All tables verified!")
        return True
    else:
        missing = expected_tables - found_tables
        print(f"\n⚠️  Missing tables: {missing}")
        return False

def main():
    print("=" * 60)
    print("QuestDB Database Initialization")
    print("=" * 60)
    print()
    
    # Test connection
    print("Connecting to QuestDB...")
    conn = create_connection()
    print(f"✅ Connected to QuestDB at {QUESTDB_HOST}:{QUESTDB_PORT}")
    print()
    
    # Create tables
    create_tables(conn)
    
    # Verify tables
    verify_tables(conn)
    
    conn.close()
    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. View database: http://localhost:9000")
    print("  2. Start data collection scripts")

if __name__ == "__main__":
    main()

