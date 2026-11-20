
import psycopg2
from datetime import datetime
import pandas as pd

def audit_db():
    conn = psycopg2.connect(
        host='localhost', port='8812', 
        user='admin', password='quest', database='qdb'
    )
    cur = conn.cursor()
    
    print("--- Database Integrity Audit ---")
    
    # 1. Check Totals by Day
    print("Snapshot Count by Day:")
    cur.execute("""
        SELECT day(timestamp) as d, count(*) 
        FROM order_book_snapshots 
        WHERE timestamp > dateadd('d', -2, now())
        GROUP BY d
        ORDER BY d
    """)
    for row in cur.fetchall():
        print(f"  Day {row[0]}: {row[1]:,} rows")
        
    # 2. Check Latest Timestamp
    cur.execute("SELECT max(timestamp) FROM order_book_snapshots")
    last_ts = cur.fetchone()[0]
    print(f"\nLatest Snapshot: {last_ts}")
    
    # 3. Check Active Markets Count (distinct IDs in last hour)
    cur.execute("""
        SELECT count(distinct market_id) 
        FROM order_book_snapshots 
        WHERE timestamp > dateadd('h', -1, now())
    """)
    active_m = cur.fetchone()[0]
    print(f"Active Markets (Last 1hr): {active_m}")
    
    # 4. Check Fundamentals for Today
    today = datetime.now().strftime('%Y-%m-%d')
    cur.execute(f"SELECT count(*) FROM sports_fundamentals WHERE game_date = '{today}'")
    fund_count = cur.fetchone()[0]
    print(f"\nFundamentals for {today}: {fund_count} records")

    conn.close()

if __name__ == "__main__":
    audit_db()
