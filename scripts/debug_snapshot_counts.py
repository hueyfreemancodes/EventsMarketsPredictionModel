
import psycopg2
import sys

def check_snapshots(market_ids):
    try:
        conn = psycopg2.connect(
            host='localhost', port='8812', 
            user='admin', password='quest', database='qdb'
        )
        cur = conn.cursor()
        
        print(f"--- Checking Snapshot Counts for {len(market_ids)} Markets ---")
        for mid in market_ids:
            # Check total count
            query = f"SELECT count(*) FROM order_book_snapshots WHERE market_id = '{mid}'"
            cur.execute(query)
            total = cur.fetchone()[0]
            
            # Check count in last hour
            query_recent = f"SELECT count(*) FROM order_book_snapshots WHERE market_id = '{mid}' AND timestamp > dateadd('h', -1, now())"
            cur.execute(query_recent)
            recent = cur.fetchone()[0]
            
            print(f"Market {mid[:8]}... : Total={total} | Recent(1h)={recent}")
            
    except Exception as e:
        print(f"DB Error: {e}")

if __name__ == "__main__":
    # Example IDs from logs: 41436885, 13974700, 10445648
    ids = ["41436885", "13974700", "10445648"] 
    # Note: These seem to be short IDs? Poly IDs are long strings?
    # Ah, the logs say "DEBUG SKIP 41436885...". Maybe it's truncated?
    # Or maybe these are valid IDs?
    # Let's check typical length.
    check_snapshots(ids)
