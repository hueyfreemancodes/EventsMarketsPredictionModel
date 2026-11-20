
import psycopg2
import time
from datetime import datetime

def check_recent_data():
    try:
        conn = psycopg2.connect(
            host='localhost', port='8812', 
            user='admin', password='quest', database='qdb'
        )
        cur = conn.cursor()
        
        # Check count of rows in the last 1 minute
        # Note: QuestDB uses microseconds for timestamps usually, or standard SQL
        query = "SELECT count(*) FROM order_book_snapshots WHERE timestamp > dateadd('m', -1, now())"
        
        cur.execute(query)
        cnt = cur.fetchone()[0]
        
        print(f"RECENT_ROWS_1MIN: {cnt}")
        
    except Exception as e:
        print(f"DB Error: {e}")

if __name__ == "__main__":
    check_recent_data()
