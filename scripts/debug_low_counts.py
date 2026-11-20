
import psycopg2
import pandas as pd

def check_low_count_markets():
    try:
        conn = psycopg2.connect(
            host='localhost', port='8812', 
            user='admin', password='quest', database='qdb'
        )
        # Get distribution of snapshot counts
        query = """
        SELECT market_id, count(*) as cnt 
        FROM order_book_snapshots 
        GROUP BY market_id 
        ORDER BY cnt ASC 
        LIMIT 20
        """
        
        df = pd.read_sql(query, conn)
        print("--- Markets with Fewest Snapshots ---")
        print(df)
        
        # Check Total Unique Markets
        query_total = "SELECT count(distinct market_id) FROM order_book_snapshots"
        cursor = conn.cursor()
        cursor.execute(query_total)
        print(f"\nTotal Markets with Data: {cursor.fetchone()[0]}")
        
    except Exception as e:
        print(f"DB Error: {e}")

if __name__ == "__main__":
    check_low_count_markets()
