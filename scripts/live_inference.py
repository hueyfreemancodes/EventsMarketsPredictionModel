import os
import time
import pandas as pd
import xgboost as xgb
import psycopg2
from datetime import datetime, timedelta

# Config
DB_CONFIG = {
    "host": os.getenv('QUESTDB_HOST', 'localhost'),
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "database": "qdb"
}

def load_data(limit_seconds=600):
    """Fetch recent market data for a single active market from QuestDB"""
    # Just grab the most active market ID from the last hour
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Find a market with recent updates
    query_id = """
        SELECT market_id, count() as cnt
        FROM microstructure_features 
        WHERE timestamp > dateadd('h', -24, now()) 
        GROUP BY market_id 
        ORDER BY cnt DESC 
        LIMIT 20
    """
    try:
        # Load known markets first
        with open('nba_game_markets.json', 'r') as f:
            markets = json.load(f)
        known_ids = set()
        for m in markets:
            t = to_hex(m.get('clob_token_id'))
            if t: known_ids.add(t)
            
        cur = conn.cursor()
        cur.execute(query_id)
        rows = cur.fetchall()
        
        market_id = None
        for row in rows:
            mid = row[0]
            if mid in known_ids:
                market_id = mid
                break
                
        if not market_id:
            print("No active markets found that match known NBA metadata.")
            # Fallback to top one
            if rows:
                market_id = rows[0][0]
                print(f"Fallback: Using {market_id} (No Metadata)")
            else:
                return None, None
            
        print(f"Tracking Market ID: {market_id}")
        
        # Now fetch features for this market
        # We need the columns expected by the model
        # For simplicity, we assume the model uses: 
        # ofi_1s, vamp, micro_price, spread_volatility, ofi_ema_05
        # AND Kalshi features if used (k_micro_price, k_vamp, k_ofi, k_volatility, arb_spread, feed_latency)
        
        # NOTE: The trained model expects exact columns. 
        # Ideally, we load the feature names from the model or logic.
        # Let's try to query ALL numeric columns from microstructure_features
        # + placeholders for Kalshi if missing (since we might just be looking at Poly data live)
        
        # Actually, let's just use the columns we KNOW we trained on in 'final_training_set_v2.csv'
        # minus the target.
        
        # Let's dump the columns from the csv header actually, or just guess standard ones.
        cols = "micro_price, vamp, spread_volatility, ofi_1s, ofi_ema_05"
        
        query_data = f"""
            SELECT {cols}
            FROM microstructure_features
            WHERE market_id = '{market_id}'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        df = pd.read_sql(query_data, conn)
        
        # Add dummy/imputed columns for Kalshi features if the model relies on them
        # (Since live inference might not have perfectly aligned Kalshi data instantly available without the complex pipeline)
        # IMPORTANT: XGBoost will error if columns are missing.
        # We need to fill missing cols with 0 or NA.
        
        required_cols = [
            'micro_price', 'vamp', 'ofi_1s', 'spread_volatility', 'ofi_ema_05',
            'k_micro_price', 'k_vamp', 'k_ofi', 'k_volatility', 'arb_spread', 'feed_latency'
        ]
        
        # Check what we have
        for c in required_cols:
            if c not in df.columns:
                # Naive imputation for demo
                df[c] = 0.0 
        
        # XGBoost tracks feature names, so order matters less if dataframe is passed, 
        # BUT we must ensure we pass a dataframe with correct names.
        
        # MISSING FEATURES PATCH:
        # The model was trained on a set that likely had merge artifacts (ofi_1s_x, etc.) 
        # and fundamental data. We must supply them.
        
        defaults = {
            'team1_win_pct': 0.5,
            'team2_win_pct': 0.5,
            'spread_vegas': 0.0,
            'ofi_1s_x': df['ofi_1s'] if 'ofi_1s' in df else 0.0,
            'ofi_1s_y': 0.0, # Artifact
        }
        
        for col, val in defaults.items():
            df[col] = val
            
        # Explicit Feature Order (Required by XGBoost)
        model_features = [
            'ofi_1s', 'vamp', 'micro_price', 'spread_volatility', 'ofi_ema_05', 
            'ofi_1s_x', 'ofi_1s_y', 'k_vamp', 'k_micro_price', 'k_volatility', 
            'k_ofi', 'arb_spread', 'feed_latency', 'team1_win_pct', 
            'team2_win_pct', 'spread_vegas'
        ]
        
        # Reorder columns
        df = df[model_features]
        
        return df, market_id
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None, None
    finally:
        conn.close()

import json

def to_hex(val):
    """Convert decimal string to hex string with 0x prefix."""
    try:
        if not val: return None
        if str(val).startswith("0x"): return val
        return hex(int(val))
    except:
        return None

import requests

def get_metadata_from_api(token_id):
    """Fetch metadata directly from Polymarket CLOB API."""
    try:
        url = f"https://clob.polymarket.com/markets/{token_id}"
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            # CLOB API structure might differ slightly, let's map it
            # Response typically has 'condition_id', 'question', 'description', etc.
            return {
                'title': "Unknown Game", # CLOB API often lacks event title, but has question
                'question': data.get('question', 'Unknown Market'),
                'market_id': data.get('condition_id') # or similar
            }
    except Exception as e:
        print(f"API Metadata lookup failed: {e}")
    return None

def get_market_metadata(market_id):
    """Load market metadata from local JSON cache, fallback to API."""
    # 1. Local Cache
    try:
        with open('nba_game_markets.json', 'r') as f:
            markets = json.load(f)
        
        # Convert decimal IDs to hex to match DB
        meta_map = {}
        for m in markets:
            tok = m.get('clob_token_id')
            if tok:
                h_tok = to_hex(tok)
                if h_tok:
                    meta_map[h_tok] = m
                    
        local_meta = meta_map.get(str(market_id))
        if local_meta:
            return local_meta
            
    except Exception as e:
        print(f"Local lookup failed: {e}")

    # 2. Fallback to API
    print("Local lookup failed. Fetching from CLOB API...")
    # market_id here is the HEX token ID (0x...)
    # CLOB API expects just the token id. 
    return get_metadata_from_api(str(market_id))

def run_inference():
    print("Loading model...")
    model = xgb.Booster()
    model.load_model('xgb_model.json')
    print("Model loaded.")
    
    print("Fetching live market data...")
    df, market_id = load_data()
    
    if df is None:
        return
        
    # Get Metadata
    meta = get_market_metadata(market_id)
    
    print(f"\n" + "="*50)
    if meta:
        print(f"🏀 GAME: {meta.get('title')}")
        print(f"📊 MARKET: {meta.get('question')}")
        print(f"🆔 ID: {market_id}")
    else:
        print(f"--- Live Signal for Market {market_id} ---")
    print("="*50)
    
    # Convert to DMatrix for XGBoost
    dtest = xgb.DMatrix(df)
    
    # Predict
    prediction = model.predict(dtest)[0]
    
    # Get current price context
    current_price = df['micro_price'].iloc[0]
    
    print(f"Current Micro-Price:  ${current_price:.3f}")
    print(f"Predicted 60s Return: ${prediction:.4f}")
    print(f"Projected Price:      ${current_price + prediction:.3f}")
    
    print("\n--- Trading Signal ---")
    if prediction > 0.02:
        print(f"🚀 BUY SIGNAL (Strong Up: +{prediction:.3f})")
    elif prediction < -0.02:
        print(f"🔻 SELL SIGNAL (Strong Down: {prediction:.3f})")
    else:
        print(f"⏸️  HOLD / WAITING (Noise: {prediction:.3f})")

if __name__ == "__main__":
    run_inference()
