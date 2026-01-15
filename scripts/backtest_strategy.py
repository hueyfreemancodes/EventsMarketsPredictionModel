
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import os

# Config
DATASET_PATH = 'final_training_set_v2.csv'
MODEL_PATH = 'xgb_model.json'
TRADE_SIZE_USD = 5.0 # $5 Risk per trade
COST_PER_TRADE = 0.01  # Spread + Fee per SHARE (not per trade) - Conservative estimate: 1 cent width
SIGNAL_THRESHOLD = 0.02 # Only trade if predicted move is > 2 cents

def backtest():
    # 1. Load Data
    print("Loading dataset...")
    df = pd.read_csv(DATASET_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # 2. Split (Same as Training)
    split_idx = int(len(df) * 0.8)
    test_df = df.iloc[split_idx:].copy()
    
    print(f"Backtesting on {len(test_df)} samples (20% Hold-out).")
    
    # 3. Load Model
    print("Loading model...")
    model = xgb.Booster()
    model.load_model(MODEL_PATH)
    
    # 4. Prepare Features
    # Filter numeric like training
    numeric_df = test_df.select_dtypes(include=['number'])
    # Exclude targets
    exclude = ['target_return_60s', 'target_return_5s', 'mid_price', 'spread'] 
    # Valid feats
    feats = [c for c in numeric_df.columns if c not in exclude and 'target' not in c]
    
    X_test = test_df[feats]
    y_true = test_df['target_return_180s'] # Use the 3-minute target
    
    # 5. Predict
    dtest = xgb.DMatrix(X_test)
    preds = model.predict(dtest)
    
    # 6. Simulate Strategy
    print("Simulating trades...")
    
    signals = np.zeros(len(preds))
    # Buy Signal
    signals[preds > SIGNAL_THRESHOLD] = 1
    # Sell Signal
    signals[preds < -SIGNAL_THRESHOLD] = -1
    
    # Calculate Returns
    # IF Buy (1): PnL = Return - Cost
    # IF Sell (-1): PnL = -Return - Cost (Shorting: profit if price drops)
    # IF Hold (0): PnL = 0
    
    # Note: target_return_60s is (FuturePrice - CurrentPrice).
    # Buying captures this delta.
    # Selling captures -(FuturePrice - CurrentPrice).
    
    # Calculate Returns (Simulating $5 per trade)
    # 1. Get Current Price
    current_prices = test_df['micro_price']
    
    # 2. Calculate Position Size (Shares = $5 / Price)
    # Handle prices near 0/1 safely
    safe_prices = current_prices.clip(0.01, 0.99)
    num_shares = TRADE_SIZE_USD / safe_prices
    
    # 3. Raw PnL ($) = Signal * Shares * PriceChange
    raw_pnl = signals * num_shares * y_true
    
    # 4. Costs
    # Spread/Fee is per share. Assuming 1 cent spread/fee per share purchased.
    costs = np.abs(signals) * num_shares * COST_PER_TRADE
    
    net_pnl = raw_pnl - costs
    
    # 7. Metrics
    n_trades = np.abs(signals).sum()
    total_pnl = net_pnl.sum()
    win_rate = (net_pnl > 0).sum() / n_trades if n_trades > 0 else 0
    
    print(f"\n--- Results (Threshold: {SIGNAL_THRESHOLD}, Cost: {COST_PER_TRADE}) ---")
    print(f"Total Trades: {int(n_trades)}")
    print(f"Total PnL:    ${total_pnl:.2f}")
    if n_trades > 0:
        print(f"Avg PnL/Trade:${total_pnl/n_trades:.4f}")
        print(f"Win Rate:     {win_rate*100:.2f}%")
        
    # 8. Plot Equity Curve
    test_df['pnl'] = net_pnl
    test_df['cumulative_pnl'] = test_df['pnl'].cumsum()
    
    plt.figure(figsize=(10, 6))
    plt.plot(test_df['timestamp'], test_df['cumulative_pnl'], label='Strategy Equity')
    plt.title(f"Backtest PnL (Thresh={SIGNAL_THRESHOLD}, Cost={COST_PER_TRADE})")
    plt.xlabel('Timestamp')
    plt.ylabel('Cumulative Profit ($)')
    plt.legend()
    plt.grid(True)
    plt.savefig('backtest_equity_curve.png')
    print("Saved equity curve to 'backtest_equity_curve.png'")

if __name__ == "__main__":
    backtest()
