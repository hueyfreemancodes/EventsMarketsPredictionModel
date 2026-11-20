import sys
import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.models.xgboost_pipeline import XGBoostPipeline

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_mock_dataset(size=500):
    """
    Creates a mock historical dataset to train the XGBoost models.
    In production, this would be generated from an SQL database of historical player logs.
    """
    np.random.seed(42)
    
    # Generate random numeric features roughly matching NBA stats
    df = pd.DataFrame({
        'prop_line': np.random.uniform(5.5, 25.5, size),
        'season_avg_pts': np.random.uniform(10, 30, size),
        'season_avg_reb': np.random.uniform(2, 12, size),
        'season_avg_ast': np.random.uniform(1, 10, size),
        'recent_avg_pts': np.random.uniform(10, 30, size),
        'recent_avg_reb': np.random.uniform(2, 12, size),
        'recent_avg_ast': np.random.uniform(1, 10, size),
        'true_usage': np.random.uniform(0.15, 0.35, size),
        'potential_ast': np.random.uniform(2, 15, size),
        'reb_chances': np.random.uniform(4, 20, size),
        'bayesian_prob': np.random.uniform(0.3, 0.7, size)  # Simulated Bayesian model output
    })
    
    # Target: Did they go OVER the line? (Rough logic for mock data: if season avg + recent avg / 2 is > prop_line + noise)
    avg_score = (df['season_avg_pts'] + df['recent_avg_pts']) / 2
    noise = np.random.normal(0, 3, size)
    df['target'] = ((avg_score + noise) > df['prop_line']).astype(int)
    
    return df

def main():
    dataset_path = os.path.join(project_root, 'data/historical_training_set.csv')
    if not os.path.exists(dataset_path):
        print(f"Historical dataset not found at {dataset_path}. Generating mock data for fallback...")
        df = create_mock_dataset(1000)
    else:
        print(f"Loading real historical dataset from {dataset_path}...")
        df = pd.read_csv(dataset_path)
        # Rename target column to match what pipeline expects if needed
        if 'target_pts' in df.columns:
            df['target'] = df['target_pts']
            
    # Live Production: Train on all available historical data
    # (Removed the April 2025 backtest split so we learn from the current Playoffs)
    cutoff_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Live Production Mode: Training on all games BEFORE {cutoff_date}")
    df['game_date'] = pd.to_datetime(df['game_date'])
    train_df_full = df[df['game_date'] < pd.to_datetime(cutoff_date)].copy()
    
    pipeline = XGBoostPipeline()
    
    markets = ['player_points', 'player_rebounds', 'player_assists', 'player_threes']
    # Drop non-numeric columns that XGBoost can't handle
    cols_to_drop = ['player_name', 'game_date', 'player_id', 'season']
    train_df = train_df_full.drop(columns=[c for c in cols_to_drop if c in train_df_full.columns])
    
    # Ensure all targets are present
    if 'target' not in train_df.columns and 'target_pts' in train_df.columns:
        train_df['target'] = train_df['target_pts']
        
    train_df = train_df.dropna(subset=['target'])
    train_df['target'] = train_df['target'].astype(int)

    for market in markets:
        print(f"Training model for {market}...")
        # For this demo, we use the same dataset for all markets
        # In production, each market would have its own historical targets
        pipeline.train(train_df, target_col='target', market=market)
        
    print("All models trained and saved successfully.")

if __name__ == "__main__":
    main()
