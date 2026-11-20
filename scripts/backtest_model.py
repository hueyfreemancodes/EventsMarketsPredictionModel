import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

# Set up paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.models.xgboost_pipeline import XGBoostPipeline
from src.models.player_bayesian_model import PlayerBayesianModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Backtester")

class ModelBacktester:
    def __init__(self):
        self.pipeline = XGBoostPipeline()
        self.player_model = PlayerBayesianModel()
        self.dataset_path = os.path.join(project_root, 'data/historical_training_set.csv')
        
    def run_backtest(self, test_size=1000, edge_threshold=0.05):
        """Run backtest on the latest rows of the dataset (holdout set)"""
        if not os.path.exists(self.dataset_path):
            logger.error("No historical dataset found. Run build_historical_dataset.py first.")
            return
            
        df = pd.read_csv(self.dataset_path)
        df['game_date'] = pd.to_datetime(df['game_date'])
        
        # UNBIASED BACKTEST: Only test on data AFTER April 1st, 2025
        cutoff_date = '2025-04-01'
        holdout_df = df[df['game_date'] >= pd.to_datetime(cutoff_date)].copy()
        
        if holdout_df.empty:
            logger.warning(f"No holdout data found after {cutoff_date}. Using tail of dataset instead for demo.")
            holdout_df = df.tail(test_size).copy()
            
        logger.info(f"Running unbiased backtest on {len(holdout_df)} games (all after {cutoff_date})...")
        
        results = []
        
        for idx, row in holdout_df.iterrows():
            # Prepare features (dropping columns XGBoost doesn't like)
            cols_to_drop = ['player_name', 'game_date', 'player_id', 'target_pts', 'actual_pts', 'season']
            feature_row = row.drop(labels=[c for c in cols_to_drop if c in row.index])
            
            # Use the pipeline's predict_over_prob which handles the stats dict mapping
            stats_dict = feature_row.to_dict()
            prob_over = self.pipeline.predict_over_prob(
                stats=stats_dict,
                prop_line=row['prop_line'],
                market='player_points',
                bayesian_prob=row['bayesian_prob']
            )
            
            # 2. Compare against Implied Probability (from "Prop Line" proxy)
            # For this demo, we'll assume a standard -114 odds (53.3% implied)
            implied_prob = 0.533 
            
            edge = prob_over - implied_prob
            actual_result = row['target_pts'] # 1 for Over, 0 for Under
            
            bet_taken = "NONE"
            win = 0
            profit = 0
            
            if edge > edge_threshold:
                bet_taken = "OVER"
                win = 1 if actual_result == 1 else 0
                profit = 87 if win == 1 else -100 # $100 bet at -114 odds
            elif edge < -edge_threshold:
                bet_taken = "UNDER"
                win = 1 if actual_result == 0 else 0
                profit = 87 if win == 1 else -100
                
            results.append({
                'player': row.get('player_name', 'Unknown'),
                'edge': edge,
                'bet': bet_taken,
                'win': win,
                'profit': profit
            })
                
        # Calculate Summary Stats
        results_df = pd.DataFrame(results)
        bets_made = results_df[results_df['bet'] != "NONE"]
        
        if bets_made.empty:
            logger.info("No bets triggered the edge threshold.")
            return
            
        win_rate = bets_made['win'].mean()
        total_profit = bets_made['profit'].sum()
        roi = (total_profit / (len(bets_made) * 100)) * 100
        
        print("\n" + "="*40)
        print("          BACKTEST RESULTS")
        print("="*40)
        print(f"Total Predictions: {len(results_df)}")
        print(f"Total Bets Made:   {len(bets_made)}")
        print(f"Win Rate:          {win_rate:.1%}")
        print(f"Total Profit:      ${total_profit:,.2f}")
        print(f"Return on Invest:  {roi:.2f}%")
        print("="*40 + "\n")
        
        return results_df

if __name__ == "__main__":
    backtester = ModelBacktester()
    backtester.run_backtest(test_size=500, edge_threshold=0.08)
