import sys
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.models.player_bayesian_model import PlayerBayesianModel

def main():
    dataset_path = 'data/historical_training_set.csv'
    if not os.path.exists(dataset_path):
        print(f"Error: {dataset_path} not found.")
        return

    print(f"Loading dataset from {dataset_path}...")
    df = pd.read_csv(dataset_path)
    print(f"Loaded {len(df)} rows.")

    model = PlayerBayesianModel()
    
    # We'll recalculate for 'player_points' as that's the primary target in the CSV
    # (The CSV doesn't specify the market per row, but target_pts implies it's points)
    metric = 'player_points'
    
    print("Recalculating Bayesian probabilities...")
    new_probs = []
    
    for _, row in tqdm(df.iterrows(), total=len(df)):
        # Construct stats dict from row
        stats = {
            'season_avg_pts': row.get('season_avg_pts', 0),
            'recent_avg_pts': row.get('recent_avg_pts', 0),
            'season_avg_reb': row.get('season_avg_reb', 0),
            'recent_avg_reb': row.get('recent_avg_reb', 0),
            'season_avg_ast': row.get('season_avg_ast', 0),
            'recent_avg_ast': row.get('recent_avg_ast', 0),
            'season_avg_fg3m': row.get('season_avg_fg3m', 0),
            'recent_avg_fg3m': row.get('recent_avg_fg3m', 0),
            'std_pts': row.get('std_pts', 0),
            'true_usage': row.get('true_usage', 0.20),
            'games_played': 50, # Approximation for mid-season
            'recent_avg_min': row.get('recent_avg_min', 30),
            'season_avg_min': row.get('season_avg_min', 30)
        }
        
        line = row['prop_line']
        
        # We pass opponent_stats=None to get a neutral matchup_adj (1.0)
        # We pass team_context=None to get a neutral context_adj (1.0)
        # This removes the systematic 15% reduction bug that poisoned the data
        prediction = model.predict_prop(
            stats=stats,
            line=line,
            metric=metric,
            opponent_stats=None,
            team_context=None
        )
        
        new_probs.append(prediction.prob_over)

    df['bayesian_prob'] = new_probs
    
    print(f"Saving fixed dataset to {dataset_path}...")
    df.to_csv(dataset_path, index=False)
    print("Success!")

    # Verify a few samples
    print("\nVerification (First 10 rows):")
    print(df[['season_avg_pts', 'prop_line', 'bayesian_prob']].head(10))

if __name__ == "__main__":
    main()
