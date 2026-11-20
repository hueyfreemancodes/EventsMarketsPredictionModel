import os
import sys
import pandas as pd
import joblib

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.models.team_xgboost_model import GameLineXGBoostModel

def main():
    print("Loading team training set...")
    df = pd.read_csv('data/team_training_set.csv')
    
    # Optional: We could do a time-based split to measure holdout performance
    # For now, train on all data
    model = GameLineXGBoostModel()
    model.fit(df)
    
    # Save the model
    os.makedirs('data', exist_ok=True)
    out_path = 'data/team_xgboost_model.joblib'
    joblib.dump(model, out_path)
    print(f"Model saved to {out_path}")

if __name__ == "__main__":
    main()
