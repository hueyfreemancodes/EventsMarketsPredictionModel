import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score
import numpy as np

# Load Data
df = pd.read_csv('final_training_set_v2.csv')
print(f"Dataset Size: {len(df)}")

# Load Model
model = xgb.XGBRegressor()
model.load_model('xgb_model.json')

# Prepare Features (Same logic as training)
NON_FEATURE_COLS = {'timestamp', 'market_id', 'outcome', 'target_return_60s', 'target_return_180s', 'target_return_300s', 'event_id', 'sport', 'league', 'game_date', 'platform'}
numeric_df = df.select_dtypes(include=['number'])
feature_cols = [c for c in numeric_df.columns if c not in NON_FEATURE_COLS and 'target' not in c]
X = df[feature_cols]
y = df['target_return_180s'] # Using the 3m target

# Predict
preds = model.predict(X)

# Directional Accuracy
pred_sign = (preds > 0).astype(int)
true_sign = (y > 0).astype(int)
meaningful = y != 0
acc = (pred_sign[meaningful] == true_sign[meaningful]).mean()

print(f"Model V4 Directional Accuracy (180s): {acc:.2%}")
