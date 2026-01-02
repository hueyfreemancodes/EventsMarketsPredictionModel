import os
import logging
from pathlib import Path
from typing import Tuple, Dict, Any

import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Setup simple logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
TRAINING_SET_PATH = Path('final_training_set_v2.csv')
NON_FEATURE_COLS = {
    'timestamp', 'market_id', 'outcome', 'target_return_60s', 
    'event_id', 'sport', 'league', 'game_date', 'platform'
}

def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}. Run generation script first.")

    dataset = pd.read_csv(path)
    dataset['timestamp'] = pd.to_datetime(dataset['timestamp'])
    
    # Sort by time to ensure strict temporal split later
    dataset = dataset.sort_values('timestamp')
    
    # Trees handle categories well, but explicit int casting is safer for boolean flags
    bool_cols = dataset.select_dtypes(include=['bool']).columns
    dataset[bool_cols] = dataset[bool_cols].astype(int)
    
    return dataset

def split_features_target(dataset: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """Separates feature matrix from target variable."""
    # Robustly select only numeric columns for features
    numeric_df = dataset.select_dtypes(include=['number'])
    feature_cols = [c for c in numeric_df.columns if c not in NON_FEATURE_COLS]
    
    # Log the features being used (for debugging)
    # logger.info(f"Using features: {feature_cols}")
    
    return dataset[feature_cols], dataset['target_return_60s']

def train_linear_baseline(X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame) -> np.ndarray:
    # Linear models choke on NaNs, tree models handle them internally
    # impute with 0.0 assuming missing feature = 0 signal
    model = LinearRegression()
    model.fit(X_train.fillna(0), y_train)
    return model.predict(X_test.fillna(0))

def train_xgboost(X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series) -> Tuple[np.ndarray, Any]:
    # TODO: Tune hyperparameters via Optuna if V2 performance plateaus
    model = xgb.XGBRegressor(
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        n_jobs=-1,
        random_state=42
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )
    return model.predict(X_test), model

def train_lgbm(X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series) -> np.ndarray:
    model = lgb.LGBMRegressor(
        n_estimators=1000,
        learning_rate=0.05,
        num_leaves=31,
        n_jobs=-1,
        random_state=42,
        verbosity=-1
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        eval_metric='l2'
    )
    return model.predict(X_test)

def train_direction_classifier(X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series) -> np.ndarray:
    """Trains a classifier to predict strictly Up/Down (binary)"""
    # Convert continuous return to binary class (1: Up, 0: Down/Flat)
    y_train_binary = (y_train > 0).astype(int)
    y_test_binary = (y_test > 0).astype(int)
    
    model = lgb.LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.05,
        n_jobs=-1,
        random_state=42,
        verbosity=-1
    )
    
    model.fit(
        X_train, y_train_binary,
        eval_set=[(X_test, y_test_binary)],
        eval_metric='binary_logloss'
    )
    
    # Return probability of 'Up' class
    return model.predict_proba(X_test)[:, 1]

def calculate_metrics(y_true: pd.Series, predictions: np.ndarray, is_prob: bool = False) -> Dict[str, float]:
    """Calculates regression and classification stats."""
    metrics = {}
    
    if not is_prob:
        metrics['MAE'] = mean_absolute_error(y_true, predictions)
        metrics['RMSE'] = np.sqrt(mean_squared_error(y_true, predictions))
        
        # Directional Accuracy for regressor (Did we get the sign right?)
        pred_sign = (predictions > 0).astype(int)
        true_sign = (y_true > 0).astype(int)
        
        # Filter out 0 moves to avoid noise
        meaningful_moves = y_true != 0
        if meaningful_moves.sum() > 0:
            metrics['Dir_Acc'] = (pred_sign[meaningful_moves] == true_sign[meaningful_moves]).mean()
        else:
            metrics['Dir_Acc'] = 0.5
    else:
        # Classifier metrics
        # predictions are probabilities
        pred_class = (predictions > 0.5).astype(int)
        true_class = (y_true > 0).astype(int)
        
        meaningful_moves = y_true != 0
        if meaningful_moves.sum() > 0:
            metrics['Dir_Acc'] = (pred_class[meaningful_moves] == true_class[meaningful_moves]).mean()
        else:
            metrics['Dir_Acc'] = 0.5
            
        metrics['MAE'] = 0.0 # Placeholder
        
    return metrics

def main():
    logger.info("Starting model training pipeline...")
    
    try:
        dataset = load_dataset(TRAINING_SET_PATH)
    except Exception as e:
        logger.error(e)
        return

    features, targets = split_features_target(dataset)
    
    # Simple Time-Series Split (80/20)
    split_idx = int(len(dataset) * 0.8)
    X_train, X_test = features.iloc[:split_idx], features.iloc[split_idx:]
    y_train, y_test = targets.iloc[:split_idx], targets.iloc[split_idx:]
    
    logger.info(f"Training on {len(X_train)} samples, testing on {len(X_test)}")
    
    results = {}
    
    # Linear Baseline
    logger.info("Fitting Linear Baseline...")
    preds_lr = train_linear_baseline(X_train, y_train, X_test)
    results['Linear'] = calculate_metrics(y_test, preds_lr)
    
    # XGBoost
    logger.info("Fitting XGBoost...")
    preds_xgb, xgb_model = train_xgboost(X_train, y_train, X_test, y_test)
    results['XGBoost'] = calculate_metrics(y_test, preds_xgb)
    
    # Save the model
    xgb_model.save_model('xgb_model.json')
    logger.info("Saved XGBoost model to 'xgb_model.json'")
    
    # Save importance plot
    xgb.plot_importance(xgb_model, max_num_features=15, importance_type='weight', title='Feature Importance (Weight)')
    plt.tight_layout()
    plt.savefig('xgb_importance.png')
    
    # LightGBM Regressor
    logger.info("Fitting LightGBM Regressor...")
    preds_lgb = train_lgbm(X_train, y_train, X_test, y_test)
    results['LGBM_Reg'] = calculate_metrics(y_test, preds_lgb)
    
    # LightGBM Classifier
    logger.info("Fitting LightGBM Classifier...")
    probs_lgb = train_direction_classifier(X_train, y_train, X_test, y_test)
    results['LGBM_Class'] = calculate_metrics(y_test, probs_lgb, is_prob=True)
    
    # Report
    print("\n" + "="*45)
    print(f"{'Model':<15} | {'MAE':<10} | {'Dir Acc':<10}")
    print("-" * 45)
    for model, m in results.items():
        print(f"{model:<15} | {m['MAE']:.6f}   | {m['Dir_Acc']:.2%}")
    print("="*45 + "\n")

if __name__ == "__main__":
    main()
