import numpy as np
import pandas as pd
try:
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
    from sklearn.calibration import CalibratedClassifierCV
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    
from typing import Dict, Any, Tuple
import os
import joblib
import logging

logger = logging.getLogger("XGBoostPipeline")

class XGBoostPipeline:
    """
    XGBoost Machine Learning Pipeline for NBA Player Props.
    Uses gradient boosting to find non-linear relationships between
    tracking metrics, true usage, rest days, and prop outcomes.
    """
    def __init__(self, model_dir="models/"):
        self.model_dir = model_dir
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
            
        self.models = {} # dict mapping market -> trained model
        self.enabled = XGB_AVAILABLE
        if self.enabled:
            self.load_models()
        else:
            logger.warning("xgboost or scikit-learn is not installed. XGBoostPipeline will remain inactive.")

    def load_models(self):
        """Load all available models from the models directory"""
        markets = ['player_points', 'player_rebounds', 'player_assists', 'player_threes']
        for m in markets:
            path = os.path.join(self.model_dir, f"{m}_xgb.joblib")
            if os.path.exists(path):
                try:
                    self.models[m] = joblib.load(path)
                    logger.debug(f"Loaded {m} model from {path}")
                except Exception as e:
                    logger.error(f"Failed to load {m} model: {e}")
            else:
                logger.debug(f"No model found for {m} at {path}")
        
    def get_feature_names(self) -> list:
        """Returns the canonical list of features used by this model"""
        return [
            'prop_line', 'season_avg_pts', 'season_avg_reb', 'season_avg_ast',
            'recent_avg_pts', 'recent_avg_reb', 'recent_avg_ast', 'true_usage',
            'potential_ast', 'reb_chances', 'bayesian_prob', 'is_home', 'rest_days'
        ]

    def preprocess_features(self, stats: Dict[str, Any], prop_line: float, bayesian_prob: float = 0.5) -> pd.DataFrame:
        """
        Extract numeric features from the stats dict and append the prop line.
        Handles missing values (NaNs) inherently expected in tracking data.
        """
        feature_names = self.get_feature_names()
        features = {}
        
        # Mapping stats dict to our canonical feature names
        features['prop_line'] = prop_line
        features['bayesian_prob'] = bayesian_prob
        features['is_home'] = stats.get('is_home', 1)
        features['rest_days'] = stats.get('rest_days', 1)
        features['season_avg_pts'] = stats.get('season_avg_pts', stats.get('season_avg_pts', 0))
        features['season_avg_reb'] = stats.get('season_avg_reb', stats.get('season_avg_reb', 0))
        features['season_avg_ast'] = stats.get('season_avg_ast', stats.get('season_avg_ast', 0))
        features['recent_avg_pts'] = stats.get('recent_avg_pts', stats.get('recent_avg_pts', 0))
        features['recent_avg_reb'] = stats.get('recent_avg_reb', stats.get('recent_avg_reb', 0))
        features['recent_avg_ast'] = stats.get('recent_avg_ast', stats.get('recent_avg_ast', 0))
        features['true_usage'] = stats.get('true_usage', 0.20)
        features['potential_ast'] = stats.get('potential_ast', 0)
        features['reb_chances'] = stats.get('reb_chances', 0)
        
        # Create DF and ensure column order
        df = pd.DataFrame([features])
        df = df.reindex(columns=feature_names).fillna(0)
        return df[feature_names] # Force order

    def train(self, df: pd.DataFrame, target_col: str, market: str):
        """
        Train an XGBoost classifier to predict OVER probability (1 = OVER, 0 = UNDER).
        Wraps the base classifier in CalibratedClassifierCV with isotonic regression
        to ensure predict_proba outputs true, well-calibrated probabilities.
        """
        if not self.enabled: return
        
        feature_names = self.get_feature_names()
        # Filter only available features that match our canonical list
        X = df[[c for c in feature_names if c in df.columns]]
        y = df[target_col]
        
        # Fill missing features with 0
        for col in feature_names:
            if col not in X.columns:
                X[col] = 0
        
        # Ensure column order matches canonical list
        X = X[feature_names]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Base XGBoost model
        base_model = xgb.XGBClassifier(
            n_estimators=100,
            learning_rate=0.05,
            max_depth=4,
            eval_metric='logloss',
            use_label_encoder=False
        )
        
        # Wrap in calibration to produce true probabilities
        # Isotonic regression is non-parametric and handles XGBoost's tendency
        # to push predictions toward extremes or compress toward 0.5
        model = CalibratedClassifierCV(base_model, method='isotonic', cv=3)
        
        logger.info(f"Training calibrated XGBoost model for {market}...")
        model.fit(X_train, y_train)
        self.models[market] = model
        
        joblib.dump(model, os.path.join(self.model_dir, f"{market}_xgb.joblib"))
        logger.info(f"Saved calibrated {market} model to {self.model_dir}")
        
    def load_model(self, market: str) -> bool:
        """
        Loads a pre-trained model for a specific market if it exists.
        """
        if not self.enabled: return False
        path = os.path.join(self.model_dir, f"xgb_{market}.joblib")
        if os.path.exists(path):
            self.models[market] = joblib.load(path)
            return True
        return False
        
    def predict_over_prob(self, stats: Dict[str, Any], prop_line: float, market: str, bayesian_prob: float = 0.5) -> float:
        """
        Predict the ML probability of going OVER the line.
        Returns -1.0 if the model is not trained/available.
        """
        if not self.enabled: return -1.0
        
        if market not in self.models:
            if not self.load_model(market):
                return -1.0 # Model not available
                
        X = self.preprocess_features(stats, prop_line, bayesian_prob=bayesian_prob)
        probs = self.models[market].predict_proba(X)
        
        # predict_proba returns [[P(Under), P(Over)]]
        return float(probs[0][1])

def ensemble_predict(bayesian_prob: float, ml_prob: float, ml_weight: float = 0.65) -> float:
    """
    Blends the mathematical Bayesian probability with the Machine Learning probability.
    XGBoost (isotonically calibrated) dominates at 65% weight because the raw Bayesian
    model is overconfident at the extremes (87% predicted → 54% actual).
    Bayesian acts as a soft prior at 35%.
    """
    if ml_prob < 0:
        return bayesian_prob # Fallback to Bayesian if ML is unavailable
        
    return (bayesian_prob * (1 - ml_weight)) + (ml_prob * ml_weight)
