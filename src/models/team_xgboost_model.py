import xgboost as xgb
import numpy as np
import pandas as pd
from scipy.stats import norm
import logging

logger = logging.getLogger("GameLineXGBoost")

class GameLineXGBoostModel:
    """
    XGBoost-based model for predicting NBA game margins and totals.
    Uses Four Factors, Pace, and Rolling 5-game averages.
    """
    
    BASE_SPREAD_STD = 12.5
    BASE_TOTAL_STD = 16.0
    BASE_TOTAL_ANCHOR = 220.0
    
    FEATURES = [
        'home_rolling_5_efg', 'home_rolling_5_tov', 'home_rolling_5_orb', 
        'home_rolling_5_ftr', 'home_rolling_5_ortg', 'home_rolling_5_drtg', 'home_rolling_5_pace',
        'home_season_efg', 'home_season_tov', 'home_season_orb', 'home_season_ftr', 
        'home_season_ortg', 'home_season_drtg', 'home_season_pace',
        'home_rest_days',
        
        'away_rolling_5_efg', 'away_rolling_5_tov', 'away_rolling_5_orb', 
        'away_rolling_5_ftr', 'away_rolling_5_ortg', 'away_rolling_5_drtg', 'away_rolling_5_pace',
        'away_season_efg', 'away_season_tov', 'away_season_orb', 'away_season_ftr', 
        'away_season_ortg', 'away_season_drtg', 'away_season_pace',
        'away_rest_days',
        
        'is_playoff'
    ]

    def __init__(self):
        self.margin_model = None
        self.total_model = None

    def fit(self, df):
        """Train both models using the dataset."""
        logger.info("Training Game Line XGBoost models...")
        
        X = df[self.FEATURES]
        y_margin = df['margin']
        y_total = df['total']
        
        params = {
            'objective': 'reg:squarederror',
            'learning_rate': 0.05,
            'max_depth': 4,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'n_estimators': 200,
            'random_state': 42
        }
        
        self.margin_model = xgb.XGBRegressor(**params)
        self.margin_model.fit(X, y_margin)
        
        self.total_model = xgb.XGBRegressor(**params)
        self.total_model.fit(X, y_total)
        
        logger.info("Models trained successfully.")

    def predict(self, feature_dict):
        """Predict margin and total for a single game."""
        df = pd.DataFrame([feature_dict])
        
        # Ensure column order matches
        X = df[self.FEATURES]
        
        pred_margin = self.margin_model.predict(X)[0]
        pred_total = self.total_model.predict(X)[0]
        
        return {
            'home_margin': float(pred_margin),
            'projected_total': float(pred_total)
        }

    def _dynamic_std(self, projected_total: float, base_std: float) -> float:
        """Scale std deviation dynamically based on pace/projected total."""
        return base_std * (projected_total / self.BASE_TOTAL_ANCHOR) ** 0.5

    def evaluate_spread(self, projected_margin: float, line: float, projected_total: float = None) -> float:
        """
        Calculates probability of covering a point spread.
        Target is -line (e.g. if home is -5, they need to win by > 5, so target is 5)
        """
        spread_std = self._dynamic_std(projected_total or self.BASE_TOTAL_ANCHOR, self.BASE_SPREAD_STD)
        target_margin = -line
        prob_cover = norm.sf(target_margin, loc=projected_margin, scale=spread_std)
        return prob_cover

    def evaluate_moneyline(self, projected_margin: float, projected_total: float = None) -> float:
        """Probability of winning straight up (Margin > 0)."""
        spread_std = self._dynamic_std(projected_total or self.BASE_TOTAL_ANCHOR, self.BASE_SPREAD_STD)
        return norm.sf(0, loc=projected_margin, scale=spread_std)

    def evaluate_total(self, projected_total: float, line: float) -> tuple[float, float]:
        """Calculates prob of Over and Under."""
        total_std = self._dynamic_std(projected_total, self.BASE_TOTAL_STD)
        prob_over = norm.sf(line, loc=projected_total, scale=total_std)
        prob_under = norm.cdf(line, loc=projected_total, scale=total_std)
        return prob_over, prob_under

    def regress_to_market(self, model_value: float, market_value: float, regression_weight: float = 0.50) -> float:
        """
        Blend model with Vegas.
        Since this is an XGBoost model, we can trust it slightly more than the parametric model,
        but Vegas is still the gold standard. We use a 50% blend by default.
        """
        return model_value * (1 - regression_weight) + market_value * regression_weight
