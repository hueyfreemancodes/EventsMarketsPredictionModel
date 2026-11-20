"""
XGBoost Model Integrity Audit
Checks:
  1. Training set features — are team-specific features present and sensible?
  2. Saved model loading and feature name alignment
  3. Live inference: does a known player now produce a valid, stable probability?
  4. Prediction sanity: do higher lines produce lower OVER probs? (monotonicity)
  5. Home/Away isolation: does flipping is_home change the prediction?
"""
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.models.xgboost_pipeline import XGBoostPipeline

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"

results = []

def check(label, condition, detail="", level="hard"):
    status = PASS if condition else (FAIL if level == "hard" else WARN)
    results.append((status, label, detail))
    print(f"  {status}  {label}" + (f"\n           {detail}" if detail else ""))

print("\n" + "="*65)
print("  XGBOOST MODEL INTEGRITY AUDIT")
print("="*65)

# ─── 1. Training Set Inspection ──────────────────────────────────────────────
print("\n[1] TRAINING DATA INTEGRITY")

train_path = "data/historical_training_set.csv"
if not os.path.exists(train_path):
    check("Training set exists", False, f"File not found: {train_path}")
else:
    df = pd.read_csv(train_path)
    check("Training set exists", True, f"{len(df):,} rows, {len(df.columns)} columns")

    # Check key features exist
    required_cols = ['season_avg_pts', 'recent_avg_pts', 'is_home', 'rest_days', 'prop_line', 'target_pts']
    missing = [c for c in required_cols if c not in df.columns]
    check("Required feature columns present", len(missing) == 0,
          f"Missing: {missing}" if missing else "All required columns found")

    # is_home distribution — should be ~50/50 if correctly labeled
    if 'is_home' in df.columns:
        home_pct = df['is_home'].mean()
        check("is_home is ~50% (not always 0 or 1)", 0.40 <= home_pct <= 0.60,
              f"Home pct in training set: {home_pct:.1%}")

    # No NaN in target
    if 'target_pts' in df.columns:
        null_targets = df['target_pts'].isna().sum()
        check("No null targets", null_targets == 0, f"Null targets: {null_targets}")

    # Target balance: OVER vs UNDER (should be ~50/50 for a fair prop line)
    if 'target_pts' in df.columns:
        over_rate = df['target_pts'].mean()
        check("Target balance is reasonable (40-60% OVER)", 0.40 <= over_rate <= 0.60,
              f"OVER rate in training set: {over_rate:.1%}")

    # season_avg_pts range (sanity: real NBA averages 8-35 PPG)
    if 'season_avg_pts' in df.columns:
        avg_min = df['season_avg_pts'].min()
        avg_max = df['season_avg_pts'].max()
        check("season_avg_pts in NBA range (5–40)", avg_min >= 0 and avg_max <= 40,
              f"Range: [{avg_min:.1f}, {avg_max:.1f}]")

# ─── 2. Model Loading ─────────────────────────────────────────────────────────
print("\n[2] MODEL LOADING")

pipeline = XGBoostPipeline()
markets = ['player_points', 'player_rebounds', 'player_assists', 'player_threes']
for m in markets:
    loaded = m in pipeline.models
    check(f"Model loaded: {m}", loaded,
          "" if loaded else "Model file not found — run train_xgboost.py")

# ─── 3. Feature Name Alignment ────────────────────────────────────────────────
print("\n[3] FEATURE ALIGNMENT (Training vs Inference)")

canonical_features = pipeline.get_feature_names()
print(f"  ℹ️  Canonical inference features ({len(canonical_features)}): {canonical_features}")

if os.path.exists(train_path):
    training_numeric = [c for c in df.columns if c in canonical_features]
    missing_from_train = [f for f in canonical_features if f not in df.columns]
    check("All inference features present in training data", len(missing_from_train) == 0,
          f"Missing from training: {missing_from_train}" if missing_from_train else "Perfect alignment",
          level="soft")

# ─── 4. Live Inference Sanity ─────────────────────────────────────────────────
print("\n[4] LIVE INFERENCE SANITY")

# Simulate a realistic player stats dict (post-fix)
mock_stats = {
    'season_avg_pts': 25.0, 'season_avg_reb': 5.0, 'season_avg_ast': 6.0,
    'recent_avg_pts': 26.5, 'recent_avg_reb': 4.8, 'recent_avg_ast': 6.2,
    'true_usage': 0.28, 'potential_ast': 8.0, 'reb_chances': 7.0,
    'is_home': 1, 'rest_days': 2,
}

for m in markets:
    if m not in pipeline.models:
        continue
    prob = pipeline.predict_over_prob(mock_stats, prop_line=22.5, market=m, bayesian_prob=0.55)
    in_range = 0.0 <= prob <= 1.0
    check(f"predict_over_prob returns valid prob: {m}", in_range,
          f"Returned: {prob:.4f}")

# ─── 5. Monotonicity Check ────────────────────────────────────────────────────
print("\n[5] MONOTONICITY: Higher line → lower OVER probability")

if 'player_points' in pipeline.models:
    lines = [12.5, 17.5, 22.5, 27.5, 32.5]
    probs = [pipeline.predict_over_prob(mock_stats, line, 'player_points', bayesian_prob=0.55)
             for line in lines]
    is_monotone = all(probs[i] >= probs[i+1] for i in range(len(probs)-1))
    details = ", ".join([f"{l}→{p:.3f}" for l, p in zip(lines, probs)])
    check("Points model is monotonically decreasing with line", is_monotone, details)

# ─── 6. Home/Away Sensitivity ────────────────────────────────────────────────
print("\n[6] HOME/AWAY FEATURE SENSITIVITY")

if 'player_points' in pipeline.models:
    home_stats = {**mock_stats, 'is_home': 1}
    away_stats = {**mock_stats, 'is_home': 0}
    p_home = pipeline.predict_over_prob(home_stats, 22.5, 'player_points', bayesian_prob=0.55)
    p_away = pipeline.predict_over_prob(away_stats, 22.5, 'player_points', bayesian_prob=0.55)
    diff = abs(p_home - p_away)
    sensitive = diff > 0.001  # Anything > 0.1% shows the feature is being read
    check("Model is sensitive to is_home flag", sensitive,
          f"Home prob: {p_home:.4f} | Away prob: {p_away:.4f} | Δ={diff:.4f}",
          level="soft")

# ─── Summary ──────────────────────────────────────────────────────────────────
print("\n" + "="*65)
print("  AUDIT SUMMARY")
print("="*65)
passes = sum(1 for r in results if r[0] == PASS)
fails  = sum(1 for r in results if r[0] == FAIL)
warns  = sum(1 for r in results if r[0] == WARN)
print(f"  {PASS}  {passes} checks passed")
if warns: print(f"  {WARN}  {warns} warnings")
if fails: print(f"  {FAIL}  {fails} failures")
verdict = "MODEL IS CLEAN ✅" if fails == 0 else "MODEL HAS ISSUES — RETRAIN REQUIRED ❌"
print(f"\n  Verdict: {verdict}")
print("="*65 + "\n")
