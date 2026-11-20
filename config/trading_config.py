
"""
Paper Trading Configuration
Centralized control for risk management and execution parameters.
"""

# Account Settings
INITIAL_CAPITAL = 10000.0  # USD

# Risk Management
MAX_TRADE_SIZE_USD = 5.0  # Strict limit per trade
MAX_POSITION_USD = 50.0   # Max exposure per single market
MAX_ACCOUNT_DRAWDOWN = 0.10 # Stop bot if account drops 10% ($250)
MAX_SPREAD_USD = 0.05       # Max allowed spread width (5 cents - Stricter)

# Execution Rules
DEFAULT_ORDER_TYPE = 'LIMIT'
SLIPPAGE_TOLERANCE_PCT = 0.01

# Strategy Parameters
SIGNAL_THRESHOLD = 0.06   # Abs(Predicted Return) must exceed 6% (Higher confidence)
TIME_LIMIT_SECONDS = 900  # Max hold time (15 minutes)
TAKE_PROFIT_ROI = 0.25    # +25% ROI
STOP_LOSS_ROI = -0.12     # -12% ROI

import os

# System
LOG_LEVEL = "INFO"
DB_HOST = os.getenv("QUESTDB_HOST", "localhost")
DB_PORT = int(os.getenv("QUESTDB_PORT", 8812))
