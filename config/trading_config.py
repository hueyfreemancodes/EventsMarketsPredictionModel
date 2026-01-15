
"""
Paper Trading Configuration
Centralized control for risk management and execution parameters.
"""

# Account Settings
INITIAL_CAPITAL = 2500.0  # USD

# Risk Management
MAX_TRADE_SIZE_USD = 5.0  # Strict limit per trade
MAX_POSITION_USD = 50.0   # Max exposure per single market
MAX_ACCOUNT_DRAWDOWN = 0.10 # Stop bot if account drops 10% ($250)
MAX_SPREAD_USD = 0.10       # Max allowed spread width (10 cents)

# Execution Rules
DEFAULT_ORDER_TYPE = 'LIMIT'
SLIPPAGE_TOLERANCE_PCT = 0.01

# Strategy Parameters
SIGNAL_THRESHOLD = 0.05   # Abs(Predicted Return) must exceed 5% (Higher confidence)
TIME_LIMIT_SECONDS = 180  # Max hold time (3 minutes)
TAKE_PROFIT_ROI = 0.25    # +25% ROI
STOP_LOSS_ROI = -0.12     # -12% ROI

# System
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 8812
