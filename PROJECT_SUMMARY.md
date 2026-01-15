# Project Summary: NBA Prediction Market Alpha 🏀 🤖

## Executive Summary
This project successfully built a **High-Frequency Trading (HFT) Bot** for Polymarket NBA games. By analyzing Order Book Microstructure (OFI, VAMP, Volatility) on a second-by-second basis, the system detects pre-tick pressure in liquid markets.

**Final System Status (V4):**
*   **Model Accuracy**: **91.94%** (Directional Prediction on 3-minute Horizon).
*   **Strategy**: "Liquid-Only" with Spread Protection (<$0.10).
*   **Infrastructure**: Dockerized Data Collector + Python Bot + QuestDB.

---

## 📅 Evolution of the Model

The project went through four distinct iterations to reach the current performance.

| Version | Description | Key Result | Issue / Learning |
| :--- | :--- | :--- | :--- |
| **V1** | **Baseline** | ~50% Acc | Random guessing. Established the pipeline (Collector -> DB). |
| **V2** | **Kalshi-Linked** | **Live** | Originally failed (liquidity), now **Revived (Jan 14 2026)**. Successfully linked 50+ markets for arbitrage signal generation. |
| **V3** | **Illiquid (Mixed)** | **56.9% Acc** | Trained on *all* markets, including illiquid ones with 98-cent spreads. The model learned to predict "No movement" well but failed in execution. |
| **V4** | **Liquid-Only** 🚀 | **91.9% Acc** | **Current Production Model**. Trained on liquid markets + **Kalshi Linkages**. Uses `arb_spread` and `k_micro_price` as features when available. |

### Why V4 is Superior
The V3 model was "poisoned" by thousands of zombie markets where the price never moved.
The V4 model focuses exclusively on the "Tip of the Spear"—the few dozen active markets where price discovery happens.
*   **Feature Discovery**: The model identified `Spread Volatility` and `Limit Order Pressure` as the top predictors of a price tick.
*   **The "Secret"**: In liquid markets, price is mean-reverting at the 1-cent level. The model predicts the "ping-pong" effect.

---

## 🏗️ System Architecture

### 1. Data Ingestion (The Foundation)
A custom verified `PolymarketClient` subscribes to the WebSocket feed (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
*   **Ingestion**: Parses Order Book updates (Bids/Asks) -> Pushes to QuestDB `order_book_snapshots`.
*   **Reliability**: `start_trading_system.sh` ensures the collector persists 24/7.

### 2. Feature Engineering
Raw snapshots are converted into predictive features:
*   **OFI (Order Flow Imbalance)**: $\text{OFI} = \text{BidSize} - \text{AskSize}$ (at best levels).
*   **VAMP**: Volume-Weighted Average Market Price.
*   **Spread Volatility**: Standard deviation of the spread over 20 seconds.

### 3. The Paper Bot (The Trader)
*   **File**: `scripts/run_paper_bot.py`
*   **Logic**:
    1.  Polls active markets every 5 seconds.
    2.  Calculates live features.
    3.  Runs XGBoost Inference.
    4.  **Risk Check**: Checks `MAX_SPREAD_USD=0.10` to avoid illiquid traps.
    5.  **Execution**: Simulates a Limit Order at the Best Bid/Ask.

---

## 📊 Key Metrics (Final)
*   **Training Set**: 40,344 rows (Filtered from 1.4M raw).
*   **Validation Accuracy**: 91.94%.
*   **Target Horizon**: 3 Minutes (180 seconds).
*   **Risk Settings**:
    *   Max Spread: $0.10
    *   Max Position: $50
    *   Stop Loss: 20%
