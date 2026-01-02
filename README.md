# NBA Prediction Market Alpha 🏀 📈

A high-frequency trading system that generates alpha by analyzing order book microstructure on Polymarket **and Kalshi**, correlating it with fundamental NBA game data.

## Overview

This project captures real-time data from Polymarket and Kalshi NBA game markets, links it with historical and live NBA statistics, and trains machine learning models to predict short-term price movements (60-second horizon). By integrating cross-exchange liquidity features, the system achieves robust predictive performance.

**Key Performance Metrics:**
*   **Model**: XGBoost Regressor (Best Performer)
*   **Directional Accuracy**: **~58.10%** (on 161k row dataset)
*   **Latency**: End-to-end processing in <50ms.

## Architecture

*   **Data Sources**: Polymarket (CLOB API), Kalshi (Trade API v2), NBA API.
*   **Storage**: QuestDB (High-performance Time-Series Database).
*   **Infrastructure**: Dockerized Collection Agent with Watchdog reliability suite.
*   **Modeling**: Python (Pandas, XGBoost, LightGBM) for feature engineering and training.

## Features

### 1. Fast Alpha (Microstructure)
Derived from the Limit Order Book (LOB) every second:
*   **OFI (Order Flow Imbalance)**: The net pressure of buy vs. sell orders at the Best Bid/Offer.
*   **Cross-Exchange Arbitrage**: Real-time spread tracking between Polymarket and Kalshi (e.g., `arb_spread`, `feed_latency`).
*   **VAMP (Volume-Weighted Average Mid-Price)**: A robust price indicator sensitive to liquidity depth.
*   **Decayed Features**: Exponential Moving Averages (EMA 0.1, 0.3, 0.5) to capture momentum.

### 2. Slow Alpha (Fundamentals)
Derived from NBA Game Data:
*   **Team Strength**: Weighted Win %, Last 5 Games.
*   **Fatigue**: Days of Rest, Back-to-Back status, Travel Distance.
*   **Matchup**: Historical Home/Away splits.

## Getting Started

### Prerequisites
*   Docker & Docker Compose
*   Python 3.9+
*   QuestDB (running on port 8812)

### Setup

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/your-repo/nba-market-alpha.git
    cd nba-market-alpha
    ```

2.  **Environment Configuration**
    Create a `.env` file or export variables:
    ```bash
    export QUESTDB_HOST=localhost
    export QUESTDB_PORT=8812
    ```
    For Kalshi access, ensure `config/kalshi.pem` and API keys are present (if using private data).

3.  **Start the Reliability Suite**
    The watchdog ensures continuous data collection from all exchanges:
    ```bash
    python3 scripts/collector_watchdog.py
    ```

### Usage

**1. Generate Training Data**
Once you have collected data (recommended: >24 hours), run the pipeline to merge features:
```bash
# 1. Update Microstructure Features (Polymarket + Kalshi)
python3 scripts/update_features.py

# 2. Merge with NBA Stats & Cross-Exchange Linkages
python3 scripts/create_training_set.py
```

**2. Train Models**
Train and evaluate the LightGBM models:
```bash
python3 scripts/train_models.py
```
*   Outputs accuracy metrics and feature importance plots.
*   Current Best: **XGBoost** (58.10%).

**3. Live Inference**
Generate real-time trading signals on active markets:
```bash
python3 scripts/live_inference.py
```
*   Loads the trained `xgb_model.json`.
*   Fetches live order book data from QuestDB.
*   Displays the predicted price change and a simple BUY/HOLD/SELL signal.

## Project Structure
*   `src/data_collection`: Clients for Polymarket (`polymarket_client.py`) and Kalshi (`kalshi_client.py`).
*   `src/feature_engineering`: Logic for computing OFI, VAMP, and Alphas.
*   `scripts/`: 
    *   `collector_watchdog.py`: Auto-restart and monitoring for collectors.
    *   `update_features.py`: Batch calculation of microstructure features.
    *   `create_training_set.py`: Joins massive time-series datasets.
    *   `train_models.py`: ML pipeline.
    *   `live_inference.py`: Real-time XGBoost inference with metadata lookup.

## Disclaimer
This software is for educational and research purposes only. Prediction markets involve real financial risk.
