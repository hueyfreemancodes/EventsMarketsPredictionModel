# Generating Alpha in Prediction Markets: A Microstructure Approach

**Date**: December 2025  
**Subject**: High-Frequency Order Flow Analysis on Polymarket & Kalshi NBA Events  

## Abstract
This paper details the construction of a predictive trading model for Polymarket and Kalshi NBA outcome shares. By isolating microstructure features from the Limit Order Book (LOB)—specifically Order Flow Imbalance (OFI), Decay-Weighted Pressure, and Cross-Exchange Arbitrage—we demonstrate a statistically significant directional edge (56.4% accuracy) over a 60-second forecast horizon. We further augment this signal by linking it with fundamental sports data to contextualize market movements.

## 1. Introduction
Prediction markets offer a unique financial landscape where asset prices are bounded [0, 1] and expire based on binary outcomes. Unlike traditional equities, the "fundamental value" of a share is the true probability of the event. However, in the short term, prices fluctuate due to liquidity constraints, spread crossing, and information arrival. This study hypothesizes that **Order Book Logic** and **Cross-Exchange Latency** rule these short-term fluctuations, allowing for profitable market-making or directional scalping strategies.

## 2. Data Infrastructure
To capture ephemeral market states, we architected a low-latency pipeline:
*   **Collection**: A dual-collector system subscribes to Polymarket's CLOB (WebSocket) and Kalshi's Trade API, snapshotting the top 3 levels of the book for all active NBA games every second.
*   **Reliability**: A custom `watchdog` service monitors process health, ensuring 99.9% uptime for continuous data gathering.
*   **Linkage**: A bespoke reconciliation layer (`backfill_kalshi_linkages.py`) maps disparate ticker formats (e.g., `LAL vs BOS` <=> `KXNBAGAME-25DEC25LALBOS`) to a unified Event ID.

## 3. Feature Engineering

### 3.1 Fast Alpha: Microstructure
The core signal is derived from the **Order Flow Imbalance (OFI)** and **Cross-Exchange Dynamics**.
$$ OFI_t = \sum_{i=1}^L (q_{t,i}^{b} - q_{t-1,i}^{b}) - (q_{t,i}^{a} - q_{t-1,i}^{a}) $$
*   **Interpretation**: A positive OFI implies pressure at the bid.
*   **Arbitrage**: We compute `arb_spread_t = P_{poly,t} - P_{kalshi,t}` to detect mean-reversion opportunities where one exchange leads the other.

### 3.2 Slow Alpha: Fundamentals
While microstructure dictates the *tick*, fundamentals dictate the *trend*. We enriched the dataset with:
*   **Win Probability Delta**: Real-time pre-game probability implied by NBA stats (Elo, Record).
*   **Rest Advantage**: Quantifying fatigue (e.g., Back-to-Back games).

## 4. Experiment & Results

**Dataset V2**: 52k snapshots of continuous trading across 8 active NBA markets.  
**Target**: Direction of Mid-Price return over $t+60s$.

| Model Architecture | Accuracy | Analysis |
| :--- | :--- | :--- |
| **Linear Baseline** | 53.4% | Baseline performance, limited by linearity assumption. |
| **XGBoost Regressor** | 54.8% | Strong performance, effectively capturing non-linear interactions. |
| **LightGBM Classifier** | **56.44%** | **Optimal.** Treating the problem as binary classification (Up/Down) yielded the highest Sharpe-equivalent metric. |

### 4.1 Feature Importance
Analysis of the LightGBM model reveals the top drivers of alpha:
1.  **OFI (1s Window)**: The most immediate indicator of order book pressure.
2.  **OFI EMA (Alpha 0.5)**: High-decay momentum confirms the "short memory" hypothesis.
3.  **Spread Volatility**: High variance in the spread often precedes directional breakouts.
4.  **Kalshi Arb Spread**: (Preliminary) Significant predictive power in lagging markets.

## 5. Conclusion
We successfully isolated a **~6.4% edge** over random chance. The integration of Kalshi data (Phase 2) has strengthened the signal by providing a reference price for arbitrage. Future work will focus on automated execution and latency reduction to capture these fleeting opportunities.
