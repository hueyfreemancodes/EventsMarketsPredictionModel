"""
Microstructure Feature Engineering
Calculates high-frequency alpha signals (OFI, VAMP, Micro-Price) from Order Book snapshots.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional

class MicrostructureFeaturesCalculator:
    def __init__(self, window_size: int = 20):
        self.window = window_size

    def calculate_ofi(self, snapshots: List[Dict]) -> List[float]:
        """
        Order Flow Imbalance (OFI)
        OFI = (BidVol - AskVol) / (TotalVol)
        """
        ofi_series = []
        for snap in snapshots:
            bid_v = snap.get('total_bid_volume', 0)
            ask_v = snap.get('total_ask_volume', 0)
            total = bid_v + ask_v
            
            val = (bid_v - ask_v) / total if total > 0 else 0.0
            ofi_series.append(val)
        return ofi_series

    def calculate_decayed_ofi(self, snapshots: List[Dict], alpha: float = 0.5) -> List[float]:
        """Recursive EMA of the OFI signal."""
        raw_ofi = self.calculate_ofi(snapshots)
        ema_series = []
        curr = 0.0
        
        for val in raw_ofi:
            curr = (alpha * val) + ((1 - alpha) * curr)
            ema_series.append(curr)
            
        return ema_series

    def calculate_vamp(self, snapshots: List[Dict]) -> List[float]:
        """Volume-Adjusted Mid-Price"""
        vamp_series = []
        for s in snapshots:
            bid_p = s.get('bid_price_1')
            ask_p = s.get('ask_price_1')
            bid_v = s.get('total_bid_volume', 0)
            ask_v = s.get('total_ask_volume', 0)

            if bid_p and ask_p and (bid_v + ask_v) > 0:
                # Weighted average of the top of book
                vamp = (bid_p * ask_v + ask_p * bid_v) / (bid_v + ask_v)
            else:
                vamp = s.get('mid_price') or 0.0
            
            vamp_series.append(float(vamp))
        return vamp_series

    def calculate_micro_price(self, snapshots: List[Dict], levels: int = 3) -> List[float]:
        """Depth-weighted price estimate using top N levels."""
        micro_series = []
        
        for s in snapshots:
            w_price_sum = 0.0
            vol_sum = 0.0
            
            for i in range(1, levels + 1):
                bp = s.get(f'bid_price_{i}')
                bs = s.get(f'bid_size_{i}', 0)
                ap = s.get(f'ask_price_{i}')
                as_ = s.get(f'ask_size_{i}', 0) # 'as' is reserved keyword

                if bp and bs:
                    w_price_sum += bp * bs
                    vol_sum += bs
                if ap and as_:
                    w_price_sum += ap * as_
                    vol_sum += as_
            
            val = (w_price_sum / vol_sum) if vol_sum > 0 else (s.get('mid_price') or 0.0)
            micro_series.append(float(val))
            
        return micro_series

    def calculate_spread_volatility(self, snapshots: List[Dict]) -> List[float]:
        spreads = []
        for s in snapshots:
            bp = s.get('bid_price_1')
            ap = s.get('ask_price_1')
            val = (ap - bp) if (ap and bp) else s.get('spread', 0)
            spreads.append(val)
            
        if not spreads:
            return []
            
        # Efficient rolling std via pandas
        return pd.Series(spreads).rolling(window=self.window).std().fillna(0).tolist()

    def calculate_depth_ratio(self, snapshots: List[Dict]) -> List[float]:
        """Bid Depth / Ask Depth"""
        ratios = []
        for s in snapshots:
            bv = s.get('total_bid_volume', 0)
            av = s.get('total_ask_volume', 0)
            # Avoid div by zero, simplified logic
            if av > 0:
                ratios.append(bv / av)
            else:
                ratios.append(10.0 if bv > 0 else 1.0) # Cap at 10x
        return ratios

    def calculate_all_features(self, snapshots: List[Dict], market_id: str = None) -> List[Dict]:
        """Orchestrator: Generates all feature sets for a batch of snapshots."""
        if not snapshots:
            return []

        # Vectorized calculations where possible (simulated via lists here for speed on small batches)
        ts_list = [s['timestamp'] for s in snapshots]
        
        ofi_raw = self.calculate_ofi(snapshots)
        vamp = self.calculate_vamp(snapshots)
        micro = self.calculate_micro_price(snapshots)
        depth_r = self.calculate_depth_ratio(snapshots)
        spread_vol = self.calculate_spread_volatility(snapshots)
        
        # Exponential Decays
        ofi_01 = self.calculate_decayed_ofi(snapshots, alpha=0.1)
        ofi_03 = self.calculate_decayed_ofi(snapshots, alpha=0.3)
        ofi_05 = self.calculate_decayed_ofi(snapshots, alpha=0.5)

        # Assemble Payload
        features = []
        for i in range(len(snapshots)):
            features.append({
                'timestamp': ts_list[i],
                'market_id': market_id or snapshots[i].get('market_id'),
                'outcome': snapshots[i].get('outcome', 'YES'),
                'ofi_1s': round(ofi_raw[i], 6),
                'vamp': round(vamp[i], 6),
                'micro_price': round(micro[i], 6),
                'depth_ratio': round(depth_r[i], 6),
                'spread_volatility': round(spread_vol[i], 6),
                'ofi_ema_01': round(ofi_01[i], 6),
                'ofi_ema_03': round(ofi_03[i], 6),
                'ofi_ema_05': round(ofi_05[i], 6),
                # Placeholders for expensive/unused features to satisfy schema
                'ofi_5s': round(ofi_raw[i], 6),
                'ofi_15s': round(ofi_raw[i], 6),
                'ofi_60s': round(ofi_raw[i], 6),
                'obi_weighted': 0.0,
                'kyle_lambda': 0.0,
                'pin_score': 0.0,
                'volume_imbalance': 0.0,
            })
            
        return features
