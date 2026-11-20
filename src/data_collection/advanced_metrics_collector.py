import sys
import os
import time
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from src.data_collection.logger import logger

try:
    from nba_api.stats.endpoints import (
        playerdashboardbygeneralsplits,
        playerdashptpass,
        playerdashptreb
    )
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False

class AdvancedMetricsCollector:
    """
    Collects advanced tracking and spatio-temporal data from NBA API.
    """
    def __init__(self):
        self.enabled = NBA_API_AVAILABLE
        
    def get_non_estimated_usage(self, player_id: int) -> float:
        """
        Retrieves actual Usage Rate from advanced box scores instead of estimating via FGA/TOV.
        """
        if not self.enabled: return 0.20
        try:
            time.sleep(0.6)
            dash = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                player_id=player_id, per_mode_detailed='PerGame'
            )
            df = dash.get_data_frames()[0] # Overall
            if not df.empty and 'USG_PCT' in df.columns:
                return float(df['USG_PCT'].iloc[0])
            return 0.20
        except Exception as e:
            logger.error(f"Error fetching usage for player {player_id}: {e}")
            return 0.20
            
    def get_potential_assists(self, player_id: int, team_id: int) -> float:
        """
        Retrieves true passing opportunities (Potential Assists) from player tracking data.
        """
        if not self.enabled or not team_id: return 0.0
        try:
            time.sleep(0.6)
            dash = playerdashptpass.PlayerDashPtPass(team_id=team_id, player_id=player_id, per_mode_simple='PerGame')
            df = dash.get_data_frames()[0] # Passes Made
            if not df.empty and 'POTENTIAL_AST' in df.columns:
                return float(df['POTENTIAL_AST'].iloc[0])
            return 0.0
        except Exception as e:
            logger.error(f"Error fetching potential assists for {player_id}: {e}")
            return 0.0
            
    def get_rebound_chances(self, player_id: int, team_id: int) -> float:
        """
        Retrieves Rebound Chances from optical tracking to measure true rebounding opportunity.
        """
        if not self.enabled or not team_id: return 0.0
        try:
            time.sleep(0.6)
            dash = playerdashptreb.PlayerDashPtReb(team_id=team_id, player_id=player_id, per_mode_simple='PerGame')
            df = dash.get_data_frames()[0] # Overall Rebounding
            if not df.empty and 'REB_CHANCES' in df.columns:
                return float(df['REB_CHANCES'].iloc[0])
            return 0.0
        except Exception as e:
            logger.error(f"Error fetching rebound chances for {player_id}: {e}")
            return 0.0

    def calculate_lineup_sensitivity(self, player_usg: float, missing_teammate_usg: float) -> float:
        """
        The 'Wally Pipp' Effect: Redistribute usage non-linearly when a high-usage teammate is missing.
        Applies a usage-efficiency tradeoff penalty.
        """
        if missing_teammate_usg <= 0:
            return 1.0
        
        # Simple redistribution model: Player absorbs 20% of missing usage
        absorbed_usg = missing_teammate_usg * 0.20
        new_usg = player_usg + absorbed_usg
        
        # Usage-Efficiency Tradeoff: For every 1% increase in usage, efficiency drops by 0.5%
        usg_increase = new_usg - player_usg
        efficiency_penalty = max(0, usg_increase * 100 * 0.005)
        
        volume_multiplier = new_usg / player_usg if player_usg > 0 else 1.0
        final_multiplier = volume_multiplier * (1 - efficiency_penalty)
        return final_multiplier
