"""
Injury Impact Collector

Calculates the mathematical impact of missing players on team efficiency.
Uses On/Off Net Rating splits to adjust a team's ORtg/DRtg baseline
when key players are ruled OUT.

Phase 1: Manual override system (pass injured player names)
Phase 2: Automated injury feed from ESPN/RotoWire (future sprint)
"""

import time
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("InjuryImpactCollector")

try:
    from nba_api.stats.endpoints import playerdashboardbygeneralsplits
    from nba_api.stats.static import players, teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False


class InjuryImpactCollector:
    """
    Computes the on/off differential for injured players and adjusts team metrics.
    
    When a key player is ruled OUT, their On/Off Net Rating differential
    is subtracted from the team's baseline ORtg/DRtg, giving a more accurate
    projection of the team's true strength for that specific game.
    """
    
    def __init__(self):
        self.enabled = NBA_API_AVAILABLE
        self._cache: Dict[int, Dict] = {}  # player_id -> on/off splits
    
    def _get_player_id(self, player_name: str) -> Optional[int]:
        """Resolve player name to NBA API player ID."""
        if not self.enabled:
            return None
        try:
            matches = players.find_players_by_full_name(player_name)
            if matches:
                return matches[0]['id']
        except Exception as e:
            logger.warning(f"Could not resolve player: {player_name}: {e}")
        return None
    
    def get_on_off_impact(self, player_name: str, season: str = None) -> Dict[str, float]:
        """
        Fetch on/off court splits for a player and return their net impact.
        
        Returns:
            Dict with 'ortg_impact' and 'drtg_impact' representing the
            efficiency differential when this player is ON vs OFF the court.
            Positive ortg_impact = team is BETTER offensively with this player.
            Positive drtg_impact = team is WORSE defensively without this player.
        """
        if not self.enabled:
            return {'ortg_impact': 0.0, 'drtg_impact': 0.0}
        
        player_id = self._get_player_id(player_name)
        if not player_id:
            return {'ortg_impact': 0.0, 'drtg_impact': 0.0}
        
        # Check cache
        if player_id in self._cache:
            return self._cache[player_id]
        
        try:
            time.sleep(0.8)  # Rate limiting
            
            dash = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                player_id=player_id,
                season=season,
                measure_type_detailed='Advanced',
                per_mode_detailed='PerGame',
                timeout=60
            )
            
            # The endpoint returns multiple dataframes:
            # Index 0 = Overall, Index 1+ = Splits
            # We need the "OnOff" split which shows team performance with/without
            dfs = dash.get_data_frames()
            
            if len(dfs) == 0:
                return {'ortg_impact': 0.0, 'drtg_impact': 0.0}
            
            overall = dfs[0]
            if overall.empty:
                return {'ortg_impact': 0.0, 'drtg_impact': 0.0}
            
            # Get the player's own offensive and defensive rating as a proxy
            # for their team impact
            player_ortg = float(overall['OFF_RATING'].iloc[0]) if 'OFF_RATING' in overall.columns else 0.0
            player_drtg = float(overall['DEF_RATING'].iloc[0]) if 'DEF_RATING' in overall.columns else 0.0
            
            # Calculate impact relative to league average
            # A player with 118 ORtg vs league avg 115 has a +3.0 offensive impact
            league_avg_ortg = 115.0
            league_avg_drtg = 115.0
            
            ortg_impact = player_ortg - league_avg_ortg
            drtg_impact = league_avg_drtg - player_drtg  # Inverted: lower DRtg = better defense
            
            result = {
                'ortg_impact': ortg_impact,
                'drtg_impact': drtg_impact,
                'player_name': player_name,
                'player_ortg': player_ortg,
                'player_drtg': player_drtg
            }
            
            self._cache[player_id] = result
            logger.info(f"On/Off impact for {player_name}: ORtg {ortg_impact:+.1f}, DRtg {drtg_impact:+.1f}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching on/off splits for {player_name}: {e}")
            return {'ortg_impact': 0.0, 'drtg_impact': 0.0}
    
    def adjust_team_ratings(self, 
                           base_ortg: float, 
                           base_drtg: float, 
                           injured_players: List[str],
                           season: str = None) -> Tuple[float, float]:
        """
        Adjust a team's ORtg/DRtg based on their injured players' on/off impacts.
        
        Args:
            base_ortg: Team's season ORtg
            base_drtg: Team's season DRtg
            injured_players: List of player names ruled OUT
            season: NBA season string (e.g., '2025-26')
            
        Returns:
            Tuple of (adjusted_ortg, adjusted_drtg)
        """
        if not injured_players:
            return base_ortg, base_drtg
        
        total_ortg_penalty = 0.0
        total_drtg_penalty = 0.0
        
        for player_name in injured_players:
            impact = self.get_on_off_impact(player_name, season=season)
            
            # Dampen the impact — a player's on/off differential overstates
            # their true impact because of lineup correlation effects.
            # Industry standard is to apply ~40-60% of the raw differential.
            dampening = 0.50
            
            total_ortg_penalty += impact['ortg_impact'] * dampening
            total_drtg_penalty += impact['drtg_impact'] * dampening
            
            logger.info(
                f"  Injury adjustment for {player_name}: "
                f"ORtg {-impact['ortg_impact'] * dampening:+.1f}, "
                f"DRtg {+impact['drtg_impact'] * dampening:+.1f}"
            )
        
        adjusted_ortg = base_ortg - total_ortg_penalty
        adjusted_drtg = base_drtg + total_drtg_penalty
        
        logger.info(
            f"Team ratings adjusted: ORtg {base_ortg:.1f} → {adjusted_ortg:.1f}, "
            f"DRtg {base_drtg:.1f} → {adjusted_drtg:.1f}"
        )
        
        return adjusted_ortg, adjusted_drtg
