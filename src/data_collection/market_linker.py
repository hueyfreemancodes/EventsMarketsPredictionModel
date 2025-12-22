"""
Market Linkage Module
Connects Polymarket and Kalshi markets by resolving Team Names and Dates.
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import dateutil.parser

from src.data_collection.nba_team_abbreviations import get_team_abbreviation

class MarketLinker:
    @staticmethod
    def extract_teams_from_polymarket(title: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extracts team abbreviations from Polymarket title e.g. "Heat vs. Celtics"
        Returns (Team1_Abbrev, Team2_Abbrev)
        """
        if " vs. " not in title and " at " not in title:
            return None, None
            
        separator = " vs. " if " vs. " in title else " at "
        parts = title.split(separator)
        if len(parts) != 2:
            return None, None
            
        t1 = get_team_abbreviation(parts[0].strip())
        t2 = get_team_abbreviation(parts[1].strip())
        return t1, t2

    @staticmethod
    def extract_teams_from_kalshi(title: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extracts team abbreviations from Kalshi title e.g. "Miami vs Boston Winner?"
        Returns (Team1_Abbrev, Team2_Abbrev)
        """
        # Clean title: Remove "Winner?", ": Total Points", etc.
        clean = title.replace(" Winner?", "").replace(": Total Points", "").replace(" Matchup", "")
        
        separator = None
        if " vs " in clean: separator = " vs "
        elif " at " in clean: separator = " at "
        elif " vs. " in clean: separator = " vs. "
        
        if not separator:
            return None, None
            
        parts = clean.split(separator)
        if len(parts) < 2:
            return None, None
            
        t1 = get_team_abbreviation(parts[0].strip())
        t2 = get_team_abbreviation(parts[1].strip())  
        return t1, t2

    @staticmethod
    def link_markets(poly_markets: List[Dict], kalshi_markets: List[Dict]) -> List[Dict]:
        """
        Links Polymarket and Kalshi markets based on Team + Date.
        Returns a list of linked pairs:
        [
            {
                "game": "MIA vs BOS",
                "date": "2025-12-19",
                "poly": {...},
                "kalshi": {...}
            },
            ...
        ]
        """
        linked = []
        
        # Index Kalshi by (TeamSet, DateString)
        # TeamSet = frozenset([T1, T2]) to handle order independence
        kalshi_map = {}
        for k in kalshi_markets:
            t1, t2 = MarketLinker.extract_teams_from_kalshi(k.get('title', ''))
            if t1 and t2:
                # Approximate date from ticker or open_date if available
                # Assuming simple day match for now. Ticker implies date e.g. KXNBAGAME-25DEC19...
                # Extract date from ticker: -25DEC19
                # This is robust for Kalshi NBA
                ticker = k.get('ticker', '')
                date_tag = None
                import re
                match = re.search(r'-(\d{2}[A-Z]{3}\d{2})', ticker)
                if match:
                    date_tag = match.group(1) # e.g. 25DEC19
                
                key = (frozenset([t1, t2]), date_tag)
                if key not in kalshi_map:
                    kalshi_map[key] = []
                kalshi_map[key].append(k)

        # Iterate Polymarket
        for p in poly_markets:
            t1, t2 = MarketLinker.extract_teams_from_polymarket(p.get('title', ''))
            if t1 and t2:
                # 1. Try Slug Date (Most reliable for Sports)
                # Format: nba-mia-bos-2025-12-19
                slug = p.get('slug', '')
                date_str = None
                
                import re
                # Match YYYY-MM-DD at end of slug
                match = re.search(r'(\d{4}-\d{2}-\d{2})$', slug)
                if match:
                    date_str = match.group(1)
                elif p.get('start_date'):
                     # Fallback to start_date (might be creation date, risky)
                     date_str = p.get('start_date')

                if date_str:
                    try:
                        dt = dateutil.parser.parse(date_str)
                        # Convert to Kalshi format: YYMMMDD (25DEC19)
                        kalshi_fmt = dt.strftime("%y%b%d").upper()
                        
                        key = (frozenset([t1, t2]), kalshi_fmt)
                        
                        if key in kalshi_map:
                            # FOUND MATCH
                            for k_match in kalshi_map[key]:
                                linked.append({
                                    "game": f"{t1} vs {t2}",
                                    "date": kalshi_fmt,
                                    "poly_title": p.get('title'),
                                    "kalshi_title": k_match.get('title'),
                                    "poly_id": p.get('slug'),
                                    "kalshi_ticker": k_match.get('ticker')
                                })
                    except:
                        pass
                        
        return linked
