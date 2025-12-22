"""
NBA Team Abbreviations Reference
Standard 3-letter abbreviations used by sportsipy
"""

NBA_TEAM_ABBREVIATIONS = {
    'ATL': 'Atlanta Hawks',
    'BOS': 'Boston Celtics',
    'BKN': 'Brooklyn Nets',
    'CHA': 'Charlotte Hornets',
    'CHI': 'Chicago Bulls',
    'CLE': 'Cleveland Cavaliers',
    'DAL': 'Dallas Mavericks',
    'DEN': 'Denver Nuggets',
    'DET': 'Detroit Pistons',
    'GSW': 'Golden State Warriors',
    'HOU': 'Houston Rockets',
    'IND': 'Indiana Pacers',
    'LAC': 'LA Clippers',
    'LAL': 'Los Angeles Lakers',
    'MEM': 'Memphis Grizzlies',
    'MIA': 'Miami Heat',
    'MIL': 'Milwaukee Bucks',
    'MIN': 'Minnesota Timberwolves',
    'NOP': 'New Orleans Pelicans',
    'NYK': 'New York Knicks',
    'OKC': 'Oklahoma City Thunder',
    'ORL': 'Orlando Magic',
    'PHI': 'Philadelphia 76ers',
    'PHX': 'Phoenix Suns',
    'POR': 'Portland Trail Blazers',
    'SAC': 'Sacramento Kings',
    'SAS': 'San Antonio Spurs',
    'TOR': 'Toronto Raptors',
    'UTA': 'Utah Jazz',
    'WAS': 'Washington Wizards',
}

# Reverse mapping: team name to abbreviation
NBA_TEAM_NAMES_TO_ABBREV = {v: k for k, v in NBA_TEAM_ABBREVIATIONS.items()}

def get_team_abbreviation(team_name: str) -> str:
    """Convert team name to abbreviation"""
    # Try exact match
    if team_name in NBA_TEAM_NAMES_TO_ABBREV:
        return NBA_TEAM_NAMES_TO_ABBREV[team_name]
    
    # Try case-insensitive match
    team_name_lower = team_name.lower()
    for name, abbrev in NBA_TEAM_NAMES_TO_ABBREV.items():
        if name.lower() == team_name_lower:
            return abbrev
    
    # Try partial match
    for name, abbrev in NBA_TEAM_NAMES_TO_ABBREV.items():
        if team_name_lower in name.lower() or name.lower() in team_name_lower:
            return abbrev
    
    return None

def is_valid_abbreviation(abbrev: str) -> bool:
    """Check if abbreviation is valid"""
    return abbrev.upper() in NBA_TEAM_ABBREVIATIONS

