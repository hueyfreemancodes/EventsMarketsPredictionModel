
# Canonical NBA Team List
NBA_TEAMS = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets", "Chicago Bulls",
    "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets", "Detroit Pistons", "Golden State Warriors",
    "Houston Rockets", "Indiana Pacers", "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies",
    "Miami Heat", "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns", "Portland Trail Blazers",
    "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors", "Utah Jazz", "Washington Wizards"
]

# Mapping for team short names to full names
TEAM_MAPPING = {
    # City/Nicknames
    "Hawks": "Atlanta Hawks", "Celtics": "Boston Celtics", "Nets": "Brooklyn Nets", "Hornets": "Charlotte Hornets",
    "Bulls": "Chicago Bulls", "Cavaliers": "Cleveland Cavaliers", "Cavs": "Cleveland Cavaliers", "Mavericks": "Dallas Mavericks",
    "Mavs": "Dallas Mavericks", "Nuggets": "Denver Nuggets", "Pistons": "Detroit Pistons", "Warriors": "Golden State Warriors",
    "Rockets": "Houston Rockets", "Pacers": "Indiana Pacers", "Clippers": "Los Angeles Clippers", "Lakers": "Los Angeles Lakers",
    "Grizzlies": "Memphis Grizzlies", "Heat": "Miami Heat", "Bucks": "Milwaukee Bucks", "Timberwolves": "Minnesota Timberwolves",
    "Wolves": "Minnesota Timberwolves", "Pelicans": "New Orleans Pelicans", "Knicks": "New York Knicks", "Thunder": "Oklahoma City Thunder",
    "OKC": "Oklahoma City Thunder", "Magic": "Orlando Magic", "76ers": "Philadelphia 76ers", "Sixers": "Philadelphia 76ers",
    "Suns": "Phoenix Suns", "Blazers": "Portland Trail Blazers", "Trail Blazers": "Portland Trail Blazers", "Kings": "Sacramento Kings",
    "Spurs": "San Antonio Spurs", "Raptors": "Toronto Raptors", "Jazz": "Utah Jazz", "Wizards": "Washington Wizards",
    
    # City Names (Handling cases like "Memphis" vs "Memphis Grizzlies")
    "Atlanta": "Atlanta Hawks", "Boston": "Boston Celtics", "Brooklyn": "Brooklyn Nets", "Charlotte": "Charlotte Hornets",
    "Chicago": "Chicago Bulls", "Cleveland": "Cleveland Cavaliers", "Dallas": "Dallas Mavericks", "Denver": "Denver Nuggets",
    "Detroit": "Detroit Pistons", "Golden State": "Golden State Warriors", "Houston": "Houston Rockets", "Indiana": "Indiana Pacers",
    "LA Clippers": "Los Angeles Clippers", "Clippers": "Los Angeles Clippers", "LA Lakers": "Los Angeles Lakers", "Lakers": "Los Angeles Lakers",
    "Memphis": "Memphis Grizzlies", "Miami": "Miami Heat", "Milwaukee": "Milwaukee Bucks", "Minnesota": "Minnesota Timberwolves",
    "New Orleans": "New Orleans Pelicans", "New York": "New York Knicks", "Oklahoma City": "Oklahoma City Thunder", "Orlando": "Orlando Magic",
    "Philadelphia": "Philadelphia 76ers", "Philly": "Philadelphia 76ers", "Phoenix": "Phoenix Suns", "Portland": "Portland Trail Blazers",
    "Sacramento": "Sacramento Kings", "San Antonio": "San Antonio Spurs", "Toronto": "Toronto Raptors", "Utah": "Utah Jazz", 
    "Washington": "Washington Wizards"
}

def normalize_team_name(name):
    """
    Normalizes a team name string to the canonical full NBA team name.
    
    Args:
        name (str): The input team name (e.g., "Lakers", "Memphis").
        
    Returns:
        str: The canonical full team name (e.g., "Los Angeles Lakers"), or None if no match found.
    """
    if not name:
        return None
        
    clean_name = name.strip()
    
    # Direct match
    if clean_name in NBA_TEAMS:
        return clean_name
        
    # Case-insensitive check against canonical
    for team in NBA_TEAMS:
        if clean_name.lower() == team.lower():
            return team
            
    # Check mapping
    return TEAM_MAPPING.get(clean_name, None)

# Canonical to Abbreviation Map
TEAM_TO_ABBR = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS"
}

def get_team_abbr(name):
    """Returns 3-letter code for a team"""
    full_name = normalize_team_name(name)
    if full_name:
        return TEAM_TO_ABBR.get(full_name)
    return None
