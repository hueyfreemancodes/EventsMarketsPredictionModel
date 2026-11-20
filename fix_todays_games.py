from nba_api.stats.static import teams
from nba_api.stats.endpoints import scoreboardv2
from datetime import datetime

def get_todays_games(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
        
    sb = scoreboardv2.ScoreboardV2(game_date=date_str)
    sb_dict = sb.get_dict()
    games = sb_dict['resultSets'][0]['rowSet']
    headers = sb_dict['resultSets'][0]['headers']
    
    # helper
    def get_val(row, field):
        return row[headers.index(field)]
        
    result = []
    for game in games:
        game_id = get_val(game, 'GAME_ID')
        home_id = get_val(game, 'HOME_TEAM_ID')
        away_id = get_val(game, 'VISITOR_TEAM_ID')
        
        home_team = teams.find_team_name_by_id(home_id)
        away_team = teams.find_team_name_by_id(away_id)
        
        home_abbr = home_team['abbreviation']
        away_abbr = away_team['abbreviation']
        
        result.append({
            'game_id': game_id,
            'home_team': home_team['nickname'],
            'away_team': away_team['nickname'],
            'home_abbrev': home_abbr,
            'away_abbrev': away_abbr,
            'game_time': get_val(game, 'GAME_STATUS_TEXT'),
            'status': get_val(game, 'GAME_STATUS_TEXT')
        })
    return result

print(get_todays_games('2026-04-24'))
