import os
import sys
import pandas as pd
import time
from nba_api.stats.endpoints import leaguegamelog

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

def fetch_season_gamelogs(season="2025-26"):
    print(f"Fetching game logs for {season}...")
    log = leaguegamelog.LeagueGameLog(season=season, season_type_all_star="Regular Season")
    df_reg = log.get_data_frames()[0]
    
    time.sleep(0.6)
    
    log_playoffs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star="Playoffs")
    df_playoffs = log_playoffs.get_data_frames()[0]
    
    df = pd.concat([df_reg, df_playoffs], ignore_index=True)
    return df

def calculate_advanced_metrics(df):
    """
    Groups by GAME_ID to match Home and Away teams and calculate Four Factors and Pace.
    """
    games = []
    game_ids = df['GAME_ID'].unique()
    
    # Sort chronologically so we can do rolling averages later
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    df = df.sort_values('GAME_DATE')
    
    print(f"Processing {len(game_ids)} games...")
    
    for gid in game_ids:
        game_rows = df[df['GAME_ID'] == gid]
        if len(game_rows) != 2:
            continue
            
        t1 = game_rows.iloc[0]
        t2 = game_rows.iloc[1]
        
        # Determine home/away based on MATCHUP string ("vs." = home, "@" = away)
        if 'vs.' in t1['MATCHUP']:
            home = t1
            away = t2
        else:
            home = t2
            away = t1
            
        # Helper for Four Factors
        def get_factors(team, opp):
            efg = (team['FGM'] + 0.5 * team['FG3M']) / team['FGA'] if team['FGA'] > 0 else 0
            tov_pct = team['TOV'] / (team['FGA'] + 0.44 * team['FTA'] + team['TOV']) if (team['FGA'] + 0.44 * team['FTA'] + team['TOV']) > 0 else 0
            orb_pct = team['OREB'] / (team['OREB'] + opp['DREB']) if (team['OREB'] + opp['DREB']) > 0 else 0
            ftr = team['FTA'] / team['FGA'] if team['FGA'] > 0 else 0
            
            # Estimate possessions (Basic formulation)
            poss = 0.5 * ((team['FGA'] + 0.4 * team['FTA'] - 1.07 * (team['OREB'] / (team['OREB'] + opp['DREB'])) * (team['FGA'] - team['FGM']) + team['TOV']) + \
                          (opp['FGA'] + 0.4 * opp['FTA'] - 1.07 * (opp['OREB'] / (opp['OREB'] + team['DREB'])) * (opp['FGA'] - opp['FGM']) + opp['TOV']))
            
            ortg = (team['PTS'] / poss) * 100 if poss > 0 else 0
            drtg = (opp['PTS'] / poss) * 100 if poss > 0 else 0
            
            return efg, tov_pct, orb_pct, ftr, ortg, drtg, poss

        h_efg, h_tov, h_orb, h_ftr, h_ortg, h_drtg, h_poss = get_factors(home, away)
        a_efg, a_tov, a_orb, a_ftr, a_ortg, a_drtg, a_poss = get_factors(away, home)
        
        pace = h_poss * (48.0 / (home['MIN'] / 5)) if home['MIN'] > 0 else h_poss
        
        games.append({
            'game_id': gid,
            'date': home['GAME_DATE'],
            'home_team_id': home['TEAM_ID'],
            'away_team_id': away['TEAM_ID'],
            'home_pts': home['PTS'],
            'away_pts': away['PTS'],
            'margin': home['PTS'] - away['PTS'],
            'total': home['PTS'] + away['PTS'],
            
            'home_efg': h_efg,
            'home_tov': h_tov,
            'home_orb': h_orb,
            'home_ftr': h_ftr,
            'home_ortg': h_ortg,
            'home_drtg': h_drtg,
            
            'away_efg': a_efg,
            'away_tov': a_tov,
            'away_orb': a_orb,
            'away_ftr': a_ftr,
            'away_ortg': a_ortg,
            'away_drtg': a_drtg,
            
            'pace': pace,
            'is_playoff': 1 if home['GAME_DATE'].month in [4, 5, 6] else 0
        })
        
    return pd.DataFrame(games)

def add_rolling_features(df):
    """
    Calculate rolling 5-game averages for each team entering the game.
    """
    print("Calculating rolling features...")
    df = df.sort_values('date').reset_index(drop=True)
    
    # We need a long-format DataFrame to compute rolling averages per team
    home_df = df[['date', 'game_id', 'home_team_id', 'home_efg', 'home_tov', 'home_orb', 'home_ftr', 'home_ortg', 'home_drtg', 'pace']].copy()
    home_df.columns = ['date', 'game_id', 'team_id', 'efg', 'tov', 'orb', 'ftr', 'ortg', 'drtg', 'pace']
    home_df['is_home'] = 1
    
    away_df = df[['date', 'game_id', 'away_team_id', 'away_efg', 'away_tov', 'away_orb', 'away_ftr', 'away_ortg', 'away_drtg', 'pace']].copy()
    away_df.columns = ['date', 'game_id', 'team_id', 'efg', 'tov', 'orb', 'ftr', 'ortg', 'drtg', 'pace']
    away_df['is_home'] = 0
    
    long_df = pd.concat([home_df, away_df]).sort_values('date')
    
    # Calculate rolling 5-game stats Shift(1) to avoid data leakage!
    metrics = ['efg', 'tov', 'orb', 'ftr', 'ortg', 'drtg', 'pace']
    
    for m in metrics:
        long_df[f'rolling_5_{m}'] = long_df.groupby('team_id')[m].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
        long_df[f'season_{m}'] = long_df.groupby('team_id')[m].transform(lambda x: x.expanding(1).mean().shift(1))
    
    # Calculate rest days
    long_df['prev_game_date'] = long_df.groupby('team_id')['date'].shift(1)
    long_df['rest_days'] = (long_df['date'] - long_df['prev_game_date']).dt.days - 1
    long_df['rest_days'] = long_df['rest_days'].fillna(3).clip(upper=4) # Cap at 4, default 3 for season opener
    
    # Merge back to wide format
    h_features = long_df[long_df['is_home'] == 1].drop(columns=metrics + ['is_home', 'prev_game_date', 'date']).rename(
        columns=lambda x: f"home_{x}" if x not in ['game_id'] else x)
    a_features = long_df[long_df['is_home'] == 0].drop(columns=metrics + ['is_home', 'prev_game_date', 'date']).rename(
        columns=lambda x: f"away_{x}" if x not in ['game_id'] else x)
        
    df = df.merge(h_features, on='game_id')
    df = df.merge(a_features, on='game_id')
    
    # Drop rows with NaN (first game of season)
    df = df.dropna()
    return df

if __name__ == "__main__":
    df_2425 = fetch_season_gamelogs("2024-25")
    df_2526 = fetch_season_gamelogs("2025-26")
    
    df = pd.concat([df_2425, df_2526], ignore_index=True)
    df_games = calculate_advanced_metrics(df)
    final_df = add_rolling_features(df_games)
    
    os.makedirs('data', exist_ok=True)
    out_path = 'data/team_training_set.csv'
    final_df.to_csv(out_path, index=False)
    print(f"Saved {len(final_df)} games to {out_path}")
