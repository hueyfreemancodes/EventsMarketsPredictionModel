import os
import sys
import json
import logging
from datetime import datetime
import time
import pandas as pd
import joblib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

logger = logging.getLogger("GameLineEV_XGB")

from src.data_collection.nba_api_collector import NBADataCollector
from src.data_collection.odds_api_collector import OddsAPICollector
from nba_api.stats.endpoints import leaguegamelog

def odds_to_prob(american_odds):
    if american_odds is None:
        return 0.0
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return -american_odds / (-american_odds + 100)

def american_to_decimal(american_odds):
    if american_odds is None:
        return 1.0
    if american_odds > 0:
        return 1 + (american_odds / 100)
    else:
        return 1 + (100 / abs(american_odds))

def calculate_dollar_ev(model_prob, american_odds):
    decimal_odds = american_to_decimal(american_odds)
    return (model_prob * decimal_odds) - 1.0

def get_team_features(team_id, season="2025-26"):
    """
    Fetches the game logs for a team and calculates season averages and 5-game rolling averages.
    """
    try:
        log = leaguegamelog.LeagueGameLog(season=season, season_type_all_star="Regular Season", player_or_team_abbreviation='T')
        df = log.get_data_frames()[0]
        
        # Filter for this team
        team_df = df[df['TEAM_ID'] == team_id].sort_values('GAME_DATE').copy()
        
        # We need opponent stats too to calculate Four Factors properly.
        # But for today's predictions, we can just use the cached training set since it has exactly what we need up to yesterday!
        # This is MUCH faster and more reliable than re-calculating Four Factors on the fly with complex dataframe joins.
        return None
    except Exception as e:
        logger.error(f"Error fetching logs for {team_id}: {e}")
        return None

def get_features_from_cache(home_id, away_id, is_playoff):
    """
    Loads the team_training_set.csv and extracts the most recent stats for home and away.
    """
    try:
        df = pd.read_csv('data/team_training_set.csv')
        df['date'] = pd.to_datetime(df['date'])
        
        df = df.rename(columns={'home_team_id_x': 'home_team_id', 'away_team_id_x': 'away_team_id'})
        
        # Extract long format
        home_df = df[['date', 'home_team_id', 'home_efg', 'home_tov', 'home_orb', 'home_ftr', 'home_ortg', 'home_drtg', 'pace']].copy()
        home_df.columns = ['date', 'team_id', 'efg', 'tov', 'orb', 'ftr', 'ortg', 'drtg', 'pace']
        
        away_df = df[['date', 'away_team_id', 'away_efg', 'away_tov', 'away_orb', 'away_ftr', 'away_ortg', 'away_drtg', 'pace']].copy()
        away_df.columns = ['date', 'team_id', 'efg', 'tov', 'orb', 'ftr', 'ortg', 'drtg', 'pace']
        
        long_df = pd.concat([home_df, away_df]).sort_values('date')
        
        # Get most recent for HOME
        h_games = long_df[long_df['team_id'] == home_id]
        if len(h_games) == 0: return None
        h_last_5 = h_games.tail(5)
        h_season = h_games
        
        # Get most recent for AWAY
        a_games = long_df[long_df['team_id'] == away_id]
        if len(a_games) == 0: return None
        a_last_5 = a_games.tail(5)
        a_season = a_games
        
        # Calculate rest days
        today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        h_rest = (today - h_games['date'].iloc[-1]).days - 1
        a_rest = (today - a_games['date'].iloc[-1]).days - 1
        
        features = {
            'home_rolling_5_efg': h_last_5['efg'].mean(),
            'home_rolling_5_tov': h_last_5['tov'].mean(),
            'home_rolling_5_orb': h_last_5['orb'].mean(),
            'home_rolling_5_ftr': h_last_5['ftr'].mean(),
            'home_rolling_5_ortg': h_last_5['ortg'].mean(),
            'home_rolling_5_drtg': h_last_5['drtg'].mean(),
            'home_rolling_5_pace': h_last_5['pace'].mean(),
            
            'home_season_efg': h_season['efg'].mean(),
            'home_season_tov': h_season['tov'].mean(),
            'home_season_orb': h_season['orb'].mean(),
            'home_season_ftr': h_season['ftr'].mean(),
            'home_season_ortg': h_season['ortg'].mean(),
            'home_season_drtg': h_season['drtg'].mean(),
            'home_season_pace': h_season['pace'].mean(),
            
            'home_rest_days': min(h_rest, 4),
            
            'away_rolling_5_efg': a_last_5['efg'].mean(),
            'away_rolling_5_tov': a_last_5['tov'].mean(),
            'away_rolling_5_orb': a_last_5['orb'].mean(),
            'away_rolling_5_ftr': a_last_5['ftr'].mean(),
            'away_rolling_5_ortg': a_last_5['ortg'].mean(),
            'away_rolling_5_drtg': a_last_5['drtg'].mean(),
            'away_rolling_5_pace': a_last_5['pace'].mean(),
            
            'away_season_efg': a_season['efg'].mean(),
            'away_season_tov': a_season['tov'].mean(),
            'away_season_orb': a_season['orb'].mean(),
            'away_season_ftr': a_season['ftr'].mean(),
            'away_season_ortg': a_season['ortg'].mean(),
            'away_season_drtg': a_season['drtg'].mean(),
            'away_season_pace': a_season['pace'].mean(),
            
            'away_rest_days': min(a_rest, 4),
            
            'is_playoff': 1 if is_playoff else 0
        }
        
        return features
    except Exception as e:
        logger.error(f"Error extracting features from cache: {e}")
        return None

def main():
    logger.info("=== Starting Game Line +EV Discovery Pipeline (XGBoost) ===")
    
    nba = NBADataCollector()
    odds = OddsAPICollector()
    
    # Load Model
    model_path = 'data/team_xgboost_model.joblib'
    if not os.path.exists(model_path):
        logger.error(f"Model not found at {model_path}. Please run train_team_xgboost.py first.")
        return
        
    team_model = joblib.load(model_path)

    raw_odds = odds.fetch_live_odds(markets="h2h,spreads,totals")
    if not raw_odds:
        logger.error("Could not fetch odds from Odds API.")
        return
        
    game_lines = odds.get_game_lines(raw_odds)
    
    # Bypass NBA API schedule fetch and use Odds API games instead
    if not game_lines:
        logger.warning("No games found in Odds API. Exiting.")
        return
        
    from datetime import datetime, timedelta, timezone
    
    # We need to filter game_lines to only include games for "today" (May 4th ET)
    # The commence_time is in UTC (e.g. 2026-05-05T00:10:00Z which is May 4th 8:10 PM ET)
    todays_games = []
    
    # Get the target date for matching (current local date)
    target_date = datetime.now().date()
    
    for g in game_lines:
        try:
            # Parse UTC time and convert to ET (UTC-4 for EDT)
            utc_time = datetime.strptime(g['commence_time'], "%Y-%m-%dT%H:%M:%SZ")
            utc_time = utc_time.replace(tzinfo=timezone.utc)
            et_time = utc_time - timedelta(hours=4)  # Rough ET conversion for filtering
            
            # If the game's ET date matches our target date
            if et_time.date() == target_date:
                todays_games.append({
                    'home_abbrev': g['home_abbr'],
                    'away_abbrev': g['away_abbr']
                })
        except Exception as e:
            logger.error(f"Error parsing date {g['commence_time']}: {e}")
            
    if not todays_games:
        logger.warning(f"No games scheduled for today ({target_date}) in Odds API.")
        return
    results = []
    
    current_month = datetime.now().month
    is_playoff = current_month in [4, 5, 6]
    
    for game in todays_games:
        home_abbr = game['home_abbrev']
        away_abbr = game['away_abbrev']
        
        home_id = nba.get_team_id(home_abbr)
        away_id = nba.get_team_id(away_abbr)
        
        if not home_id or not away_id:
            continue
            
        # Get Features
        features = get_features_from_cache(home_id, away_id, is_playoff)
        if not features:
            logger.warning(f"Missing features for {away_abbr} @ {home_abbr}. Skipping.")
            continue
            
        # Predict
        projections = team_model.predict(features)
        
        vegas_game = next((g for g in game_lines if g['home_abbr'] == home_abbr and g['away_abbr'] == away_abbr), None)
        if not vegas_game:
            continue
            
        first_book = next(iter(vegas_game['books'].values()), {})
        vegas_total = first_book.get('total')
        vegas_spread = first_book.get('home_spread')
        
        proj_total = projections['projected_total']
        proj_margin = projections['home_margin']
        
        # XGBoost is better, but still needs slight market regression to stay calibrated (50%)
        if vegas_total is not None:
            proj_total = team_model.regress_to_market(proj_total, vegas_total, regression_weight=0.50)
        if vegas_spread is not None:
            vegas_margin = -vegas_spread 
            proj_margin = team_model.regress_to_market(proj_margin, vegas_margin, regression_weight=0.50)
            
        logger.info(f"[{away_abbr} @ {home_abbr}] Proj Margin: {proj_margin:.1f} (Vegas: {-vegas_spread if vegas_spread else 'N/A'}) | Proj Total: {proj_total:.1f} (Vegas: {vegas_total})")
        
        for book_name, lines in vegas_game['books'].items():
            # Moneyline
            if lines.get('home_ml') is not None and lines.get('away_ml') is not None:
                home_prob = team_model.evaluate_moneyline(proj_margin, projected_total=proj_total)
                away_prob = 1.0 - home_prob
                home_implied = odds_to_prob(lines['home_ml'])
                away_implied = odds_to_prob(lines['away_ml'])
                
                if home_prob > home_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Moneyline', 'team': home_abbr, 'line': 'ML', 'book': book_name, 'odds': lines['home_ml'], 'prob': home_prob, 'ev': calculate_dollar_ev(home_prob, lines['home_ml'])})
                if away_prob > away_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Moneyline', 'team': away_abbr, 'line': 'ML', 'book': book_name, 'odds': lines['away_ml'], 'prob': away_prob, 'ev': calculate_dollar_ev(away_prob, lines['away_ml'])})
                    
            # Spread
            if lines.get('home_spread') is not None and lines.get('away_spread') is not None:
                home_prob = team_model.evaluate_spread(proj_margin, lines['home_spread'], projected_total=proj_total)
                away_prob = team_model.evaluate_spread(-proj_margin, lines['away_spread'], projected_total=proj_total)
                home_implied = odds_to_prob(lines['home_spread_odds'])
                away_implied = odds_to_prob(lines['away_spread_odds'])
                
                if home_prob > home_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Spread', 'team': home_abbr, 'line': lines['home_spread'], 'book': book_name, 'odds': lines['home_spread_odds'], 'prob': home_prob, 'ev': calculate_dollar_ev(home_prob, lines['home_spread_odds'])})
                if away_prob > away_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Spread', 'team': away_abbr, 'line': lines['away_spread'], 'book': book_name, 'odds': lines['away_spread_odds'], 'prob': away_prob, 'ev': calculate_dollar_ev(away_prob, lines['away_spread_odds'])})
                    
            # Total
            if lines.get('total') is not None:
                prob_over, prob_under = team_model.evaluate_total(proj_total, lines['total'])
                over_implied = odds_to_prob(lines['over_odds'])
                under_implied = odds_to_prob(lines['under_odds'])
                
                if prob_over > over_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Total', 'team': 'OVER', 'line': lines['total'], 'book': book_name, 'odds': lines['over_odds'], 'prob': prob_over, 'ev': calculate_dollar_ev(prob_over, lines['over_odds'])})
                if prob_under > under_implied:
                    results.append({'game': f"{away_abbr} @ {home_abbr}", 'market': 'Total', 'team': 'UNDER', 'line': lines['total'], 'book': book_name, 'odds': lines['under_odds'], 'prob': prob_under, 'ev': calculate_dollar_ev(prob_under, lines['under_odds'])})
                    
    # Log and print
    EV_THRESHOLD = 0.03
    WIN_PROB_THRESHOLD = 0.50
    
    top_bets = [r for r in results if r['ev'] > EV_THRESHOLD and r['prob'] > WIN_PROB_THRESHOLD]
    top_bets.sort(key=lambda x: x['ev'], reverse=True)
    
    if top_bets:
        os.makedirs("data", exist_ok=True)
        log_file = "data/logged_game_bets.json"
        existing_bets = []
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                try: existing_bets = json.load(f)
                except: existing_bets = []
        
        today = datetime.now().strftime("%Y-%m-%d")
        for b in top_bets:
            b['timestamp'] = datetime.now().isoformat()
            is_dup = any(eb['game'] == b['game'] and eb['market'] == b['market'] and eb['team'] == b['team'] and eb['timestamp'].startswith(today) for eb in existing_bets)
            if not is_dup:
                existing_bets.append(b)
                
        with open(log_file, "w") as f:
            json.dump(existing_bets, f, indent=2)
        logger.info(f"Logged {len(top_bets)} high-confidence game lines to {log_file}")

    print("\n" + "="*90)
    print(f" NBA GAME LINE +EV REPORT (XGBOOST UPGRADE) - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*90)
    
    if not top_bets:
        print("No high-confidence game lines found meeting thresholds.")
    else:
        print(f"{'GAME':<12} | {'MARKET':<10} | {'TEAM/SIDE':<10} | {'LINE':<5} | {'BOOK':<12} | {'ODDS':<6} | {'EV%'}")
        print("-" * 90)
        for p in top_bets:
            print(f"{p['game']:<12} | {p['market']:<10} | {p['team']:<10} | {p['line']:<5} | {p['book']:<12} | {p['odds']:>6} | {p['ev']*100:>6.1f}%")
    print("\n" + "="*90)

if __name__ == "__main__":
    main()
