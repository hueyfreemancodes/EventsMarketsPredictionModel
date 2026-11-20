import os
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

logger = logging.getLogger("PlayerPropEVFinder")

from src.data_collection.nba_api_collector import NBADataCollector
from src.data_collection.odds_api_collector import OddsAPICollector
from src.models.player_bayesian_model import PlayerBayesianModel
from src.utils.nba_mapping import get_team_abbr
from scripts.fetch_markets import fetch_active_nba_markets
from src.data_collection.advanced_metrics_collector import AdvancedMetricsCollector
from src.models.xgboost_pipeline import XGBoostPipeline, ensemble_predict
from src.models.team_bayesian_model import TeamParametricModel

def main():
    logger.info("Starting Phase 2: Player Prop EV Discovery Pipeline (Upgraded)")
    
    # 1. Initialize Collectors
    nba = NBADataCollector(lookback_games=10)
    odds_api = OddsAPICollector()
    model = PlayerBayesianModel()
    adv_metrics = AdvancedMetricsCollector()
    xgb_pipeline = XGBoostPipeline()
    team_model = TeamParametricModel()
    
    # 2. Fetch Market Data
    logger.info("Fetching active events from The Odds API...")
    
    # Define target date context
    target_date = datetime.now().date()
    game_date_str = datetime.now().strftime("%Y-%m-%d")
    
    raw_odds = odds_api.fetch_live_odds()
    all_game_lines = odds_api.get_game_lines(raw_odds)
    
    active_teams = set()
    from datetime import timezone
    for g in all_game_lines:
        try:
            utc_time = datetime.strptime(g['commence_time'], "%Y-%m-%dT%H:%M:%SZ")
            utc_time = utc_time.replace(tzinfo=timezone.utc)
            et_time = utc_time - timedelta(hours=4)
            if et_time.date() == target_date:
                active_teams.add(g['home_abbr'])
                active_teams.add(g['away_abbr'])
        except Exception:
            pass
            
    if not active_teams:
        logger.warning(f"No active teams found for today ({target_date}) from Odds API.")
        return
        
    logger.info(f"Today's active teams: {active_teams}")

    # 2.2. Player Props from Vegas (The Odds API)
    live_odds = odds_api.fetch_live_odds()
    all_game_lines = odds_api.get_game_lines(live_odds)
    game_lines = [g for g in all_game_lines if g['home_abbr'] in active_teams and g['away_abbr'] in active_teams]
    
    if not game_lines:
        logger.warning("Vegas hasn't released lines for today's active teams yet. Exiting.")
        return
    
    # 2.3. Polymarket Player Sentiment
    poly_markets = fetch_active_nba_markets(mode='props')
    poly_signals = {} 
    for m in poly_markets:
        p_name = m.get('player_name')
        if not p_name: continue
        q = m['question'].lower()
        metric = None
        if "points" in q: metric = "player_points"
        elif "rebounds" in q: metric = "player_rebounds"
        elif "assists" in q: metric = "player_assists"
        elif "threes" in q: metric = "player_threes"
        if not metric: continue
        if p_name not in poly_signals: poly_signals[p_name] = {}
        if m['token_index'] == 0:
            poly_signals[p_name][metric] = m['price']

    # 3. Analyze Props
    results = []
    prop_markets = ["player_points", "player_rebounds", "player_assists", "player_threes"]
    
    for game in game_lines:
        event_id = game['event_id']
        home_abbr = game['home_abbr']
        away_abbr = game['away_abbr']
        
        logger.info(f"Analyzing Props for {away_abbr} @ {home_abbr}...")
        raw_props = odds_api.fetch_player_props(event_id, prop_markets)
        clean_props = odds_api.process_props(raw_props)
        if not clean_props: continue
            
        player_props = {}
        for p in clean_props:
            name = p['player']
            if name not in player_props: player_props[name] = []
            player_props[name].append(p)
            
        # Fetch Team Stats for Matchup Adjustments
        h_team_stats = nba.fetch_team_stats(home_abbr, game_date_str=game_date_str)
        a_team_stats = nba.fetch_team_stats(away_abbr, game_date_str=game_date_str)
        
        # Fetch Team Context (Fatigue/Altitude)
        h_team_context = nba.get_team_context(home_abbr, game_date_str=game_date_str)
        a_team_context = nba.get_team_context(away_abbr, game_date_str=game_date_str)
        
        # === GAME SCRIPT BRIDGING ===
        # Run the Game Lines model to get the projected spread.
        # If we project a blowout, penalize star player props (they'll sit in the 4th).
        team_adv_stats = nba.get_team_advanced_stats(game_date_str=game_date_str)
        home_team_info = None
        away_team_info = None
        try:
            from nba_api.stats.static import teams as nba_teams
            home_team_info = nba_teams.find_team_by_abbreviation(home_abbr)
            away_team_info = nba_teams.find_team_by_abbreviation(away_abbr)
        except: pass
        
        blowout_risk = 0.0
        if team_adv_stats and home_team_info and away_team_info:
            h_metrics = team_adv_stats.get(home_team_info['id'], {})
            a_metrics = team_adv_stats.get(away_team_info['id'], {})
            if h_metrics and a_metrics:
                game_proj = team_model.calculate_game_projections(
                    home_stats=h_metrics,
                    away_stats=a_metrics,
                    home_rest_days=h_team_context.get('rest_days', 2),
                    away_rest_days=a_team_context.get('rest_days', 2),
                    is_high_altitude=h_team_context.get('is_high_altitude', False)
                )
                proj_margin = abs(game_proj['home_margin'])
                # Blowout risk: 0% at 8pt margin, scales linearly to ~30% at 14pt
                blowout_risk = max(0, (proj_margin - 8) / 20)
                if blowout_risk > 0:
                    logger.info(f"  Blowout risk for {away_abbr}@{home_abbr}: {blowout_risk:.1%} (proj margin: {proj_margin:.1f})")
        
        # Inject blowout risk into team contexts for player model consumption
        h_team_context['blowout_risk'] = blowout_risk
        a_team_context['blowout_risk'] = blowout_risk
            
        for player_name, props in player_props.items():
            stats = nba.fetch_player_stats(player_name, game_date_str=game_date_str)
            if not stats: continue
            
            # Identify Opponent and Context
            p_team = stats.get('team_abbr')
            
            # STRICT FILTER: Ensure the player actually plays for one of the two teams in this event
            # (Odds API occasionally mixes up players or includes cross-game props in a single event)
            if p_team not in [home_abbr, away_abbr]:
                logger.debug(f"Player {player_name} plays for {p_team}, not {home_abbr} or {away_abbr}. Skipping.")
                continue
                
            opp_stats = h_team_stats if p_team == away_abbr else a_team_stats
            
            if p_team == away_abbr:
                team_context = a_team_context.copy()
                team_context['is_home'] = False
            else:
                team_context = h_team_context.copy()
                team_context['is_home'] = True
            
            # Phase 2: Advanced Metrics Integration
            player_id = stats.get('player_id')
            team_id = nba.get_team_id(p_team) if p_team else None
            
            if player_id:
                stats['true_usage'] = adv_metrics.get_non_estimated_usage(player_id)
                if team_id:
                    if any(p['market'] == 'player_assists' for p in props):
                        stats['potential_ast'] = adv_metrics.get_potential_assists(player_id, team_id)
                    if any(p['market'] == 'player_rebounds' for p in props):
                        stats['reb_chances'] = adv_metrics.get_rebound_chances(player_id, team_id)
            
            # Deep Features for XGBoost Alignment
            stats['is_home'] = 1 if team_context['is_home'] else 0
            stats['rest_days'] = team_context.get('rest_days', 2)
            
            for prop in props:
                metric = prop['market']
                line = prop['line']
                v_over_odds = prop['over']
                v_under_odds = prop['under']
                
                market_prob = poly_signals.get(player_name, {}).get(metric)
                
                # Upgraded Prediction with Matchup Defense and Context
                prediction = model.predict_prop(
                    stats=stats, 
                    line=line, 
                    metric=metric, 
                    market_prob=market_prob, 
                    opponent_stats=opp_stats,
                    team_context=team_context
                )
                
                # Fetch ML Probability and Ensemble
                ml_prob = xgb_pipeline.predict_over_prob(stats, line, metric, bayesian_prob=prediction.prob_over)
                if ml_prob >= 0.0:
                    final_over_prob = ensemble_predict(prediction.prob_over, ml_prob)
                    model_type = "🚀 XGBoost Boosted"
                else:
                    final_over_prob = prediction.prob_over
                    model_type = "📈 Bayesian Only"
                
                # PROBABILITY DAMPENING (Bayesian shrinkage toward 50%)
                # No retail model should claim >65% or <35% on a player prop.
                # Even the sharpest props models in the world top out around 58-62%.
                # We shrink extreme probabilities toward 0.50 to produce realistic edges.
                SHRINKAGE = 0.15  # Pull 15% toward 0.50
                final_over_prob = final_over_prob * (1 - SHRINKAGE) + 0.50 * SHRINKAGE
                
                prediction.prob_over = final_over_prob
                prediction.prob_under = 1.0 - final_over_prob
                def american_to_prob(american):
                    if american > 0: return 100 / (american + 100)
                    else: return abs(american) / (abs(american) + 100)
                
                def american_to_decimal(american):
                    """Convert american odds to decimal payout"""
                    if american > 0: return 1 + (american / 100)
                    else: return 1 + (100 / abs(american))
                
                def dollar_ev(prob, american):
                    """True dollar EV: (prob × decimal_payout) - 1"""
                    return (prob * american_to_decimal(american)) - 1.0
                
                over_implied = american_to_prob(v_over_odds)
                under_implied = american_to_prob(v_under_odds)
                
                # Dollar EV: expected profit per $1 wagered
                over_ev = dollar_ev(prediction.prob_over, v_over_odds)
                under_ev = dollar_ev(prediction.prob_under, v_under_odds)
                
                results.append({
                    "player": player_name,
                    "team": p_team,
                    "game": f"{away_abbr} @ {home_abbr}",
                    "metric": metric.replace("player_", ""),
                    "line": line,
                    "book": prop['book'],
                    "over_odds": v_over_odds,
                    "under_odds": v_under_odds,
                    "prob_over": prediction.prob_over,
                    "prob_under": prediction.prob_under,
                    "over_ev": over_ev,
                    "under_ev": under_ev,
                    "model": model_type
                })

    # 4. Report & Log
    EV_THRESHOLD = 0.02  # 2% dollar EV minimum (realistic for calibrated model)
    WIN_PROB_THRESHOLD = 0.50
    
    top_props = []
    bets_to_log = []
    
    for r in results:
        # FILTER 1: Threes and Assists are too volatile based on historical backtesting
        if r['metric'] in ['threes', 'assists']:
            continue
            
        if r['over_ev'] > EV_THRESHOLD and r['prob_over'] > WIN_PROB_THRESHOLD:
            # FILTER 2: Plus money odds (> 0) historically underperform (44% win rate)
            if r['over_odds'] <= 0:
                bet = {**r, "side": "OVER", "ev": r['over_ev'], "odds": r['over_odds'], "prob": r['prob_over'], "timestamp": datetime.now().isoformat()}
                top_props.append(bet)
                bets_to_log.append(bet)
                
        if r['under_ev'] > EV_THRESHOLD and r['prob_under'] > WIN_PROB_THRESHOLD:
            if r['under_odds'] <= 0:
                bet = {**r, "side": "UNDER", "ev": r['under_ev'], "odds": r['under_odds'], "prob": r['prob_under'], "timestamp": datetime.now().isoformat()}
                top_props.append(bet)
                bets_to_log.append(bet)
            
    top_props.sort(key=lambda x: x['ev'], reverse=True)
    
    if bets_to_log:
        os.makedirs("data", exist_ok=True)
        log_file = "data/logged_bets.json"
        existing_bets = []
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                try: existing_bets = json.load(f)
                except: existing_bets = []
        
        today = datetime.now().strftime("%Y-%m-%d")
        for b in bets_to_log:
            is_dup = any(
                eb.get('player') == b.get('player') and 
                eb.get('metric') == b.get('metric') and 
                eb.get('side') == b.get('side') and 
                eb.get('timestamp', '').startswith(today) 
                for eb in existing_bets
            )
            if not is_dup:
                existing_bets.append(b)
                
        with open(log_file, "w") as f:
            json.dump(existing_bets, f, indent=2)
        logger.info(f"Logged {len(bets_to_log)} high-confidence props to {log_file}")

    print("\n" + "="*90)
    print(f" NBA PLAYER PROP +EV REPORT (UPGRADED) - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*90)
    
    if not top_props:
        print("No high-confidence player props found meeting thresholds.")
    else:
        print(f"{'PLAYER':<22} | {'TEAM':<5} | {'PROP':<10} | {'LINE':<5} | {'SIDE':<5} | {'MODEL':<22} | {'BOOK':<12} | {'ODDS':<6} | {'EV%'}")
        print("-" * 120)
        for p in top_props:
            print(f"{p['player']:<22} | {p.get('team', '???'):<5} | {p['metric']:<10} | {p['line']:<5} | {p['side']:<5} | {p['model']:<22} | {p['book']:<12} | {p['odds']:>6} | {p['ev']*100:>6.1f}%")
    print("\n" + "="*110)

if __name__ == "__main__":
    main()
