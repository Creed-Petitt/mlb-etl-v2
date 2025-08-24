#!/usr/bin/env python3
"""
Stats processor - handles box scores and WPA data
Split from monolithic processor for better modularity
"""

import sys
import os
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from models import BoxScore, GameWPA
from ..utils import get_session, get_etl_logger

logger = get_etl_logger("stats_processor")

class StatsProcessor:
    """Handles box score and WPA statistics processing"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'box_scores_loaded': 0,
            'wpa_loaded': 0
        }
        
    def process_stats_data(self, game_data, game_pk):
        """
        Process box score and WPA statistics
        Returns True if successful, False otherwise
        """
        try:
            # Load box score data
            self._load_box_score_data(game_data, game_pk)
            
            # Load WPA data
            self._load_wpa_data(game_data, game_pk)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing stats data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _load_box_score_data(self, game_data, game_pk):
        """Load box score statistics from API boxscore data"""
        try:
            # Get boxscore data from the API response
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            
            for team_type in ['home', 'away']:
                team_data = teams.get(team_type, {})
                players = team_data.get('players', {})
                
                for player_key, player_data in players.items():
                    if not player_key.startswith('ID'):
                        continue
                    
                    player_id = int(player_key.replace('ID', ''))
                    person = player_data.get('person', {})
                    player_name = person.get('fullName', f'Player {player_id}')
                    
                    # Get batting stats from the boxscore
                    batting_stats = player_data.get('stats', {}).get('batting', {})
                    
                    # Skip if no batting stats
                    if not batting_stats:
                        continue
                    
                    # Get position
                    position_info = player_data.get('position', {})
                    position = position_info.get('name', 'Unknown')
                    
                    existing_box = self.session.query(BoxScore).filter_by(
                        game_pk=game_pk, 
                        player_id=player_id
                    ).first()
                    
                    if existing_box:
                        continue
                    
                    box_score = BoxScore(
                        game_pk=game_pk,
                        player_id=player_id,
                        team_type=team_type,
                        player_name=player_name,
                        position=position,
                        at_bats=batting_stats.get('atBats', 0),
                        runs=batting_stats.get('runs', 0),
                        hits=batting_stats.get('hits', 0),
                        rbi=batting_stats.get('rbi', 0),
                        walks=batting_stats.get('baseOnBalls', 0),
                        strikeouts=batting_stats.get('strikeOuts', 0),
                        created_at=datetime.now()
                    )
                    
                    self.session.add(box_score)
                    self.stats['box_scores_loaded'] += 1
            
            logger.debug(f"Loaded {self.stats['box_scores_loaded']} box score records from API boxscore data")
            return True
            
        except Exception as e:
            logger.error(f"Error loading box scores: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _load_wpa_data(self, game_data, game_pk):
        """Load WPA data from scoreboard.stats.wpa.gameWpa"""
        try:
            scoreboard = game_data.get('scoreboard', {})
            stats = scoreboard.get('stats', {})
            wpa_data = stats.get('wpa', {})
            game_wpa = wpa_data.get('gameWpa', [])
            
            logger.debug(f"Processing {len(game_wpa)} WPA records")
            
            for wpa_record in game_wpa:
                if not isinstance(wpa_record, dict):
                    continue
                
                # Extract inning info
                inning_str = wpa_record.get('i', '')
                if len(inning_str) >= 2:
                    inning_half = 'top' if inning_str[0] == 'T' else 'bottom'
                    try:
                        inning = int(inning_str[1:])
                    except ValueError:
                        inning = 1
                else:
                    inning_half = 'top'
                    inning = 1
                
                at_bat_index = wpa_record.get('atBatIndex', 0)
                play_id = f"{game_pk}_wpa_{at_bat_index}"
                
                wpa = GameWPA(
                    play_id=play_id,
                    game_pk=game_pk,
                    inning=inning,
                    inning_half=inning_half,
                    home_win_exp=wpa_record.get('homeTeamWinProbability', 0.0) / 100.0,
                    away_win_exp=wpa_record.get('awayTeamWinProbability', 0.0) / 100.0,
                    win_exp_added=wpa_record.get('homeTeamWinProbabilityAdded', 0.0) / 100.0,
                    batter_id=None,
                    pitcher_id=None,
                    created_at=datetime.now()
                )
                
                self.session.add(wpa)
                self.stats['wpa_loaded'] += 1
            
            logger.debug(f"Loaded {self.stats['wpa_loaded']} WPA records")
            return True
            
        except Exception as e:
            logger.error(f"Error loading WPA data: {e}")
            return False
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()