#!/usr/bin/env python3
"""
Player data processor - handles player extraction and creation
Split from monolithic processor for better modularity
"""

import sys
import os
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from models import Player
from ..utils import get_session, get_etl_logger

logger = get_etl_logger("player_data_processor")

class PlayerDataProcessor:
    """Handles player data extraction and database operations"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'players_loaded': 0
        }
        
    def process_player_data(self, game_data):
        """
        Process all player data from game
        Returns True if successful, False otherwise
        """
        try:
            self._load_all_players(game_data)
            return True
            
        except Exception as e:
            logger.error(f"Error processing player data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
        
    def _load_all_players(self, game_data):
        """Load all player data from the actual game data structure"""
        try:
            players_seen = set()
            
            # Extract players from home_batters, away_batters, home_pitchers, away_pitchers
            player_sources = [
                ('home_batters', game_data.get('home_batters', {})),
                ('away_batters', game_data.get('away_batters', {})),
                ('home_pitchers', game_data.get('home_pitchers', {})),
                ('away_pitchers', game_data.get('away_pitchers', {}))
            ]
            
            for source_name, source_data in player_sources:
                if not isinstance(source_data, dict):
                    continue
                
                # Each source_data is a dict where keys are player IDs and values are lists of play data
                for player_id_str, plays_list in source_data.items():
                    try:
                        mlb_id = int(player_id_str)  # Convert to int
                    except ValueError:
                        continue
                    
                    if mlb_id in players_seen:
                        continue
                    players_seen.add(mlb_id)
                    
                    # Check if player exists
                    existing_player = self.session.query(Player).filter_by(mlb_id=mlb_id).first()
                    if existing_player:
                        continue
                    
                    # Extract player information from first play in the list
                    if not isinstance(plays_list, list) or not plays_list:
                        continue
                    
                    first_play = plays_list[0]
                    if not isinstance(first_play, dict):
                        continue
                    
                    # Determine if this is batter or pitcher data and extract name accordingly
                    full_name = None
                    if 'batter_name' in first_play and first_play.get('batter') == mlb_id:
                        full_name = first_play.get('batter_name')
                    elif 'pitcher_name' in first_play and first_play.get('pitcher') == mlb_id:
                        full_name = first_play.get('pitcher_name')
                    
                    if not full_name:
                        full_name = f'Player {mlb_id}'
                    
                    # Split name into first and last
                    name_parts = full_name.split(' ', 1)
                    first_name = name_parts[0] if len(name_parts) > 0 else ''
                    last_name = name_parts[1] if len(name_parts) > 1 else ''
                    
                    # Determine position based on source
                    primary_position_name = None
                    if 'batter' in source_name:
                        # Extract position from stand (batting stance)
                        stand = first_play.get('stand', 'U')  # R, L, S for switch, U for unknown
                        primary_position_name = f"Batter ({stand})"
                    elif 'pitcher' in source_name:
                        # Extract throwing hand
                        p_throws = first_play.get('p_throws', 'U')  # R, L, U for unknown
                        primary_position_name = f"Pitcher ({p_throws})"
                    
                    player = Player(
                        mlb_id=mlb_id,
                        full_name=full_name,
                        first_name=first_name,
                        last_name=last_name,
                        active=True,  # Assume active since they're playing
                        primary_position_name=primary_position_name,
                        bat_side_code=first_play.get('stand') if 'batter' in source_name else None,
                        pitch_hand_code=first_play.get('p_throws') if 'pitcher' in source_name else None,
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    
                    self.session.add(player)
                    self.stats['players_loaded'] += 1
            
            logger.debug(f"Loaded {self.stats['players_loaded']} new players from pitch data")
            return True
            
        except Exception as e:
            logger.error(f"Error loading players: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()