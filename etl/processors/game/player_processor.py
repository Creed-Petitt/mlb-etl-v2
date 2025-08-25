#!/usr/bin/env python3

import logging
from datetime import datetime

from models import Player, get_session

logger = logging.getLogger(__name__)

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
        """Load all player data using reliable boxscore data"""
        try:
            players_seen = set()
            
            # Use boxscore data which has reliable player names
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            
            for team_type in ['home', 'away']:
                team_data = teams.get(team_type, {})
                players = team_data.get('players', {})
                
                logger.debug(f"Processing {len(players)} players from {team_type} team boxscore")
                
                for player_key, player_data in players.items():
                    if not player_key.startswith('ID'):
                        continue
                    
                    try:
                        mlb_id = int(player_key.replace('ID', ''))
                    except ValueError:
                        continue
                    
                    if mlb_id in players_seen:
                        continue
                    players_seen.add(mlb_id)
                    
                    # Check if player exists
                    existing_player = self.session.query(Player).filter_by(mlb_id=mlb_id).first()
                    if existing_player:
                        continue
                    
                    # Extract reliable player information from boxscore
                    person = player_data.get('person', {})
                    full_name = person.get('fullName', f'Player {mlb_id}')
                    
                    # Split name into first and last
                    name_parts = full_name.split(' ', 1)
                    first_name = name_parts[0] if len(name_parts) > 0 else ''
                    last_name = name_parts[1] if len(name_parts) > 1 else ''
                    
                    # Get position and batting/pitching info
                    position_info = player_data.get('position', {})
                    primary_position_name = position_info.get('name', 'Unknown')
                    
                    # Get batting and pitching stats for handedness
                    stats = player_data.get('stats', {})
                    batting = stats.get('batting', {})
                    pitching = stats.get('pitching', {})
                    
                    # Extract handedness info - use person data as primary source
                    bat_side = person.get('batSide', {}).get('code') if person.get('batSide') else None
                    pitch_hand = person.get('pitchHand', {}).get('code') if person.get('pitchHand') else None
                    
                    # Validate player data before adding
                    if self._validate_player_data(mlb_id, full_name):
                        player = Player(
                            mlb_id=mlb_id,
                            full_name=full_name,
                            first_name=first_name,
                            last_name=last_name,
                            active=True,  # Assume active since they're playing
                            primary_position_name=primary_position_name,
                            bat_side_code=bat_side,
                            pitch_hand_code=pitch_hand,
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        
                        self.session.add(player)
                        self.stats['players_loaded'] += 1
                        logger.debug(f"Added new player: {full_name} (ID: {mlb_id})")
                    else:
                        logger.warning(f"Skipped potentially corrupted player data: {full_name} (ID: {mlb_id})")
            
            logger.debug(f"Loaded {self.stats['players_loaded']} new players from boxscore data")
            return True
            
        except Exception as e:
            logger.error(f"Error loading players: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _validate_player_data(self, mlb_id: int, full_name: str) -> bool:
        """Validate player data to prevent corruption"""
        
        # Basic sanity checks
        if not full_name or full_name.strip() == '' or full_name == f'Player {mlb_id}':
            return False
            
        # Check for obviously wrong patterns
        suspicious_patterns = [
            'unknown', 'error', 'null', 'undefined',
            'player player', 'test', 'temp'
        ]
        
        full_name_lower = full_name.lower()
        for pattern in suspicious_patterns:
            if pattern in full_name_lower:
                return False
                
        # Length check - names should be reasonable
        if len(full_name) < 3 or len(full_name) > 50:
            return False
            
        # Check for existing player with same name but different ID (potential duplicate/corruption)
        existing = self.session.query(Player).filter_by(full_name=full_name).first()
        if existing and existing.mlb_id != mlb_id:
            logger.warning(f"Potential duplicate: {full_name} exists with ID {existing.mlb_id}, new ID {mlb_id}")
            
        return True
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()