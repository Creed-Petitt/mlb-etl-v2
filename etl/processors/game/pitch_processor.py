#!/usr/bin/env python3

import logging
from datetime import datetime

from models import StatcastPitch, BattedBall, get_session

logger = logging.getLogger(__name__)

class PitchDataProcessor:
    """Handles pitch-by-pitch and batted ball data processing"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'pitches_loaded': 0,
            'batted_balls_loaded': 0
        }
        
    def process_pitch_data(self, game_data, game_pk):
        """
        Process all pitch and batted ball data
        Returns True if successful, False otherwise
        """
        try:
            self._load_pitch_data(game_data, game_pk)
            return True
            
        except Exception as e:
            logger.error(f"Error processing pitch data: {e}")
            return False
        
    def _load_pitch_data(self, game_data, game_pk):
        """Load all pitch-by-pitch data"""
        try:
            for player_id, player_pitches in game_data.items():
                if not isinstance(player_pitches, list):
                    continue
                
                for pitch_data in player_pitches:
                    if not isinstance(pitch_data, dict):
                        continue
                    
                    pitch_id = pitch_data.get('play_id', f"{game_pk}_{pitch_data.get('pitch_number', 0)}")
                    
                    # Check if pitch exists
                    existing_pitch = self.session.query(StatcastPitch).filter_by(pitch_id=pitch_id).first()
                    if existing_pitch:
                        continue
                    
                    # Create pitch record with correct field mappings
                    pitch = StatcastPitch(
                        pitch_id=pitch_id,
                        game_pk=game_pk,
                        ab_number=pitch_data.get('ab_number'),
                        pitch_number=pitch_data.get('pitch_number'),
                        pitcher_id=pitch_data.get('pitcher'),
                        batter_id=pitch_data.get('batter'),
                        inning=pitch_data.get('inning'),
                        inning_half='top' if pitch_data.get('team_batting') != pitch_data.get('team_fielding') else 'bottom',
                        strikes=pitch_data.get('strikes'),
                        balls=pitch_data.get('balls'),
                        outs=pitch_data.get('outs'),
                        # Field mappings based on API response
                        pitch_type=pitch_data.get('pitch_type'),
                        pitch_name=pitch_data.get('pitch_name'),
                        release_speed=pitch_data.get('start_speed'),
                        release_pos_x=pitch_data.get('x0'),
                        release_pos_z=pitch_data.get('z0'),
                        release_extension=pitch_data.get('extension'),
                        pfx_x=pitch_data.get('pfxX'),
                        pfx_z=pitch_data.get('pfxZ'),
                        plate_x=pitch_data.get('px'),
                        plate_z=pitch_data.get('pz'),
                        vx0=pitch_data.get('vx0'),
                        vy0=pitch_data.get('vy0'),
                        vz0=pitch_data.get('vz0'),
                        ax=pitch_data.get('ax'),
                        ay=pitch_data.get('ay'),
                        az=pitch_data.get('az'),
                        release_spin_rate=pitch_data.get('spin_rate'),
                        pitch_result=pitch_data.get('call_name'),
                        play_result=pitch_data.get('result'),
                        zone=pitch_data.get('zone'),
                        created_at=datetime.now()
                    )
                    
                    self.session.add(pitch)
                    self.stats['pitches_loaded'] += 1
                    
                    # Create batted ball if applicable
                    if pitch_data.get('call') == 'X' and 'hc_x' in pitch_data and 'hc_y' in pitch_data:
                        batted_ball = BattedBall(
                            play_id=pitch_id,
                            game_pk=game_pk,
                            batter_id=pitch_data.get('batter'),
                            pitcher_id=pitch_data.get('pitcher'),
                            inning=pitch_data.get('inning'),
                            inning_half='top' if pitch_data.get('team_batting') != pitch_data.get('team_fielding') else 'bottom',
                            # Field mappings based on API response
                            exit_velocity=pitch_data.get('hit_speed'),
                            launch_angle=pitch_data.get('hit_angle'),
                            launch_speed=pitch_data.get('hit_speed'),
                            launch_direction=pitch_data.get('hit_angle'),
                            hit_distance=pitch_data.get('hit_distance'),
                            hit_coord_x=pitch_data.get('hc_x'),
                            hit_coord_y=pitch_data.get('hc_y'),
                            result=pitch_data.get('result'),
                            bb_type=pitch_data.get('call_name'),
                            created_at=datetime.now()
                        )
                        
                        self.session.add(batted_ball)
                        self.stats['batted_balls_loaded'] += 1
            
            logger.debug(f"Loaded {self.stats['pitches_loaded']} pitches and {self.stats['batted_balls_loaded']} batted balls")
            return True
            
        except Exception as e:
            logger.error(f"Error loading pitch data: {e}")
            return False
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()