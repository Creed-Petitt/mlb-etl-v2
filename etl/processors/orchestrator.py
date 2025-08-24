#!/usr/bin/env python3
"""
Game data processor - orchestrates modular processors for single games
Refactored for better modularity and maintainability
"""

from datetime import datetime
from sqlalchemy.exc import IntegrityError

from ..utils import get_session, get_etl_logger
from models import StatcastPitch, BattedBall, GameWPA, BoxScore, GameLineScore

# Import new modular processors
from .core_game_processor import CoreGameProcessor
from .player_data_processor import PlayerDataProcessor
from .pitch_data_processor import PitchDataProcessor
from .box_score_processor import BoxScoreProcessor
from .stats_processor import StatsProcessor
from .season_stats_processor import SeasonStatsProcessor

logger = get_etl_logger("game_data_processor")

class GameDataProcessor:
    """Orchestrates modular processors for single game data loading"""
    
    def __init__(self):
        self.session = get_session()
        
        # Initialize modular processors with shared session
        self.core_processor = CoreGameProcessor(self.session)
        self.player_processor = PlayerDataProcessor(self.session)
        self.pitch_processor = PitchDataProcessor(self.session)
        self.box_score_processor = BoxScoreProcessor(self.session)
        self.stats_processor = StatsProcessor(self.session)  # For WPA only
        self.season_stats_processor = SeasonStatsProcessor(self.session)
        
        self.stats = {
            'players_loaded': 0,
            'pitches_loaded': 0,
            'batted_balls_loaded': 0,
            'box_scores_loaded': 0,
            'wpa_loaded': 0,
            'line_scores_loaded': 0,
            'games_loaded': 0,
            'venues_loaded': 0,
            'player_season_stats_loaded': 0,
            'team_season_stats_loaded': 0
        }
        
    def process_game(self, game_data):
        """
        Process complete game data using modular processors
        Returns True if successful, False otherwise
        """
        try:
            # Reset stats
            self.stats = {k: 0 for k in self.stats}
            
            # Clean existing data first (if any)
            game_pk = self._extract_game_pk(game_data)
            if game_pk:
                self._clean_existing_data(game_pk)
            
            # 1. Process core game data (metadata, venue, line scores)
            game_pk = self.core_processor.process_core_game_data(game_data)
            if not game_pk:
                logger.error("Failed to process core game data")
                return False
            
            # 2. Process player data
            if not self.player_processor.process_player_data(game_data):
                logger.warning("Failed to process player data")
            
            # 3. Process pitch data
            if not self.pitch_processor.process_pitch_data(game_data, game_pk):
                logger.warning("Failed to process pitch data")
            
            # 4. Process box scores (batting and pitching stats)
            if not self.box_score_processor.process_box_scores(game_data, game_pk):
                logger.warning("Failed to process box scores")
            
            # 4b. Process WPA data
            if not self.stats_processor._load_wpa_data(game_data, game_pk):
                logger.warning("Failed to process WPA data")
            
            # 5. Process season stats (only for most recent games)
            if not self.season_stats_processor.process_season_stats(game_data, game_pk):
                logger.warning("Failed to process season stats")
            # Collect stats from all processors
            self._collect_stats_from_processors()
            
            # Commit all changes
            self.session.commit()
            
            self._log_completion_stats(game_pk)
            return True
            
        except Exception as e:
            logger.error(f"Error processing game: {e}")
            self.session.rollback()
            return False
            
    def _extract_game_pk(self, game_data):
        """Extract game_pk from various possible locations"""
        # Try scoreboard first
        scoreboard = game_data.get('scoreboard', {})
        game_pk = scoreboard.get('gamePk')
        
        if game_pk:
            return game_pk
            
        # Try top level
        game_pk = game_data.get('game_pk') or game_data.get('gamePk')
        return game_pk
        
    def _clean_existing_data(self, game_pk):
        """Delete all existing data for fresh load"""
        try:
            logger.info(f"Cleaning existing data for game {game_pk}")
            
            # Delete all related data
            self.session.query(StatcastPitch).filter_by(game_pk=game_pk).delete()
            self.session.query(BattedBall).filter_by(game_pk=game_pk).delete()
            self.session.query(GameWPA).filter_by(game_pk=game_pk).delete()
            self.session.query(BoxScore).filter_by(game_pk=game_pk).delete()
            self.session.query(GameLineScore).filter_by(game_pk=game_pk).delete()
            self.session.commit()
            
        except Exception as e:
            logger.error(f"Error cleaning existing data for game {game_pk}: {e}")
    
    def _collect_stats_from_processors(self):
        """Collect statistics from all modular processors"""
        core_stats = self.core_processor.get_stats()
        player_stats = self.player_processor.get_stats()
        pitch_stats = self.pitch_processor.get_stats()
        box_score_stats = self.box_score_processor.stats
        wpa_stats = self.stats_processor.stats
        season_stats = self.season_stats_processor.get_stats()
        
        # Merge all stats
        self.stats.update({
            'games_loaded': core_stats.get('games_loaded', 0),
            'venues_loaded': core_stats.get('venues_loaded', 0),
            'line_scores_loaded': core_stats.get('line_scores_loaded', 0),
            'players_loaded': player_stats.get('players_loaded', 0),
            'pitches_loaded': pitch_stats.get('pitches_loaded', 0),
            'batted_balls_loaded': pitch_stats.get('batted_balls_loaded', 0),
            'box_scores_loaded': box_score_stats.get('box_scores_loaded', 0),
            'wpa_loaded': wpa_stats.get('wpa_loaded', 0),
            'player_season_stats_loaded': season_stats.get('player_season_stats_loaded', 0),
            'team_season_stats_loaded': season_stats.get('team_season_stats_loaded', 0)
        })
    
    def _log_completion_stats(self, game_pk):
        """Log comprehensive completion statistics"""
        logger.info(f"GAME {game_pk} LOADED SUCCESSFULLY:")
        logger.info(f"  Games: {self.stats['games_loaded']}")
        logger.info(f"  Venues: {self.stats['venues_loaded']}")
        logger.info(f"  Line scores: {self.stats['line_scores_loaded']}")
        logger.info(f"  Players: {self.stats['players_loaded']}")
        logger.info(f"  Pitches: {self.stats['pitches_loaded']}")
        logger.info(f"  Batted balls: {self.stats['batted_balls_loaded']}")
        logger.info(f"  Box scores: {self.stats['box_scores_loaded']}")
        logger.info(f"  WPA records: {self.stats['wpa_loaded']}")
        logger.info(f"  Player season stats: {self.stats['player_season_stats_loaded']}")
        logger.info(f"  Team season stats: {self.stats['team_season_stats_loaded']}")
        
    def close(self):
        """Close database session and all processors"""
        if self.core_processor:
            self.core_processor.close()
        if self.player_processor:
            self.player_processor.close()
        if self.pitch_processor:
            self.pitch_processor.close()
        if self.stats_processor:
            self.stats_processor.close()
        if self.season_stats_processor:
            self.season_stats_processor.close()
        if self.session:
            self.session.close()