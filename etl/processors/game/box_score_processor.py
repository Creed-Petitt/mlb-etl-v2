#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any

from models import BoxScore, get_session

logger = logging.getLogger(__name__)

class BoxScoreProcessor:
    """Processes game-specific player statistics (batting, pitching, fielding)"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self._owns_session = session is None  # Track if we created the session
        self.stats = {
            'box_scores_loaded': 0,
            'batting_records': 0,
            'pitching_records': 0,
            'fielding_records': 0
        }
    
    def process_box_scores(self, game_data: Dict[str, Any], game_pk: int) -> bool:
        """
        Process box score data for all players in the game
        Handles batting stats, pitching stats, and fielding stats
        """
        try:
            
            # Reset stats
            self.stats = {k: 0 for k in self.stats}
            
            # Get boxscore data from the API response
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            
            for team_type in ['home', 'away']:
                team_data = teams.get(team_type, {})
                players = team_data.get('players', {})
                
                for player_key, player_data in players.items():
                    if not player_key.startswith('ID'):
                        continue
                    
                    self._process_player_box_score(player_data, player_key, team_type, game_pk)
            
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing box scores: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _process_player_box_score(self, player_data: Dict, player_key: str, team_type: str, game_pk: int):
        """Process box score data for a single player"""
        try:
            player_id = int(player_key.replace('ID', ''))
            person = player_data.get('person', {})
            player_name = person.get('fullName', f'Player {player_id}')
            
            # Get position
            position_info = player_data.get('position', {})
            position = position_info.get('name', 'Unknown')
            
            # Check if player already has box score record
            existing_box = self.session.query(BoxScore).filter_by(
                game_pk=game_pk, 
                player_id=player_id,
                team_type=team_type
            ).first()
            
            # If record exists, we might be backfilling missing pitching stats
            if existing_box:
                updated = self._update_existing_box_score(existing_box, player_data)
                return
            
            # Create new box score record
            box_score = self._create_box_score_record(
                player_data, player_id, player_name, position, team_type, game_pk
            )
            
            if box_score:
                self.session.add(box_score)
                self.stats['box_scores_loaded'] += 1
            
        except Exception as e:
            logger.error(f"Error processing player {player_key}: {e}")
    
    def _create_box_score_record(self, player_data: Dict, player_id: int, player_name: str, 
                                position: str, team_type: str, game_pk: int) -> BoxScore:
        """Create a new box score record with batting, pitching, and fielding stats"""
        
        # Get stats from player data
        stats = player_data.get('stats', {})
        batting_stats = stats.get('batting', {})
        pitching_stats = stats.get('pitching', {})
        fielding_stats = stats.get('fielding', {})
        
        # Get batting order if available
        batting_order = player_data.get('battingOrder')
        if batting_order and batting_order.isdigit():
            batting_order = int(batting_order)
        else:
            batting_order = None
        
        # Determine if this player has meaningful stats
        has_batting = bool(batting_stats and (batting_stats.get('atBats', 0) > 0 or 
                                            batting_stats.get('plateAppearances', 0) > 0))
        has_pitching = bool(pitching_stats and (pitching_stats.get('inningsPitched') or 
                                              pitching_stats.get('battersFaced', 0) > 0))
        
        # Skip players with no meaningful stats (bench players, etc.)
        if not has_batting and not has_pitching:
            return None
        
        # Create box score record
        box_score = BoxScore(
            game_pk=game_pk,
            player_id=player_id,
            team_type=team_type,
            player_name=player_name,
            position=position,
            batting_order=batting_order,
            created_at=datetime.now()
        )
        
        # Add batting stats
        if has_batting:
            self._add_batting_stats(box_score, batting_stats)
            self.stats['batting_records'] += 1
        
        # Add pitching stats
        if has_pitching:
            self._add_pitching_stats(box_score, pitching_stats)
            self.stats['pitching_records'] += 1
        
        # Add fielding stats (if available)
        if fielding_stats:
            self._add_fielding_stats(box_score, fielding_stats)
            self.stats['fielding_records'] += 1
        
        return box_score
    
    def _add_batting_stats(self, box_score: BoxScore, batting_stats: Dict):
        """Add batting statistics to box score record"""
        box_score.at_bats = batting_stats.get('atBats', 0)
        box_score.runs = batting_stats.get('runs', 0)
        box_score.hits = batting_stats.get('hits', 0)
        box_score.rbi = batting_stats.get('rbi', 0)
        box_score.walks = batting_stats.get('baseOnBalls', 0)
        box_score.strikeouts = batting_stats.get('strikeOuts', 0)
        box_score.doubles = batting_stats.get('doubles', 0)
        box_score.triples = batting_stats.get('triples', 0)
        box_score.home_runs = batting_stats.get('homeRuns', 0)
        
    
    def _add_pitching_stats(self, box_score: BoxScore, pitching_stats: Dict):
        """Add pitching statistics to box score record"""
        # Handle innings pitched - can be string like "5.1" or float
        innings_pitched_raw = pitching_stats.get('inningsPitched')
        if innings_pitched_raw:
            try:
                box_score.innings_pitched = float(innings_pitched_raw)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse innings pitched: {innings_pitched_raw}")
                box_score.innings_pitched = 0.0
        else:
            box_score.innings_pitched = 0.0
        
        box_score.earned_runs = pitching_stats.get('earnedRuns', 0)
        box_score.pitcher_hits = pitching_stats.get('hits', 0)
        box_score.pitcher_walks = pitching_stats.get('baseOnBalls', 0)
        box_score.pitcher_strikeouts = pitching_stats.get('strikeOuts', 0)
        
    
    def _add_fielding_stats(self, box_score: BoxScore, fielding_stats: Dict):
        """Add fielding statistics to box score record (future enhancement)"""
        # Note: Current BoxScore model doesn't have fielding columns
        # This is a placeholder for future enhancement
        # Fielding stats available but not stored (model limitation)
        pass
    
    def _update_existing_box_score(self, existing_box: BoxScore, player_data: Dict) -> bool:
        """Update existing box score record with missing data (for backfill)"""
        try:
            updated = False
            stats = player_data.get('stats', {})
            pitching_stats = stats.get('pitching', {})
            
            # Check if we need to add missing pitching stats
            if pitching_stats and existing_box.innings_pitched is None:
                self._add_pitching_stats(existing_box, pitching_stats)
                updated = True
            
            return updated
            
        except Exception as e:
            logger.error(f"Error updating existing box score: {e}")
            return False
    
    def close(self):
        """Clean up resources"""
        if self.session and self._owns_session:
            self.session.close()