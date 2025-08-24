#!/usr/bin/env python3
"""
Core game processor - handles game metadata, venues, and line scores
Split from monolithic processor for better modularity
"""

from datetime import datetime

from ..utils import get_session, get_etl_logger
from models import Game, Venue, GameLineScore

logger = get_etl_logger("core_game_processor")

class CoreGameProcessor:
    """Handles core game data: metadata, venues, line scores"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'games_loaded': 0,
            'venues_loaded': 0,
            'line_scores_loaded': 0
        }
        
    def process_core_game_data(self, game_data):
        """
        Process core game data: metadata, venue, line scores
        Returns game_pk if successful, None otherwise
        """
        try:
            # 1. Load game metadata
            game_pk = self._load_game_metadata(game_data)
            if not game_pk:
                return None
            
            # 2. Load venue data
            self._load_venue_data(game_data)
            
            # 3. Load line scores
            self._load_line_scores(game_data, game_pk)
            
            return game_pk
            
        except Exception as e:
            logger.error(f"Error processing core game data: {e}")
            return None
    
    def _extract_venue_name(self, game_data):
        """Extract venue name from boxscore.info array"""
        try:
            # First try direct venue name
            venue_name = game_data.get('venue_name')
            if venue_name and venue_name != 'MLB Stadium':
                return venue_name
            
            # Try scoreboard venue
            scoreboard = game_data.get('scoreboard', {})
            venue_name = scoreboard.get('venue', {}).get('name')
            if venue_name and venue_name != 'MLB Stadium':
                return venue_name
            
            # Try boxscore.info array
            boxscore = game_data.get('boxscore', {})
            info_items = boxscore.get('info', [])
            
            for info_item in info_items:
                if isinstance(info_item, dict) and info_item.get('label') == 'Venue':
                    venue_value = info_item.get('value', '')
                    venue_name = venue_value.rstrip('.')
                    if venue_name:
                        return venue_name
            
            return f"MLB Venue {game_data.get('venue_id', 'Unknown')}"
            
        except Exception as e:
            logger.error(f"Error extracting venue name: {e}")
            return f"MLB Venue {game_data.get('venue_id', 'Unknown')}"
        
    def _load_game_metadata(self, game_data):
        """Load main game record with complete data"""
        try:
            # Extract game_pk from scoreboard
            scoreboard = game_data.get('scoreboard', {})
            game_pk = scoreboard.get('gamePk')
            
            if not game_pk:
                logger.error("Could not find game_pk in scoreboard data")
                return None
            
            # Check if game already exists and delete ALL related data for fresh load
            existing_game = self.session.query(Game).filter_by(game_pk=game_pk).first()
            if existing_game:
                logger.info(f"Game {game_pk} already exists, deleting for fresh load...")
                self.session.delete(existing_game)
                self.session.commit()
            
            # Extract complete game data
            linescore = scoreboard.get('linescore', {})
            teams = linescore.get('teams', {})
            home_team = teams.get('home', {})
            away_team = teams.get('away', {})
            
            # Parse game date
            game_date_str = game_data.get('gameDate', '3/17/2024')
            try:
                game_date = datetime.strptime(game_date_str, '%m/%d/%Y')
            except:
                game_date = datetime.now()
            
            # Extract venue
            venue_id = game_data.get('venue_id')
            
            # Extract team data from top-level fields
            home_team_data = game_data.get('home_team_data', {})
            away_team_data = game_data.get('away_team_data', {})
            
            game = Game(
                game_pk=game_pk,
                game_guid=scoreboard.get('gameGuid') or game_data.get('game_guid') or f"mlb-{game_pk}-{game_date.strftime('%Y%m%d')}",
                season="2025",
                game_type=game_data.get('gamedayType', 'S'),
                game_date=game_date,
                official_date=game_date.date(),
                home_team_id=game_data.get('team_home_id') or home_team_data.get('id'),
                home_team_name=home_team_data.get('teamName') or home_team_data.get('name'),
                home_team_abbreviation=home_team_data.get('abbreviation'),
                away_team_id=game_data.get('team_away_id') or away_team_data.get('id'),
                away_team_name=away_team_data.get('teamName') or away_team_data.get('name'),
                away_team_abbreviation=away_team_data.get('abbreviation'),
                venue_id=venue_id,
                venue_name=self._extract_venue_name(game_data),
                status_abstract=scoreboard.get('status', {}).get('abstractGameState', 'Final'),
                status_coded=scoreboard.get('status', {}).get('codedGameState', 'F'),
                status_detailed=scoreboard.get('status', {}).get('detailedState', 'Final'),
                current_inning=linescore.get('currentInning'),
                current_inning_ordinal=linescore.get('currentInningOrdinal'),
                inning_state=linescore.get('inningState'),
                scheduled_innings=linescore.get('scheduledInnings', 9),
                home_score=home_team.get('runs'),
                away_score=away_team.get('runs'),
                home_hits=home_team.get('hits'),
                away_hits=away_team.get('hits'),
                home_errors=home_team.get('errors'),
                away_errors=away_team.get('errors'),
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            self.session.add(game)
            self.stats['games_loaded'] += 1
            logger.debug(f"Created game metadata for {game_pk}")
            return game_pk
            
        except Exception as e:
            logger.error(f"Error loading game metadata: {e}")
            return None
            
    def _load_venue_data(self, game_data):
        """Load venue data with actual venue information"""
        try:
            venue_id = game_data.get('venue_id')
            if not venue_id:
                return True
            
            existing_venue = self.session.query(Venue).filter_by(mlb_id=venue_id).first()
            if existing_venue:
                return True
            
            # Get venue name from game data or scoreboard
            venue_name = (game_data.get('venue_name') or 
                         game_data.get('scoreboard', {}).get('venue', {}).get('name') or 
                         f"Venue {venue_id}")
            
            venue = Venue(
                mlb_id=venue_id,
                name=venue_name,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            self.session.add(venue)
            self.stats['venues_loaded'] += 1
            logger.debug(f"Created venue record for ID: {venue_id}, Name: {venue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading venue data: {e}")
            return False
        
    def _load_line_scores(self, game_data, game_pk):
        """Load inning-by-inning line scores"""
        try:
            scoreboard = game_data.get('scoreboard', {})
            linescore = scoreboard.get('linescore', {})
            innings = linescore.get('innings', [])
            
            for inning_data in innings:
                inning_num = inning_data.get('num')
                if not inning_num:
                    continue
                
                # Load home team inning
                home_data = inning_data.get('home', {})
                if home_data:
                    home_line = GameLineScore(
                        game_pk=game_pk,
                        inning=inning_num,
                        team_type='home',
                        runs=home_data.get('runs', 0),
                        hits=home_data.get('hits', 0),
                        errors=home_data.get('errors', 0),
                        left_on_base=home_data.get('leftOnBase', 0),
                        created_at=datetime.now()
                    )
                    self.session.add(home_line)
                    self.stats['line_scores_loaded'] += 1
                
                # Load away team inning
                away_data = inning_data.get('away', {})
                if away_data:
                    away_line = GameLineScore(
                        game_pk=game_pk,
                        inning=inning_num,
                        team_type='away',
                        runs=away_data.get('runs', 0),
                        hits=away_data.get('hits', 0),
                        errors=away_data.get('errors', 0),
                        left_on_base=away_data.get('leftOnBase', 0),
                        created_at=datetime.now()
                    )
                    self.session.add(away_line)
                    self.stats['line_scores_loaded'] += 1
            
            logger.debug(f"Loaded {self.stats['line_scores_loaded']} line score records")
            return True
            
        except Exception as e:
            logger.error(f"Error loading line scores: {e}")
            return False
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()