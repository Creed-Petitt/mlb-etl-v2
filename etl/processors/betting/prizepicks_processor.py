#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from models import (
    PrizePicksPlayer, PrizePicksTeam, PrizePicksGame, 
    PrizePicksProjection, get_session
)

logger = logging.getLogger(__name__)

class PrizePicksProcessor:
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        
        # Stats tracking
        self.stats = {
            'players_processed': 0,
            'teams_processed': 0,
            'games_processed': 0,
            'projections_processed': 0,
            'errors': 0
        }
        
    def process_included_data(self, included_items: List[Dict]) -> Dict:
        """Process included data (players, teams, games)"""
        
        # Separate included items by type
        players = [item for item in included_items if item.get('type') == 'new_player']
        teams = [item for item in included_items if item.get('type') == 'team']
        games = [item for item in included_items if item.get('type') == 'game']
        
        logger.info(f"Processing {len(players)} players, {len(teams)} teams, {len(games)} games")
        
        # Process each type
        self._upsert_players(players)
        self._upsert_teams(teams)
        self._upsert_games(games)
        
        return {
            'players': len(players),
            'teams': len(teams),
            'games': len(games)
        }
    
    def _upsert_players(self, players: List[Dict]):
        """Upsert player records"""
        for player_data in players:
            try:
                player_id = player_data.get('id')
                attrs = player_data.get('attributes', {})
                
                # Check if player exists
                existing = self.session.query(PrizePicksPlayer).filter_by(
                    prizepicks_player_id=player_id
                ).first()
                
                if existing:
                    # Update existing player
                    existing.name = attrs.get('name')
                    existing.display_name = attrs.get('display_name')
                    existing.team = attrs.get('team')
                    existing.team_name = attrs.get('team_name')
                    existing.position = attrs.get('position')
                    existing.jersey_number = attrs.get('jersey_number')
                    existing.image_url = attrs.get('image_url')
                    existing.updated_at = datetime.now()
                else:
                    # Create new player
                    player = PrizePicksPlayer(
                        prizepicks_player_id=player_id,
                        name=attrs.get('name'),
                        display_name=attrs.get('display_name'),
                        team=attrs.get('team'),
                        team_name=attrs.get('team_name'),
                        position=attrs.get('position'),
                        jersey_number=attrs.get('jersey_number'),
                        league=attrs.get('league', 'MLB'),
                        image_url=attrs.get('image_url')
                    )
                    self.session.add(player)
                
                self.stats['players_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing player {player_data.get('id')}: {e}")
                self.stats['errors'] += 1
    
    def _upsert_teams(self, teams: List[Dict]):
        """Upsert team records"""
        for team_data in teams:
            try:
                team_id = team_data.get('id')
                attrs = team_data.get('attributes', {})
                
                # Check if team exists
                existing = self.session.query(PrizePicksTeam).filter_by(
                    prizepicks_team_id=team_id
                ).first()
                
                if existing:
                    # Update existing team
                    existing.team_code = attrs.get('team')
                    existing.team_name = attrs.get('name')
                    existing.market = attrs.get('market')
                    existing.updated_at = datetime.now()
                else:
                    # Create new team
                    team = PrizePicksTeam(
                        prizepicks_team_id=team_id,
                        team_code=attrs.get('team'),
                        team_name=attrs.get('name'),
                        market=attrs.get('market'),
                        league=attrs.get('league', 'MLB')
                    )
                    self.session.add(team)
                
                self.stats['teams_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing team {team_data.get('id')}: {e}")
                self.stats['errors'] += 1
    
    def _upsert_games(self, games: List[Dict]):
        """Upsert game records"""
        for game_data in games:
            try:
                game_id = game_data.get('id')
                attrs = game_data.get('attributes', {})
                
                # Check if game exists
                existing = self.session.query(PrizePicksGame).filter_by(
                    prizepicks_game_id=game_id
                ).first()
                
                # Parse start time
                start_time = None
                start_time_str = attrs.get('start_time')
                if start_time_str:
                    try:
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                if existing:
                    # Update existing game
                    existing.external_game_id = attrs.get('game_id')
                    existing.start_time = start_time
                    existing.status = attrs.get('status')
                    existing.updated_at = datetime.now()
                else:
                    # Create new game
                    game = PrizePicksGame(
                        prizepicks_game_id=game_id,
                        external_game_id=attrs.get('game_id'),
                        start_time=start_time,
                        status=attrs.get('status')
                    )
                    self.session.add(game)
                
                self.stats['games_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing game {game_data.get('id')}: {e}")
                self.stats['errors'] += 1
    
    def process_projections(self, projections: List[Dict]) -> int:
        """Process projection records (player props)"""
        logger.info(f"Processing {len(projections)} projections")
        
        for proj_data in projections:
            try:
                proj_id = proj_data.get('id')
                attrs = proj_data.get('attributes', {})
                relationships = proj_data.get('relationships', {})
                
                # Get player reference
                player_ref = relationships.get('new_player', {}).get('data', {})
                player_id_ref = player_ref.get('id')
                
                # Get game reference
                game_ref = relationships.get('game', {}).get('data', {})
                game_id_ref = game_ref.get('id')
                
                # Look up player and game in database
                player = None
                game = None
                
                if player_id_ref:
                    player = self.session.query(PrizePicksPlayer).filter_by(
                        prizepicks_player_id=player_id_ref
                    ).first()
                
                if game_id_ref:
                    game = self.session.query(PrizePicksGame).filter_by(
                        prizepicks_game_id=game_id_ref
                    ).first()
                
                # Check if projection exists
                existing = self.session.query(PrizePicksProjection).filter_by(
                    prizepicks_id=proj_id
                ).first()
                
                # Parse timestamps
                start_time = None
                board_time = None
                
                start_time_str = attrs.get('start_time')
                if start_time_str:
                    try:
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                board_time_str = attrs.get('board_time')
                if board_time_str:
                    try:
                        board_time = datetime.fromisoformat(board_time_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                # Get current line
                current_line = float(attrs.get('line_score', 0))
                
                if existing:
                    # Update existing projection
                    existing.current_line_score = current_line
                    existing.status = attrs.get('status')
                    existing.description = attrs.get('description')
                    existing.is_live = attrs.get('is_live', False)
                    existing.is_promo = attrs.get('is_promo', False)
                    existing.odds_type = attrs.get('odds_type')
                    existing.last_updated = datetime.now()
                    
                    # Update player/game if found
                    if player:
                        existing.player_id = player.id
                    if game:
                        existing.game_id = game.id
                else:
                    # Create new projection
                    projection = PrizePicksProjection(
                        prizepicks_id=proj_id,
                        player_id=player.id if player else None,
                        game_id=game.id if game else None,
                        stat_type=attrs.get('stat_type'),
                        current_line_score=current_line,
                        description=attrs.get('description'),
                        status=attrs.get('status'),
                        start_time=start_time,
                        board_time=board_time,
                        last_updated=datetime.now(),
                        is_live=attrs.get('is_live', False),
                        is_promo=attrs.get('is_promo', False),
                        odds_type=attrs.get('odds_type'),
                        is_active=True
                    )
                    self.session.add(projection)
                
                self.stats['projections_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing projection {proj_data.get('id')}: {e}")
                self.stats['errors'] += 1
        
        return self.stats['projections_processed']
    
    def commit_changes(self):
        """Commit all changes to database"""
        try:
            self.session.commit()
            logger.info("Successfully committed all changes")
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing changes: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return self.stats.copy()
    
    def close(self):
        """Close session if we own it"""
        if self.owns_session and self.session:
            self.session.close()