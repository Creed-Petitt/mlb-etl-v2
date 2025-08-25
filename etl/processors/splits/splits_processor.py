#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, List, Optional

from models import PlayerSplits, PitcherSplits, get_session

logger = logging.getLogger(__name__)

class SplitsProcessor:
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        
        # Stats tracking
        self.stats = {
            'splits_processed': 0,
            'splits_loaded': 0,
            'splits_failed': 0
        }
        
        # Split categorization mapping
        self.category_mapping = {
            # Platoon splits
            'vr': 'platoon',
            'vl': 'platoon',
            
            # Location/time
            'h': 'location',
            'a': 'location',
            'd': 'time',
            'n': 'time',
            'g': 'surface',
            't': 'surface',
            
            # Situational/leverage
            'risp': 'situational',
            'risp2': 'situational', 
            'lc': 'leverage',
            'sah': 'score',
            'sbh': 'score',
            'sti': 'score',
            'ac': 'count',
            'bc': 'count',
            '2s': 'count',
            'ron': 'situational',
            'ron2': 'situational',
            'r0': 'situational',
            
            # Season timing
            '4': 'monthly',
            '5': 'monthly', 
            '6': 'monthly',
            '7': 'monthly',
            '8': 'monthly',
            '9': 'monthly',
            
            # Pitching specific
            'sp': 'role',
            'rp': 'role',
            'pi000': 'fatigue'
        }
    
    def process_hitting_split(self, api_response: Dict, season: int, sitcode: str, description: str) -> List[PlayerSplits]:
        splits_records = []
        
        try:
            players_data = api_response.get('stats', [])
            
            for player_data in players_data:
                split_record = self._create_hitting_split_record(
                    player_data, season, sitcode, description
                )
                
                if split_record:
                    splits_records.append(split_record)
                    self.stats['splits_processed'] += 1
                    
        except Exception as e:
            logger.error(f"Error processing hitting split {sitcode}: {e}")
            self.stats['splits_failed'] += 1
            
        return splits_records
    
    def process_pitching_split(self, api_response: Dict, season: int, sitcode: str, description: str) -> List[PitcherSplits]:
        splits_records = []
        
        try:
            players_data = api_response.get('stats', [])
            
            for player_data in players_data:
                split_record = self._create_pitching_split_record(
                    player_data, season, sitcode, description
                )
                
                if split_record:
                    splits_records.append(split_record)
                    self.stats['splits_processed'] += 1
                    
        except Exception as e:
            logger.error(f"Error processing pitching split {sitcode}: {e}")
            self.stats['splits_failed'] += 1
            
        return splits_records
    
    def _create_hitting_split_record(self, player_data: Dict, season: int, sitcode: str, description: str) -> Optional[PlayerSplits]:
        try:
            player_id = player_data.get('playerId')
            if not player_id:
                return None
                
            # Get split categorization
            split_category = self.category_mapping.get(sitcode, 'other')
            
            # Extract hitting stats (stats are at top level, not nested)
            stats = player_data
            
            split_record = PlayerSplits(
                player_id=player_id,
                season=season,
                split_category=split_category,
                split_type=sitcode,
                split_value=description,
                
                # Basic stats
                games=stats.get('gamesPlayed'),
                games_started=stats.get('gamesStarted'),
                plate_appearances=stats.get('plateAppearances'),
                at_bats=stats.get('atBats'),
                runs=stats.get('runs'),
                hits=stats.get('hits'),
                doubles=stats.get('doubles'),
                triples=stats.get('triples'),
                home_runs=stats.get('homeRuns'),
                rbi=stats.get('rbi'),
                walks=stats.get('baseOnBalls'),
                strikeouts=stats.get('strikeOuts'),
                stolen_bases=stats.get('stolenBases'),
                caught_stealing=stats.get('caughtStealing'),
                
                # Advanced stats
                batting_average=stats.get('avg'),
                on_base_percentage=stats.get('obp'),
                slugging_percentage=stats.get('slg'),
                ops=stats.get('ops'),
                
                # Timestamps
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            return split_record
            
        except Exception as e:
            logger.error(f"Error creating hitting split record for player {player_data.get('playerId', 'unknown')}: {e}")
            return None
    
    def _create_pitching_split_record(self, player_data: Dict, season: int, sitcode: str, description: str) -> Optional[PitcherSplits]:
        try:
            player_id = player_data.get('playerId')
            if not player_id:
                return None
                
            # Get split categorization
            split_category = self.category_mapping.get(sitcode, 'other')
            
            # Extract pitching stats (stats are at top level, not nested)
            stats = player_data
            
            split_record = PitcherSplits(
                pitcher_id=player_id,
                season=season,
                split_category=split_category,
                split_type=sitcode,
                split_value=description,
                
                # Pitching stats
                games=stats.get('gamesPlayed'),
                games_started=stats.get('gamesStarted'),
                wins=stats.get('wins'),
                losses=stats.get('losses'),
                era=stats.get('era'),
                innings_pitched=stats.get('inningsPitched'),
                hits_allowed=stats.get('hits'),
                runs_allowed=stats.get('runs'),
                earned_runs=stats.get('earnedRuns'),
                home_runs_allowed=stats.get('homeRuns'),
                walks_allowed=stats.get('baseOnBalls'),
                strikeouts_pitched=stats.get('strikeOuts'),
                whip=stats.get('whip'),
                
                # Timestamps
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            return split_record
            
        except Exception as e:
            logger.error(f"Error creating pitching split record for player {player_data.get('playerId', 'unknown')}: {e}")
            return None
    
    def bulk_upsert_splits(self, splits_records) -> int:
        # Create a new session for this operation to avoid concurrency issues
        session = get_session()
        
        try:
            loaded_count = 0
            
            for split_record in splits_records:
                # Determine the model type and appropriate query
                if isinstance(split_record, PlayerSplits):
                    existing = session.query(PlayerSplits).filter_by(
                        player_id=split_record.player_id,
                        season=split_record.season,
                        split_type=split_record.split_type
                    ).first()
                elif isinstance(split_record, PitcherSplits):
                    existing = session.query(PitcherSplits).filter_by(
                        pitcher_id=split_record.pitcher_id,
                        season=split_record.season,
                        split_type=split_record.split_type
                    ).first()
                else:
                    continue
                
                if existing:
                    # Update existing record
                    for attr in split_record.__dict__:
                        if not attr.startswith('_') and attr != 'id':
                            setattr(existing, attr, getattr(split_record, attr))
                    existing.updated_at = datetime.now()
                else:
                    # Insert new record
                    session.add(split_record)
                
                loaded_count += 1
            
            session.commit()
            self.stats['splits_loaded'] += loaded_count
            return loaded_count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error bulk upserting splits: {e}")
            return 0
        finally:
            session.close()
    
    def get_stats(self) -> Dict:
        return self.stats.copy()
        
    def close(self):
        if self.owns_session and self.session:
            self.session.close()