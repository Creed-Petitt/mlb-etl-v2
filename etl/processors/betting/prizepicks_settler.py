#!/usr/bin/env python3
"""
PrizePicks Settlement Processor
Settles completed projections by comparing to actual box score results
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from models.database import get_session
from models.mlb_models import Game, BoxScore
from models.betting_models import (
    PrizePicksPlayer,
    PrizePicksProjection,
    PrizePicksSettlement
)

logger = logging.getLogger(__name__)

# Supported stat calculations from box scores
BATTER_STATS = {
    "Hits": lambda bs: bs.hits or 0,
    "Runs": lambda bs: bs.runs or 0,
    "RBIs": lambda bs: bs.rbi or 0,
    "Walks": lambda bs: bs.walks or 0,
    "Home Runs": lambda bs: bs.home_runs or 0,
    "Hitter Strikeouts": lambda bs: bs.strikeouts or 0,
    "Doubles": lambda bs: bs.doubles or 0,
    "Singles": lambda bs: max(0, (bs.hits or 0) - (bs.doubles or 0) - (bs.triples or 0) - (bs.home_runs or 0)),
    "Total Bases": lambda bs: (
        max(0, (bs.hits or 0) - (bs.doubles or 0) - (bs.triples or 0) - (bs.home_runs or 0))  # singles
        + 2 * (bs.doubles or 0)
        + 3 * (bs.triples or 0)
        + 4 * (bs.home_runs or 0)
    ),
    "Hits+Runs+RBIs": lambda bs: (bs.hits or 0) + (bs.runs or 0) + (bs.rbi or 0),
    # "Stolen Bases": lambda bs: bs.stolen_bases or 0,  # Not available in current BoxScore model
}

PITCHER_STATS = {
    "Pitcher Strikeouts": lambda bs: bs.pitcher_strikeouts or 0,
    "Hits Allowed": lambda bs: bs.pitcher_hits or 0,
    "Walks Allowed": lambda bs: bs.pitcher_walks or 0,
    "Earned Runs Allowed": lambda bs: bs.earned_runs or 0,
    "Pitching Outs": lambda bs: int(round((bs.innings_pitched or 0.0) * 3)),
    "Pitches Thrown": lambda bs: None,  # Not available in box scores
    "1st Inning Runs Allowed": lambda bs: None,  # Not available in box scores
    "1st Inning Walks Allowed": lambda bs: None,  # Not available in box scores
}

class PrizePicksSettler:
    def __init__(self, session: Session = None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'projections_checked': 0,
            'already_settled': 0,
            'settled': 0,
            'no_player_match': 0,
            'no_game_match': 0,
            'unsupported_stat': 0,
            'errors': 0
        }
    
    def normalize_player_name(self, name: str) -> str:
        """Normalize player names for matching"""
        if not name:
            return ""
        
        # Remove common suffixes
        name = name.replace(" Jr.", "").replace(" Sr.", "").replace(" III", "").replace(" II", "")
        # Remove periods from initials
        name = name.replace(".", "")
        # Normalize spaces
        name = " ".join(name.split())
        
        return name.strip()
    
    def calculate_actual_value(self, stat_type: str, box_scores: List[BoxScore]) -> Optional[float]:
        """Calculate actual stat value from box scores"""
        
        # Check if stat is supported
        calc_func = None
        if stat_type in BATTER_STATS:
            calc_func = BATTER_STATS[stat_type]
        elif stat_type in PITCHER_STATS:
            calc_func = PITCHER_STATS[stat_type]
        else:
            return None
        
        # Calculate total across all games (for doubleheaders)
        total = 0
        for bs in box_scores:
            value = calc_func(bs)
            if value is None:
                return None
            total += value
        
        return float(total)
    
    def determine_outcome(self, actual: float, line: float) -> str:
        """Determine if result is over, under, or push"""
        epsilon = 0.001  # Small tolerance for floating point comparison
        
        if abs(actual - line) < epsilon:
            return "push"
        elif actual > line:
            return "over"
        else:
            return "under"
    
    def settle_projection(self, projection: PrizePicksProjection) -> bool:
        """Settle a single projection"""
        try:
            # Check if already settled
            existing = self.session.query(PrizePicksSettlement).filter_by(
                projection_id=projection.id
            ).first()
            
            if existing:
                self.stats['already_settled'] += 1
                return False
            
            # Get player info
            if not projection.player_id:
                self.stats['no_player_match'] += 1
                return False
            
            player = self.session.query(PrizePicksPlayer).filter_by(
                id=projection.player_id
            ).first()
            
            if not player or not player.name:
                self.stats['no_player_match'] += 1
                return False
            
            # Check if stat type is supported
            if projection.stat_type not in BATTER_STATS and projection.stat_type not in PITCHER_STATS:
                self.stats['unsupported_stat'] += 1
                # Don't even log for known unsupported types
                if projection.stat_type not in ["Stolen Bases", "Hitter Fantasy Score", "Pitcher Fantasy Score"]:
                    logger.debug(f"Unsupported stat type: {projection.stat_type}")
                return False
            
            # Get game date from start_time
            if not projection.start_time:
                return False
            
            game_date = projection.start_time.date()
            
            # Try exact name match first
            player_name = player.name
            box_scores = (
                self.session.query(BoxScore)
                .join(Game, Game.game_pk == BoxScore.game_pk)
                .filter(
                    Game.official_date == game_date,
                    BoxScore.player_name == player_name
                )
                .all()
            )
            
            # If no exact match, try normalized name
            if not box_scores:
                normalized_name = self.normalize_player_name(player_name)
                box_scores = (
                    self.session.query(BoxScore)
                    .join(Game, Game.game_pk == BoxScore.game_pk)
                    .filter(Game.official_date == game_date)
                    .all()
                )
                # Filter by normalized name
                box_scores = [
                    bs for bs in box_scores 
                    if self.normalize_player_name(bs.player_name) == normalized_name
                ]
            
            if not box_scores:
                self.stats['no_game_match'] += 1
                logger.debug(f"No box score found for {player_name} on {game_date}")
                return False
            
            # Calculate actual value
            actual_value = self.calculate_actual_value(projection.stat_type, box_scores)
            if actual_value is None:
                self.stats['unsupported_stat'] += 1
                return False
            
            # Determine outcome
            line = float(projection.current_line_score)
            outcome = self.determine_outcome(actual_value, line)
            
            # Create settlement record
            settlement = PrizePicksSettlement(
                projection_id=projection.id,
                final_line_score=projection.current_line_score,
                actual_result=actual_value,
                settlement_result=outcome,
                settled_at=datetime.now(timezone.utc),
                game_pk=box_scores[0].game_pk if box_scores else None,
                player_name_used=player_name
            )
            
            self.session.add(settlement)
            
            # Mark projection as inactive
            projection.is_active = False
            projection.last_updated = datetime.now(timezone.utc)
            
            self.stats['settled'] += 1
            
            logger.info(
                f"Settled: {player_name} {projection.stat_type} "
                f"Line: {line} Actual: {actual_value} Result: {outcome}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error settling projection {projection.id}: {e}")
            self.stats['errors'] += 1
            return False
    
    def settle_all(self, days_back: int = 7, limit: Optional[int] = None):
        """Settle all eligible projections"""
        try:
            now = datetime.now(timezone.utc)
            cutoff_date = now - timedelta(days=days_back)
            
            # Query eligible projections
            query = (
                self.session.query(PrizePicksProjection)
                .filter(
                    PrizePicksProjection.start_time < now,
                    PrizePicksProjection.start_time > cutoff_date,
                    PrizePicksProjection.is_active == True
                )
                .order_by(PrizePicksProjection.start_time.desc())
            )
            
            if limit:
                query = query.limit(limit)
            
            projections = query.all()
            
            logger.info(f"Found {len(projections)} projections to check for settlement")
            self.stats['projections_checked'] = len(projections)
            
            # Process each projection
            for projection in projections:
                self.settle_projection(projection)
                
                # Commit periodically
                if self.stats['settled'] % 50 == 0 and self.stats['settled'] > 0:
                    self.session.commit()
                    logger.info(f"Committed {self.stats['settled']} settlements so far...")
            
            # Final commit
            self.session.commit()
            
            # Log final stats
            logger.info("Settlement complete:")
            for key, value in self.stats.items():
                logger.info(f"  {key}: {value}")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Settlement run failed: {e}")
            self.session.rollback()
            raise
        finally:
            if self.owns_session:
                self.session.close()

def main():
    """Run settlement for recent projections"""
    import sys
    sys.path.append('/home/highs/mlb-scraping-dashboardv2')
    
    from core.logger import setup_logger
    setup_logger('prizepicks_settler')
    
    settler = PrizePicksSettler()
    
    # Settle projections from last 30 days
    stats = settler.settle_all(days_back=30)
    
    print("\nSettlement Summary:")
    print(f"  Projections Checked: {stats['projections_checked']}")
    print(f"  Already Settled: {stats['already_settled']}")
    print(f"  Newly Settled: {stats['settled']}")
    print(f"  No Player Match: {stats['no_player_match']}")
    print(f"  No Game Match: {stats['no_game_match']}")
    print(f"  Unsupported Stats: {stats['unsupported_stat']}")
    print(f"  Errors: {stats['errors']}")
    
    return 0 if stats['errors'] == 0 else 1

if __name__ == "__main__":
    exit(main())