#!/usr/bin/env python3

import sys
import argparse
import time
import logging
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from models import get_session
from etl.clients.prizepicks_client import PrizePicksClient
from etl.processors.betting.prizepicks_processor import PrizePicksProcessor

logger = logging.getLogger(__name__)

class PrizePicksLoader:
    
    def __init__(self):
        # Initialize components
        self.client = PrizePicksClient()
        self.processor = PrizePicksProcessor()
        
        # Stats tracking
        self.stats = {
            'start_time': None,
            'projections_found': 0,
            'included_items_found': 0,
            'players_processed': 0,
            'teams_processed': 0,
            'games_processed': 0,
            'projections_processed': 0,
            'errors': 0
        }
    
    def load_current_projections(self):
        """Load current PrizePicks projections"""
        logger.info("Starting PrizePicks projections load")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Fetch data from PrizePicks API
            projections, included = self.client.fetch_projections_data()
            
            if not projections and not included:
                logger.error("Failed to fetch PrizePicks data")
                self.stats['errors'] += 1
                return False
            
            self.stats['projections_found'] = len(projections)
            self.stats['included_items_found'] = len(included)
            
            # Process included data first (players, teams, games)
            included_stats = self.processor.process_included_data(included)
            self.stats['players_processed'] = included_stats.get('players', 0)
            self.stats['teams_processed'] = included_stats.get('teams', 0)
            self.stats['games_processed'] = included_stats.get('games', 0)
            
            # Process projections (player props)
            projections_count = self.processor.process_projections(projections)
            self.stats['projections_processed'] = projections_count
            
            # Commit all changes
            if self.processor.commit_changes():
                self._log_final_results()
                return True
            else:
                self.stats['errors'] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error in PrizePicks loading process: {e}")
            self.stats['errors'] += 1
            return False
    
    def _log_final_results(self):
        """Log final processing results"""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"PrizePicks loading completed in {elapsed:.1f}s")
        logger.info(f"Projections found: {self.stats['projections_found']}")
        logger.info(f"Included items found: {self.stats['included_items_found']}")
        logger.info(f"Players processed: {self.stats['players_processed']}")
        logger.info(f"Teams processed: {self.stats['teams_processed']}")
        logger.info(f"Games processed: {self.stats['games_processed']}")
        logger.info(f"Projections processed: {self.stats['projections_processed']}")
        
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        
        # Get processor stats
        processor_stats = self.processor.get_stats()
        if processor_stats.get('errors', 0) > 0:
            logger.warning(f"Processor errors: {processor_stats['errors']}")
    
    def close(self):
        """Close all resources"""
        if self.client:
            self.client.close()
        if self.processor:
            self.processor.close()

def main():
    parser = argparse.ArgumentParser(description='Load current PrizePicks projections data')
    
    args = parser.parse_args()
    
    loader = PrizePicksLoader()
    
    try:
        # Load current projections
        success = loader.load_current_projections()
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"PrizePicks loader failed: {e}")
        return 1
    finally:
        loader.close()

if __name__ == "__main__":
    sys.exit(main())