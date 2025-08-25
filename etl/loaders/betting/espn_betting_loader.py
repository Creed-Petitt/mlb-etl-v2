#!/usr/bin/env python3

import sys
import argparse
import time
import logging
from datetime import datetime, timedelta

from models import get_session
from etl.clients.espn_betting_client import ESPNBettingClient
from etl.processors.betting.espn_processor import ESPNBettingProcessor

logger = logging.getLogger(__name__)

class ESPNBettingLoader:
    
    def __init__(self):
        # Initialize components
        self.client = ESPNBettingClient()
        self.processor = ESPNBettingProcessor()
        
        # Stats tracking
        self.stats = {
            'start_time': None,
            'games_found': 0,
            'games_matched': 0,
            'odds_created': 0,
            'odds_updated': 0,
            'errors': 0
        }
    
    def load_current_odds(self):
        logger.info("Starting ESPN betting odds load for today's games")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Fetch data from ESPN API (always returns today's games)
            espn_data = self.client.fetch_odds_data()
            
            if not espn_data:
                logger.error("Failed to fetch ESPN data")
                self.stats['errors'] += 1
                return False
            
            # Process the ESPN response
            games_with_odds = self.processor.process_espn_response(espn_data)
            self.stats['games_found'] = len(games_with_odds)
            
            # Store odds records in database
            created, updated = self.processor.store_odds_records(games_with_odds)
            self.stats['odds_created'] = created
            self.stats['odds_updated'] = updated
            
            # Get processor stats
            processor_stats = self.processor.get_stats()
            self.stats['games_matched'] = processor_stats.get('games_matched', 0)
            
            self._log_final_results()
            return True
            
        except Exception as e:
            logger.error(f"Error in ESPN odds loading process: {e}")
            self.stats['errors'] += 1
            return False
    
    def _log_final_results(self):
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"ESPN odds loading completed in {elapsed:.1f}s")
        logger.info(f"Games found: {self.stats['games_found']}")
        logger.info(f"Games matched with database: {self.stats['games_matched']}")
        logger.info(f"Odds records created: {self.stats['odds_created']}")
        logger.info(f"Odds records updated: {self.stats['odds_updated']}")
        
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        
        # Log processor stats
        processor_stats = self.processor.get_stats()
        logger.info(f"Total odds extracted: {processor_stats.get('odds_extracted', 0)}")
        logger.info(f"Total odds stored: {processor_stats.get('odds_stored', 0)}")
    
    def close(self):
        if self.client:
            self.client.close()
        if self.processor:
            self.processor.close()

def main():
    parser = argparse.ArgumentParser(description='Load current ESPN betting odds data')
    
    args = parser.parse_args()
    
    loader = ESPNBettingLoader()
    
    try:
        # Load today's odds (only option with ESPN API)
        loader.load_current_odds()
        return 0
        
    except Exception as e:
        logger.error(f"ESPN betting loader failed: {e}")
        return 1
    finally:
        loader.close()

if __name__ == "__main__":
    sys.exit(main())