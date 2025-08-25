#!/usr/bin/env python3

import sys
import logging
import time
from datetime import datetime

from etl.clients.pybaseball_client import PybaseballClient
from etl.processors.pybaseball.pybaseball_processor import PybaseballProcessor

logger = logging.getLogger(__name__)

class PybaseballStatcastLoader:
    
    def __init__(self):
        self.client = PybaseballClient()
        # Simple stats tracking
        self.stats = {
            'batters_classified': 0,
            'pitchers_classified': 0,
            'batter_records_loaded': 0,
            'pitcher_records_loaded': 0,
            'total_records_loaded': 0,
            'start_time': None
        }
    
    def load_all_data(self, year=2025):

        logger.info(f"Starting pybaseball load for {year}")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Initialize processor with year
            processor = PybaseballProcessor()
            
            # Get player classifications
            batters, pitchers = processor.get_player_classifications()
            self.stats['batters_classified'] = len(batters)
            self.stats['pitchers_classified'] = len(pitchers)
            
            # Fetch and process batter data
            batter_data = self.client.get_batter_data(year)
            processor.process_batter_data(batter_data, batters)
            
            # Update stats
            batter_stats = processor.get_stats()
            self.stats['batter_records_loaded'] = batter_stats['batters_processed']
            self.stats['total_records_loaded'] = self.stats['batter_records_loaded']
            
            # Fetch and process pitcher data
            pitcher_data = self.client.get_pitcher_data(year)
            processor.process_pitcher_data(pitcher_data, pitchers)
            
            # Update stats
            final_stats = processor.get_stats()
            self.stats['pitcher_records_loaded'] = final_stats['pitchers_processed']
            self.stats['total_records_loaded'] = self.stats['batter_records_loaded'] + self.stats['pitcher_records_loaded']
            
            # Log final results
            self._log_final_results()
            
            return self.stats.copy()
            
        except Exception as e:
            logger.error(f"Error in pybaseball data load: {e}")
            raise
        finally:
            processor.close()
    
    def _log_final_results(self):
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"Pybaseball load complete: {self.stats['total_records_loaded']:,} records in {elapsed:.1f}s "
                   f"({self.stats['batters_classified']} batters, {self.stats['pitchers_classified']} pitchers)")

def main():

    try:
        loader = PybaseballStatcastLoader()
        stats = loader.load_all_data(2025)
        print(f"Load completed successfully: {stats}")
        
    except Exception as e:
        print(f"Load failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())