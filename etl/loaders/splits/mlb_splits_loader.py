#!/usr/bin/env python3

import sys
import argparse
import concurrent.futures
import time
import logging
from datetime import datetime

from models import get_session
from etl.clients.mlb_splits_client import MLBSplitsClient
from etl.processors.splits.splits_processor import SplitsProcessor

logger = logging.getLogger(__name__)

class MLBSplitsLoader:
    
    def __init__(self, max_workers=20):
        self.max_workers = max_workers
        
        # Initialize components
        self.client = MLBSplitsClient()
        
        # Stats tracking
        self.stats = {
            'splits_requested': 0,
            'splits_successful': 0,
            'splits_failed': 0,
            'total_players_processed': 0,
            'start_time': None
        }
    
    def load_splits_for_season(self, season=2025, test_mode=False):
        logger.info(f"Starting MLB splits load for season {season}")
        
        self.stats['start_time'] = time.time()
        
        # Get all priority splits
        hitting_splits = self.client.get_all_priority_splits()  # Core + pitch types
        pitching_splits = self.client.get_pitching_priority_splits()  # Core + pitching specific
        
        if test_mode:
            # Limit splits for testing
            hitting_splits = {k: v for i, (k, v) in enumerate(hitting_splits.items()) if i < 5}
            pitching_splits = {k: v for i, (k, v) in enumerate(pitching_splits.items()) if i < 5}
            logger.info(f"Test mode: Loading {len(hitting_splits)} hitting splits and {len(pitching_splits)} pitching splits")
        
        # Prepare all split tasks
        split_tasks = []
        
        # Add hitting splits
        for sitcode, description in hitting_splits.items():
            split_tasks.append({
                'season': season,
                'group': 'hitting',
                'sitcode': sitcode,
                'description': description
            })
        
        # Add pitching splits
        for sitcode, description in pitching_splits.items():
            split_tasks.append({
                'season': season,
                'group': 'pitching', 
                'sitcode': sitcode,
                'description': description
            })
        
        self.stats['splits_requested'] = len(split_tasks)
        logger.info(f"Processing {len(split_tasks)} total splits ({len(hitting_splits)} hitting, {len(pitching_splits)} pitching)")
        
        # Process splits in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all split processing tasks
            future_to_split = {
                executor.submit(self._process_single_split, task): task
                for task in split_tasks
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_split):
                split_task = future_to_split[future]
                try:
                    success, players_count = future.result()
                    
                    if success:
                        self.stats['splits_successful'] += 1
                        self.stats['total_players_processed'] += players_count
                        logger.info(f"Completed {split_task['group']} split '{split_task['sitcode']}' - {players_count} players")
                    else:
                        self.stats['splits_failed'] += 1
                        logger.error(f"Failed {split_task['group']} split '{split_task['sitcode']}'")
                        
                except Exception as e:
                    self.stats['splits_failed'] += 1
                    logger.error(f"Exception processing {split_task['group']} split '{split_task['sitcode']}': {e}")
        
        self._log_final_results()
    
    def _process_single_split(self, split_task):
        season = split_task['season']
        group = split_task['group']
        sitcode = split_task['sitcode']
        description = split_task['description']
        
        try:
            # Create processor instance for this task to avoid session conflicts
            processor = SplitsProcessor()
            
            # Fetch data from API
            api_response = self.client.fetch_split_stats(season, group, sitcode)
            
            if not api_response:
                processor.close()
                return False, 0
            
            # Process the data
            if group == 'hitting':
                splits_records = processor.process_hitting_split(
                    api_response, season, sitcode, description
                )
            else:
                splits_records = processor.process_pitching_split(
                    api_response, season, sitcode, description
                )
            
            if not splits_records:
                processor.close()
                return False, 0
            
            # Bulk upsert to database
            loaded_count = processor.bulk_upsert_splits(splits_records)
            processor.close()
            
            return loaded_count > 0, len(splits_records)
            
        except Exception as e:
            logger.error(f"Error processing {group} split {sitcode}: {e}")
            return False, 0
    
    def _log_final_results(self):
        elapsed = time.time() - self.stats['start_time']
        success_rate = self.stats['splits_successful'] / max(1, self.stats['splits_requested']) * 100
        
        logger.info(f"Splits load complete: {self.stats['splits_requested']} requested, "
                   f"{self.stats['splits_successful']} successful, {self.stats['splits_failed']} failed "
                   f"({success_rate:.1f}% success rate)")
        logger.info(f"Total execution time: {elapsed:.1f}s")
        logger.info(f"Total players processed: {self.stats['total_players_processed']}")
    
    def close(self):
        if self.client:
            self.client.close()

def main():
    parser = argparse.ArgumentParser(description='Load MLB player splits from official API')
    parser.add_argument('--season', type=int, default=2025, help='Season to load (default: 2025)')
    parser.add_argument('--test', action='store_true', help='Test mode - load only a few splits')
    parser.add_argument('--max-workers', type=int, default=20, help='Max parallel workers (default: 20)')
    
    args = parser.parse_args()
    
    loader = MLBSplitsLoader(max_workers=args.max_workers)
    
    try:
        loader.load_splits_for_season(season=args.season, test_mode=args.test)
        return 0
    except Exception as e:
        logger.error(f"Splits load failed: {e}")
        return 1
    finally:
        loader.close()

if __name__ == "__main__":
    sys.exit(main())