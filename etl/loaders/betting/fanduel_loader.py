#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

import argparse
import time
import logging
from datetime import datetime

from models import get_session
from etl.clients.fanduel_client import FanDuelClient
from etl.processors.betting.fanduel_processor import FanDuelProcessor

logger = logging.getLogger(__name__)

class FanDuelLoader:
    
    def __init__(self):
        # Initialize components
        self.client = FanDuelClient()
        self.processor = FanDuelProcessor()
        
        # Stats tracking
        self.stats = {
            'start_time': None,
            'events_found': 0,
            'markets_found': 0,
            'prices_fetched': 0,
            'futures_count': 0,
            'props_count': 0,
            'game_lines_count': 0,
            'errors': 0
        }
    
    def load_all_markets(self, fetch_prices: bool = True):
        """Load all FanDuel markets and optionally fetch prices"""
        
        logger.info("Starting FanDuel complete market load")
        self.stats['start_time'] = time.time()
        
        try:
            # Step 1: Fetch MLB page with all markets
            mlb_page = self.client.fetch_mlb_page()
            
            if not mlb_page:
                logger.error("Failed to fetch FanDuel MLB page")
                self.stats['errors'] += 1
                return False
            
            # Extract counts for logging
            attachments = mlb_page.get('attachments', {})
            self.stats['events_found'] = len(attachments.get('events', {}))
            self.stats['markets_found'] = len(attachments.get('markets', {}))
            
            # Step 2: Process all markets and events
            process_result = self.processor.process_mlb_page(mlb_page)
            market_ids = process_result.get('market_ids', [])
            processor_stats = process_result.get('stats', {})
            
            # Update our stats
            self.stats['futures_count'] = processor_stats.get('futures_processed', 0)
            self.stats['props_count'] = processor_stats.get('props_processed', 0)
            self.stats['game_lines_count'] = processor_stats.get('game_lines_processed', 0)
            
            # Step 3: Optionally fetch current prices
            if fetch_prices and market_ids:
                logger.info(f"Fetching prices for {len(market_ids)} markets")
                
                prices_data = self.client.fetch_market_prices(market_ids)
                
                if prices_data:
                    prices_count = self.processor.process_market_prices(prices_data)
                    self.stats['prices_fetched'] = prices_count
                    logger.info(f"Stored {prices_count} price records")
                else:
                    logger.warning("Failed to fetch market prices")
            
            # Step 4: Commit all changes
            if self.processor.commit_changes():
                self._log_final_results()
                return True
            else:
                self.stats['errors'] += 1
                return False
            
        except Exception as e:
            logger.error(f"Error in FanDuel loading process: {e}")
            self.stats['errors'] += 1
            return False
    
    def _log_final_results(self):
        """Log final processing results"""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"FanDuel loading completed in {elapsed:.1f}s")
        logger.info(f"Events found: {self.stats['events_found']}")
        logger.info(f"Markets found: {self.stats['markets_found']}")
        logger.info(f"  - Futures: {self.stats['futures_count']}")
        logger.info(f"  - Player Props: {self.stats['props_count']}")
        logger.info(f"  - Game Lines: {self.stats['game_lines_count']}")
        logger.info(f"Prices fetched: {self.stats['prices_fetched']}")
        
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        
        # Get processor stats
        processor_stats = self.processor.get_stats()
        logger.info(f"Total events processed: {processor_stats.get('events_processed', 0)}")
        logger.info(f"Total markets processed: {processor_stats.get('markets_processed', 0)}")
        logger.info(f"Total runners processed: {processor_stats.get('runners_processed', 0)}")
    
    def close(self):
        """Close all resources"""
        if self.client:
            self.client.close()
        if self.processor:
            self.processor.close()

def main():
    parser = argparse.ArgumentParser(description='Load FanDuel MLB markets and odds')
    parser.add_argument(
        '--no-prices',
        action='store_true',
        help='Skip fetching current prices (only load market structure)'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    loader = FanDuelLoader()
    
    try:
        # Load all markets (with or without prices)
        success = loader.load_all_markets(fetch_prices=not args.no_prices)
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"FanDuel loader failed: {e}")
        return 1
    finally:
        loader.close()

if __name__ == "__main__":
    sys.exit(main())