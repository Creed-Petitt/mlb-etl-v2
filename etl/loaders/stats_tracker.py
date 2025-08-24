import time
import threading
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class StatsTracker:
    
    def __init__(self):
        self.stats = {
            'games_processed': 0,
            'games_successful': 0,
            'games_failed': 0,
            'games_skipped': 0,
            'start_time': None,
            'total_games': 0
        }
        self.lock = threading.Lock()
        
    def start_tracking(self):

        self.stats['start_time'] = time.time()
        
    def update_stats(self, success: bool):

        with self.lock:
            self.stats['games_processed'] += 1
            if success:
                self.stats['games_successful'] += 1
            else:
                self.stats['games_failed'] += 1
                
    def increment_skipped(self, count: int = 1):

        with self.lock:
            self.stats['games_skipped'] += count
            
    def set_total_games(self, total: int):

        self.stats['total_games'] = total
        
    def log_progress(self):

        with self.lock:
            if self.stats['games_processed'] % 10 == 0:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['games_processed'] / elapsed if elapsed > 0 else 0
                
                logger.info(f"PROGRESS: {self.stats['games_processed']}/{self.stats['total_games']} "
                           f"({self.stats['games_processed']/self.stats['total_games']*100:.1f}%) "
                           f"Success: {self.stats['games_successful']} "
                           f"Failed: {self.stats['games_failed']} "
                           f"Rate: {rate:.2f} games/sec")
                           
    def log_final_results(self, start_date, end_date):
        elapsed = time.time() - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("SMART DAILY ETL COMPLETE")
        logger.info("="*60)
        logger.info(f"Processing window: {start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}")
        logger.info(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        logger.info(f"Games processed: {self.stats['games_processed']}")
        logger.info(f"Games successful: {self.stats['games_successful']}")
        logger.info(f"Games failed: {self.stats['games_failed']}")
        logger.info(f"Games skipped (already Final): {self.stats['games_skipped']}")
        
        success_rate = self.stats['games_successful']/max(1,self.stats['games_processed'])*100
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        avg_rate = self.stats['games_processed']/elapsed if elapsed > 0 else 0
        logger.info(f"Average rate: {avg_rate:.2f} games/sec")
        logger.info("")
        
        # Smart guidance for next run
        self._log_next_run_guidance(end_date)
        logger.info("="*60)
        
    def _log_next_run_guidance(self, end_date):
        logger.info("SMART DAILY ETL GUIDANCE:")
        
        from datetime import datetime
        from models import Game, get_session
        
        # Calculate what the next run will detect
        try:
            session = get_session()
            
            # Check current processing window completion status
            end_date_only = end_date.date()
            
            # Show today's context
            today = datetime.now().date()
            days_behind = (today - end_date_only).days
            
            if days_behind > 0:
                logger.info(f"- Current window is {days_behind} days behind today")
                logger.info("- System will catch up by processing more recent games")
            elif days_behind == 0:
                logger.info("- Current window processes through today - system is current")
            else:
                logger.info(f"- Current window extends {abs(days_behind)} days into future")
                logger.info("- System is ahead of schedule")
                
            session.close()
            
        except Exception as e:
            logger.error(f"Error generating next run guidance: {e}")
            logger.info("- Next run will perform fresh date detection and set new processing window")
        
        logger.info("")
        logger.info("NEXT RUN BEHAVIOR:")
        logger.info("- Automatically detect last complete Final date")
        logger.info("- Set new 1-2 day processing window from that point")
        logger.info("- Skip any games already marked as Final")
        logger.info("- Process only newly completed games")
        logger.info("- Terminate gracefully when hitting unplayed games")
        
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return self.stats.copy()