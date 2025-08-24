#!/usr/bin/env python3

import sys
import concurrent.futures
import time
import logging
from datetime import datetime

from models import Game, StatcastPitch, get_session
from etl.clients.baseball_savant import BaseballSavantAPI
from etl.loaders.game.date_manager import DateManager
from etl.processors.game.orchestrator import GameDataProcessor

logger = logging.getLogger(__name__)

class BatchGameLoader:

    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        
        # Initialize components
        self.date_manager = DateManager()
        self.api_client = BaseballSavantAPI()
        
        # Simple stats tracking
        self.stats = {
            'games_processed': 0,
            'games_successful': 0,
            'games_failed': 0,
            'games_skipped': 0,
            'start_time': None,
            'total_games': 0
        }
        
        # Smart date detection
        logger.info("SMART DAILY ETL INITIALIZATION:")
        last_final_date = self.date_manager.find_last_final_date()
        self.start_date, self.end_date = self.date_manager.calculate_processing_window(last_final_date)
        
        # Log the smart processing window
        self.date_manager.log_processing_window()
        
        # Track recent games for termination check
        self.recent_games_processed = []
        
    def run_batch_load(self):

        logger.info("="*60)
        logger.info("STARTING BATCH GAME LOAD")
        logger.info(f"Date range: {self.start_date.strftime('%m/%d/%Y')} - {self.end_date.strftime('%m/%d/%Y')}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info("="*60)
        
        self.stats['start_time'] = time.time()
        
        # Get all games in date range
        games_to_process = self._get_games_to_process()
        self.stats['total_games'] = len(games_to_process)
        
        logger.info(f"Found {len(games_to_process)} games to process")
        
        # Process games in parallel with graceful termination
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all game processing tasks
            future_to_game = {
                executor.submit(self._process_single_game, game): game 
                for game in games_to_process
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_game):
                game = future_to_game[future]
                try:
                    success = future.result()
                    self.stats['games_processed'] += 1
                    if success:
                        self.stats['games_successful'] += 1
                    else:
                        self.stats['games_failed'] += 1
                    
                    # Log specific game progress with data status
                    game_pk = game['game_pk']
                    game_date = game['date'].strftime('%m/%d/%Y')
                    status = "SUCCESS" if success else "FAILED"
                    
                    # Check if game has data
                    data_status = self._check_game_data_status(game_pk) if success else "NO_DATA"
                    logger.info(f"Game {game_pk} ({game_date}): {status} - {data_status}")
                    
                    # Log progress every 10 games
                    if self.stats['games_processed'] % 10 == 0:
                        elapsed = time.time() - self.stats['start_time']
                        rate = self.stats['games_processed'] / elapsed if elapsed > 0 else 0
                        logger.info(f"PROGRESS: {self.stats['games_processed']}/{self.stats['total_games']} "
                                   f"({self.stats['games_processed']/self.stats['total_games']*100:.1f}%) "
                                   f"Success: {self.stats['games_successful']} "
                                   f"Failed: {self.stats['games_failed']} "
                                   f"Rate: {rate:.2f} games/sec")
                    
                    # Track recent games for termination check
                    self.recent_games_processed.append(game['game_pk'])
                    
                    # Check if we should terminate due to hitting unplayed games
                    if self._check_for_termination():
                        logger.info("="*60)
                        logger.info("GRACEFUL TERMINATION DETECTED")
                        logger.info(f"Last 5 games processed: {self.recent_games_processed[-5:]}")
                        logger.info("Detected unplayed games - stopping to avoid processing future games")
                        logger.info("="*60)
                        # Cancel remaining futures
                        for remaining_future in future_to_game:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
                        
                except Exception as e:
                    logger.error(f"Game {game['game_pk']} failed with exception: {e}")
                    self.stats['games_processed'] += 1
                    self.stats['games_failed'] += 1
        
        self._log_final_results()
        
    def _get_games_to_process(self):

        logger.info("Scanning for games to process...")
        
        session = get_session()
        games_to_process = []
        
        # Track game statuses for reporting
        status_counts = {
            'Final': 0,
            'In Progress': 0, 
            'Scheduled': 0,
            'Postponed': 0,
            'Other': 0,
            'To Process': 0,
            'Skipped': 0
        }
        
        try:
            # Get all potential games in date range
            all_games = self.api_client.get_games_for_date_range(
                self.start_date, 
                self.end_date
            )
            
            logger.info(f"Found {len(all_games)} total games in date range")
            
            for game in all_games:
                game_pk = game['game_pk']
                game_date = game['date'].strftime('%m/%d/%Y')
                
                # Check existing game status in database
                existing_game = session.query(Game).filter_by(game_pk=game_pk).first()
                
                if existing_game:
                    db_status = existing_game.status_detailed or 'Unknown'
                    
                    # Count statuses
                    if db_status in ['Final', 'F']:
                        status_counts['Final'] += 1
                        logger.debug(f"Skipping Final game {game_pk} ({game_date})")
                        status_counts['Skipped'] += 1
                        continue
                    elif 'Progress' in db_status or 'Live' in db_status:
                        status_counts['In Progress'] += 1
                    elif 'Schedule' in db_status:
                        status_counts['Scheduled'] += 1
                    elif 'Postponed' in db_status or 'Suspended' in db_status:
                        status_counts['Postponed'] += 1
                    else:
                        status_counts['Other'] += 1
                        
                    logger.debug(f"Processing {db_status} game {game_pk} ({game_date})")
                else:
                    # New game not in database
                    status_counts['Scheduled'] += 1
                    logger.debug(f"Processing new game {game_pk} ({game_date})")
                
                games_to_process.append(game)
                status_counts['To Process'] += 1
                
        finally:
            session.close()
            
        # Log detailed status breakdown
        logger.info(f"GAME STATUS BREAKDOWN:")
        logger.info(f"  Final (skipped): {status_counts['Final']}")
        logger.info(f"  In Progress: {status_counts['In Progress']}")
        logger.info(f"  Scheduled: {status_counts['Scheduled']}")
        logger.info(f"  Postponed: {status_counts['Postponed']}")
        logger.info(f"  Other: {status_counts['Other']}")
        logger.info(f"  To Process: {status_counts['To Process']}")
        logger.info(f"  Skipped: {status_counts['Skipped']}")
        
        self.stats['games_skipped'] = status_counts['Skipped']
        return games_to_process
        
    def _process_single_game(self, game_info):

        game_pk = game_info['game_pk']
        game_date = game_info['date']
        
        start_time = time.time()
        logger.debug(f"Starting game {game_pk} ({game_date.strftime('%m/%d/%Y')})")
        
        try:
            # Fetch game data from API
            game_data = self.api_client.fetch_game_data(game_date, game_pk)
            if not game_data:
                logger.warning(f"No data available for game {game_pk}")
                return False
                
            # Process game data using the orchestrator
            processor = GameDataProcessor()
            success = processor.process_game(game_data)
            processor.close()
            
            processing_time = time.time() - start_time
            if success:
                logger.info(f"Game {game_pk} completed in {processing_time:.2f}s")
            else:
                logger.error(f"Game {game_pk} processing failed after {processing_time:.2f}s")
                
            return success
                
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Game {game_pk} failed after {processing_time:.2f}s: {e}")
            return False
            
    def _check_for_termination(self):

        if len(self.recent_games_processed) < 5:
            return False
        
        session = get_session()
        try:
            # Analyze last 5 games for termination patterns
            termination_data = []
            unplayed_count = 0
            future_games_count = 0
            today = datetime.now().date()
            
            for game_pk in self.recent_games_processed[-5:]:
                # Get game info
                game = session.query(Game).filter_by(game_pk=game_pk).first()
                pitch_count = session.query(StatcastPitch).filter_by(game_pk=game_pk).count()
                
                game_date = game.official_date if game else None
                game_status = game.status_detailed if game else 'Unknown'
                is_future = game_date > today if game_date else False
                
                termination_data.append({
                    'game_pk': game_pk,
                    'date': game_date,
                    'status': game_status,
                    'pitch_count': pitch_count,
                    'is_future': is_future
                })
                
                if pitch_count == 0:
                    unplayed_count += 1
                if is_future:
                    future_games_count += 1
            
            # Decision logic for termination
            should_terminate = False
            termination_reason = None
            
            # Pattern 1: 5 consecutive games with no pitch data
            if unplayed_count >= 5:
                should_terminate = True
                termination_reason = "NO_PITCH_DATA"
                
            # Pattern 2: 3+ future games (beyond today)
            elif future_games_count >= 3:
                should_terminate = True
                termination_reason = "FUTURE_GAMES"
                
            # Pattern 3: All games are Scheduled status
            elif all(data['status'] in ['Scheduled', 'Pre-Game'] for data in termination_data):
                should_terminate = True
                termination_reason = "ALL_SCHEDULED"
            
            # Log termination decision
            if should_terminate:
                logger.info("="*60)
                logger.info("ENHANCED GRACEFUL TERMINATION TRIGGERED")
                logger.info(f"Termination Reason: {termination_reason}")
                logger.info(f"Analysis of last {len(termination_data)} games:")
                
                for data in termination_data:
                    future_flag = " (FUTURE)" if data['is_future'] else ""
                    logger.info(f"  Game {data['game_pk']}: {data['date']} | {data['status']} | {data['pitch_count']} pitches{future_flag}")
                
                if termination_reason == "NO_PITCH_DATA":
                    logger.info(f"Decision: {unplayed_count}/5 games have no pitch data - likely hit unplayed games")
                elif termination_reason == "FUTURE_GAMES":
                    logger.info(f"Decision: {future_games_count}/5 games are beyond today - processing future schedule")
                elif termination_reason == "ALL_SCHEDULED":
                    logger.info("Decision: All recent games are Scheduled - likely processing future games")
                
                logger.info("Action: Stopping ETL to avoid processing unplayed/future games")
                logger.info("="*60)
                
            return should_terminate
            
        except Exception as e:
            logger.error(f"Error in enhanced termination check: {e}")
            return False
        finally:
            session.close()
    
    def _check_game_data_status(self, game_pk):

        session = get_session()
        try:
            pitch_count = session.query(StatcastPitch).filter_by(game_pk=game_pk).count()
            
            if pitch_count == 0:
                return "NO_PITCH_DATA"
            elif pitch_count < 50:
                return f"PARTIAL_DATA({pitch_count}_pitches)"
            else:
                return f"FULL_DATA({pitch_count}_pitches)"
        except Exception:
            return "UNKNOWN_DATA"
        finally:
            session.close()
    
    def _log_final_results(self):
        elapsed = time.time() - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("BATCH GAME LOAD COMPLETE")
        logger.info("="*60)
        logger.info(f"Processing window: {self.start_date.strftime('%m/%d/%Y')} - {self.end_date.strftime('%m/%d/%Y')}")
        logger.info(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        logger.info(f"Games processed: {self.stats['games_processed']}")
        logger.info(f"Games successful: {self.stats['games_successful']}")
        logger.info(f"Games failed: {self.stats['games_failed']}")
        logger.info(f"Games skipped (already Final): {self.stats['games_skipped']}")
        
        success_rate = self.stats['games_successful']/max(1,self.stats['games_processed'])*100
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        avg_rate = self.stats['games_processed']/elapsed if elapsed > 0 else 0
        logger.info(f"Average rate: {avg_rate:.2f} games/sec")
        logger.info("="*60)
    
    def close(self):

        if self.api_client:
            self.api_client.close()

def main():
    loader = BatchGameLoader(max_workers=10)
    
    try:
        loader.run_batch_load()
        return 0
    except Exception as e:
        logger.error(f"Batch load failed: {e}")
        return 1
    finally:
        loader.close()

if __name__ == "__main__":
    sys.exit(main())