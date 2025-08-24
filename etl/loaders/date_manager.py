from datetime import datetime, timedelta
from sqlalchemy import and_
import logging

from models import Game, get_session

logger = logging.getLogger(__name__)

class DateManager:
    
    def __init__(self):
        self.last_final_date = None
        self.start_date = None
        self.end_date = None
        
    def find_last_final_date(self):
 
        session = get_session()
        try:
            # Get all dates that have games
            dates_with_games = session.query(Game.official_date).distinct().order_by(Game.official_date.desc()).all()
            
            for date_tuple in dates_with_games:
                date_to_check = date_tuple[0]
                
                # Count total games on this date
                total_games = session.query(Game).filter(Game.official_date == date_to_check).count()
                
                # Count Final games on this date
                final_games = session.query(Game).filter(
                    and_(
                        Game.official_date == date_to_check,
                        Game.status_detailed.in_(['Final', 'F'])
                    )
                ).count()
                
                logger.debug(f"Date {date_to_check}: {final_games}/{total_games} games Final")
                
                # If ALL games on this date are Final, this is our last complete date
                if total_games > 0 and final_games == total_games:
                    logger.info(f"Found last complete Final date: {date_to_check} ({final_games}/{total_games} games Final)")
                    self.last_final_date = date_to_check
                    return date_to_check
            
            # Fallback to March 27, 2025 if no complete Final dates found
            logger.warning("No complete Final dates found, falling back to 2025-03-27")
            fallback_date = datetime(2025, 3, 27).date()
            self.last_final_date = fallback_date
            return fallback_date
                
        except Exception as e:
            logger.error(f"Error finding last final date: {e}")
            fallback_date = datetime(2025, 3, 27).date()
            self.last_final_date = fallback_date
            return fallback_date
        finally:
            session.close()
    
    def calculate_processing_window(self, last_final_date=None):

        if last_final_date is None:
            last_final_date = self.last_final_date or self.find_last_final_date()
            
        # Start processing the day after last complete Final date
        start_date = last_final_date + timedelta(days=1)
        
        # End date: maximum 2 days from start date, but not beyond today + 1
        max_end_date = start_date + timedelta(days=1)  # Max 2 days processing window
        today_plus_one = datetime.now().date() + timedelta(days=1)
        
        # Don't process beyond tomorrow
        end_date = min(max_end_date, today_plus_one)
        
        # Convert to datetime objects for API compatibility
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        logger.debug(f"Processing window calculation:")
        logger.debug(f"  Last final date: {last_final_date}")
        logger.debug(f"  Start date: {start_date} (last_final + 1 day)")
        logger.debug(f"  Max end date: {max_end_date} (start + 1 day, max 2-day window)")
        logger.debug(f"  Today + 1: {today_plus_one}")
        logger.debug(f"  Actual end date: {end_date} (min of max_end and today+1)")
        
        self.start_date = start_datetime
        self.end_date = end_datetime
        
        return start_datetime, end_datetime
    
    def get_processing_window(self):

        if self.start_date is None or self.end_date is None:
            self.calculate_processing_window()
        return self.start_date, self.end_date
    
    def log_processing_window(self):

        if self.start_date and self.end_date:
            logger.info(f"SMART PROCESSING WINDOW:")
            logger.info(f"  Last complete Final date: {self.last_final_date}")
            logger.info(f"  Processing from: {self.start_date.strftime('%m/%d/%Y')} to {self.end_date.strftime('%m/%d/%Y')}")
            logger.info(f"  Days to process: {(self.end_date.date() - self.start_date.date()).days + 1}")
            logger.info(f"  Reason: Smart daily ETL - automatic date detection")