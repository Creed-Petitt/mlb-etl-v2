import os
import requests
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

from models import Game, get_session

load_dotenv()

logger = logging.getLogger(__name__)

class BaseballSavantAPI:

    def __init__(self):
        self.base_url = os.getenv('BASEBALL_SAVANT_BASE_URL')
        self.session = requests.Session()
        # Configure session for speed
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def fetch_game_data(self, game_date, game_pk=None):
        try:
            logger.info(f"Fetching data for game_pk: {game_pk}")
            
            # Use the real Baseball Savant API endpoint
            params = {'game_pk': game_pk}
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            game_data = response.json()
            logger.info(f"Successfully fetched data for game {game_pk}")
            return game_data
            
        except Exception as e:
            logger.error(f"Failed to fetch data for game {game_pk}: {e}")
            return None
            
    def get_games_for_date_range(self, start_date, end_date):

        session = get_session()
        games = []
        
        try:
            # Query actual games from our database schedule
            db_games = session.query(Game).filter(
                Game.game_date >= start_date,
                Game.game_date <= end_date
            ).all()
            
            for game in db_games:
                games.append({
                    'date': game.game_date,
                    'game_pk': game.game_pk
                })
            
            logger.info(f"Found {len(games)} real games in database for date range")
            return games
            
        finally:
            session.close()
        
    def close(self):
        if self.session:
            self.session.close()