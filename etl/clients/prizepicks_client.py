#!/usr/bin/env python3

import requests
import logging
import os
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class PrizePicksClient:
    
    def __init__(self):
        self.session = requests.Session()
        
        # Configure session headers - match working script
        self.session.headers.update({
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache", 
            "referer": "https://app.prizepicks.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
        })
        
        # PrizePicks API URL for MLB projections
        self.projections_url = os.getenv(
            'PRIZEPICKS_API_BASE_URL', 
            "https://api.prizepicks.com/projections?league_id=2&per_page=250&single_stat=true&in_game=true&state_code=OK&game_mode=pickem"
        )
    
    def fetch_projections_data(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch current projections data from PrizePicks API
        Returns tuple of (projections, included) or ([], []) if failed
        """
        try:
            logger.info("Fetching PrizePicks projections data...")
            
            response = self.session.get(self.projections_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            projections = data.get("data", [])
            included = data.get("included", [])
            
            logger.info(f"Successfully fetched {len(projections)} projections, {len(included)} included items")
            return projections, included
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching PrizePicks data: {e}")
            return [], []
        except Exception as e:
            logger.error(f"Unexpected error fetching PrizePicks data: {e}")
            return [], []
    
    def close(self):
        if self.session:
            self.session.close()