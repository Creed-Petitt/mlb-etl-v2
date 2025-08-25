#!/usr/bin/env python3

import requests
import logging
import os
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class ESPNBettingClient:
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # ESPN API configuration
        self.base_url = os.getenv('ESPN_API_BASE_URL', 'https://site.api.espn.com/apis/personalized/v2/scoreboard/header')
        self.params = {
            'league': 'mlb',
            'sport': 'baseball',
            'configuration': 'SITE_DEFAULT',
            'playabilitySource': 'playbackId',
            'lang': 'en',
            'region': 'us',
            'contentorigin': 'espn',
            'tz': 'America/New_York',
            'platform': 'web',
            'showAirings': 'buy,live,replay',
            'showZipLookup': 'true',
            'buyWindow': '1m',
            'postalCode': '73012'
        }
    
    def fetch_odds_data(self) -> Optional[Dict]:
        try:
            logger.info("Fetching current ESPN odds data")
            
            response = self.session.get(self.base_url, params=self.params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Quick validation
            sports = data.get('sports', [])
            if sports:
                events = sports[0].get('leagues', [{}])[0].get('events', [])
                logger.info(f"Successfully fetched ESPN data with {len(events)} games")
            else:
                logger.warning("No sports data found in ESPN response")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching ESPN data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching ESPN data: {e}")
            return None
    
    def close(self):
        if self.session:
            self.session.close()