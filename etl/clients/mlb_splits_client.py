#!/usr/bin/env python3

import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class MLBSplitsClient:
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # API endpoints
        self.sitcodes_url = "https://statsapi.mlb.com/api/v1/situationCodes"
        self.stats_url = "https://bdfed.stitch.mlbinfra.com/bdfed/stats/player"
        
        # Load sitCodes on initialization
        self.sitcodes = self.fetch_situation_codes()
        
        # Priority splits for ML training
        self.core_splits = {
            'vr': 'vs Right',
            'vl': 'vs Left',
            'h': 'Home Games',
            'a': 'Away Games', 
            'd': 'Day Games',
            'n': 'Night Games',
            'risp': 'Scoring Position',
            'risp2': 'Scoring Position - 2 Outs',
            'lc': 'Late / Close',
            'sah': 'Team is ahead',
            'sbh': 'Team is behind',
            'ac': 'Ahead in Count',
            'bc': 'Behind in Count', 
            '2s': 'Two Strikes'
        }
        
        # Pitch type splits - removed (not available in 2025 API)
        self.pitch_splits = {}
        
        # Pitching-specific splits
        self.pitching_splits = {
            'sp': 'Starter',
            'rp': 'Reliever',
            'pi000': 'First 75 Pitches'
        }
        
    def fetch_situation_codes(self) -> Dict:
        try:
            response = self.session.get(self.sitcodes_url, timeout=30)
            response.raise_for_status()
            
            sitcodes = {}
            for item in response.json():
                sitcodes[item['code']] = item['description']
            
            logger.info(f"Loaded {len(sitcodes)} situation codes from MLB API")
            return sitcodes
            
        except Exception as e:
            logger.error(f"Failed to fetch situation codes: {e}")
            return {}
    
    def fetch_split_stats(self, season: int, group: str, sitcode: str, limit: int = 1000) -> Optional[Dict]:
        try:
            params = {
                'env': 'prod',
                'season': season,
                'stats': 'season',
                'group': group,  # 'hitting' or 'pitching'
                'gameType': 'R',  # Regular season
                'limit': limit,
                'offset': 0,
                'sortStat': 'homeRuns' if group == 'hitting' else 'strikeouts',
                'order': 'desc',
                'sitCodes': sitcode
            }
            
            response = self.session.get(self.stats_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Fetched {group} stats for sitCode '{sitcode}' - {len(data.get('stats', []))} players")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch {group} stats for sitCode '{sitcode}': {e}")
            return None
    
    def get_all_priority_splits(self) -> Dict[str, str]:
        all_splits = {}
        all_splits.update(self.core_splits)
        all_splits.update(self.pitch_splits)
        return all_splits
        
    def get_pitching_priority_splits(self) -> Dict[str, str]:
        all_splits = {}
        all_splits.update(self.core_splits)
        all_splits.update(self.pitching_splits)
        return all_splits
        
    def validate_sitcode(self, sitcode: str) -> bool:
        return sitcode in self.sitcodes
        
    def get_sitcode_description(self, sitcode: str) -> str:
        return self.sitcodes.get(sitcode, f"Unknown sitCode: {sitcode}")
    
    def close(self):
        if self.session:
            self.session.close()