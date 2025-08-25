#!/usr/bin/env python3

import logging
import pybaseball as pyb
from datetime import datetime

logger = logging.getLogger(__name__)

class PybaseballClient:

    def __init__(self):
        self.cache_enabled = True
    
    def get_batter_data(self, year=2025):
        
        try:
            # Batter datasets - only the 4 that actually exist
            batter_exit_velocity = pyb.statcast_batter_exitvelo_barrels(year=year, minBBE=10)
            batter_expected_stats = pyb.statcast_batter_expected_stats(year=year, minPA=50)
            batter_percentile_ranks = pyb.statcast_batter_percentile_ranks(year=year)
            batter_pitch_arsenal = pyb.statcast_batter_pitch_arsenal(year=year, minPA=10)
            
            return {
                'exit_velocity': batter_exit_velocity,
                'expected_stats': batter_expected_stats,
                'percentile_ranks': batter_percentile_ranks,
                'pitch_arsenal': batter_pitch_arsenal
            }
            
        except Exception as e:
            logger.error(f"Error fetching batter data: {e}")
            raise
    
    def get_pitcher_data(self, year=2025):
        
        try:
            # Pitcher datasets - only the 5 that actually exist  
            pitcher_exit_velocity = pyb.statcast_pitcher_exitvelo_barrels(year=year, minBBE=10)
            pitcher_expected_stats = pyb.statcast_pitcher_expected_stats(year=year, minPA=50)
            pitcher_percentile_ranks = pyb.statcast_pitcher_percentile_ranks(year=year)
            pitcher_arsenal_stats = pyb.statcast_pitcher_arsenal_stats(year=year, minPA=10)
            pitcher_pitch_arsenal_usage = pyb.statcast_pitcher_pitch_arsenal(year=year, arsenal_type="n_")
            
            return {
                'exit_velocity': pitcher_exit_velocity,
                'expected_stats': pitcher_expected_stats,
                'percentile_ranks': pitcher_percentile_ranks,
                'arsenal_stats': pitcher_arsenal_stats,
                'pitch_arsenal_usage': pitcher_pitch_arsenal_usage
            }
            
        except Exception as e:
            logger.error(f"Error fetching pitcher data: {e}")
            raise