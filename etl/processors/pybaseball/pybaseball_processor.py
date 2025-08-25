#!/usr/bin/env python3

import logging
import pandas as pd
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from models import (
    get_session, Player, BatterExitVelocityBarrels, BatterExpectedStats,
    BatterPercentileRanks, BatterPitchArsenal, PitcherExitVelocityBarrels, 
    PitcherExpectedStats, PitcherPercentileRanks, PitcherArsenalStats, 
    PitcherPitchArsenalUsage
)

logger = logging.getLogger(__name__)

class PybaseballProcessor:
    
    def __init__(self):
        self.session = get_session()
        self.stats = {
            'batters_processed': 0,
            'pitchers_processed': 0,
            'total_records': 0
        }
    
    def get_player_classifications(self):

        # Get player mapping and positions 
        result = self.session.execute(text("""
            SELECT mlb_id, full_name, primary_position_name
            FROM players 
            WHERE active = true
        """))
        
        batters = set()
        pitchers = set()
        
        for mlb_id, name, position_name in result:
            if position_name and 'Pitcher' in position_name:  # Pitcher (R) or Pitcher (L)
                pitchers.add(mlb_id)
            else:  # Batter (R), Batter (L), or NULL
                batters.add(mlb_id)
        
        return batters, pitchers
    
    def process_batter_data(self, batter_data, batters):
        
        try:
            self.load_batter_exit_velocity_barrels(batter_data['exit_velocity'], batters)
            self.load_batter_expected_stats(batter_data['expected_stats'], batters)
            self.load_batter_percentile_ranks(batter_data['percentile_ranks'], batters)
            self.load_batter_pitch_arsenal(batter_data['pitch_arsenal'], batters)
            
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error processing batter data: {e}")
            raise
    
    def process_pitcher_data(self, pitcher_data, pitchers):
        
        try:
            self.load_pitcher_exit_velocity_barrels(pitcher_data['exit_velocity'], pitchers)
            self.load_pitcher_expected_stats(pitcher_data['expected_stats'], pitchers)
            self.load_pitcher_percentile_ranks(pitcher_data['percentile_ranks'], pitchers)
            self.load_pitcher_arsenal_stats(pitcher_data['arsenal_stats'], pitchers)
            self.load_pitcher_pitch_arsenal_usage(pitcher_data['pitch_arsenal_usage'], pitchers)
            
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error processing pitcher data: {e}")
            raise
    
    def load_batter_exit_velocity_barrels(self, data, batters):

        # Clear existing data
        self.session.query(BatterExitVelocityBarrels).delete()
        
        clean_data = data[data['player_id'].isin(batters)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = BatterExitVelocityBarrels(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                attempts=None if pd.isna(row.get('attempts')) else int(row.get('attempts')),
                avg_hit_angle=None if pd.isna(row.get('avg_hit_angle')) else float(row.get('avg_hit_angle')),
                anglesweetspotpercent=None if pd.isna(row.get('anglesweetspotpercent')) else float(row.get('anglesweetspotpercent')),
                max_hit_speed=None if pd.isna(row.get('max_hit_speed')) else float(row.get('max_hit_speed')),
                avg_hit_speed=None if pd.isna(row.get('avg_hit_speed')) else float(row.get('avg_hit_speed')),
                ev50=None if pd.isna(row.get('ev50')) else float(row.get('ev50')),
                ev95plus=None if pd.isna(row.get('ev95plus')) else int(row.get('ev95plus')),
                ev95percent=None if pd.isna(row.get('ev95percent')) else float(row.get('ev95percent')),
                max_distance=None if pd.isna(row.get('max_distance')) else int(row.get('max_distance')),
                avg_distance=None if pd.isna(row.get('avg_distance')) else int(row.get('avg_distance')),
                avg_hr_distance=None if pd.isna(row.get('avg_hr_distance')) else int(row.get('avg_hr_distance')),
                fbld=None if pd.isna(row.get('fbld')) else float(row.get('fbld')),
                gb=None if pd.isna(row.get('gb')) else float(row.get('gb')),
                barrels=None if pd.isna(row.get('barrels')) else int(row.get('barrels')),
                brl_percent=None if pd.isna(row.get('brl_percent')) else float(row.get('brl_percent')),
                brl_pa=None if pd.isna(row.get('brl_pa')) else float(row.get('brl_pa'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['batters_processed'] += inserted
    
    def load_batter_expected_stats(self, data, batters):

        # Clear existing data
        self.session.query(BatterExpectedStats).delete()
        
        clean_data = data[data['player_id'].isin(batters)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = BatterExpectedStats(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                pa=None if pd.isna(row.get('pa')) else int(row.get('pa')),
                bip=None if pd.isna(row.get('bip')) else int(row.get('bip')),
                ba=None if pd.isna(row.get('ba')) else float(row.get('ba')),
                est_ba=None if pd.isna(row.get('est_ba')) else float(row.get('est_ba')),
                est_ba_minus_ba_diff=None if pd.isna(row.get('est_ba_minus_ba_diff')) else float(row.get('est_ba_minus_ba_diff')),
                slg=None if pd.isna(row.get('slg')) else float(row.get('slg')),
                est_slg=None if pd.isna(row.get('est_slg')) else float(row.get('est_slg')),
                est_slg_minus_slg_diff=None if pd.isna(row.get('est_slg_minus_slg_diff')) else float(row.get('est_slg_minus_slg_diff')),
                woba=None if pd.isna(row.get('woba')) else float(row.get('woba')),
                est_woba=None if pd.isna(row.get('est_woba')) else float(row.get('est_woba')),
                est_woba_minus_woba_diff=None if pd.isna(row.get('est_woba_minus_woba_diff')) else float(row.get('est_woba_minus_woba_diff'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['batters_processed'] += inserted
    
    def load_batter_percentile_ranks(self, data, batters):

        # Clear existing data
        self.session.query(BatterPercentileRanks).delete()
        
        clean_data = data[data['player_id'].isin(batters)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = BatterPercentileRanks(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                xwoba=None if pd.isna(row.get('xwoba')) else float(row.get('xwoba')),
                xba=None if pd.isna(row.get('xba')) else float(row.get('xba')),
                xslg=None if pd.isna(row.get('xslg')) else float(row.get('xslg')),
                xiso=None if pd.isna(row.get('xiso')) else float(row.get('xiso')),
                xobp=None if pd.isna(row.get('xobp')) else float(row.get('xobp')),
                brl=None if pd.isna(row.get('brl')) else float(row.get('brl')),
                brl_percent=None if pd.isna(row.get('brl_percent')) else float(row.get('brl_percent')),
                exit_velocity=None if pd.isna(row.get('exit_velocity_avg')) else float(row.get('exit_velocity_avg')),
                max_ev=None if pd.isna(row.get('max_ev')) else float(row.get('max_ev')),
                hard_hit_percent=None if pd.isna(row.get('hard_hit_percent')) else float(row.get('hard_hit_percent')),
                k_percent=None if pd.isna(row.get('k_percent')) else float(row.get('k_percent')),
                bb_percent=None if pd.isna(row.get('bb_percent')) else float(row.get('bb_percent')),
                whiff_percent=None if pd.isna(row.get('whiff_percent')) else float(row.get('whiff_percent')),
                chase_percent=None if pd.isna(row.get('chase_percent')) else float(row.get('chase_percent')),
                arm_strength=None if pd.isna(row.get('arm_strength')) else float(row.get('arm_strength')),
                sprint_speed=None if pd.isna(row.get('sprint_speed')) else float(row.get('sprint_speed')),
                oaa=None if pd.isna(row.get('oaa')) else float(row.get('oaa')),
                bat_speed=None if pd.isna(row.get('bat_speed')) else float(row.get('bat_speed')),
                squared_up_rate=None if pd.isna(row.get('squared_up_rate')) else float(row.get('squared_up_rate')),
                swing_length=None if pd.isna(row.get('swing_length')) else float(row.get('swing_length'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['batters_processed'] += inserted
    
    def load_batter_pitch_arsenal(self, data, batters):

        # Clear existing data
        self.session.query(BatterPitchArsenal).delete()
        
        clean_data = data[data['player_id'].isin(batters)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = BatterPitchArsenal(
                player_id=int(row['player_id']),
                year=2025,
                pitch_type=row.get('pitch_type'),
                player_name=row.get('player_name'),
                team_name_alt=row.get('team_name_alt'),
                pitch_name=row.get('pitch_name'),
                pitches=None if pd.isna(row.get('pitches')) else int(row.get('pitches')),
                pitch_usage=None if pd.isna(row.get('pitch_usage')) else float(row.get('pitch_usage')),
                pa=None if pd.isna(row.get('pa')) else int(row.get('pa')),
                ba=None if pd.isna(row.get('ba')) else float(row.get('ba')),
                slg=None if pd.isna(row.get('slg')) else float(row.get('slg')),
                woba=None if pd.isna(row.get('woba')) else float(row.get('woba')),
                est_ba=None if pd.isna(row.get('est_ba')) else float(row.get('est_ba')),
                est_slg=None if pd.isna(row.get('est_slg')) else float(row.get('est_slg')),
                est_woba=None if pd.isna(row.get('est_woba')) else float(row.get('est_woba')),
                run_value_per_100=None if pd.isna(row.get('run_value_per_100')) else float(row.get('run_value_per_100')),
                run_value=None if pd.isna(row.get('run_value')) else float(row.get('run_value')),
                whiff_percent=None if pd.isna(row.get('whiff_percent')) else float(row.get('whiff_percent')),
                k_percent=None if pd.isna(row.get('k_percent')) else float(row.get('k_percent')),
                put_away=None if pd.isna(row.get('put_away')) else float(row.get('put_away')),
                hard_hit_percent=None if pd.isna(row.get('hard_hit_percent')) else float(row.get('hard_hit_percent'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['batters_processed'] += inserted
    
    # PITCHER METHODS - EXACT copy from working file using ORM
    
    def load_pitcher_exit_velocity_barrels(self, data, pitchers):

        # Clear existing data
        self.session.query(PitcherExitVelocityBarrels).delete()
        
        clean_data = data[data['player_id'].isin(pitchers)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = PitcherExitVelocityBarrels(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                attempts=None if pd.isna(row.get('attempts')) else int(row.get('attempts')),
                avg_hit_angle=None if pd.isna(row.get('avg_hit_angle')) else float(row.get('avg_hit_angle')),
                anglesweetspotpercent=None if pd.isna(row.get('anglesweetspotpercent')) else float(row.get('anglesweetspotpercent')),
                max_hit_speed=None if pd.isna(row.get('max_hit_speed')) else float(row.get('max_hit_speed')),
                avg_hit_speed=None if pd.isna(row.get('avg_hit_speed')) else float(row.get('avg_hit_speed')),
                ev50=None if pd.isna(row.get('ev50')) else float(row.get('ev50')),
                ev95plus=None if pd.isna(row.get('ev95plus')) else int(row.get('ev95plus')),
                ev95percent=None if pd.isna(row.get('ev95percent')) else float(row.get('ev95percent')),
                max_distance=None if pd.isna(row.get('max_distance')) else int(row.get('max_distance')),
                avg_distance=None if pd.isna(row.get('avg_distance')) else int(row.get('avg_distance')),
                avg_hr_distance=None if pd.isna(row.get('avg_hr_distance')) else int(row.get('avg_hr_distance')),
                fbld=None if pd.isna(row.get('fbld')) else float(row.get('fbld')),
                gb=None if pd.isna(row.get('gb')) else float(row.get('gb')),
                barrels=None if pd.isna(row.get('barrels')) else int(row.get('barrels')),
                brl_percent=None if pd.isna(row.get('brl_percent')) else float(row.get('brl_percent')),
                brl_pa=None if pd.isna(row.get('brl_pa')) else float(row.get('brl_pa'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['pitchers_processed'] += inserted
    
    def load_pitcher_expected_stats(self, data, pitchers):

        # Clear existing data
        self.session.query(PitcherExpectedStats).delete()
        
        clean_data = data[data['player_id'].isin(pitchers)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = PitcherExpectedStats(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                pa=None if pd.isna(row.get('pa')) else int(row.get('pa')),
                bip=None if pd.isna(row.get('bip')) else int(row.get('bip')),
                ba=None if pd.isna(row.get('ba')) else float(row.get('ba')),
                est_ba=None if pd.isna(row.get('est_ba')) else float(row.get('est_ba')),
                est_ba_minus_ba_diff=None if pd.isna(row.get('est_ba_minus_ba_diff')) else float(row.get('est_ba_minus_ba_diff')),
                slg=None if pd.isna(row.get('slg')) else float(row.get('slg')),
                est_slg=None if pd.isna(row.get('est_slg')) else float(row.get('est_slg')),
                est_slg_minus_slg_diff=None if pd.isna(row.get('est_slg_minus_slg_diff')) else float(row.get('est_slg_minus_slg_diff')),
                woba=None if pd.isna(row.get('woba')) else float(row.get('woba')),
                est_woba=None if pd.isna(row.get('est_woba')) else float(row.get('est_woba')),
                est_woba_minus_woba_diff=None if pd.isna(row.get('est_woba_minus_woba_diff')) else float(row.get('est_woba_minus_woba_diff')),
                era=None if pd.isna(row.get('era')) else float(row.get('era')),
                xera=None if pd.isna(row.get('xera')) else float(row.get('xera')),
                era_minus_xera_diff=None if pd.isna(row.get('era_minus_xera_diff')) else float(row.get('era_minus_xera_diff'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['pitchers_processed'] += inserted
    
    def load_pitcher_percentile_ranks(self, data, pitchers):

        # Clear existing data
        self.session.query(PitcherPercentileRanks).delete()
        
        clean_data = data[data['player_id'].isin(pitchers)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = PitcherPercentileRanks(
                player_id=int(row['player_id']),
                year=2025,
                player_name=row.get('player_name'),
                xwoba=None if pd.isna(row.get('xwoba')) else float(row.get('xwoba')),
                xba=None if pd.isna(row.get('xba')) else float(row.get('xba')),
                xslg=None if pd.isna(row.get('xslg')) else float(row.get('xslg')),
                xiso=None if pd.isna(row.get('xiso')) else float(row.get('xiso')),
                xobp=None if pd.isna(row.get('xobp')) else float(row.get('xobp')),
                xera=None if pd.isna(row.get('xera')) else float(row.get('xera')),
                brl=None if pd.isna(row.get('brl')) else float(row.get('brl')),
                brl_percent=None if pd.isna(row.get('brl_percent')) else float(row.get('brl_percent')),
                exit_velocity=None if pd.isna(row.get('exit_velocity_avg')) else float(row.get('exit_velocity_avg')),
                max_ev=None if pd.isna(row.get('max_ev')) else float(row.get('max_ev')),
                hard_hit_percent=None if pd.isna(row.get('hard_hit_percent')) else float(row.get('hard_hit_percent')),
                k_percent=None if pd.isna(row.get('k_percent')) else float(row.get('k_percent')),
                bb_percent=None if pd.isna(row.get('bb_percent')) else float(row.get('bb_percent')),
                whiff_percent=None if pd.isna(row.get('whiff_percent')) else float(row.get('whiff_percent')),
                chase_percent=None if pd.isna(row.get('chase_percent')) else float(row.get('chase_percent')),
                arm_strength=None if pd.isna(row.get('arm_strength')) else float(row.get('arm_strength')),
                fb_velocity=None if pd.isna(row.get('fb_velocity')) else float(row.get('fb_velocity')),
                fb_spin=None if pd.isna(row.get('fb_spin')) else float(row.get('fb_spin')),
                curve_spin=None if pd.isna(row.get('curve_spin')) else float(row.get('curve_spin'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['pitchers_processed'] += inserted
    
    def load_pitcher_arsenal_stats(self, data, pitchers):

        # Clear existing data
        self.session.query(PitcherArsenalStats).delete()
        
        clean_data = data[data['player_id'].isin(pitchers)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = PitcherArsenalStats(
                player_id=int(row['player_id']),
                year=2025,
                pitch_type=row.get('pitch_type'),
                player_name=row.get('player_name'),
                team_name_alt=row.get('team_name_alt'),
                pitch_name=row.get('pitch_name'),
                pitches=None if pd.isna(row.get('pitches')) else int(row.get('pitches')),
                pitch_usage=None if pd.isna(row.get('pitch_usage')) else float(row.get('pitch_usage')),
                pa=None if pd.isna(row.get('pa')) else int(row.get('pa')),
                ba=None if pd.isna(row.get('ba')) else float(row.get('ba')),
                slg=None if pd.isna(row.get('slg')) else float(row.get('slg')),
                woba=None if pd.isna(row.get('woba')) else float(row.get('woba')),
                est_ba=None if pd.isna(row.get('est_ba')) else float(row.get('est_ba')),
                est_slg=None if pd.isna(row.get('est_slg')) else float(row.get('est_slg')),
                est_woba=None if pd.isna(row.get('est_woba')) else float(row.get('est_woba')),
                run_value_per_100=None if pd.isna(row.get('run_value_per_100')) else float(row.get('run_value_per_100')),
                run_value=None if pd.isna(row.get('run_value')) else float(row.get('run_value')),
                whiff_percent=None if pd.isna(row.get('whiff_percent')) else float(row.get('whiff_percent')),
                k_percent=None if pd.isna(row.get('k_percent')) else float(row.get('k_percent')),
                put_away=None if pd.isna(row.get('put_away')) else float(row.get('put_away')),
                hard_hit_percent=None if pd.isna(row.get('hard_hit_percent')) else float(row.get('hard_hit_percent'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['pitchers_processed'] += inserted
    
    def load_pitcher_pitch_arsenal_usage(self, data, pitchers):

        # Clear existing data
        self.session.query(PitcherPitchArsenalUsage).delete()
        
        # Note: uses 'pitcher' column not 'player_id' - EXACT from working file
        clean_data = data[data['pitcher'].isin(pitchers)]
        
        inserted = 0
        for _, row in clean_data.iterrows():
            record = PitcherPitchArsenalUsage(
                player_id=int(row['pitcher']),  # Note: uses 'pitcher' column
                year=2025,
                player_name=row.get('last_name, first_name'),
                n_ff=None if pd.isna(row.get('n_ff')) else float(row.get('n_ff')),
                n_si=None if pd.isna(row.get('n_si')) else float(row.get('n_si')),
                n_fc=None if pd.isna(row.get('n_fc')) else float(row.get('n_fc')),
                n_sl=None if pd.isna(row.get('n_sl')) else float(row.get('n_sl')),
                n_ch=None if pd.isna(row.get('n_ch')) else float(row.get('n_ch')),
                n_cu=None if pd.isna(row.get('n_cu')) else float(row.get('n_cu')),
                n_fs=None if pd.isna(row.get('n_fs')) else float(row.get('n_fs')),
                n_kn=None if pd.isna(row.get('n_kn')) else float(row.get('n_kn')),
                n_st=None if pd.isna(row.get('n_st')) else float(row.get('n_st')),
                n_sv=None if pd.isna(row.get('n_sv')) else float(row.get('n_sv'))
            )
            self.session.add(record)
            inserted += 1
        
        self.stats['pitchers_processed'] += inserted
    
    def get_stats(self):

        return self.stats
    
    def close(self):

        self.session.close()