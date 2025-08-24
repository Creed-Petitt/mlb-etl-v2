from sqlalchemy import Column, Integer, String, DateTime, Date, Boolean, Text, Float, Numeric, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Game(Base):
    __tablename__ = 'games'
    
    # Primary identifiers
    game_pk = Column(Integer, primary_key=True)
    game_guid = Column(String(50), unique=True)
    link = Column(String(100))
    
    # Game details
    game_type = Column(String(10))  # R for regular, P for playoffs, etc
    season = Column(String(4))
    game_date = Column(DateTime)
    official_date = Column(Date)
    
    # Teams
    home_team_id = Column(Integer)
    home_team_name = Column(String(100))
    home_team_abbreviation = Column(String(10))
    away_team_id = Column(Integer)
    away_team_name = Column(String(100))
    away_team_abbreviation = Column(String(10))
    
    # Venue
    venue_id = Column(Integer)
    venue_name = Column(String(100))
    
    # Status
    status_abstract = Column(String(20))
    status_coded = Column(String(10))
    status_detailed = Column(String(50))
    
    # Weather data
    temperature = Column(String(50))
    weather_condition = Column(String(100))
    wind_speed = Column(String(50))
    wind_direction = Column(String(50))
    
    # Game state
    current_inning = Column(Integer)
    current_inning_ordinal = Column(String(10))
    inning_state = Column(String(20))
    scheduled_innings = Column(Integer)
    
    # Final scores
    home_score = Column(Integer)
    away_score = Column(Integer)
    home_hits = Column(Integer)
    away_hits = Column(Integer)
    home_errors = Column(Integer)
    away_errors = Column(Integer)
    
    # Metadata
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class StatcastPitch(Base):
    __tablename__ = 'statcast_pitches'
    
    # Primary key
    pitch_id = Column(String(50), primary_key=True)
    game_pk = Column(Integer, index=True)
    
    # Pitch identifiers
    ab_number = Column(Integer)
    pitch_number = Column(Integer)
    
    # Players
    pitcher_id = Column(Integer)
    batter_id = Column(Integer)
    
    # Game state
    inning = Column(Integer)
    inning_half = Column(String(10))
    balls = Column(Integer)
    strikes = Column(Integer)
    outs = Column(Integer)
    
    # Pitch characteristics
    pitch_type = Column(String(10))
    pitch_name = Column(String(50))
    release_speed = Column(Float)
    release_pos_x = Column(Float)
    release_pos_z = Column(Float)
    release_extension = Column(Float)
    
    # Pitch movement
    pfx_x = Column(Float)
    pfx_z = Column(Float)
    plate_x = Column(Float)
    plate_z = Column(Float)
    vx0 = Column(Float)
    vy0 = Column(Float)
    vz0 = Column(Float)
    ax = Column(Float)
    ay = Column(Float)
    az = Column(Float)
    
    # Spin data
    release_spin_rate = Column(Float)
    spin_axis = Column(Float)
    
    # Result
    pitch_result = Column(String(50))
    play_result = Column(String(100))
    
    # Strike zone
    zone = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime)

class BattedBall(Base):
    __tablename__ = 'batted_balls'
    
    # Primary key
    play_id = Column(String(50), primary_key=True)
    game_pk = Column(Integer, index=True)
    
    # Players
    batter_id = Column(Integer)
    pitcher_id = Column(Integer)
    
    # Game state
    inning = Column(Integer)
    inning_half = Column(String(10))
    
    # Batted ball data
    exit_velocity = Column(Float)
    launch_angle = Column(Float)
    launch_speed = Column(Float)
    launch_direction = Column(Float)
    hit_distance = Column(Float)
    hang_time = Column(Float)
    
    # Coordinates
    hit_coord_x = Column(Float)
    hit_coord_y = Column(Float)
    
    # Result
    bb_type = Column(String(50))
    result = Column(String(100))
    
    # Created timestamp
    created_at = Column(DateTime)

class GameLineScore(Base):
    __tablename__ = 'game_line_scores'
    
    # Composite primary key
    game_pk = Column(Integer, primary_key=True)
    inning = Column(Integer, primary_key=True)
    team_type = Column(String(10), primary_key=True)  # 'home' or 'away'
    
    # Inning stats
    runs = Column(Integer)
    hits = Column(Integer)
    errors = Column(Integer)
    left_on_base = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime)

class GameWPA(Base):
    __tablename__ = 'game_wpa'
    
    # Primary key
    play_id = Column(String(50), primary_key=True)
    game_pk = Column(Integer, index=True)
    
    # Play details
    inning = Column(Integer)
    inning_half = Column(String(10))
    
    # WPA data
    home_win_exp = Column(Float)
    away_win_exp = Column(Float)
    win_exp_added = Column(Float)
    
    # Players
    batter_id = Column(Integer)
    pitcher_id = Column(Integer)
    
    # Created timestamp
    created_at = Column(DateTime)

class BoxScore(Base):
    __tablename__ = 'box_scores'
    
    # Composite primary key
    game_pk = Column(Integer, primary_key=True)
    player_id = Column(Integer, primary_key=True)
    team_type = Column(String(10), primary_key=True)  # 'home' or 'away'
    
    # Player info
    player_name = Column(String(100))
    position = Column(String(50))
    batting_order = Column(Integer)
    
    # Batting stats
    at_bats = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    rbi = Column(Integer)
    walks = Column(Integer)
    strikeouts = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    home_runs = Column(Integer)
    
    # Pitching stats (if applicable)
    innings_pitched = Column(Float)
    earned_runs = Column(Integer)
    pitcher_hits = Column(Integer)
    pitcher_walks = Column(Integer)
    pitcher_strikeouts = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime)


class Player(Base):
    __tablename__ = 'players'
    
    # Primary key
    mlb_id = Column(Integer, primary_key=True)
    
    # Basic info
    full_name = Column(String(100))
    first_name = Column(String(50))
    last_name = Column(String(50))
    
    # Physical info
    height = Column(String(20))
    weight = Column(Integer)
    birth_date = Column(Date)
    
    # Career info
    mlb_debut_date = Column(Date)
    active = Column(Boolean)
    
    # Birth location
    birth_city = Column(String(100))
    birth_country = Column(String(100))
    birth_state_province = Column(String(100))
    
    # Position info
    primary_position_name = Column(String(50))
    primary_position_code = Column(String(10))
    primary_position_abbreviation = Column(String(10))
    
    # Batting/Pitching sides
    bat_side_code = Column(String(10))
    bat_side_description = Column(String(50))
    pitch_hand_code = Column(String(10))
    pitch_hand_description = Column(String(50))
    
    # Team info
    current_team_id = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class Venue(Base):
    __tablename__ = 'venues'
    
    # Primary key
    mlb_id = Column(Integer, primary_key=True)
    
    # Basic info
    name = Column(String(100))
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class Team(Base):
    __tablename__ = 'teams'
    
    # Primary key - mlb_id
    mlb_id = Column(Integer, primary_key=True)
    
    # Team identifiers
    abbreviation = Column(String(10), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    full_name = Column(String(100))
    
    # Team info
    city = Column(String(50))
    division = Column(String(50))
    league = Column(String(10))  # AL or NL
    
    # Baseball Reference mapping
    bref_abbreviation = Column(String(10))
    
    # Active status
    active = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class StatcastBatterAggregates(Base):
    __tablename__ = 'statcast_batter_aggregates'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key
    player_id = Column(Integer)  # Links to Player.mlb_id
    season = Column(Integer)
    
    # Exit velocity metrics
    avg_hit_speed = Column(Float)
    max_hit_speed = Column(Float)
    
    # Contact quality metrics
    barrel_batted_rate = Column(Float)
    hard_hit_percent = Column(Float)
    sweet_spot_percent = Column(Float)
    
    # Expected stats
    estimated_ba_using_speedangle = Column(Float)
    estimated_slg_using_speedangle = Column(Float)
    estimated_woba_using_speedangle = Column(Float)
    
    # Percentile ranks
    exit_velocity_avg_percentile = Column(Float)
    hard_hit_percent_percentile = Column(Float)
    barrel_batted_rate_percentile = Column(Float)
    xba_percentile = Column(Float)
    xslg_percentile = Column(Float)
    xwoba_percentile = Column(Float)
    
    # Sample size
    attempts = Column(Integer)

class BatterPitchPerformance(Base):
    __tablename__ = 'batter_pitch_performance'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key
    player_id = Column(Integer)  # Links to Player.mlb_id
    season = Column(Integer)
    
    # Pitch type and performance
    pitch_type = Column(String(10))
    pa = Column(Integer)  # Plate appearances
    whiff_percent = Column(Float)

class BatterExitVelocityBarrels(Base):
    __tablename__ = 'batter_exitvelo_barrels'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Batted ball metrics
    attempts = Column(Integer)
    avg_hit_angle = Column(Float)
    anglesweetspotpercent = Column(Float)
    
    # Exit velocity metrics
    max_hit_speed = Column(Float)
    avg_hit_speed = Column(Float)
    ev50 = Column(Float)
    ev95plus = Column(Integer)
    ev95percent = Column(Float)
    
    # Distance metrics
    max_distance = Column(Integer)
    avg_distance = Column(Integer)
    avg_hr_distance = Column(Integer)
    
    # Ball type percentages
    fbld = Column(Float)
    gb = Column(Float)
    
    # Barrel metrics
    barrels = Column(Integer)
    brl_percent = Column(Float)
    brl_pa = Column(Float)

class BatterExpectedStats(Base):
    __tablename__ = 'batter_expected_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Basic stats
    pa = Column(Integer)
    bip = Column(Integer)
    
    # Actual vs Expected Batting Average
    ba = Column(Float)
    est_ba = Column(Float)
    est_ba_minus_ba_diff = Column(Float)
    
    # Actual vs Expected Slugging
    slg = Column(Float)
    est_slg = Column(Float)
    est_slg_minus_slg_diff = Column(Float)
    
    # Actual vs Expected wOBA
    woba = Column(Float)
    est_woba = Column(Float)
    est_woba_minus_woba_diff = Column(Float)

class BatterPercentileRanks(Base):
    __tablename__ = 'batter_percentile_ranks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Expected stats percentiles
    xwoba = Column(Float)
    xba = Column(Float)
    xslg = Column(Float)
    xiso = Column(Float)
    xobp = Column(Float)
    
    # Barrel/exit velocity percentiles
    brl = Column(Float)
    brl_percent = Column(Float)
    exit_velocity = Column(Float)
    max_ev = Column(Float)
    hard_hit_percent = Column(Float)
    
    # Plate discipline percentiles
    k_percent = Column(Float)
    bb_percent = Column(Float)
    whiff_percent = Column(Float)
    chase_percent = Column(Float)
    
    # Physical/athletic percentiles
    arm_strength = Column(Float)
    sprint_speed = Column(Float)
    
    # Fielding percentile
    oaa = Column(Float)
    
    # Bat tracking percentiles
    bat_speed = Column(Float)
    squared_up_rate = Column(Float)
    swing_length = Column(Float)

class BatterPitchArsenal(Base):
    __tablename__ = 'batter_pitch_arsenal'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    pitch_type = Column(String(10))
    
    # Player info
    player_name = Column(String(100))
    team_name_alt = Column(String(10))
    
    # Pitch info
    pitch_name = Column(String(50))
    
    # Usage and volume
    pitches = Column(Integer)
    pitch_usage = Column(Float)
    pa = Column(Integer)
    
    # Performance metrics
    ba = Column(Float)
    slg = Column(Float)
    woba = Column(Float)
    
    # Expected performance
    est_ba = Column(Float)
    est_slg = Column(Float)
    est_woba = Column(Float)
    
    # Advanced metrics
    run_value_per_100 = Column(Float)
    run_value = Column(Float)
    whiff_percent = Column(Float)
    k_percent = Column(Float)
    put_away = Column(Float)
    hard_hit_percent = Column(Float)


class PitcherExitVelocityBarrels(Base):
    __tablename__ = 'pitcher_exitvelo_barrels'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Batted ball metrics allowed
    attempts = Column(Integer)
    avg_hit_angle = Column(Float)
    anglesweetspotpercent = Column(Float)
    
    # Exit velocity metrics allowed
    max_hit_speed = Column(Float)
    avg_hit_speed = Column(Float)
    ev50 = Column(Float)
    ev95plus = Column(Integer)
    ev95percent = Column(Float)
    
    # Distance metrics allowed
    max_distance = Column(Integer)
    avg_distance = Column(Integer)
    avg_hr_distance = Column(Integer)
    
    # Ball type percentages allowed
    fbld = Column(Float)
    gb = Column(Float)
    
    # Barrel metrics allowed
    barrels = Column(Integer)
    brl_percent = Column(Float)
    brl_pa = Column(Float)

class PitcherExpectedStats(Base):
    __tablename__ = 'pitcher_expected_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Basic stats
    pa = Column(Integer)
    bip = Column(Integer)
    
    # Actual vs Expected Batting Average Against
    ba = Column(Float)
    est_ba = Column(Float)
    est_ba_minus_ba_diff = Column(Float)
    
    # Actual vs Expected Slugging Against
    slg = Column(Float)
    est_slg = Column(Float)
    est_slg_minus_slg_diff = Column(Float)
    
    # Actual vs Expected wOBA Against
    woba = Column(Float)
    est_woba = Column(Float)
    est_woba_minus_woba_diff = Column(Float)
    
    # ERA metrics
    era = Column(Float)
    xera = Column(Float)
    era_minus_xera_diff = Column(Float)

class PitcherPercentileRanks(Base):
    __tablename__ = 'pitcher_percentile_ranks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Expected stats percentiles
    xwoba = Column(Float)
    xba = Column(Float)
    xslg = Column(Float)
    xiso = Column(Float)
    xobp = Column(Float)
    xera = Column(Float)
    
    # Barrel/exit velocity percentiles (against)
    brl = Column(Float)
    brl_percent = Column(Float)
    exit_velocity = Column(Float)
    max_ev = Column(Float)
    hard_hit_percent = Column(Float)
    
    # Plate discipline percentiles
    k_percent = Column(Float)
    bb_percent = Column(Float)
    whiff_percent = Column(Float)
    chase_percent = Column(Float)
    
    # Physical percentiles
    arm_strength = Column(Float)
    
    # Pitch quality percentiles
    fb_velocity = Column(Float)
    fb_spin = Column(Float)
    curve_spin = Column(Float)

class PitcherPitchArsenalUsage(Base):
    __tablename__ = 'pitcher_pitch_arsenal_usage'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Pitch usage percentages
    n_ff = Column(Float)  # Four-seam fastball usage %
    n_si = Column(Float)  # Sinker usage %
    n_fc = Column(Float)  # Cutter usage %
    n_sl = Column(Float)  # Slider usage %
    n_ch = Column(Float)  # Changeup usage %
    n_cu = Column(Float)  # Curveball usage %
    n_fs = Column(Float)  # Splitter usage %
    n_kn = Column(Float)  # Knuckleball usage %
    n_st = Column(Float)  # Sweeper usage %
    n_sv = Column(Float)  # Slurve usage %

class PitcherArsenalStats(Base):
    __tablename__ = 'pitcher_arsenal_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    pitch_type = Column(String(10))
    
    # Player info
    player_name = Column(String(100))
    team_name_alt = Column(String(10))
    
    # Pitch info
    pitch_name = Column(String(50))
    
    # Usage and volume
    pitches = Column(Integer)
    pitch_usage = Column(Float)
    pa = Column(Integer)
    
    # Performance metrics against
    ba = Column(Float)
    slg = Column(Float)
    woba = Column(Float)
    
    # Expected performance against
    est_ba = Column(Float)
    est_slg = Column(Float)
    est_woba = Column(Float)
    
    # Advanced metrics
    run_value_per_100 = Column(Float)
    run_value = Column(Float)
    whiff_percent = Column(Float)
    k_percent = Column(Float)
    put_away = Column(Float)
    hard_hit_percent = Column(Float)

class PitcherPitchMovement(Base):
    __tablename__ = 'pitcher_pitch_movement'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    pitch_type = Column(String(10))
    
    # Player info
    player_name = Column(String(100))
    
    # Movement metrics
    avg_speed = Column(Float)
    avg_spin = Column(Float)
    pfx_x = Column(Float)  # Horizontal movement
    pfx_z = Column(Float)  # Vertical movement
    break_x = Column(Float)
    break_z = Column(Float)
    
    # Usage
    pitches = Column(Integer)

class PitcherActiveSpin(Base):
    __tablename__ = 'pitcher_active_spin'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    
    # Active spin metrics
    pitches = Column(Integer)
    active_spin_avg = Column(Float)
    spin_rate_avg = Column(Float)
    active_spin_pct = Column(Float)

class PitcherSpinDirectionComparison(Base):
    __tablename__ = 'pitcher_spin_dir_comp'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)  # Links to Player.mlb_id
    year = Column(Integer)
    
    # Player info
    player_name = Column(String(100))
    name_abbrev = Column(String(50))
    pitch_hand = Column(String(1))
    
    # Pitch comparison info
    pitch_comp = Column(String(100))
    pitch_a = Column(String(10))
    pitch_name_a = Column(String(50))
    pitch_b = Column(String(10))
    pitch_name_b = Column(String(50))
    
    # Volume
    n_pitches = Column(Integer)
    n_pitch_a = Column(Integer)
    n_pitch_b = Column(Integer)
    
    # Pitch A spin metrics
    pitch_a_active_spin_formatted = Column(String(50))
    pitch_a_hawkeye_measured_clock = Column(Float)
    pitch_a_hawkeye_measured_clock_label = Column(String(50))
    pitch_a_movement_inferred_clock = Column(Float)
    pitch_a_movement_inferred_clock_label = Column(String(50))
    pitch_a_diff_measured_inferred_minutes = Column(Float)
    
    # Pitch B spin metrics
    pitch_b_active_spin_formatted = Column(String(50))
    pitch_b_hawkeye_measured_clock = Column(Float)
    pitch_b_hawkeye_measured_clock_label = Column(String(50))
    pitch_b_movement_inferred_clock = Column(Float)
    pitch_b_movement_inferred_clock_label = Column(String(50))
    pitch_b_diff_measured_inferred_minutes = Column(Float)
    
    # Comparison metrics
    gap_diff_measured_inferred_minutes = Column(Float)
    diff_clock_label = Column(String(50))
    diff_measured_hours = Column(Float)
    diff_inferred_hours = Column(Float)

class PlayerSplits(Base):
    __tablename__ = 'player_splits'
    
    # Primary key - composite to allow for multiple splits per player/season
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to players table
    player_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    
    # Split classification
    split_category = Column(String(50), nullable=False)  # platoon, monthly, clutch, etc.
    split_type = Column(String(50), nullable=False)      # vs_rhp, april, risp, etc.
    split_value = Column(String(50), nullable=False)     # Human readable split name
    
    # Basic batting stats
    games = Column(Integer)
    games_started = Column(Integer) 
    plate_appearances = Column(Integer)
    at_bats = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    home_runs = Column(Integer)
    rbi = Column(Integer)
    walks = Column(Integer)
    strikeouts = Column(Integer)
    stolen_bases = Column(Integer)
    caught_stealing = Column(Integer)
    sacrifice_bunts = Column(Integer)
    sacrifice_flies = Column(Integer)
    hit_by_pitch = Column(Integer)
    
    # Advanced batting stats
    batting_average = Column(Float)
    on_base_percentage = Column(Float)
    slugging_percentage = Column(Float)
    ops = Column(Float)
    babip = Column(Float)
    iso = Column(Float)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class PitcherSplits(Base):
    __tablename__ = 'pitcher_splits'
    
    # Primary key - composite to allow for multiple splits per pitcher/season
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to players table
    pitcher_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    
    # Split classification
    split_category = Column(String(50), nullable=False)  # platoon, monthly, clutch, etc.
    split_type = Column(String(50), nullable=False)      # vs_rhb, april, risp, etc.
    split_value = Column(String(50), nullable=False)     # Human readable split name
    
    # Opponent batting stats (what batters did against this pitcher)
    opponent_plate_appearances = Column(Integer)
    opponent_at_bats = Column(Integer)
    opponent_runs = Column(Integer)
    opponent_hits = Column(Integer)
    opponent_doubles = Column(Integer)
    opponent_triples = Column(Integer)
    opponent_home_runs = Column(Integer)
    opponent_stolen_bases = Column(Integer)
    opponent_caught_stealing = Column(Integer)
    opponent_walks = Column(Integer)
    opponent_strikeouts = Column(Integer)
    opponent_batting_average = Column(Float)
    opponent_on_base_percentage = Column(Float)
    opponent_slugging_percentage = Column(Float)
    opponent_ops = Column(Float)
    opponent_total_bases = Column(Integer)
    opponent_ground_into_double_play = Column(Integer)
    opponent_sacrifice_flies = Column(Integer)
    opponent_sacrifice_hits = Column(Integer)
    opponent_reached_on_error = Column(Integer)
    opponent_babip = Column(Float)
    opponent_t_ops_plus = Column(Float)
    opponent_s_ops_plus = Column(Float)
    opponent_so_per_walk = Column(Float)
    
    # Pitcher performance stats
    wins = Column(Integer)
    losses = Column(Integer)
    win_percentage = Column(Float)
    era = Column(Float)
    games = Column(Integer)
    games_started = Column(Integer)
    games_finished = Column(Integer)
    complete_games = Column(Integer)
    shutouts = Column(Integer)
    saves = Column(Integer)
    innings_pitched = Column(Float)
    hits_allowed = Column(Integer)
    runs_allowed = Column(Integer)
    earned_runs = Column(Integer)
    home_runs_allowed = Column(Integer)
    walks_allowed = Column(Integer)
    intentional_walks_allowed = Column(Integer)
    strikeouts_pitched = Column(Integer)
    hit_batters = Column(Integer)
    balks = Column(Integer)
    wild_pitches = Column(Integer)
    batters_faced = Column(Integer)
    whip = Column(Float)
    strikeouts_per_nine = Column(Float)
    strikeouts_per_walk = Column(Float)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class TeamSplits(Base):
    __tablename__ = 'team_splits'
    
    # Primary key - composite to allow for multiple splits per team/season
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to teams (using mlb_id)
    team_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    
    # Split classification  
    split_category = Column(String(50), nullable=False)  # home_away, monthly, vs_opponent, etc.
    split_type = Column(String(50), nullable=False)      # home, april, vs_nyy, etc.
    split_value = Column(String(50), nullable=False)     # Human readable split name
    
    # Team performance stats
    games = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    win_percentage = Column(Float)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


