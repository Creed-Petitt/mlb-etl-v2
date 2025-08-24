from sqlalchemy import Column, Integer, String, DateTime, Date, Boolean, Text, Float, Numeric, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class PlayerSeasonStats(Base):
    __tablename__ = 'player_season_stats'
    
    # Composite primary key
    player_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)
    
    # Player info
    player_name = Column(String(100))
    primary_position = Column(String(50))
    
    # Batting season statistics
    batting_games_played = Column(Integer)
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
    batting_avg = Column(Float)  # Stored as decimal like .325
    on_base_pct = Column(Float)
    slugging_pct = Column(Float)
    ops = Column(Float)
    plate_appearances = Column(Integer)
    hit_by_pitch = Column(Integer)
    sac_flies = Column(Integer)
    sac_bunts = Column(Integer)
    intentional_walks = Column(Integer)
    ground_into_double_play = Column(Integer)
    total_bases = Column(Integer)
    
    # Pitching season statistics
    pitching_games_played = Column(Integer)
    games_started = Column(Integer)
    complete_games = Column(Integer)
    shutouts = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    saves = Column(Integer)
    save_opportunities = Column(Integer)
    holds = Column(Integer)
    blown_saves = Column(Integer)
    innings_pitched = Column(Float)  # Can be like 123.1 (123.33)
    hits_allowed = Column(Integer)
    runs_allowed = Column(Integer)
    earned_runs = Column(Integer)
    home_runs_allowed = Column(Integer)
    walks_allowed = Column(Integer)
    hit_batsmen = Column(Integer)
    pitcher_strikeouts = Column(Integer)
    wild_pitches = Column(Integer)
    balks = Column(Integer)
    era = Column(Float)
    whip = Column(Float)
    batters_faced = Column(Integer)
    pitches_thrown = Column(Integer)
    strikes = Column(Integer)
    balls = Column(Integer)
    strike_percentage = Column(Float)
    wins_above_replacement = Column(Float)  # WAR if available
    
    # Fielding season statistics  
    fielding_games = Column(Integer)
    fielding_games_started = Column(Integer)
    innings_fielded = Column(Float)
    putouts = Column(Integer)
    assists = Column(Integer)
    errors = Column(Integer)
    double_plays = Column(Integer)
    fielding_percentage = Column(Float)
    range_factor = Column(Float)
    
    # Advanced metrics (if available)
    wrc_plus = Column(Integer)  # Weighted Runs Created Plus
    war_batting = Column(Float)  # Batting WAR
    war_pitching = Column(Float)  # Pitching WAR
    war_fielding = Column(Float)  # Fielding WAR
    
    # Metadata
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_player_season_stats_player_season', 'player_id', 'season'),
        Index('idx_player_season_stats_season', 'season'),
        Index('idx_player_season_stats_position', 'primary_position'),
    )

class TeamSeasonStats(Base):
    __tablename__ = 'team_season_stats'
    
    # Composite primary key
    team_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)
    
    # Team identification
    team_name = Column(String(100))  # "Toronto Blue Jays"
    team_abbreviation = Column(String(10))  # "TOR"
    club_name = Column(String(50))  # "Blue Jays"
    location_name = Column(String(50))  # "Toronto"
    
    # League/Division info
    league_id = Column(Integer)
    league_name = Column(String(50))  # "American League"
    division_id = Column(Integer)
    division_name = Column(String(50))  # "American League East"
    
    # Season record
    games_played = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    ties = Column(Integer, default=0)
    winning_percentage = Column(Float)  # .582
    
    # Standings
    division_rank = Column(Integer)
    is_division_leader = Column(Boolean, default=False)
    games_back_division = Column(Float)  # Could be fractional like 2.5
    games_back_wildcard = Column(Float)
    games_back_league = Column(Float)
    
    # League record breakdown
    league_wins = Column(Integer)
    league_losses = Column(Integer)
    league_ties = Column(Integer, default=0)
    league_pct = Column(Float)
    
    # Additional team stats (if available in API)
    runs_scored = Column(Integer)
    runs_allowed = Column(Integer)
    run_differential = Column(Integer)
    home_wins = Column(Integer)
    home_losses = Column(Integer)
    away_wins = Column(Integer)
    away_losses = Column(Integer)
    
    # Streak information
    current_streak = Column(String(10))  # "W3", "L2", etc.
    longest_win_streak = Column(Integer)
    longest_loss_streak = Column(Integer)
    
    # Playoff status
    playoff_odds = Column(Float)  # If available
    magic_number = Column(Integer)  # If available
    elimination_number = Column(Integer)  # If available
    
    # Metadata
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_team_season_stats_team_season', 'team_id', 'season'),
        Index('idx_team_season_stats_season', 'season'),
        Index('idx_team_season_stats_division', 'division_id', 'season'),
        Index('idx_team_season_stats_league', 'league_id', 'season'),
    )

