from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Text,
    ForeignKey,
    Numeric,
    UniqueConstraint,
    Index,
    DECIMAL
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class EspnOdds(Base):
    __tablename__ = 'espn_odds'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to existing games table
    game_pk = Column(Integer, nullable=False)  # Links to Game.game_pk
    
    # ESPN game ID for reference
    espn_game_id = Column(String(20))
    
    # Bet type and side
    bet_type = Column(String(20), nullable=False)  # moneyline, runline, total
    bet_side = Column(String(10), nullable=False)  # home, away, over, under
    
    # Odds tracking (open, close, final)
    open_line = Column(String(10))    # For runline/total
    open_odds = Column(String(10))
    close_line = Column(String(10))   # For runline/total
    close_odds = Column(String(10))
    final_line = Column(String(10))   # For runline/total
    final_odds = Column(String(10))
    
    # Betting analysis fields
    outcome = Column(String(10))      # WIN, LOSS, PUSH for completed games
    favorite = Column(Boolean)        # True if team is favorite
    favorite_at_open = Column(Boolean) # True if team was favorite at open
    underdog = Column(Boolean)        # True if team is underdog
    spread_odds = Column(String(10))  # Spread odds (separate from line)
    
    # Provider
    provider_name = Column(String(50), default="ESPN BET")
    
    # Tracking
    created_at = Column(DateTime)
    updated_at = Column(DateTime)



class DraftKingsBook(Base):
    __tablename__ = "dk_books"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)  # e.g., "DraftKings"
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DraftKingsEvent(Base):
    __tablename__ = "dk_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("dk_books.id"), nullable=False)

    # DraftKings identifiers/fields
    book_event_id = Column(String(40), index=True)
    competition_id = Column(String(40))
    event_type_id = Column(String(40))
    country_code = Column(String(5))
    event_name = Column(String(200))
    market_group = Column(String(100))
    open_date = Column(DateTime)
    status = Column(String(20))

    game_pk = Column(Integer, index=True, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    markets = relationship("DraftKingsMarket", back_populates="event")

    __table_args__ = (
        Index("ix_dke_book_event", "book_id", "book_event_id"),
    )

class DraftKingsMarket(Base):
    __tablename__ = "dk_markets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("dk_books.id"), nullable=False)
    event_id = Column(Integer, ForeignKey("dk_events.id"), nullable=False)

    book_market_id = Column(String(64), nullable=False, unique=True)
    market_type = Column(String(100))
    market_name = Column(String(200))
    market_level = Column(String(30))
    market_time = Column(DateTime)
    in_play = Column(Boolean, default=False)
    sgm_market = Column(Boolean, default=False)
    status = Column(String(20))

    market_category = Column(String(30), index=True)
    market_key = Column(String(100), index=True)
    source_category_id = Column(Integer, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    event = relationship("DraftKingsEvent", back_populates="markets")
    runners = relationship("DraftKingsRunner", back_populates="market")
    prices = relationship("DraftKingsPrice", back_populates="market")

class DraftKingsRunner(Base):
    __tablename__ = "dk_runners"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, ForeignKey("dk_markets.id"), nullable=False)

    selection_id = Column(String(128), index=True)
    runner_name = Column(String(150))
    handicap = Column(Numeric(10, 3))
    is_player = Column(Boolean, default=False)
    runner_status = Column(String(20))
    sort_priority = Column(Integer)

    player_mlb_id = Column(Integer, nullable=True)
    team_mlb_id = Column(Integer, nullable=True)
    team_abbrev = Column(String(10))

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    market = relationship("DraftKingsMarket", back_populates="runners")
    prices = relationship("DraftKingsPrice", back_populates="runner")

    __table_args__ = (
        Index("ix_dkr_mkt_sel", "market_id", "selection_id"),
    )

class DraftKingsPrice(Base):
    __tablename__ = "dk_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, ForeignKey("dk_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("dk_runners.id"), nullable=True)

    selection_id = Column(String(128), index=True)

    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    in_play = Column(Boolean, default=False)

    american_odds = Column(Integer)
    decimal_odds = Column(Numeric(10, 3))
    fractional_numerator = Column(Integer)
    fractional_denominator = Column(Integer)
    true_decimal_odds = Column(Numeric(10, 3))

    line = Column(Numeric(10, 3))

    raw_price_json = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)

    market = relationship("DraftKingsMarket", back_populates="prices")
    runner = relationship("DraftKingsRunner", back_populates="prices")

    __table_args__ = (
        Index("ix_dkp_market_time", "market_id", "fetched_at"),
    )

class DraftKingsLineMovement(Base):
    __tablename__ = "dk_line_movements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("dk_books.id"), nullable=False)
    market_id = Column(Integer, ForeignKey("dk_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("dk_runners.id"), nullable=True)
    selection_id = Column(String(128), index=True)

    old_line = Column(Numeric(10,3))
    new_line = Column(Numeric(10,3))
    old_american_odds = Column(Integer)
    new_american_odds = Column(Integer)
    movement_type = Column(String(20))
    moved_at = Column(DateTime, default=datetime.utcnow)

class DraftKingsTeamAlias(Base):
    __tablename__ = "dk_team_aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_name = Column(String(50), nullable=False)
    book_team = Column(String(100), nullable=False)
    mlb_team_abbrev = Column(String(10))
    mlb_team_id = Column(Integer)

    __table_args__ = (
        UniqueConstraint("book_name", "book_team", name="uq_dk_team_alias"),
    )

class DraftKingsPlayerAlias(Base):
    __tablename__ = "dk_player_aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_name = Column(String(50), nullable=False)
    selection_id = Column(String(64), nullable=True)
    runner_name = Column(String(150), nullable=False)
    mlb_player_id = Column(Integer)

    __table_args__ = (
        UniqueConstraint("book_name", "runner_name", name="uq_dk_player_alias"),
    )

class DraftKingsSettlement(Base):
    __tablename__ = "dk_settlements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("dk_books.id"), nullable=False)
    market_id = Column(Integer, ForeignKey("dk_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("dk_runners.id"), nullable=True)

    selection_id = Column(String(64))
    market_key = Column(String(100))
    result = Column(String(10))
    final_line = Column(Numeric(10,3))
    actual_value = Column(Numeric(10,3))
    settled_at = Column(DateTime, default=datetime.utcnow)
    note = Column(Text)



class FanDuelBook(Base):
    __tablename__ = "fd_books"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)  # e.g., "FanDuel"
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class FanDuelEvent(Base):
    __tablename__ = "fd_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("fd_books.id"), nullable=False)

    # FanDuel identifiers/fields
    book_event_id = Column(String(30), index=True)  # e.g., "34596985"
    competition_id = Column(String(30))            # e.g., "11196870"
    event_type_id = Column(String(30))             # e.g., "7511"
    country_code = Column(String(5))               # e.g., "US"/"GB"
    event_name = Column(String(200))               # Full display name
    market_group = Column(String(100))             # e.g., "MLB", "MLB - Player Markets"
    open_date = Column(DateTime)
    status = Column(String(20))                    # OPEN/CLOSED/etc.

    # Linkage to MLB game if resolvable
    game_pk = Column(Integer, index=True, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    markets = relationship("FanDuelMarket", back_populates="event")

    __table_args__ = (
        Index("ix_fd_event_book_event", "book_id", "book_event_id"),
    )

class FanDuelMarket(Base):
    __tablename__ = "fd_markets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("fd_books.id"), nullable=False)
    event_id = Column(Integer, ForeignKey("fd_events.id"), nullable=False)

    # FanDuel identifiers/fields
    book_market_id = Column(String(40), nullable=False, unique=True)  # e.g., "734.135624394"
    market_type = Column(String(100))          # e.g., "MATCH_HANDICAP_(2-WAY)", "PLAYER_TO_RECORD_A_HIT"
    market_name = Column(String(200))          # e.g., "Run Line", "To Record A Hit"
    market_level = Column(String(30))          # e.g., "AVB_EVENT", "COMPETITION"
    market_time = Column(DateTime)             # ISO from API
    in_play = Column(Boolean, default=False)
    sgm_market = Column(Boolean, default=False)
    status = Column(String(20))                # OPEN/CLOSED/etc.

    # Derived normalization
    market_category = Column(String(30), index=True)   # game | player_prop | team_future | player_future
    market_key = Column(String(100), index=True)       # run_line | total_runs | pitcher_strikeouts | ...
    source_category_id = Column(Integer, index=True)   # Book-native category id (e.g., DK 493)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    event = relationship("FanDuelEvent", back_populates="markets")
    runners = relationship("FanDuelRunner", back_populates="market")
    prices = relationship("FanDuelPrice", back_populates="market")

class FanDuelRunner(Base):
    __tablename__ = "fd_runners"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, ForeignKey("fd_markets.id"), nullable=False)

    selection_id = Column(String(128), index=True)  # Book selection identifier (FanDuel/DK)
    runner_name = Column(String(150))
    handicap = Column(Numeric(10, 3))              # spread/total line if applicable
    is_player = Column(Boolean, default=False)
    runner_status = Column(String(20))
    sort_priority = Column(Integer)

    # Optional resolved mapping to our MLB IDs
    player_mlb_id = Column(Integer, nullable=True)
    team_mlb_id = Column(Integer, nullable=True)
    team_abbrev = Column(String(10))

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    market = relationship("FanDuelMarket", back_populates="runners")
    prices = relationship("FanDuelPrice", back_populates="runner")

    __table_args__ = (
        Index("ix_fd_runner_mkt_sel", "market_id", "selection_id"),
    )

class FanDuelPrice(Base):
    __tablename__ = "fd_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, ForeignKey("fd_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("fd_runners.id"), nullable=True)

    # Keys to identify selection if runner row not created yet
    selection_id = Column(String(128), index=True)

    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    in_play = Column(Boolean, default=False)

    # Odds formats
    american_odds = Column(Integer)               # +110 / -150
    decimal_odds = Column(Numeric(10, 3))
    fractional_numerator = Column(Integer)
    fractional_denominator = Column(Integer)
    true_decimal_odds = Column(Numeric(10, 3))    # From trueOdds if present

    # Line context when applicable (totals/spreads/handicaps)
    line = Column(Numeric(10, 3))

    # Raw payloads for audit/debug
    raw_price_json = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    market = relationship("FanDuelMarket", back_populates="prices")
    runner = relationship("FanDuelRunner", back_populates="prices")

    __table_args__ = (
        Index("ix_fd_price_market_time", "market_id", "fetched_at"),
    )

class FanDuelLineMovement(Base):
    __tablename__ = "fd_line_movements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("fd_books.id"), nullable=False)
    market_id = Column(Integer, ForeignKey("fd_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("fd_runners.id"), nullable=True)
    selection_id = Column(String(128), index=True)

    old_line = Column(Numeric(10,3))
    new_line = Column(Numeric(10,3))
    old_american_odds = Column(Integer)
    new_american_odds = Column(Integer)
    movement_type = Column(String(20))  # line_up/line_down/odds_up/odds_down/new
    moved_at = Column(DateTime, default=datetime.utcnow)

class FanDuelTeamAlias(Base):
    __tablename__ = "fd_team_aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_name = Column(String(50), nullable=False)        # e.g., FanDuel
    book_team = Column(String(100), nullable=False)       # display name the book uses
    mlb_team_abbrev = Column(String(10))
    mlb_team_id = Column(Integer)

    __table_args__ = (
        UniqueConstraint("book_name", "book_team", name="uq_fd_team_alias"),
    )

class FanDuelPlayerAlias(Base):
    __tablename__ = "fd_player_aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_name = Column(String(50), nullable=False)
    selection_id = Column(String(40), nullable=True)      # FanDuel selectionId if stable
    runner_name = Column(String(150), nullable=False)
    mlb_player_id = Column(Integer)

    __table_args__ = (
        UniqueConstraint("book_name", "runner_name", name="uq_fd_player_alias"),
    )

class FanDuelSettlement(Base):
    __tablename__ = "fd_settlements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("fd_books.id"), nullable=False)
    market_id = Column(Integer, ForeignKey("fd_markets.id"), nullable=False)
    runner_id = Column(Integer, ForeignKey("fd_runners.id"), nullable=True)

    selection_id = Column(String(40))
    market_key = Column(String(100))           # e.g., total_runs, run_line, moneyline
    result = Column(String(10))                # win | loss | push
    final_line = Column(Numeric(10,3))
    actual_value = Column(Numeric(10,3))
    settled_at = Column(DateTime, default=datetime.utcnow)

    note = Column(Text)                        # optional context (e.g., final score)


class PrizePicksPlayer(Base):
    """Player master data from PrizePicks"""
    __tablename__ = 'prizepicks_players'
    
    id = Column(Integer, primary_key=True)
    prizepicks_player_id = Column(String(50), unique=True, nullable=False)  # "67499"
    name = Column(String(100))                                              # "Carson Kelly"
    display_name = Column(String(100))                                      # Same as name usually
    team = Column(String(10))                                               # "CHC"
    team_name = Column(String(50))                                          # "Cubs"
    position = Column(String(10))                                           # "C", "1B", etc.
    jersey_number = Column(String(10))                                      # "15"
    league = Column(String(10), default='MLB')
    image_url = Column(Text)                                                # Player photo URL
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
class PrizePicksTeam(Base):
    """Team data from PrizePicks"""
    __tablename__ = 'prizepicks_teams'
    
    id = Column(Integer, primary_key=True)
    prizepicks_team_id = Column(String(50), unique=True, nullable=False)    # "2657"
    team_code = Column(String(10))                                          # "CHC"
    team_name = Column(String(50))                                          # "Cubs"
    market = Column(String(50))                                             # "Chicago"
    league = Column(String(10), default='MLB')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class PrizePicksGame(Base):
    """Game information from PrizePicks"""
    __tablename__ = 'prizepicks_games'
    
    id = Column(Integer, primary_key=True)
    prizepicks_game_id = Column(String(50), unique=True, nullable=False)    # "60966"
    external_game_id = Column(String(100))                                  # "MLB_game_OgkEVJi8ECp64BudemBSIw7u"
    start_time = Column(DateTime)
    status = Column(String(20))                                             # From projections
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
class PrizePicksProjection(Base):
    """Main betting props table - current active props"""
    __tablename__ = 'prizepicks_projections'
    
    id = Column(Integer, primary_key=True)
    prizepicks_id = Column(String(50), unique=True, nullable=False)         # "5836744"
    player_id = Column(Integer, ForeignKey('prizepicks_players.id'))
    game_id = Column(Integer, ForeignKey('prizepicks_games.id'))
    stat_type = Column(String(50))                                          # "Total Bases", "Hits", etc.
    current_line_score = Column(DECIMAL(5,2))                               # Current line: 3.5, 1.5, etc.
    description = Column(String(100))                                       # "BAL" (team abbreviation)
    status = Column(String(20))                                             # "pre_game", "live", etc.
    start_time = Column(DateTime)                                           # When game starts
    board_time = Column(DateTime)                                           # When prop was first posted
    last_updated = Column(DateTime)                                         # Last time we saw this prop
    is_live = Column(Boolean, default=False)
    is_promo = Column(Boolean, default=False)
    odds_type = Column(String(20))                                          # "demon", "standard", etc.
    is_active = Column(Boolean, default=True)                               # False when prop is removed/settled
    created_at = Column(DateTime, default=datetime.utcnow)

class PrizePicksSettlement(Base):
    """Settlement results for PrizePicks projections"""
    __tablename__ = 'prizepicks_settlements'
    
    id = Column(Integer, primary_key=True)
    projection_id = Column(Integer, ForeignKey('prizepicks_projections.id'), unique=True, nullable=False)
    final_line_score = Column(DECIMAL(5,2), nullable=False)                 # Line at settlement time
    actual_result = Column(DECIMAL(5,2), nullable=False)                    # Actual stat value achieved
    settlement_result = Column(String(10), nullable=False)                  # "over", "under", "push"
    settled_at = Column(DateTime, default=datetime.utcnow)
    
    # Optional metadata
    game_pk = Column(Integer)                                               # MLB game_pk if matched
    player_name_used = Column(String(100))                                  # Name used for matching
    notes = Column(Text)                                                    # Any issues or special cases
    
    created_at = Column(DateTime, default=datetime.utcnow)


