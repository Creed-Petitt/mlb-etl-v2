import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Import all model bases to ensure they're registered
from .mlb_models import Base as MLBBase
from .betting_models import Base as BettingBase  
from .season_models import Base as SeasonBase

load_dotenv()

def get_database_engine():
    try:
        from core.logger import setup_logger
        logger = setup_logger("database")
        db_url = os.getenv('DATABASE_URL')
        logger.info(f"Connecting to database: {db_url.split('@')[1]}")
        return create_engine(db_url)
    except ImportError:
        # Fallback if logger not available
        db_url = os.getenv('DATABASE_URL')
        print(f"Connecting to database: {db_url.split('@')[1]}")
        return create_engine(db_url)

def create_all_tables():
    try:
        from core.logger import setup_logger
        logger = setup_logger("database")
        logger.info("Creating all database tables...")
        
        engine = get_database_engine()
        
        # Create tables from all bases
        MLBBase.metadata.create_all(engine)
        BettingBase.metadata.create_all(engine)
        SeasonBase.metadata.create_all(engine)
        
        logger.info("All tables created successfully")
    except ImportError:
        # Fallback if logger not available
        print("Creating all database tables...")
        
        engine = get_database_engine()
        
        MLBBase.metadata.create_all(engine)
        BettingBase.metadata.create_all(engine) 
        SeasonBase.metadata.create_all(engine)
        
        print("All tables created successfully")

def get_session():
    engine = get_database_engine()
    Session = sessionmaker(bind=engine)
    return Session()

# Individual sportsbook table creation functions
def create_draftkings_tables(engine):
    dk_tables = [table for table in BettingBase.metadata.tables.values() 
                 if table.name.startswith('dk_')]
    for table in dk_tables:
        table.create(engine, checkfirst=True)

def create_fanduel_tables(engine):
    fd_tables = [table for table in BettingBase.metadata.tables.values() 
                 if table.name.startswith('fd_')]
    for table in fd_tables:
        table.create(engine, checkfirst=True)

def create_prizepicks_tables(engine):
    pp_tables = [table for table in BettingBase.metadata.tables.values() 
                 if table.name.startswith('prizepicks_')]
    for table in pp_tables:
        table.create(engine, checkfirst=True)
    print("Created PrizePicks tables successfully")

def create_all_betting_tables(engine):
    BettingBase.metadata.create_all(engine)
    print("Created all betting tables successfully")

def create_season_tables(engine):
    SeasonBase.metadata.create_all(engine)
    print("Created season statistics tables successfully")

if __name__ == "__main__":
    create_all_tables()