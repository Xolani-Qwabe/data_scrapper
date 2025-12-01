# nba_database.py (Fixed Schema)
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid
from datetime import datetime

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/nba_stats"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NBAGameBasic(Base):
    __tablename__ = "nba_game_basic"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    player = Column(String(100), nullable=False)
    pos = Column(String(10))
    mp = Column(String(20))
    fg = Column(Integer, default=0)
    fga = Column(Integer, default=0)
    fg_pct = Column(Float, default=0.0)
    fg3 = Column(Integer, default=0)
    fg3a = Column(Integer, default=0)
    fg3_pct = Column(Float, default=0.0)
    ft = Column(Integer, default=0)
    fta = Column(Integer, default=0)
    ft_pct = Column(Float, default=0.0)
    orb = Column(Integer, default=0)
    drb = Column(Integer, default=0)
    trb = Column(Integer, default=0)
    ast = Column(Integer, default=0)
    stl = Column(Integer, default=0)
    blk = Column(Integer, default=0)
    tov = Column(Integer, default=0)
    pf = Column(Integer, default=0)
    pts = Column(Integer, default=0)
    gm_sc = Column(Float, default=0.0)
    plus_minus = Column(Integer, default=0)
    
    # Metadata - MAKE SURE THESE FIELDS EXIST
    game_date = Column(Date, nullable=False)
    team = Column(String(10), nullable=False)
    opponent = Column(String(10), nullable=False)
    home_away = Column(String(10))
    period = Column(String(10))
    is_starter = Column(Boolean, default=False)
    source_file = Column(String(200))
    created_at = Column(DateTime, default=datetime.now)

class NBAGameAdvanced(Base):
    __tablename__ = "nba_game_advanced"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    player = Column(String(100), nullable=False)
    pos = Column(String(10))
    mp = Column(String(20))
    ts_pct = Column(Float, default=0.0)
    efg_pct = Column(Float, default=0.0)
    fg3a_per_fga_pct = Column(Float, default=0.0)
    fta_per_fga_pct = Column(Float, default=0.0)
    orb_pct = Column(Float, default=0.0)
    drb_pct = Column(Float, default=0.0)
    trb_pct = Column(Float, default=0.0)
    ast_pct = Column(Float, default=0.0)
    stl_pct = Column(Float, default=0.0)
    blk_pct = Column(Float, default=0.0)
    tov_pct = Column(Float, default=0.0)
    usg_pct = Column(Float, default=0.0)
    off_rtg = Column(Float, default=0.0)
    def_rtg = Column(Float, default=0.0)
    bpm = Column(Float, default=0.0)
    
    # Metadata - MAKE SURE THESE FIELDS EXIST
    game_date = Column(Date, nullable=False)
    team = Column(String(10), nullable=False)
    opponent = Column(String(10), nullable=False)
    home_away = Column(String(10))
    is_starter = Column(Boolean, default=False)
    source_file = Column(String(200))
    created_at = Column(DateTime, default=datetime.now)

def create_tables():
    """Create all tables and print confirmation"""
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ Database tables created successfully")
        print("📋 Tables created:")
        for table in Base.metadata.tables.keys():
            print(f"   - {table}")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()