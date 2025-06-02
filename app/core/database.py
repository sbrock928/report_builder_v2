"""Database configuration and session management."""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

from app.core.config import settings

# Main application database (for storing calculations)
engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for application models
Base = declarative_base()

# Data warehouse database (your existing financial data)
dw_engine = create_engine(
    settings.DW_DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DW_DATABASE_URL else {}
)

DWSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=dw_engine)

# Base class for data warehouse models
DWBase = declarative_base()

def get_db_session() -> Generator[Session, None, None]:
    """Get main application database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_dw_session() -> Generator[Session, None, None]:
    """Get data warehouse database session."""
    db = DWSessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Create all tables in the main database."""
    Base.metadata.create_all(bind=engine)

def create_dw_tables():
    """Create all tables in the data warehouse database (for demo)."""
    DWBase.metadata.create_all(bind=dw_engine)