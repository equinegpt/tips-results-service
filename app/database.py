# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


# For SQLite we need special connect_args, for Postgres we don't.
connect_args = {}
pool_settings = {}

if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    # PostgreSQL: add connection pool health checks
    pool_settings = {
        "pool_pre_ping": True,  # Test connections before using, auto-reconnect stale
        "pool_recycle": 300,    # Recycle connections every 5 minutes
    }

engine = create_engine(
    settings.database_url,
    connect_args=connect_args,
    **pool_settings,
)

# 👇 This is what results_ra.py wants to import
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency that yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
