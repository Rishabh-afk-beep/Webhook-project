"""Database initialization helper.

Used during startup or setup to verify database connectivity and
optionally create tables if running in development mode without Alembic.
"""
import logging
from sqlalchemy import text
from app.db.base import Base
from app.db.session import get_engine, create_session

logger = logging.getLogger(__name__)


def init_db(create_tables: bool = False) -> bool:
    """Verify database connection and optionally create tables.

    Args:
        create_tables: If True, creates all tables defined in Base metadata.

    Returns:
        bool: True if connection was successful.
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully.")

        if create_tables:
            logger.info("Creating all database tables...")
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables verified/created successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


if __name__ == "__main__":
    init_db(create_tables=True)
