from sqlalchemy_utils import database_exists, drop_database, create_database
from app.database import engine
from app import models
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_database():
    """Reset the database by dropping and recreating it"""
    try:
        # Drop database if it exists
        if database_exists(engine.url):
            logger.info("Dropping existing database...")
            drop_database(engine.url)
        
        # Create new database
        logger.info("Creating new database...")
        create_database(engine.url)
        
        # Create all tables
        logger.info("Creating tables...")
        models.Base.metadata.create_all(bind=engine)
        
        logger.info("Database reset complete!")
        
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise

if __name__ == "__main__":
    reset_database()