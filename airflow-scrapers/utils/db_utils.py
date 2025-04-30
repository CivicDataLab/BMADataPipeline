import os
import logging
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, select,MetaData,Table
from sqlalchemy.orm import Session
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
# extracting setting up engine and metadata using SQLAlchemy Core pattern.
def setup_engine_and_metadata():
    db_host = os.getenv("POSTGRES_HOST")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Missing one or more environment variables")
    engine=None
    metadata=None
    try:

        engine=create_engine(
            f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
        )
        metadata=MetaData()
        metadata.reflect(bind=engine)
    except Exception as e:
        logging.exception(f"error connecting to the database: {str(e)}")
    return engine,metadata