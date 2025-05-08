import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from models.models import RainfallAndFloodSummary 

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

def setup_engine_and_metadata():
    db_host = os.getenv("POSTGRES_HOST")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Missing one or more environment variables")

    engine = create_engine(
        f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
    )
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return engine, metadata

def upload_large_csv_file():
    engine, _ = setup_engine_and_metadata()
    csv_path = os.getenv("CSV_FILE_PATH", "rainfall_and_flood_summary.csv")

    # Build the COPY SQL (no HEADER, we skip it manually)
    copy_sql = f"""
        COPY {RainfallAndFloodSummary.__tablename__}
        (risk_id,
         risk_name,
         category,
         district,
         flood_sensor_code,
         flood_code,
         hour,
         max_flood,
         flood_duration_minutes,
         rainfall_sensor_code,
         rf1hr)
        FROM STDIN
        WITH CSV
        DELIMITER ','
    """

    with engine.connect() as conn:
        # Get at the raw DBAPI connection & cursor
        dbapi_conn = conn.connection
        cursor = dbapi_conn.cursor()

        try:
            with open(csv_path, "r") as f:
                # Skip the header line ("risk_id,risk_name,…,rain_code,rf1hr")
                f.readline()
                # Stream the rest into COPY
                cursor.copy_expert(copy_sql, f)

            # Commit once all data is in
            dbapi_conn.commit()
            logging.info("Finished bulk COPY into rainfall_and_flood_summary")
        finally:
            cursor.close()

    engine.dispose()


def main():
    return False
  

if __name__ == "__main__":
    main()
