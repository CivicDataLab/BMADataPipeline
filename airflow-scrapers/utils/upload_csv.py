import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from models.models import CanalDredgingProgress 
from utils.canals_translation_map_utils import thai_to_column_mapping
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

def upload_large_canal_csv_file():
    # 1. get engine & table name
    engine, _ = setup_engine_and_metadata()
    table_name = CanalDredgingProgress.__tablename__
    csv_path   = os.getenv("CSV_FILE_PATH", "canal_dredging_progress.csv")

    # 2. read & parse the header line
    with open(csv_path, newline='', encoding="utf-8-sig") as f:
        header_line = f.readline().strip()
    csv_headers = [h.strip() for h in header_line.split(',')]

    # 3. map to English, skipping any unmapped column
    english_cols = []
    for h in csv_headers:
        eng = thai_to_column_mapping.get(h)
        if eng:
            english_cols.append(eng)
        else:
            logging.debug(f"Skipping unmapped CSV column: {h}")

    if not english_cols:
        raise RuntimeError("No CSV columns mapped – check your CSV headers vs mapping")

    # 4. build COPY statement
    copy_sql = f"""
        COPY {table_name} ({', '.join(english_cols)})
        FROM STDIN
        WITH CSV
        DELIMITER ','
    """

    # 5. stream into the table
    with engine.connect() as conn:
        raw_conn = conn.connection      # psycopg2 connection
        cur      = raw_conn.cursor()
        try:
            with open(csv_path, 'r', encoding="utf-8-sig") as f:
                f.readline()             # skip header row
                cur.copy_expert(copy_sql, f)
            raw_conn.commit()
            logging.info(f"Finished bulk COPY into {table_name}")
        finally:
            cur.close()

    engine.dispose()


  

if __name__ == "__main__":
    upload_large_canal_csv_file()
