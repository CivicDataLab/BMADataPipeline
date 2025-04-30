import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, Float,DateTime, func, ForeignKey,text
)
from airflow.decorators import dag, task
import logging
import pendulum
from pendulum.tz.timezone import Timezone
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.canals_translation_map_utils import thai_to_column_mapping
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(messages)s",
    level=logging.INFO
)

# "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1?output_format=json&problem_type=น้ำท่วม&start=2025-04-21&end=2025-04-21"
TRAFFY_FONDUE_CITIZEN_COMPLAINT_URL=os.getenv("TRAFFY_FONDUE_CITIZEN_COMPLAINT_URL")
BANGKOK_TIMEZONE=Timezone("Asia/Bangkok")
@dag(
    dag_id="traffy_fondue_citizen_complaint_scraper",
    schedule="0 1 * * *", #runs every one hour everyday
    start_date=pendulum.datetime(2025,4,24, tz="local"),
    catchup=False,
    tags=['api', 'bangkok', 'traffy_fondue_complaint_scraper'] 
)

def traffy_fondue_citizen_complaint_pipeline():
    @task()
    def create_table_and_insert():
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
        traffy_fondue_citizen_complaint= Table(
        'traffy_fondue_citizen_complaint', metadata,
        Column('id', Integer, primary_key=True),
        Column('problem_type', String(255)),
        Column('description', String(255)),
        Column('address', String(255)),
        Column('district', String(255)),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('state',String),
        Column('complaint_raised_at', DateTime(timezone=True)),
        Column('created_at', DateTime(timezone=True), server_default=func.now()),
        Column('updated_at', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
        )

        inspector=inspect(engine)
        if "traffy_fondue_citizen_complaint" in inspector.get_table_names():
            # Check for column mismatches (very basic check on column names and types)
            # WARNING this is temporary logic for migration. DO NOT USE IN PRODUCTION
            # WRITE a migration script if time permits. 
            #  Or use ALTER TABLE statement below.
            existing_columns = {col["name"]: col["type"] for col in inspector.get_columns("traffy_fondue_citizen_complaint")}
            defined_columns = {col.name: col.type for col in traffy_fondue_citizen_complaint.columns}

            mismatch = set(existing_columns.keys()) ^ set(defined_columns.keys())  # check for added/removed columns
            if mismatch:
                logging.warning("Schema mismatch detected. Dropping and recreating traffy_fondue_citizen_complaint table.")
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS traffy_fondue_citizen_complaint CASCADE"))
                metadata.create_all(engine)
                logging.info("Recreated traffy_fondue_citizen_complaint table with updated schema.")
            else:
                logging.info("traffy_fondue_citizen_complaint table already exists and matches schema.")
     
        else:
            metadata.create_all(engine)
            logging.info("Created traffy_fondue_citizen_complaint table.")
    
    @task()
    def fetch_and_store_traffy_fondue_citizen_progress():


        # canal_dredging_api_url=f"{TRAFFY_FONDUE_CITIZEN_COMPLAINT_URL}&start={}&end={}"

        def transform_sewarage_dredging_data(data):
            transformed_data=[]
            try:

                logging.info(f"Received {len(data)} items from the API")

                if data:
                    logging.info(f"First item sample: {json.dumps(data[0],ensure_ascii=False)}")
                for item in data:
                   transformed_record={}
                   for thai_key, eng_col in thai_to_column_mapping_street_cleaning.items():
                        raw_value=item.get(thai_key, "")
                        # typecasting based on column pattern
                        # for meter based fields
                        if eng_col.endswith("_m"):
                            try:
                                transformed_record[eng_col]=int(raw_value.replace(",","")) if raw_value not in [None, "-", ""] else 0
                            except Exception:
                                transformed_record[eng_col]=0
                        elif eng_col.startswith("percent_") or eng_col.startswith("total_percent"):
                            try:
                                transformed_record[eng_col]=float(raw_value) if raw_value not in [None, "-", ""] else 0.0
                            except Exception:
                                transformed_record[eng_col]=0
                        elif eng_col in ["buddhist_year"]:
                            try:
                                transformed_record[eng_col] = int(raw_value) if raw_value else None
                            except Exception:
                                transformed_record[eng_col] = None
                        elif eng_col in ["day_period"]:
                            try:
                                transformed_record[eng_col]=str(raw_value) if raw_value else str(period)
                            except Exception:
                                transformed_record[eng_col]=str(period)
                        elif eng_col in ["record_date"]:
                            try:
                                transformed_record[eng_col]=str(raw_value) if raw_value else current_date
                            except Exception:
                                transformed_record[eng_col]=current_date

                        # elif eng_col in ["record_number", "total_dredged_canals", "planned_dredging_canals", "additional_budgeted_canals", "annual_budgeted_canals", "regularly_maintained_canals", "periodically_flowed_canals", "total_canals_assigned"]:
                        #     try:
                        #         transformed_record[eng_col] = int(raw_value) if raw_value not in [None, "-", ""] else 0
                        #     except Exception:
                        #         transformed_record[eng_col] = 0
                        
                        else:
                            transformed_record[eng_col]=raw_value if raw_value not in ["-",""] else None
                        
                   transformed_data.append(transformed_record)
                if not transformed_data:
                    raise ValueError("No data was transformed. Check API response and transformation step. ")
                logging.info(f"Transformed {len(transformed_data)}")
                return transformed_data
            except Exception as e:
                logging.exception(f"error transforming risk point data: {str(e)}")
                return []
        logging.info("Bulk inserting all risk point data")
        operator=ApiToPostgresOperator(
            task_id="fetch_and_store_sewarage_dredging_progress",
            api_url=canal_dredging_api_url,
            transform_func=transform_sewarage_dredging_data,
            table_name="sewerage_dredging_progress",
            # headers=headers,
            db_type="BMA",
            
        )
        operator.execute(context={})
    create_table_and_insert()
traffy_fondue_citizen_complaint_pipeline()
