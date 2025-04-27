import sys
import os
import json
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
from utils.canals_translation_map_utils import thai_to_column_mapping_street_cleaning
from utils.buddhist_year_converter_utils import convert_current_time_to_bkk_timezone_and_buddhist_year
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(messages)s",
    level=logging.INFO
)


BMA_WEATHER_API_URL_NEW=os.getenv("BMA_WEATHER_API_URL_NEW")
BANGKOK_TIMEZONE=Timezone("Asia/Bangkok")
@dag(
    dag_id="sewarage_dredging_scraper",
    schedule="0 0 1,16 * *", #runs every 1st and 16th day of month 
    start_date=pendulum.datetime(2025,4,23, tz="local"),
    catchup=False,
    tags=['api', 'bangkok', 'sewarage_dredging_progress_report']
)

def sewarage_dredging_pipeline():
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
        sewerage_dredging_table = Table(
        'sewerage_dredging_progress', metadata,
        Column('id', Integer, primary_key=True),
        Column('record_number', Integer),
        Column('district_agency', String(255)),
        Column('total_alleys', Integer),
        Column('total_length_m', Integer),
        Column('alleys_not_cleaned_this_year', Integer),
        Column('length_not_cleaned_this_year_m', Integer),
        Column('alleys_cleaned_this_year', Integer),
        Column('length_cleaned_this_year_m', Integer),
        Column('planned_alleys_by_district_labor', Integer),
        Column('planned_length_by_district_labor_m', Integer),
        Column('executed_alleys_by_district_labor', Integer),
        Column('executed_length_by_district_labor_m', Integer),
        Column('percent_executed_by_district_labor', Float),
        Column('planned_alleys_by_dop', Integer),
        Column('planned_length_by_dop_m', Integer),
        Column('annual_budgeted_alleys_by_dop', Integer),
        Column('annual_budgeted_length_by_dop_m', Integer),
        Column('executed_length_annual_by_dop_m', Integer),
        Column('percent_executed_annual_by_dop', Float),
        Column('supplementary_budgeted_alleys_by_dop', Integer),
        Column('supplementary_budgeted_length_by_dop_m', Integer),
        Column('executed_length_supplementary_by_dop_m', Integer),
        Column('percent_executed_supplementary_by_dop', Float),
        Column('planned_alleys_by_private', Integer),
        Column('planned_length_by_private_m', Integer),
        Column('annual_budgeted_alleys_by_private', Integer),
        Column('annual_budgeted_length_by_private_m', Integer),
        Column('executed_length_annual_by_private_m', Integer),
        Column('percent_executed_annual_by_private', Float),
        Column('supplementary_budgeted_alleys_by_private', Integer),
        Column('supplementary_budgeted_length_by_private_m', Integer),
        Column('executed_length_supplementary_by_private_m', Integer),
        Column('percent_executed_supplementary_by_private', Float),
        Column('manhole_cover_repaired', Integer),
        Column('grating_repaired', Integer),
        Column('curb_repaired', Integer),
        Column('remarks', String(255)),
        Column('district_code', String(50)),
        Column('buddhist_year', Integer),
        Column('day_period', String(50)),
        Column('month', String(50)),
        Column('record_date', String(50)),
        Column('reporting_date', String(50)),
        Column('plan_sequence', String(50)),
        Column('annual_budget_by_private_baht', Float),
        Column('supplementary_budget_by_private_baht', Float),
        Column('executed_alleys_annual_by_private', Integer),
        Column('executed_alleys_supplementary_by_private', Integer),
        Column('annual_budget_by_dop_baht', Float),
        Column('supplementary_budget_by_dop_baht', Float),
        Column('executed_alleys_annual_by_dop', Integer),
        Column('executed_alleys_supplementary_by_dop', Integer),
        Column('created_at', DateTime(timezone=True), server_default=func.now()),
        Column('updated_at', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
        )

        inspector=inspect(engine)
        if "sewerage_dredging_progress" in inspector.get_table_names():
            # Check for column mismatches (very basic check on column names and types)
            #WARNING this is temporary logic for migration. DO NOT USE IN PRODUCTION
            existing_columns = {col["name"]: col["type"] for col in inspector.get_columns("sewerage_dredging_progress")}
            defined_columns = {col.name: col.type for col in sewerage_dredging_progress.columns}

            mismatch = set(existing_columns.keys()) ^ set(defined_columns.keys())  # check for added/removed columns
            if mismatch:
                logging.warning("Schema mismatch detected. Dropping and recreating sewerage_dredging_progress table.")
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS sewerage_dredging_progress CASCADE"))
                metadata.create_all(engine)
                logging.info("Recreated sewerage_dredging_progress table with updated schema.")
            else:
                logging.info("sewerage_dredging_progress table already exists and matches schema.")
        else:
            metadata.create_all(engine)
            logging.info("Created sewerage_dredging_progress table.")
    
    @task()
    def fetch_and_store_sewerage_dredging_progress():
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        if not weather_api_key:
            raise ValueError("Missing one or more database environment variables.")
        
        headers={
            "KeyId":weather_api_key
        }
        current_date=pendulum.now(tz="Asia/Bangkok")
        buddhist_year,month,date=convert_current_time_to_bkk_timezone_and_buddhist_year(current_date)
        
        period='01-15' if date ==1 else '16-30' if date==16 else '16-30'
        canal_dredging_api_url=f"{BMA_WEATHER_API_URL_NEW}/ProgressReport/DDS?ReportType=01&Period={period}&Month={month}&Year={buddhist_year}"

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
            headers=headers,
            db_type="BMA",
            
        )
        operator.execute(context={})
    create_table_and_insert() >> fetch_and_store_sewerage_dredging_progress() 
sewarage_dredging_pipeline()
