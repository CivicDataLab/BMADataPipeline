import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, Float,DateTime, func, ForeignKey,text,update,select
)
from airflow.decorators import dag, task
import logging
import pendulum
from pendulum.tz.timezone import Timezone
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.canals_translation_map_utils import thai_to_column_mapping
from utils.buddhist_year_converter_utils import convert_current_time_to_bkk_timezone_and_buddhist_year
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(messages)s",
    level=logging.INFO
)


BMA_WEATHER_API_URL_NEW=os.getenv("BMA_WEATHER_API_URL_NEW")
BANGKOK_TIMEZONE=Timezone("Asia/Bangkok")
@dag(
    dag_id="canal_dredging_scraper",
    schedule="0 0 1,16 * *", #runs every 1st and 16th day forever
    start_date=pendulum.datetime(2025,4,23, tz="local"),
    catchup=False,
    tags=['api', 'bangkok', 'canal_dredging_progress_report']
)

def canal_dredging_pipeline():
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
        canal_dredging_progress = Table(
        'canal_dredging_progress', metadata,
        Column('id', Integer, primary_key=True),
        Column('record_number', Integer),
        Column('district_agency', String(255)),
        Column('total_canals_assigned', Integer),
        Column('total_length_assigned_m', Integer),
        Column('regularly_maintained_canals', Integer),
        Column('regularly_maintained_length_m', Integer),
        Column('periodically_flowed_canals', Integer),
        Column('periodically_flowed_length_m', Integer),
        Column('total_maintained_flowed_length_m', Integer),
        Column('percent_maintained_flowed', Float),
        Column('planned_dredging_canals', Integer),
        Column('planned_dredging_length_m', Integer),
        Column('annual_budgeted_canals', Integer),
        Column('annual_budgeted_length_m', Integer),
        Column('annual_budget', Integer),
        Column('annual_committed_budget', String(255)),
        Column('dredged_length_annual_budget_m', Integer),
        Column('percent_dredged_annual_budget', Float),
        Column('additional_budgeted_canals', Integer),
        Column('additional_budgeted_length_m', Integer),
        Column('supplementary_budget', Integer),
        Column('supplementary_committed_budget', Integer),
        Column('dredged_length_supplementary_budget_m', Integer),
        Column('percent_dredged_supplementary_budget', Float),
        Column('total_dredged_canals', Integer),
        Column('total_percent_dredged', Float),
        Column('remarks', String(255)),
        Column('district_code', String(50)),
        Column('buddhist_year', Integer),
        Column('day_period', String(50)),
        Column('month', String(50)),
        Column('record_date', String(50)),
        Column('reporting_date', String(50)),
        Column('plan_sequence', String(50)),
        Column('created_at', DateTime(timezone=True), server_default=func.now()),
        Column('updated_at', DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    )
        inspector=inspect(engine)
        if "canal_dredging_progress" in inspector.get_table_names():
            # Check for column mismatches (very basic check on column names and types)
            #WARNING this is temporary logic for migration. DO NOT USE IN PRODUCTION
            existing_columns = {col["name"]: col["type"] for col in inspector.get_columns("canal_dredging_progress")}
            defined_columns = {col.name: col.type for col in canal_dredging_progress.columns}

            mismatch = set(existing_columns.keys()) ^ set(defined_columns.keys())  # check for added/removed columns
            if mismatch:
                logging.warning("Schema mismatch detected. Dropping and recreating canal_dredging_progress table.")
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS canal_dredging_progress CASCADE"))
                metadata.create_all(engine)
                logging.info("Recreated canal_dredging_progress table with updated schema.")
            else:
                logging.info("canal_dredging_progress table already exists and matches schema.")
        else:
            metadata.create_all(engine)
            logging.info("Created canal_dredging_progress table.")
    
    @task()
    def fetch_and_store_canal_dredging_progress():
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        if not weather_api_key:
            raise ValueError("Missing one or more database environment variables.")
        
        headers={
            "KeyId":weather_api_key
        }

        current_date=pendulum.now(tz="Asis/Bagkok")
        query_params={}


        def transform_canal_dredging_data(data):
            transformed_data=[]
            try:

                logging.info(f"Received {len(data)} items from the API")

                if data:
                    logging.info(f"First item sample: {json.dumps(data[0],ensure_ascii=False)}")
                for item in data:
                   transformed_record={}
                   for thai_key, eng_col in thai_to_column_mapping.items():
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

                        elif eng_col in ["record_number", "total_dredged_canals", "planned_dredging_canals", "additional_budgeted_canals", "annual_budgeted_canals", "regularly_maintained_canals", "periodically_flowed_canals", "total_canals_assigned"]:
                            try:
                                transformed_record[eng_col] = int(raw_value) if raw_value not in [None, "-", ""] else 0
                            except Exception:
                                transformed_record[eng_col] = 0
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
            task_id="fetch_and_store_canal_dredging_progress",
            api_url=BMA_WEATHER_API_URL_NEW,
            transform_func=transform_canal_dredging_data,
            table_name="canal_dredging_progress",
            headers=headers,
            db_type="BMA",
            
        )
        operator.execute(context={})
    # @task
    # def enrich_riskpoints_with_rainfall_sensor():
    #     engine,metadata=setup_engine_and_metadata()
    #     riskpoint_table=metadata.tables.get("risk_points")

    #     with engine.connect() as conn:
    #         # fetchALL risk points
    #         all_riskpoints=conn.execute(select(riskpoint_table)).mappings().all()

    #         # load sensor rows
    #         sensor_rows=get_sensors(engine, metadata,sensor_table="rainfall_sensor")
    #         for riskpoint in all_riskpoints:
    #            try:
    #                 closest_sensor_id, closest_sensor_code, min_distance = get_distance_between_riskpoint_and_sensors(
    #                     riskpoint, "rainfall_sensor", sensor_rows
    #                 )

    #                 # Update riskpoint row with closest rainfall_sensor_id
    #                 update_stmt = (
    #                     update(riskpoint_table)
    #                     .where(riskpoint_table.c.id == riskpoint["id"])
    #                     .values(
    #                         rainfall_sensor_id=closest_sensor_id,
    #                         closest_rainfall_sensor_code=closest_sensor_code,
    #                         closest_rainfall_sensor_distance=min_distance
    #                         )
    #                 )
    #                 conn.execute(update_stmt)
    #                 logging.info(f"Updated risk_point {riskpoint['id']} with sensor {closest_sensor_id}")

    #            except Exception as e:
    #                 logging.warning(f"Failed to update risk_point {riskpoint.get('objectid')} due to: {e}")

    create_table_and_insert() >> fetch_and_store_canal_dredging_progress() 
canal_dredging_pipeline()
