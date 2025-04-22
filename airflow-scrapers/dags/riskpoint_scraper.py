import sys
import os
import re
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, Float,DateTime, func, ForeignKey
)

from sqlalchemy.orm import relationship
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from airflow.decorators import dag, task
import pendulum
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


WEATHER_APIBMA_RISK_POINT_URL_NEW_URL_NEW = os.getenv("BMA_RISK_POINT_URL_NEW")
@dag(
    dag_id="riskpoint_location_and_metadata",
    schedule="0 0 15 * *", # 15th midnight everymonth
    start_date=pendulum.datetime(2025, 5,22,tz="UTC"),
    catchup=False,
    tags=['api', 'bangkok', 'risk_point']
)

def riskpoint_pipeline():
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

        riskpoint_table = Table(
            'risk_points', metadata,
            Column('id', Integer, primary_key=True),
            Column('objectid', Integer),
            Column('risk_name', String(255)),
            Column('problems', String(255)),
            Column('district_t', String(255)),
            Column('group_t', String(255)),
            Column('status_num', Integer),
            Column('status_det', String(255)),
            Column('long', String(255)),
            Column('lat', String(255)),
            Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
            Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())
        )

        inspector = inspect(engine)
        if not inspector.has_table("risk_points"):
            metadata.create_all(engine)
            logging.info("Created risk_points table.")
        else:
            logging.info("risk_points table already exists.")
    
    @task()
    def fetch_and_store_riskpoint_metadata():
        riskpoint_api_key=os.getenv("BMA_WEATHER_API_KEY")
        if not riskpoint_api_key:
            raise ValueError("Missing one or more database environment variables.")
        
        headers={
            "KeyId":riskpoint_api_key
        }

        def transform_riskpoint_data(data):
            transformed_data=[]
            try:

                logging.info(f"Received {len(data)} items from the API")

                if data:
                    logging.info(f"First item sample: {json.dumps(data[0],ensure_ascii=False)}")
                for item in data:
                    props=item.get("properties", {})
                    transformed_record={
                        "objectid":props.get("objectid"),
                        "risk_name":props.get("risk_name"),
                        "problems":props.get("problems"),
                        "district_t":props.get("district_t"),
                        "group_t":props.get("group_t"),
                        "status_num":props.get("status_num"),
                        "status_det":props.get("status_det"),
                        "long":props.get("long"),
                        "lat":props.get("lat")

                    }
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
            task_id="fetch_and_store_riskpoint_metadata",
            api_url=WEATHER_APIBMA_RISK_POINT_URL_NEW_URL_NEW,
            transform_func=transform_riskpoint_data,
            table_name="risk_points",
            headers=headers,
            db_type="BMA",
            data_key="features"
        )
        operator.execute(context={})

    create_table_and_insert() >> fetch_and_store_riskpoint_metadata()


riskpoint_pipeline()
