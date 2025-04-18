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
from include.api_utils import get_bma_weather_api_auth
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


WEATHER_API_URL = os.getenv("BMA_WEATHER_API_URL")
WEATHER_API_URL_FOR_ROAD_SENSOR_LOCATION = f"{WEATHER_API_URL}/flood/info"
WEATHER_API_URL_FOR_ROAD_FLOOD_SENSOR_24HR_DATA_STREAM = f"{WEATHER_API_URL}/flood/history"


@dag(
    dag_id='road_flood_sensor',
    schedule='0 * * * *',  # every hour
    start_date=pendulum.datetime(2025, 4, 16, tz="UTC"),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor'],
)
def road_flood_sensor_location_pipeline():

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

        flood_sensor_table = Table(
            'flood_sensor', metadata,
            Column('id', Integer, primary_key=True),
            Column('code', String(255)),
            Column('name', String(255)),
            Column('road', String(255)),
            Column('district', String(255)),
            Column('latitude', String(255)),
            Column('longitude', String(255)),
            Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
            Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())
        )

        inspector = inspect(engine)
        if not inspector.has_table("flood_sensor"):
            metadata.create_all(engine)
            logging.info("Created flood_sensor table.")
        else:
            logging.info("flood_sensor table already exists.")

    @task()
    def fetch_and_store_sensor_details():
        operator = ApiToPostgresOperator(
            task_id="fetch_and_store_flood_sensor_details",
            api_url=WEATHER_API_URL_FOR_ROAD_SENSOR_LOCATION,
            table_name="flood_sensor",
            auth_callable=get_bma_weather_api_auth,
            db_type="BMA"
        )
        operator.execute(context={})

    @task()
    def create_flood_sensor_streaming_data_table():
        db_host = os.getenv("POSTGRES_HOST")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        engine = create_engine(
            f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
        )
        metadata = MetaData()

        # create metadata binding with below streaming data table for proper FK assignment
        flood_sensor__table=Table(
            'flood_sensor', metadata,
            Column('id', Integer,primary_key=True)
        )
        flood_sensor_streaming_data_table = Table(
            'flood_sensor_streaming_data', metadata,
            Column("id", Integer, primary_key=True),
            Column("sensor_id", Integer, ForeignKey("flood_sensor.id", ondelete="SET NULL", onupdate="CASCADE"),nullable=True),
            Column("flood",Float,nullable=True),
            Column("site_time", DateTime(timezone=True), nullable=True),
            Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
            Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())

        )

        inspector = inspect(engine)
        if not inspector.has_table("flood_sensor_streaming_data"):
            metadata.create_all(engine)
            logging.info("Created flood_sensor_streaming_data table.")
        else:
            logging.info("flood_sensor_streaming_table already exists.")

    @task
    def fetch_and_store_streaming_data():
        db_host = os.getenv("POSTGRES_HOST")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")

        if not all([db_host, db_user, db_password, db_name]):
            raise ValueError("Missing one or more database env variables")

        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT code, id FROM flood_sensor;")
            sensors = cursor.fetchall()

        for sensor in sensors:
            code = sensor["code"]
            sensor_id=sensor["id"]
            sensor_streaming_url = f"{WEATHER_API_URL_FOR_ROAD_FLOOD_SENSOR_24HR_DATA_STREAM}?id={code}"

            def transform_with_fk(data):
                for row in data:
                    row["sensor_id"]=sensor_id

                    flood_val=row.get("flood",None)
                    if flood_val in ["", None]:
                        row["flood"]=0.0
                    else:
                        row["flood"]=float(flood_val)

                return data
            # replace anything invalid with _
            sanitized_task_id=re.sub(r'[^a-zA-Z0-9_\-\.]', '_', code)
            logging.info(f"Original code {code} , sanitized task id: fetch_streaming_data__{sanitized_task_id}")
            operator = ApiToPostgresOperator(
                task_id=f"fetch_streaming_data_{sanitized_task_id}",
                api_url=sensor_streaming_url,
                table_name="flood_sensor_streaming_data",
                auth_callable=get_bma_weather_api_auth,
                transform_func=transform_with_fk,
                data_key=None,
                db_type="BMA",
                # schema=schema
            )
            operator.execute(context={})
    create_table_and_insert() >> fetch_and_store_sensor_details() >> create_flood_sensor_streaming_data_table() >> fetch_and_store_streaming_data()


road_flood_sensor_location_pipeline()
