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


WEATHER_API_URL = os.getenv("BMA_WEATHER_API_URL_NEW")
WEATHER_API_URL_FOR_RAIN_SENSOR_LOCATION = f"{WEATHER_API_URL}/rain/info"
WEATHER_API_URL_FOR_RAIN_SENSOR_LATEST_DATA_UPTO_24HOUR= f"{WEATHER_API_URL}/rain/lastdata"


@dag(
    dag_id='rainfall_flood_sensor',
    schedule='0 * * * *',  # every hour
    start_date=pendulum.datetime(2025, 4, 16, tz="UTC"),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor'],
)
def rainfall_sensor_location_pipeline():

    @task()
    def create_table():
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

        rainfall_sensor_table = Table(
            'rainfall_sensor', metadata,
            Column('id', Integer, primary_key=True),
            Column('code', String(255)),
            Column('name', String(255)),
            Column('district', String(255)),
            Column('latitude', String(255)),
            Column('longitude', String(255)),
            Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
            Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())
        )

        inspector = inspect(engine)
        if not inspector.has_table("rainfall_sensor"):
            metadata.create_all(engine)
            logging.info("Created rainfall_sensor table.")
        else:
            logging.info("rainfall_sensor table already exists.")

    @task()
    def fetch_and_store_rainfall_sensor_details():
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        headers={
            "KeyId":weather_api_key
        }
        operator = ApiToPostgresOperator(
            task_id="fetch_and_store_rainfall_sensor_details",
            api_url=WEATHER_API_URL_FOR_RAIN_SENSOR_LOCATION,
            table_name="rainfall_sensor",
            headers=headers,
            db_type="BMA"
        )
        operator.execute(context={})

    @task()
    def create_rainfall_sensor_streaming_data_table():
        db_host = os.getenv("POSTGRES_HOST")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        engine = create_engine(
            f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
        )
        metadata = MetaData()

        # create metadata binding with below streaming data table for proper FK assignment
        rainfall_sensor__table=Table(
            'rainfall_sensor', metadata,
            Column('id', Integer,primary_key=True)
        )
        rainfall_sensor_streaming_data_table = Table(
            'rainfall_sensor_streaming_data', metadata,
            Column("id", Integer, primary_key=True),
            Column("code", String(255)),
            Column("sensor_id", Integer, ForeignKey("rainfall_sensor.id", ondelete="SET NULL", onupdate="CASCADE"),nullable=True),
            Column("rf24rh",Float,nullable=True),
            Column("site_time", DateTime(timezone=True), nullable=True),
            Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
            Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())

        )

        inspector = inspect(engine)
        if not inspector.has_table("rainfall_sensor_streaming_data"):
            metadata.create_all(engine)
            logging.info("Created rainfall_sensor_streaming_data table.")
        else:
            logging.info("rainfall_sensor_streaming_data already exists.")

    @task
    def fetch_and_store_streaming_data():
        db_host = os.getenv("POSTGRES_HOST")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        sensor_streaming_url = WEATHER_API_URL_FOR_RAIN_SENSOR_LATEST_DATA_UPTO_24HOUR
        headers={
            "KeyId":weather_api_key
        }
        if not all([db_host, db_user, db_password, db_name]):
            raise ValueError("Missing one or more database env variables")

        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT code, id FROM rainfall_sensor;")
            sensors = cursor.fetchall()
        for sensor in sensors:
            code=sensor["code"]
            sensor_id=sensor["id"]
            

            def transform_with_fk(data):
                
                for row in data:
                    if row["code"]==code:

                        row["sensor_id"]=sensor_id
                    else:
                        continue

                return data
        operator = ApiToPostgresOperator(
            task_id="fetch_streaming_data",
            api_url=sensor_streaming_url,
            headers=headers,
            table_name="rainfall_sensor_streaming_data",
            transform_func=transform_with_fk,
            data_key=None,
            db_type="BMA",
        )
        operator.execute(context={})
    create_table() >>fetch_and_store_rainfall_sensor_details() >> create_rainfall_sensor_streaming_data_table() >> fetch_and_store_streaming_data()

rainfall_sensor_location_pipeline()
