from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, DateTime, func
)
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from include.api_utils import get_bma_weather_api_auth
from airflow.decorators import dag, task
import pendulum
import logging
import os
import json
import sys

load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


WEATHER_API_URL = os.getenv("BMA_WEATHER_API_URL")
WEATHER_API_URL_FOR_ROAD_SENSOR_LOCATION = f"{WEATHER_API_URL}/rain/info"


@dag(
    dag_id='road_flood_sensor_location',
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

    create_table_and_insert() >> fetch_and_store_sensor_details()


road_flood_sensor_location_pipeline()
