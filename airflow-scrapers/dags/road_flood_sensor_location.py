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


WEATHER_API_URL_NEW = os.getenv("BMA_WEATHER_API_URL_NEW")
WEATHER_API_URL_FOR_ROAD_SENSOR_LOCATION_NEW = f"{WEATHER_API_URL_NEW}/flood/items/sensor_profile?meta=*&limit=300"


@dag(
    dag_id='road_flood_sensor',
    schedule='0 5 * * *',  # every 5 mins
    start_date=pendulum.datetime(2025, 4, 16, tz="UTC"),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor'],
)
def road_flood_sensor_pipeline():

    @task()
    def fetch_and_store_sensor_details():
        base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
        api_url=f"{base_url}/flood/items/sensor_profile?meta=*&limit=300"
        operator = ApiToPostgresOperator(
            task_id="fetch_and_store_flood_sensor_details",
            api_url=api_url,
            table_name="flood_sensor",
            data_key="data",
            headers={
                "KeyId":os.getenv("BMA_WEATHER_API_KEY")
            },
            db_type="BMA"
        )
        operator.execute(context={})

    @task
    def fetch_and_store_streaming_data():
        base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
        api_url=f"{base_url}/flood/fetch-and-save-data/sensor-flood-latest-record?limit=300"
        api_key=os.getenv("BMA_WEATHER_API_KEY")
        
        def transform_func(data):

            for row in data:
                device_property=row.get("sensor_profile", {})
                row["device_status"]=device_property.get("device_status", {})
            return data
        
        operator=ApiToPostgresOperator(
            task_id="fetch_streaming_data",
            api_url=api_url,
            headers={
                "KeyId":api_key
            },
            table_name="flood_sensor_streaming_data",
            transform_func=transform_func,
            data_key="data",
            db_type="BMA",
        )
        operator.execute(context={})

        
        





    fetch_and_store_sensor_details() >> fetch_and_store_streaming_data()


road_flood_sensor_pipeline()
