import sys
import os
import re
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from airflow.decorators import dag, task
import pendulum
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)




@dag(
    dag_id='rainfall_sensor',
    schedule='0 * * * *',  # every hour
    start_date=pendulum.datetime(2025, 4, 16, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor'],
)
def rainfall_sensor_location_pipeline():
    

    @task()
    def fetch_and_store_rainfall_sensor_details():
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
        api_url=f"{base_url}/rain/info"
        headers={
            "KeyId":weather_api_key
        }
        operator = ApiToPostgresOperator(
            task_id="fetch_and_store_rainfall_sensor_details",
            api_url=api_url,
            table_name="rainfall_sensor",
            headers=headers,
            db_type="BMA"
        )
        operator.execute(context={})

  

    @task
    def fetch_and_store_streaming_data():
        db_host = os.getenv("BMA_DB_HOST")
        db_user = os.getenv("BMA_DB_USER")
        db_password = os.getenv("BMA_DB_PASSWORD")
        db_name = os.getenv("BMA_DB_NAME")
        db_port=os.getenv("BMA_DB_PORT")

        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
        api_url=f"{base_url}/rain/lastdata"
        if not all([db_host, db_user, db_password, db_name, db_port,weather_api_key,base_url,api_url]):
            raise ValueError("Missing one or more database env variables")

        headers={
            "KeyId":weather_api_key
        }
        
    # fetch sensor → id map
        sensor_map = {}
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            port=db_port,
            password=db_password
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT code, id FROM rainfall_sensor;")
            for r in cursor.fetchall():
                sensor_map[r["code"]] = r["id"]
        conn.close()

        def transform_with_fk(data):
            for row in data:
                row["sensor_id"] = sensor_map.get(row.get("code"))
            return data

        operator = ApiToPostgresOperator(
            task_id="fetch_streaming_data",
            api_url=api_url,
            headers=headers,
            table_name="rainfall_sensor_streaming_data",
            transform_func=transform_with_fk,
            data_key=None,
            db_type="BMA",
        )
        operator.execute(context={})
    fetch_and_store_rainfall_sensor_details() >>  fetch_and_store_streaming_data()

rainfall_sensor_location_pipeline()
