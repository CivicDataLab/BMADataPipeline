import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, Float,DateTime, func, ForeignKey,text,update,select
)


from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.distance_calculation_utils import (
    setup_engine_and_metadata, get_sensors, get_distance_between_riskpoint_and_sensors
)
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
    start_date=pendulum.datetime(2025, 4,22,tz="UTC"),
    catchup=False,
    tags=['api', 'bangkok', 'risk_point']
)

def riskpoint_pipeline(): 
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
    @task
    def enrich_riskpoints_with_rainfall_sensor():
        engine,metadata=setup_engine_and_metadata()
        riskpoint_table=metadata.tables.get("risk_points")

        with engine.connect() as conn:
            # fetchALL risk points
            all_riskpoints=conn.execute(select(riskpoint_table)).mappings().all()

            # load sensor rows
            sensor_rows=get_sensors(engine, metadata,sensor_table="rainfall_sensor")
            for riskpoint in all_riskpoints:
               try:
                    closest_sensor_id, closest_sensor_code, min_distance = get_distance_between_riskpoint_and_sensors(
                        riskpoint, "rainfall_sensor", sensor_rows
                    )

                    # Update riskpoint row with closest rainfall_sensor_id
                    update_stmt = (
                        update(riskpoint_table)
                        .where(riskpoint_table.c.id == riskpoint["id"])
                        .values(
                            rainfall_sensor_id=closest_sensor_id,
                            closest_rainfall_sensor_code=closest_sensor_code,
                            closest_rainfall_sensor_distance=min_distance
                            )
                    )
                    conn.execute(update_stmt)
                    logging.info(f"Updated risk_point {riskpoint['id']} with sensor {closest_sensor_id}")

               except Exception as e:
                    logging.warning(f"Failed to update risk_point {riskpoint.get('id')} due to: {e}")
    @task
    def enrich_riskpoints_with_flood_sensor():
        engine,metadata=setup_engine_and_metadata()
        riskpoint_table=metadata.tables.get("risk_points")

        with engine.connect() as conn:
            # fetchALL risk points
            all_riskpoints=conn.execute(select(riskpoint_table)).mappings().all()

            # load sensor rows
            sensor_rows=get_sensors(engine, metadata,sensor_table="flood_sensor")
            for riskpoint in all_riskpoints:
               logging.info(f"The risk point is {riskpoint}")
               try:
                    closest_sensor_id, closest_sensor_code, min_distance = get_distance_between_riskpoint_and_sensors(
                        riskpoint, "flood_sensor", sensor_rows
                    )
                    logging.info(f"closes sensor_id :{closest_sensor_id}, {closest_sensor_code} and {min_distance}")

                    # Update riskpoint row with closest rainfall_sensor_id
                    update_stmt = (
                        update(riskpoint_table)
                        .where(riskpoint_table.c.id == riskpoint["id"])
                        .values(
                            road_flood_sensor_id=closest_sensor_id,
                            closest_road_flood_sensor_code=closest_sensor_code,
                            closest_road_flood_sensor_distance=min_distance
                            )
                    )
                    conn.execute(update_stmt)
                    logging.info(f"Updated risk_point {riskpoint['id']} with sensor {closest_sensor_id}")

               except Exception as e:
                    logging.warning(f"Failed to update risk_point {riskpoint.get('id')} due to: {e}")


    fetch_and_store_riskpoint_metadata() >>enrich_riskpoints_with_rainfall_sensor() >> enrich_riskpoints_with_flood_sensor()


riskpoint_pipeline()
