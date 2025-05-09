import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from sqlalchemy import update,select
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.distance_calculation_utils import (
     get_sensors, get_distance_between_riskpoint_and_sensors
)
import numpy as np
from utils.db_utils import setup_engine_and_metadata
from utils.bangkok_districts import bangkok_districts
from airflow.decorators import dag, task
import pendulum
import logging
import json
load_dotenv()


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
@dag(
    dag_id="riskpoint_scraper",
    schedule="0 0 15 * *", # 15th midnight everymonth
    start_date=pendulum.datetime(2025, 4,22,tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'bangkok', 'risk_point']
)

def riskpoint_pipeline(): 
    @task()
    def fetch_and_store_riskpoint_metadata():
        riskpoint_api_key=os.getenv("BMA_WEATHER_API_KEY")
        api_url= os.getenv("BMA_RISK_POINT_URL_NEW")
        if not all ([riskpoint_api_key, api_url]):
            raise ValueError("Missing one or more  environment variables.")
        
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
            api_url=api_url,
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
        rp_table=metadata.tables.get("risk_points")
        rf_sensor_table=metadata.tables.get("rainfall_sensor")
        with engine.connect() as conn:
            # fetchALL risk points
    
            for district in bangkok_districts:
                # load sensor rows filtered by a district
                sensor_stmt=select(rf_sensor_table).where(rf_sensor_table.c.district==district)
                district_sensors=conn.execute(sensor_stmt).mappings().all()
                # load risk points rows filtered by a district
                risk_stmt=select(rp_table).where(rp_table.c.district_t==district)
                district_risk_points=conn.execute(risk_stmt).mappings().all()
                logging.info(f"District Risk Points are: {district_risk_points}")
                for riskpoint in district_risk_points:
                    try:    
                            
                            closest_sensor_id, closest_sensor_code, min_distance = get_distance_between_riskpoint_and_sensors(
                                riskpoint, "rainfall_sensor", district_sensors
                            )

                            # Update riskpoint row with closest rainfall_sensor_id
                            update_stmt = (
                                update(rp_table)
                                .where(rp_table.c.id == riskpoint["id"])
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
        engine, metadata = setup_engine_and_metadata()
        riskpoint_table = metadata.tables.get("risk_points")
        csv_path=os.getenv("RISK_MAPPED_FLOOD_SENSOR_CSV_PATH")
        df = pd.read_csv(csv_path)

        # only those with a valid sensor_id
        mapped_df = df[df["sensor_id"].notna()]
        mapped_ids = mapped_df["risk_id"].astype(int).tolist()

        with engine.begin() as conn:
            # set unmapped risk points to NULL
            conn.execute(
                update(riskpoint_table)
                .where(~riskpoint_table.c.objectid.in_(mapped_ids))
                .values(
                    road_flood_sensor_id=None,
                    closest_road_flood_sensor_code=None,
                    closest_road_flood_sensor_distance=None
                )
            )

            # update mapped risk points
            for _, row in mapped_df.iterrows():
                sensor_id = int(row["sensor_id"])
                code = row["code"] if pd.notna(row["code"]) else None
                distance = float(row["distance_to_sensor_m"]) if pd.notna(row["distance_to_sensor_m"]) else None

                try:
                    conn.execute(
                        update(riskpoint_table)
                        .where(riskpoint_table.c.objectid == int(row["risk_id"]))
                        .values(
                            road_flood_sensor_id=sensor_id,
                            closest_road_flood_sensor_code=code,
                            closest_road_flood_sensor_distance=distance
                        )
                    )
                except Exception as e:
                    logging.warning(f"Failed to update risk_point {row['risk_id']} due to: {e}")
    fetch_and_store_riskpoint_metadata() >>enrich_riskpoints_with_rainfall_sensor() >> enrich_riskpoints_with_flood_sensor()


riskpoint_pipeline()
