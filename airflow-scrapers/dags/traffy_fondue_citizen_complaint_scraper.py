import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from airflow.decorators import dag, task
from sqlalchemy import update, select
import logging
import json
import pendulum
from geopy.distance import geodesic
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.db_utils import setup_engine_and_metadata
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(messages)s",
    level=logging.INFO
)

@dag(
    dag_id="traffy_fondue_citizen_complaint_scraper",
    schedule="0 1 * * *", #runs every one hour everyday
    start_date=pendulum.datetime(2025,4,24, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'bangkok', 'traffy_fondue_complaint_scraper']
)

def traffy_fondue_citizen_complaint_pipeline():
    
    @task()
    def fetch_and_store_traffy_fondue_citizen_progress():
        base_url=os.getenv("TRAFFY_FONDUE_CITIZEN_COMPLAINT_URL")
        dt=pendulum.now().to_date_string()
        api_url=f"{base_url}&start=2025-04-01&end=2025-04-30"
        def transform_func(data):
            transformed_data=[]
            try:
                logging.info(f"Received {len(data)} items from the API")
                if data:
                    logging.info(f"First item sample: {json.dumps(data[0], ensure_ascii=False)}")
                
                for item in data:
                    properties=item.get("properties", {})
                    geometry=item.get("geometry", {})
                    transformed_record={
                        "longitude":geometry["coordinates"][0],
                        "latitude":geometry["coordinates"][1],
                        "description":properties.get("description", {}),
                        "address":properties.get("address", {}),
                        "timestamp":properties.get("timestamp", {}),
                        "note":properties.get("note", {}),
                        "type":properties.get("type", {}),
                        "district":properties.get("district", {}),
                        "state_type_latest":properties.get("state_type_latest", {})

                    }
                    transformed_data.append(transformed_record)
                if not transformed_data:
                    raise ValueError("No data was transformed. Check API response and transformation step")
                logging.info(f"Transformed {len(transformed_data)}")
                return transformed_data
            except Exception as e:
                logging.exception(f"error transforming risk point data: {str(e)}")
                return []
        logging.info("Bulk inserting traffy fondue records")
        operator=ApiToPostgresOperator(
            task_id="fetch_and_store_traffy_fondue_citizen_progress",
            api_url=api_url,
            transform_func=transform_func,
            table_name="traffy_fondue_citizen_complaint",
            db_type="BMA",
            data_key="features"
        )
        operator.execute(context={})
    @task()
    def enrich_traffy_fondue_with_risk_object_id():
        engine, metadata = setup_engine_and_metadata()
        tf_table = metadata.tables["traffy_fondue_citizen_complaint"]
        rp_table = metadata.tables["risk_points"]

        with engine.begin() as conn:
            # fetch all complaints and risk points
            complaints = conn.execute(select(tf_table)).mappings().all()
            risk_points = conn.execute(select(rp_table)).mappings().all()

            for comp in complaints:
                comp_coord = (comp["latitude"], comp["longitude"])
                logging.info(f"The comp-coords are: {comp_coord}")
                matched_objectid = None

                # find first risk point within 200m
                for rp in risk_points:
                    try:
                        rp_coord = (float(rp["lat"]), float(rp["long"]))
                        logging.info(f"The rp-coords are : {rp_coord}")
                    except (TypeError, ValueError):
                        # skip if lat/long not parseable
                        continue

                    if geodesic(comp_coord, rp_coord).meters <= 500:
                        matched_objectid = rp["objectid"]

                        break

                # apply update (will set to None if no match)
                stmt = (
                    update(tf_table)
                    .where(tf_table.c.id == comp["id"])
                    .values(riskpoint_objectid=matched_objectid)
                )
                conn.execute(stmt)

            
            


    fetch_and_store_traffy_fondue_citizen_progress() >> enrich_traffy_fondue_with_risk_object_id()
traffy_fondue_citizen_complaint_pipeline()
