import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from airflow.decorators import dag, task
import logging
import json
import pendulum
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.canals_translation_map_utils import thai_to_column_mapping
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
        api_url=f"{base_url}&start={dt}&end={dt}"
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
    fetch_and_store_traffy_fondue_citizen_progress()
traffy_fondue_citizen_complaint_pipeline()
