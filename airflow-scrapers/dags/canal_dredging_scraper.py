import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

from airflow.decorators import dag, task
import logging
import pendulum
from utils.buddhist_year_converter_utils import get_bangkok_date_info
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from utils.canals_translation_map_utils import thai_to_column_mapping
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(messages)s",
    level=logging.INFO
)

@dag(
    dag_id="canal_dredging_scraper",
    schedule="0 0 1,16 * *", #runs every 1st and 16th day forever
    start_date=pendulum.datetime(2025,4,23, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'bangkok', 'canal_dredging_progress_report']
)

def canal_dredging_pipeline():

    @task()
    def fetch_and_store_canal_dredging_progress():
        weather_api_key=os.getenv("BMA_WEATHER_API_KEY")
        base_url=os.getenv("BMA_WEATHER_API_URL_NEW")
        if not all ([base_url, weather_api_key]):
            raise ValueError("Missing one or more database environment variables.")
        
        headers={
            "KeyId":weather_api_key
        }
        current_date, current_month, buddhist_year=get_bangkok_date_info()
        # fetch for previous month's progress report instead
        # think about dedup logic incase manual trigger
        previous_month=12 if current_month==1 else current_month-1
        period='01-15' if 1<=current_month<=15 else '16-30'
        canal_dredging_api_url=f"{base_url}/ProgressReport/DDS?ReportType=02&Period={period}&Month={previous_month}&Year={buddhist_year}"

        def transform_canal_dredging_data(data):
            transformed_data=[]
            try:

                logging.info(f"Received {len(data)} items from the API")

                if data:
                    logging.info(f"First item sample: {json.dumps(data[0],ensure_ascii=False)}")
                for item in data:
                   transformed_record={}
                   for thai_key, eng_col in thai_to_column_mapping.items():
                        raw_value=item.get(thai_key, "")
                        # typecasting based on column pattern
                        # for meter based fields
                        if eng_col.endswith("_m"):
                            try:
                                transformed_record[eng_col]=int(raw_value.replace(",","")) if raw_value not in [None, "-", ""] else 0
                            except Exception:
                                transformed_record[eng_col]=0
                        elif eng_col.startswith("percent_") or eng_col.startswith("total_percent"):
                            try:
                                transformed_record[eng_col]=float(raw_value) if raw_value not in [None, "-", ""] else 0.0
                            except Exception:
                                transformed_record[eng_col]=0
                        elif eng_col in ["buddhist_year"]:
                            try:
                                transformed_record[eng_col] = int(raw_value) if raw_value else None
                            except Exception:
                                transformed_record[eng_col] = None
                        elif eng_col in ["day_period"]:
                            try:
                                transformed_record[eng_col]=str(raw_value) if raw_value else str(period)
                            except Exception:
                                transformed_record[eng_col]=str(period)
                        elif eng_col in ["record_date"]:
                            try:
                                transformed_record[eng_col]=str(raw_value) if raw_value else current_date
                            except Exception:
                                transformed_record[eng_col]=current_date

                        elif eng_col in ["record_number", "total_dredged_canals", "planned_dredging_canals", "additional_budgeted_canals", "annual_budgeted_canals", "regularly_maintained_canals", "periodically_flowed_canals", "total_canals_assigned"]:
                            try:
                                transformed_record[eng_col] = int(raw_value) if raw_value not in [None, "-", ""] else 0
                            except Exception:
                                transformed_record[eng_col] = 0
                        
                        else:
                            transformed_record[eng_col]=raw_value if raw_value not in ["-",""] else None
                        
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
            task_id="fetch_and_store_canal_dredging_progress",
            api_url=canal_dredging_api_url,
            transform_func=transform_canal_dredging_data,
            table_name="canal_dredging_progress",
            headers=headers,
            db_type="BMA",
            
        )
        operator.execute(context={})
    fetch_and_store_canal_dredging_progress() 
canal_dredging_pipeline()
