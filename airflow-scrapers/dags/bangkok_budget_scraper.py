import os
import sys
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import logging
import pendulum

from dotenv import load_dotenv
from urllib.parse import urlencode
from airflow.decorators import dag, task
from airflow.models import Variable
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from include.api_utils import get_bma_api_auth
from include.superset_utils import create_superset_dataset
from utils.translation_ner_geocoding import extract_entities_from_thai_text
from dags.bangkok_budget_updater import pad


load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

@dag(
    dag_id='bangkok_budget_scraper',
    schedule='0 0 1 * *',  # runs on first of every month forever
    start_date=pendulum.datetime(2025, 3, 22, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'postgres', 'bangkok', 'budget'],
)
def bangkok_budget_scraper_pipeline():

    @task()
    def fetch_and_store_budget_data():
        budget_mis_api_url=os.getenv("BMA_MIS_API_URL")
     
        current_date=pendulum.now(tz="local")
        buddhist_year=current_date.year+543
        fiscal_year_last_two_chars=buddhist_year%100
        default_api_params = {
            'source_id': '01',
            'book_id': '0',
            'fiscal_year': fiscal_year_last_two_chars,
            'department_id': '11000000', #For drainage buruue level
            
        }
        try:
            bangkok_params = Variable.get('bangkok_budget_params', deserialize_json=True, default_var=None)
            if bangkok_params:
                default_api_params.update(bangkok_params)
        except Exception as e:
            logging.info(f"Using default Bangkok budget parameters due to: {e}")

        def transform_budget_translation_data(data):

            transformed_data = []
            
            logging.info(f"Received {len(data)} items from the API")
            if data:
                logging.info(f"First item sample: {json.dumps(data[0], ensure_ascii=False)}")
            for item in data:
                original_text = item.get("DETAIL", item.get("detail", ""))
                if not original_text:
                    continue
                # NER+Translation
                result = {}
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        time.sleep(1)  # avoid timeout from Claude
                        result = extract_entities_from_thai_text(original_text)
                        break
                    except Exception as e:
                        logging.warning(f"Attempt {attempt+1} failed: {e}")
                        if attempt == max_retries - 1:
                            logging.warning(f"NER translation failed permanently for item: {original_text}")
                            continue
                # build MISID
                
                fy    = fiscal_year_last_two_chars
                dept  = pad(item.get("DEPARTMENT_ID"),8)
                func  = pad(item.get("FUNC_ID"),6)
                year  = pad(item.get("FUNC_YEAR"),2)
                seq   = pad(item.get("FUNC_SEQ"),2)
                obj   = pad(item.get("EXP_OBJECT_ID"),2)
                sub   = pad(item.get("EXP_SUBOBJECT_ID"),2)
                exp   = pad(item.get("EXPENDITURE_ID"),2)
                itm   = pad(item.get("ITEM_ID"),3)

                mis_id = fy+dept+func+year+seq+obj+sub+exp+itm
                logging.info(f"The current mis id is: {mis_id}")
                transformed_record = {
                    "mis_id":mis_id,
                    "department_name": item.get("DEPARTMENT_NAME", ""),
                    "sector_name": item.get("SECTOR_NAME", ""),
                    "program_name": item.get("PROGRAM_NAME", ""),
                    "func_name": item.get("FUNC_NAME", ""),
                    "expenditure_name": item.get("EXPENDITURE_NAME", ""),
                    "subobject_name": item.get("SUBOBJECT_NAME", ""),
                    "func_id": item.get("FUNC_ID", ""),
                    "func_year": item.get("FUNC_YEAR", ""),
                    "func_seq": item.get("FUNC_SEQ", ""),
                    "exp_object_id": item.get("EXP_OBJECT_ID", ""),
                    "exp_subobject_id": item.get("EXP_SUBOBJECT_ID", ""),
                    "expenditure_id": item.get("EXPENDITURE_ID", ""),
                    "item_id": item.get("ITEM_ID", ""),
                    "detail": item.get("DETAIL", ""),
                    "approve_on_hand": item.get("APPROVE_ON_HAND", ""),
                    "allot_approve": item.get("ALLOT_APPROVE", ""),
                    "allot_date": item.get("ALLOT_DATE", ""),
                    "agr_date": item.get("AGR_DATE", ""),
                    "open_date": item.get("OPEN_DATE", ""),
                    "acc_date": item.get("ACC_DATE", ""),
                    "acc_amt": item.get("ACC_AMT", ""),
                    "sgn_date": item.get("SGN_DATE", ""),
                    "st_sgn_date": item.get("ST_SGN_DATE", ""),
                    "end_sgn_date": item.get("END_SGN_DATE", ""),
                    "last_rcv_date": item.get("LAST_RCV_DATE", ""),
                    "vendor_type_id": item.get("VENDOR_TYPE_ID", ""),
                    "vendor_no": item.get("VENDOR_NO", ""),
                    "vendor_description": item.get("VENDOR_DESCRIPTION", ""),
                    "pay_total_amt": item.get("PAY_TOTAL_AMT", ""),
                    "fin_dept_amt": item.get("FIN_DEPT_AMT", ""),
                    "net_amt": float(item.get("NET_AMT", item.get("net_amt", 0)) or 0),
                    "pur_hire_status": item.get("PUR_HIRE_STATUS", ""),
                    "pur_hire_status_name": item.get("PUR_HIRE_STATUS_NAME", ""),
                    "contract_id": item.get("CONTRACT_ID", ""),
                    "purchasing_department": item.get("PURCHASING_DEPARTMENT", ""),
                    "contract_name": item.get("CONTRACT_NAME", ""),
                    "contract_amount": item.get("CONTRACT_AMOUNT", ""),
                    "pur_hire_method": item.get("PUR_HIRE_METHOD", ""),
                    "egp_project_code": item.get("EGP_PROJECT_CODE", ""),
                    "egp_po_control_code": item.get("EGP_PO_CONTROL_CODE", ""),
                    "original_text": original_text,
                    "translation": result.get("translation"),
                    "entities": result.get("entities"),
                    "geocoding_of": result.get("geocoding_of"),
                    "lat": result.get("lat"),
                    "lon": result.get("lon"),
                    "raw_data": json.dumps(item)
                }

                transformed_data.append(transformed_record)

            if not transformed_data:
                raise ValueError("No data was transformed. Check API response and transformation step.")

            logging.info(f"Transformed {len(transformed_data)} records with NER + geocoding")
            return transformed_data
        for exp_id in ['05','07']:
            params={**default_api_params, 'exp_object_id':exp_id}
            qs=urlencode(params)
            api_url=f"{budget_mis_api_url}?{qs}"
            logging.info(f"Fetching budget data for ex_object_id={exp_id}=>{api_url}")


            operator = ApiToPostgresOperator(
                task_id        = f'fetch_bangkok_budget_data_{exp_id}',
                api_url        = api_url,
                table_name     = 'bangkok_budget',
                transform_func = transform_budget_translation_data,
                auth_callable  = get_bma_api_auth,
                db_type        = 'BMA'
            )
            operator.execute(context={})

    @task
    def update_and_store_budget_data():
        """Fetches all data budget data from the budget table and updates columns"""
        

    @task()
    def create_superset_dataset_task():
        table_name = 'bangkok_budget'
        db_type = 'BMA'
        schema = 'public'
        logging.info(f"Creating Superset dataset for {table_name} using db_type={db_type}")
        success = create_superset_dataset(table_name, db_type, schema)

        if success:
            logging.info(f"Successfully created Superset dataset for {table_name}")
        else:
            logging.warning(f"Failed to create Superset dataset for {table_name}")

    fetch_and_store_budget_data() >> create_superset_dataset_task()

bangkok_budget_scraper_pipeline()
