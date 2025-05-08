import os
import sys
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import logging
import pendulum
import requests
from dotenv import load_dotenv
from urllib.parse import urlencode
from airflow.decorators import dag, task
from airflow.models import Variable
from include.api_utils import get_bma_api_auth
# from utils.translation_ner_geocoding import extract_entities_from_thai_text
from utils.db_utils import setup_engine_and_metadata
from sqlalchemy.dialects.postgresql import insert
from utils.buddhist_year_converter_utils import get_bangkok_date_info

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
def pad(val,width):
    s=str(val) if val not in (None, "") else ""
    return s.zfill(width)

@dag(
    dag_id='bangkok_budget_scraper',
    schedule='0 0 1 * *',  # runs on first of every month forever   
    start_date=pendulum.datetime(2025, 3, 22, tz="Asia/Bangkok"),
    catchup=False,
    tags=['api', 'postgres', 'bangkok', 'budget'],
)
def bangkok_budget_scraper_pipeline():

    @task()
    def upsert_budget():

        engine,metadata=setup_engine_and_metadata()
        budget_table=metadata.tables["bangkok_budget"]

        base_url=os.getenv("BMA_MIS_API_URL")

        if not base_url:
            raise ValueError("Missing environment variables")
        
        _, _, buddhist_year=get_bangkok_date_info()
        fiscal_suffix=buddhist_year%100
        default_api_params = {
            'source_id': '01',
            'book_id': '0',
            'fiscal_year': fiscal_suffix,
            'department_id': '11000000',
            
        }
        try:
            bangkok_params = Variable.get('bangkok_budget_params', deserialize_json=True, default_var=None)
            if bangkok_params:
                default_api_params.update(bangkok_params)
        except Exception as e:
            logging.info(f"Using default Bangkok budget parameters due to: {e}")

        auth=get_bma_api_auth()

        

        for exp_id in ["05", "07"]:
            params={
                **default_api_params, "exp_object_id":exp_id
            }
            url=f"{base_url}?{urlencode(params)}"
            logging.info("Fetching URL: %s", url)
            # build MISID
            
            response=requests.get(url, auth=auth, timeout=120)
            response.raise_for_status()
            data=response.json() 
            transformed_records = []

            for item in data:
                parts = [
                        pad(fiscal_suffix, 2),
                        pad(item.get("DEPARTMENT_ID"), 8),
                        pad(item.get("FUNC_ID"), 6),
                        pad(item.get("FUNC_YEAR"), 2),
                        pad(item.get("FUNC_SEQ"), 2),
                        pad(item.get("EXP_OBJECT_ID"), 2),
                        pad(item.get("EXP_SUBOBJECT_ID"), 2),
                        pad(item.get("EXPENDITURE_ID"), 2),
                        pad(item.get("ITEM_ID"), 3),
                    ]

                mis_id = "".join(parts)
                logging.info(f"The current mis id is: {mis_id}")
                record = {
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
                    "raw_data": json.dumps(item)
                }

                transformed_records.append(record)

            if not transformed_records:
                logging.info(f"No records to upsert for exp_object_id: {exp_id}")
                continue
            insert_statement=insert(budget_table).values(transformed_records)
            # explicitly exclude the mapping
            excluded=insert_statement.excluded
            update_mapping={}
            for col in budget_table.columns:
                if col.name=="id":
                    continue
                # on conflict, set table.col=Excluded.col
                update_mapping[col.name]=getattr(excluded,col.name)
            upsert_stmt=insert_statement.on_conflict_do_update(
                index_elements=["mis_id"],
                set_=update_mapping
            )
            with engine.begin() as conn:
                conn.execute(upsert_stmt)


            logging.info(f"Transformed {len(transformed_records)} records with NER + geocoding")


    upsert_budget() 

bangkok_budget_scraper_pipeline()
