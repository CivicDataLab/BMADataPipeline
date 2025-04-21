import os
import sys
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import logging
import pendulum
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, String, Float, Text, DateTime, JSON
)
from sqlalchemy.dialects.postgresql import JSONB
from airflow.decorators import dag, task
from airflow.models import Variable
from plugins.operators.api_to_postgres_operator import ApiToPostgresOperator
from include.api_utils import get_bma_api_auth
from include.superset_utils import create_superset_dataset
from utils.translation_ner import extract_entities_from_thai_text

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

@dag(
    dag_id='bangkok_budget_scraper',
    schedule='0 0 * * *',  # daily at midnight
    start_date=pendulum.datetime(2025, 3, 22, tz="UTC"),
    catchup=False,
    tags=['api', 'postgres', 'bangkok', 'budget'],
)
def bangkok_budget_scraper_pipeline():

    @task()
    def create_bangkok_budget_table():
        db_host = os.getenv('BMA_DB_HOST', os.getenv('POSTGRES_HOST', 'localhost'))
        db_port = os.getenv('BMA_DB_PORT', os.getenv('POSTGRES_PORT', '5432'))
        db_name = os.getenv('BMA_DB_NAME', os.getenv('POSTGRES_DB', 'airflow'))
        db_user = os.getenv('BMA_DB_USER', os.getenv('POSTGRES_USER', 'airflow'))
        db_password = os.getenv('BMA_DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', 'airflow'))

        engine = create_engine(
            f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        )
        metadata = MetaData()

        budget_table = Table(
            'bangkok_budget', metadata,
            Column('id', Integer, primary_key=True),
            Column('department_name', String(255)),
            Column('sector_name', String(255)),
            Column('program_name', String(255)),
            Column('func_name', String(255)),
            Column('expenditure_name', String(255)),
            Column('subobject_name', String(255)),
            Column('func_id', String(50)),
            Column('func_year', String(50)),
            Column('func_seq', String(50)),
            Column('exp_object_id', String(50)),
            Column('exp_subobject_id', String(50)),
            Column('expenditure_id', String(50)),
            Column('item_id', String(50)),
            Column('detail', Text),
            Column('approve_on_hand', String(255)),
            Column('allot_approve', String(255)),
            Column('allot_date', String(50)),
            Column('agr_date', String(50)),
            Column('open_date', String(50)),
            Column('acc_date', String(50)),
            Column('acc_amt', String(255)),
            Column('sgn_date', String(50)),
            Column('st_sgn_date', String(50)),
            Column('end_sgn_date', String(50)),
            Column('last_rcv_date', String(50)),
            Column('vendor_type_id', String(50)),
            Column('vendor_no', String(50)),
            Column('vendor_description', String(255)),
            Column('pay_total_amt', String(255)),
            Column('fin_dept_amt', String(255)),
            Column('net_amt', Float),
            Column('pur_hire_status', String(50)),
            Column('pur_hire_status_name', String(255)),
            Column('contract_id', String(50)),
            Column('purchasing_department', String(255)),
            Column('contract_name', Text),
            Column('contract_amount', String(255)),
            Column('pur_hire_method', String(255)),
            Column('egp_project_code', String(50)),
            Column('egp_po_control_code', String(50)),
            Column('original_text', Text),
            Column('translation', Text),
            Column('entities', JSONB),
            Column('geocoding_of', String(255)),
            Column('lat', Float),
            Column('lon', Float),
            Column('created_at', DateTime, default=pendulum.now),
            Column('updated_at', DateTime, default=pendulum.now, onupdate=pendulum.now),
            Column('raw_data', JSONB)
        )

        inspector = inspect(engine)
        if not inspector.has_table('bangkok_budget'):
            metadata.create_all(engine)
            logging.info("Created bangkok_budget table")
        else:
            logging.info("bangkok_budget table already exists")

  
    @task()
    def fetch_and_store_budget_data():
        budget_mis_api_url=os.getenv("BMA_MIS_API_URL")
        default_api_params = {
            'source_id': '01',
            'book_id': '0',
            'fiscal_year': '68',
            'department_id': '11000000',
            'exp_object_id': '05'
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

                transformed_record = {
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

        operator = ApiToPostgresOperator(
            task_id='fetch_bangkok_budget_data',
            api_url=f"{budget_mis_api_url}?source_id=01&book_id=0&fiscal_year=67&department_id=11000000&exp_object_id=05", #hardcoded for testing
            table_name='bangkok_budget',
            transform_func=transform_budget_translation_data,
            auth_callable=get_bma_api_auth,
            # params=default_api_params,
            db_type='BMA'
        )
        operator.execute(context={})

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

    create_bangkok_budget_table() >> fetch_and_store_budget_data() >> create_superset_dataset_task()

bangkok_budget_scraper_pipeline()
