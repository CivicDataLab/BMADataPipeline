from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import json
import logging
from dotenv import load_dotenv
from sqlalchemy import String, Integer, Float, Text, DateTime, JSON
from sqlalchemy.dialects.postgresql import JSONB

# Add the plugins and include directories to the path
airflow_home = os.environ.get('AIRFLOW_HOME', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(airflow_home, 'plugins'))
sys.path.append(os.path.join(airflow_home, 'include'))

# Import custom operators and utilities
from operators.api_to_postgres_operator import ApiToPostgresOperator
from api_utils import get_bma_api_auth
from superset_utils import create_superset_dataset

# Load environment variables
load_dotenv()

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'bangkok_budget_scraper',
    default_args=default_args,
    description='Scrape Bangkok budget data from connect.bangkok.go.th API',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2025, 3, 22),
    catchup=False,
    tags=['api', 'postgres', 'bangkok', 'budget'],
)

# Function to transform the budget data before saving to database
def transform_bangkok_budget_data(data):
    """
    Transform the Bangkok budget data before saving to database
    
    Args:
        data: Raw budget data from the API
        
    Returns:
        Transformed data ready for database insertion
    """
    transformed_data = []
    
    # Add debug logging
    logging.info(f"Received {len(data)} items from the API")
    if len(data) > 0:
        logging.info(f"First item keys: {list(data[0].keys())}")
    
    for item in data:
        # Convert numeric strings to float where appropriate
        try:
            # Handle uppercase field names in the API response
            net_amt = float(item.get('NET_AMT', item.get('net_amt', 0)))
        except (ValueError, TypeError):
            net_amt = 0.0
            
        # Create a transformed record
        transformed_record = {
            'department_name': item.get('DEPARTMENT_NAME', item.get('department_name', '')),
            'sector_name': item.get('SECTOR_NAME', item.get('sector_name', '')),
            'program_name': item.get('PROGRAM_NAME', item.get('program_name', '')),
            'func_name': item.get('FUNC_NAME', item.get('func_name', '')),
            'expenditure_name': item.get('EXPENDITURE_NAME', item.get('expenditure_name', '')),
            'subobject_name': item.get('SUBOBJECT_NAME', item.get('subobject_name', '')),
            'func_id': item.get('FUNC_ID', item.get('func_id', '')),
            'func_year': item.get('FUNC_YEAR', item.get('func_year', '')),
            'func_seq': item.get('FUNC_SEQ', item.get('func_seq', '')),
            'exp_object_id': item.get('EXP_OBJECT_ID', item.get('exp_object_id', '')),
            'exp_subobject_id': item.get('EXP_SUBOBJECT_ID', item.get('exp_subobject_id', '')),
            'expenditure_id': item.get('EXPENDITURE_ID', item.get('expenditure_id', '')),
            'item_id': item.get('ITEM_ID', item.get('item_id', '')),
            'detail': item.get('DETAIL', item.get('detail', '')),
            'approve_on_hand': item.get('APPROVE_ON_HAND', item.get('approve_on_hand', '')),
            'allot_approve': item.get('ALLOT_APPROVE', item.get('allot_approve', '')),
            'allot_date': item.get('ALLOT_DATE', item.get('allot_date', '')),
            'agr_date': item.get('AGR_DATE', item.get('agr_date', '')),
            'open_date': item.get('OPEN_DATE', item.get('open_date', '')),
            'acc_date': item.get('ACC_DATE', item.get('acc_date', '')),
            'acc_amt': item.get('ACC_AMT', item.get('acc_amt', '')),
            'sgn_date': item.get('SGN_DATE', item.get('sgn_date', '')),
            'st_sgn_date': item.get('ST_SGN_DATE', item.get('st_sgn_date', '')),
            'end_sgn_date': item.get('END_SGN_DATE', item.get('end_sgn_date', '')),
            'last_rcv_date': item.get('LAST_RCV_DATE', item.get('last_rcv_date', '')),
            'vendor_type_id': item.get('VENDOR_TYPE_ID', item.get('vendor_type_id', '')),
            'vendor_no': item.get('VENDOR_NO', item.get('vendor_no', '')),
            'vendor_description': item.get('VENDOR_DESCRIPTION', item.get('vendor_description', '')),
            'pay_total_amt': item.get('PAY_TOTAL_AMT', item.get('pay_total_amt', '')),
            'fin_dept_amt': item.get('FIN_DEPT_AMT', item.get('fin_dept_amt', '')),
            'net_amt': net_amt,
            'pur_hire_status': item.get('PUR_HIRE_STATUS', item.get('pur_hire_status', '')),
            'pur_hire_status_name': item.get('PUR_HIRE_STATUS_NAME', item.get('pur_hire_status_name', '')),
            'contract_id': item.get('CONTRACT_ID', item.get('contract_id', '')),
            'purchasing_department': item.get('PURCHASING_DEPARTMENT', item.get('purchasing_department', '')),
            'contract_name': item.get('CONTRACT_NAME', item.get('contract_name', '')),
            'contract_amount': item.get('CONTRACT_AMOUNT', item.get('contract_amount', '')),
            'pur_hire_method': item.get('PUR_HIRE_METHOD', item.get('pur_hire_method', '')),
            'egp_project_code': item.get('EGP_PROJECT_CODE', item.get('egp_project_code', '')),
            'egp_po_control_code': item.get('EGP_PO_CONTROL_CODE', item.get('egp_po_control_code', '')),
            'raw_data': json.dumps(item)  # Convert the dictionary to a JSON string
        }
        
        transformed_data.append(transformed_record)
    
    if not transformed_data:
        raise ValueError("No data was transformed. Check the API response structure.")
        
    logging.info(f"Transformed {len(transformed_data)} records")
    return transformed_data

# Function to create the bangkok_budget table if it doesn't exist
def create_bangkok_budget_table(**kwargs):
    """
    Create the bangkok_budget table in PostgreSQL if it doesn't exist
    """
    db_host = os.getenv('BMA_DB_HOST', os.getenv('POSTGRES_HOST', 'localhost'))
    db_port = os.getenv('BMA_DB_PORT', os.getenv('POSTGRES_PORT', '5432'))
    db_name = os.getenv('BMA_DB_NAME', os.getenv('POSTGRES_DB', 'airflow'))
    db_user = os.getenv('BMA_DB_USER', os.getenv('POSTGRES_USER', 'airflow'))
    db_password = os.getenv('BMA_DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', 'airflow'))
    
    # Create SQLAlchemy engine
    from sqlalchemy import create_engine, MetaData, Table, Column, inspect
    
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    metadata = MetaData()
    
    # Define the budget table
    budget_table = Table(
        'bangkok_budget',
        metadata,
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
        Column('created_at', DateTime, default=datetime.utcnow),
        Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
        Column('raw_data', JSONB)
    )
    
    # Create the table if it doesn't exist
    inspector = inspect(engine)
    if not inspector.has_table('bangkok_budget'):
        metadata.create_all(engine)
        logging.info("Created bangkok_budget table")
        
        # Create indexes for better query performance
        with engine.connect() as conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bangkok_budget_department_name ON bangkok_budget(department_name);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bangkok_budget_sector_name ON bangkok_budget(sector_name);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bangkok_budget_func_id ON bangkok_budget(func_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bangkok_budget_contract_id ON bangkok_budget(contract_id);")
    else:
        logging.info("bangkok_budget table already exists")



# Function to get authentication for the Bangkok API
def get_auth_for_bangkok_api(**kwargs):
    """
    Get basic authentication credentials for the Bangkok API
    
    Returns:
        Tuple of (username, password) for basic authentication
    """
    return get_bma_api_auth()

# Define the task to create the bangkok_budget table
create_table = PythonOperator(
    task_id='create_bangkok_budget_table',
    python_callable=create_bangkok_budget_table,
    dag=dag,
)

# Default API parameters - can be overridden by Airflow variables or at runtime
from airflow.models import Variable

default_api_params = {
    'source_id': '01',
    'book_id': '0',
    'fiscal_year': '68',
    'department_id': '11000000',
    'exp_object_id': '07'
}

# Try to get parameters from Airflow variables, fall back to defaults if not found
try:
    # Get parameters from Airflow variables if they exist
    bangkok_params = Variable.get('bangkok_budget_params', deserialize_json=True, default_var=None)
    if bangkok_params:
        # Update default parameters with values from Airflow variables
        default_api_params.update(bangkok_params)
except:
    # If there's an error (e.g., Variable doesn't exist), use defaults
    logging.info('Using default Bangkok budget parameters')

# Define the API task for Bangkok budget data
fetch_bangkok_budget = ApiToPostgresOperator(
    task_id='fetch_bangkok_budget',
    api_url='https://connect.bangkok.go.th/misbudget/bmabudget',
    table_name='bangkok_budget',
    params=default_api_params,
    transform_func=transform_bangkok_budget_data,
    auth_callable=get_auth_for_bangkok_api,
    db_type='BMA',
    dag=dag,
)

# Task to create Superset dataset after data is loaded
def create_superset_dataset_task(**kwargs):
    """
    Create a Superset dataset for the bangkok_budget table
    """
    table_name = 'bangkok_budget'
    db_type = 'BMA'
    schema = 'public'
    
    logging.info(f"Creating Superset dataset for {table_name} using db_type={db_type}")
    success = create_superset_dataset(table_name, db_type, schema)
    
    if success:
        logging.info(f"Successfully created Superset dataset for {table_name}")
    else:
        logging.warning(f"Failed to create Superset dataset for {table_name}")
        # Don't fail the DAG if Superset dataset creation fails
        # This ensures the data pipeline continues to work even if Superset is down

# Define the task to create the Superset dataset
create_superset_dataset_op = PythonOperator(
    task_id='create_superset_dataset',
    python_callable=create_superset_dataset_task,
    dag=dag,
)

# Set task dependencies
create_table >> fetch_bangkok_budget >> create_superset_dataset_op
