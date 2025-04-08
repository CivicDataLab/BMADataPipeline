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
from .bangkok_budget_scraper import default_args
# Add the plugins and include directories to the path
airflow_home = os.environ.get('AIRFLOW_HOME', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(airflow_home, 'plugins'))
sys.path.append(os.path.join(airflow_home, 'include'))


from plugin.operators.api_to_postgres_operator import ApiToPostgresOperator
from include.api_utils import get_bma_api_auth
from include.superset_utils import create_superset_dataset

load_dotenv()

dag=DAG(
    'Road Flood Sensor Data Reading',
    default_args=default_args,
    description="Fetches Road Flood Sensor Data Reading",
    schedule_interval='0 * * * *', #Every hour
    start_date=datetime(2025,3,27),
    catchup=False,
    tags=['api', 'bangkok', 'flood_sensor']
    
)
