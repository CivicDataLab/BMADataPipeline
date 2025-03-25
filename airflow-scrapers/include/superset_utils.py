import requests
import json
import logging
import os
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


class SupersetAPI:
    """
    Class for interacting with the Superset API
    """
    def __init__(self, base_url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize the SupersetAPI class
        
        Args:
            base_url: Base URL for the Superset API (e.g., http://localhost:8088)
            username: Superset username
            password: Superset password
        """
        self.base_url = base_url or os.getenv('SUPERSET_URL', 'http://localhost:8088')
        self.username = username or os.getenv('SUPERSET_USERNAME', 'admin')
        self.password = password or os.getenv('SUPERSET_PASSWORD', 'admin')
        self.access_token = None
        self.csrf_token = None
    
    def login(self) -> bool:
        """
        Login to Superset and get access token
        
        Returns:
            True if login successful, False otherwise
        """
        try:
            login_url = f"{self.base_url}/api/v1/security/login"
            payload = {
                "username": self.username,
                "password": self.password,
                "provider": "db"
            }
            response = requests.post(login_url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                # Get CSRF token
                csrf_response = requests.get(
                    f"{self.base_url}/api/v1/security/csrf_token/", 
                    headers={"Authorization": f"Bearer {self.access_token}"}
                )
                if csrf_response.status_code == 200:
                    self.csrf_token = csrf_response.json().get('result')
                    return True
            
            logger.error(f"Failed to login to Superset: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Error logging into Superset: {str(e)}")
            return False
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests
        
        Returns:
            Headers dictionary
        """
        if not self.access_token:
            self.login()
            
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-CSRFToken": self.csrf_token
        }
    
    def get_database_id(self, database_name: str) -> Optional[int]:
        """
        Get database ID by name
        
        Args:
            database_name: Name of the database
            
        Returns:
            Database ID if found, None otherwise
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/database/",
                headers=self.get_headers()
            )
            
            if response.status_code == 200:
                databases = response.json().get('result', [])
                for db in databases:
                    if db.get('database_name') == database_name:
                        return db.get('id')
            
            logger.error(f"Failed to get database ID: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Error getting database ID: {str(e)}")
            return None
    
    def get_dataset_id(self, dataset_name: str) -> Optional[int]:
        """
        Get dataset ID by name
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dataset ID if found, None otherwise
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/dataset/",
                headers=self.get_headers()
            )
            
            if response.status_code == 200:
                datasets = response.json().get('result', [])
                for dataset in datasets:
                    if dataset.get('table_name') == dataset_name:
                        return dataset.get('id')
            
            return None  # Dataset not found is not an error
        except Exception as e:
            logger.error(f"Error getting dataset ID: {str(e)}")
            return None
    
    def create_dataset(self, table_name: str, database_name: str, schema: Optional[str] = None) -> Optional[int]:
        """
        Create a new dataset in Superset
        
        Args:
            table_name: Name of the table
            database_name: Name of the database
            schema: Schema name (optional)
            
        Returns:
            Dataset ID if created successfully, None otherwise
        """
        try:
            # Check if dataset already exists
            dataset_id = self.get_dataset_id(table_name)
            if dataset_id:
                logger.info(f"Dataset {table_name} already exists with ID {dataset_id}")
                return dataset_id
            
            # Get database ID
            database_id = self.get_database_id(database_name)
            if not database_id:
                logger.error(f"Database {database_name} not found")
                return None
            
            # Create dataset
            payload = {
                "database": database_id,
                "schema": schema or "public",
                "table_name": table_name
            }
            
            response = requests.post(
                f"{self.base_url}/api/v1/dataset/",
                headers=self.get_headers(),
                json=payload
            )
            
            if response.status_code in [201, 200]:
                result = response.json()
                dataset_id = result.get('id')
                logger.info(f"Created dataset {table_name} with ID {dataset_id}")
                return dataset_id
            
            logger.error(f"Failed to create dataset: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            return None


def get_db_config(db_type: Optional[str] = None) -> Dict[str, str]:
    """
    Get database configuration based on db_type
    
    Args:
        db_type: Type of database (e.g., 'BMA', 'default')
        
    Returns:
        Dictionary with database configuration
    """
    if db_type == 'BMA' or db_type == 'bma':
        return {
            'host': os.getenv('BMA_DB_HOST', os.getenv('POSTGRES_HOST', 'localhost')),
            'port': os.getenv('BMA_DB_PORT', os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('BMA_DB_NAME', os.getenv('POSTGRES_DB', 'airflow')),
            'user': os.getenv('BMA_DB_USER', os.getenv('POSTGRES_USER', 'airflow')),
            'password': os.getenv('BMA_DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', 'airflow')),
        }
    else:  # default
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'airflow'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
        }

def create_superset_dataset(table_name: str, db_type: Optional[str] = None, schema: Optional[str] = None) -> bool:
    """
    Create a dataset in Superset for the given table
    
    Args:
        table_name: Name of the table
        db_type: Type of database connection
        schema: Schema name
        
    Returns:
        True if dataset created successfully, False otherwise
    """
    try:
        # Get database configuration based on db_type
        db_config = get_db_config(db_type)
        database_name = db_config['database']
        schema = schema or 'public'
        
        logger.info(f"Attempting to create Superset dataset for table {table_name} in database {database_name} using db_type={db_type}")
        
        # Initialize Superset API
        api = SupersetAPI(
            base_url=os.getenv('SUPERSET_URL', 'http://localhost:8088'),
            username=os.getenv('SUPERSET_USERNAME', 'admin'),
            password=os.getenv('SUPERSET_PASSWORD', 'admin')
        )
        
        # Login to Superset
        if not api.login():
            logger.error("Failed to login to Superset. Check your credentials and Superset URL.")
            return False
        
        # Create dataset
        dataset_id = api.create_dataset(table_name, database_name, schema)
        
        if dataset_id is not None:
            logger.info(f"Successfully created or found Superset dataset with ID {dataset_id}")
            return True
        else:
            logger.error(f"Failed to create Superset dataset for table {table_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error creating Superset dataset: {str(e)}")
        return False
