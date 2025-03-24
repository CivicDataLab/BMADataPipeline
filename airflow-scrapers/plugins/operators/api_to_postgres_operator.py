from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, Optional, Callable, Tuple, Union
import logging
import sys
import os
import base64

# Add the include directory to the path so we can import api_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from include.api_utils import api_to_db_pipeline, create_table_if_not_exists

class ApiToPostgresOperator(BaseOperator):
    """
    Custom Airflow operator for fetching data from APIs and storing it in PostgreSQL.
    
    This operator handles the common logic for making API requests and storing
    the response data in a PostgreSQL database table.
    
    Parameters:
    -----------
    api_url : str
        The URL of the API endpoint
    table_name : str
        Name of the PostgreSQL table to store the data
    headers : Optional[Dict[str, str]]
        Optional HTTP headers for the API request
    params : Optional[Dict[str, Any]]
        Optional query parameters for the API request
    transform_func : Optional[Callable]
        Optional function to transform the API response before storing
    is_paginated : bool
        Whether the API endpoint is paginated
    data_key : Optional[str]
        Key in the API response that contains the data array
    schema : Optional[Dict[str, Any]]
        Schema definition for the PostgreSQL table
    db_type : Optional[str]
        Type of database to connect
    """
    
    @apply_defaults
    def __init__(
        self,
        api_url: str,
        table_name: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        transform_func: Optional[Callable] = None,
        is_paginated: bool = False,
        data_key: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        auth_callable: Optional[Callable] = None,
        db_type: Optional[str] = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.table_name = table_name
        self.headers = headers
        self.params = params
        self.transform_func = transform_func
        self.is_paginated = is_paginated
        self.data_key = data_key
        self.schema = schema
        self.auth_callable = auth_callable
        self.db_type = db_type
        self.logger = logging.getLogger(__name__)
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the API to PostgreSQL pipeline
        
        Args:
            context: Airflow task context
            
        Returns:
            Dictionary with status and metadata about the operation
        """
        self.logger.info(f"Fetching data from API: {self.api_url} and storing in table: {self.table_name}")
        
        try:
            # Create table if schema is provided and table doesn't exist
            if self.schema:
                create_table_if_not_exists(self.table_name, self.schema)
            
            if self.auth_callable:
                try:
                    auth = self.auth_callable(context)
                except TypeError:
                    # If the callable doesn't accept context, try without it
                    auth = self.auth_callable()
                
                # Process authentication if available
                if auth and isinstance(auth, tuple) and len(auth) == 2:
                    # Initialize headers dictionary if None
                    self.headers = self.headers or {}
                    
                    # Create Basic Authentication header
                    auth_str = f"{auth[0]}:{auth[1]}"
                    encoded = base64.b64encode(auth_str.encode()).decode()
                    self.headers["Authorization"] = f"Basic {encoded}"
                
            result = api_to_db_pipeline(
                api_url=self.api_url,
                table_name=self.table_name,
                headers=self.headers,
                params=self.params,
                transform_func=self.transform_func,
                is_paginated=self.is_paginated,
                data_key=self.data_key,
                db_type=self.db_type
            )
            
            # Log success or failure
            if result['status'] == 'success':
                self.logger.info(f"Successfully processed {result.get('records_processed', 0)} records")
            else:
                self.logger.error(f"Failed to process API data: {result.get('message', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in ApiToPostgresOperator: {str(e)}")
            raise
