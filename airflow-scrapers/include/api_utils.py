import requests
import json
import logging
import time
from typing import Dict, Any, Optional, List, Union
from ratelimit import limits, sleep_and_retry
from retrying import retry
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Base class for SQLAlchemy models
Base = declarative_base()

# Default rate limits
DEFAULT_CALLS = 60
DEFAULT_PERIOD = 60  # 60 calls per 60 seconds = 1 call per second

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_db_connection_string(db_type=None) -> str:
    """
    Get database connection string from environment variables

    Args:
        db_type: Optional type of database to connect

    Returns:
        Database connection string
    """
    # If a specific database type is specified, use its environment variables
    if db_type == 'BMA':
        db_user = os.getenv('BMA_DB_USER', os.getenv(
            'POSTGRES_USER', 'airflow'))
        db_password = os.getenv('BMA_DB_PASSWORD', os.getenv(
            'POSTGRES_PASSWORD', 'airflow'))
        db_host = os.getenv('BMA_DB_HOST', os.getenv(
            'POSTGRES_HOST', 'localhost'))
        db_port = os.getenv('BMA_DB_PORT', os.getenv('POSTGRES_PORT', '5432'))
        db_name = os.getenv('BMA_DB_NAME', 'bma_data')
    else:
        # Default Airflow database connection
        db_user = os.getenv('POSTGRES_USER', 'airflow')
        db_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        db_host = os.getenv('POSTGRES_HOST', 'localhost')
        db_port = os.getenv('POSTGRES_PORT', '5432')
        db_name = os.getenv('POSTGRES_DB', 'airflow')

    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def create_db_engine(db_type=None):
    """
    Create and return a SQLAlchemy database engine

    Args:
        db_type: Optional type of database to connect

    Returns:
        SQLAlchemy engine
    """
    connection_string = get_db_connection_string(db_type)
    return create_engine(connection_string)


def create_db_session(db_type=None):
    """
    Create and return a SQLAlchemy session

    Args:
        db_type: Optional type of database to connect

    Returns:
        SQLAlchemy session
    """
    engine = create_db_engine(db_type)
    Session = sessionmaker(bind=engine)
    return Session()


def create_table_if_not_exists(table_name: str, columns: Dict[str, Any], db_type=None):
    """
    Create a table if it doesn't exist

    Args:
        table_name: Name of the table to create
        columns: Dictionary of column names and their SQLAlchemy types
        db_type: Optional type of database to connect

    Returns:
        SQLAlchemy Table object
    """
    engine = create_db_engine(db_type)
    metadata = MetaData()

    # Define the table
    table = Table(table_name, metadata,
                  Column('id', Integer, primary_key=True),
                  *[Column(name, col_type) for name, col_type in columns.items()]
                  )

    # Create the table if it doesn't exist
    metadata.create_all(engine)

    return table


def insert_data_to_db(table_name: str, data: Dict[str, Any], db_type=None):
    """
    Insert data into a PostgreSQL table

    Args:
        table_name: Name of the table to insert data into
        data: List of dictionaries containing the data to insert
        db_type: Optional type of database to connect
    """
    if not data:
        logger.warning(f"No data to insert into {table_name}")
        return
    db_host = os.getenv("POSTGRES_HOST")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    engine = create_engine(
        f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode=require'
    )

    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name not in metadata.tables:
        raise ValueError(
            f"Table '{table_name}' does not exist in the database.")

    table = metadata.tables[table_name]

    try:
        with engine.begin() as conn:
            
            # for row in data:
            #     row.setdefault("created_at", datetime.utcnow())
            # logger.info(f"The first data to be inserted are {data[0]}")
            conn.execute(table.insert(), data)
            logger.info(f"Inserted {len(data)} rows into {table_name}")
    except Exception as e:
        logger.info(f"Failed to insert into {table_name}: {str(e)}")
        pass #even if the insert fails continue


@sleep_and_retry
@limits(calls=DEFAULT_CALLS, period=DEFAULT_PERIOD)
@retry(stop_max_attempt_number=MAX_RETRIES, wait_fixed=RETRY_DELAY*1000)
def make_api_request(url: str, method: str = 'GET', headers: Optional[Dict[str, str]] = None,
                     params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                     auth: Optional[Any] = None, timeout: int = 30, cookies: Optional[Dict[str, str]] = None) -> requests.Response:
    """
    Make an API request with rate limiting and retry logic

    Args:
        url: URL to request
        method: HTTP method (GET, POST, PUT, DELETE)
        headers: Optional request headers
        params: Optional query parameters
        data: Optional request body
        auth: Optional authentication
        timeout: Request timeout in seconds

    Returns:
        Response object
    """
    headers = headers or {}
    if 'User-Agent' not in headers:
        headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=data if method in ['POST', 'PUT', 'PATCH'] else None,
            auth=auth,
            timeout=timeout,
            cookies=cookies
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise


def get_json_data(url: str, headers: Optional[Dict[str, str]] = None,
                  params: Optional[Dict[str, Any]] = None, auth: Optional[Any] = None,
                  cookies: Optional[Dict[str, str]] = None) -> Any:
    """
    Make a GET request to an API and return the JSON data

    Args:
        url: URL to request
        headers: Optional request headers
        params: Optional query parameters
        auth: Optional authentication

    Returns:
        JSON data from the API
    """
    response = make_api_request(
        url=url, method='GET', headers=headers, params=params, auth=auth, cookies=cookies)
    return response.json()


def post_data(url: str, data: Dict[str, Any], headers: Optional[Dict[str, str]] = None,
              auth: Optional[Any] = None) -> Any:
    """
    Make a POST request to an API

    Args:
        url: URL to request
        data: Data to send in the request body
        headers: Optional request headers
        auth: Optional authentication

    Returns:
        JSON data from the API response
    """
    response = make_api_request(
        url=url, method='POST', headers=headers, data=data, auth=auth)
    return response.json()


def paginated_api_request(base_url: str, params: Dict[str, Any], headers: Optional[Dict[str, str]] = None,
                          page_param: str = 'page', limit_param: str = 'limit', limit: int = 100,
                          max_pages: Optional[int] = None, data_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Make paginated API requests and combine the results

    Args:
        base_url: Base URL for the API
        params: Query parameters
        headers: Optional request headers
        page_param: Name of the page parameter
        limit_param: Name of the limit parameter
        limit: Number of items per page
        max_pages: Maximum number of pages to request
        data_key: Key in the response that contains the data array

    Returns:
        Combined data from all pages
    """
    all_data = []
    page = 1
    params = params.copy()
    params[limit_param] = limit

    while True:
        params[page_param] = page
        response = make_api_request(
            url=base_url, method='GET', headers=headers, params=params)
        response_data = response.json()

        # Extract data based on data_key if provided
        page_data = response_data.get(
            data_key, response_data) if data_key else response_data

        # Handle different API response formats
        if isinstance(page_data, list):
            all_data.extend(page_data)
        elif isinstance(page_data, dict) and 'results' in page_data:
            all_data.extend(page_data['results'])
        else:
            logger.warning(f"Unexpected response format: {type(page_data)}")
            break

        # Check if we've reached the last page
        if len(page_data) < limit or (max_pages and page >= max_pages):
            break

        page += 1
        time.sleep(1)  # Be nice to the API :)

    return all_data


def save_api_response_to_db(data: Union[List[Dict[str, Any]], Dict[str, Any]], table_name: str,
                            transform_func: Optional[callable] = None, db_type=None):
    """
    Save API response data to a PostgreSQL database

    Args:
        data: Data from the API response
        table_name: Name of the table to save data to
        transform_func: Optional function to transform the data before saving
        db_type: Optional type of database to connect
    """
    # Convert single item to list
    if isinstance(data, dict):
        data = [data]

    # Transform data if a transform function is provided
    if transform_func:
        data = transform_func(data)

    # Insert data into the database
    insert_data_to_db(table_name, data, db_type)


def get_bma_api_auth():
    """
    Get basic authentication credentials for the BMA budget MIS API

    Returns:
        Tuple of (username, password) for basic authentication
    """
    username = os.getenv('BMA_MIS_API_USERNAME')
    password = os.getenv('BMA_MIS_API_PASSWORD')

    if not all([username,password]):
        raise ValueError(
            "BMA API credentials not found in environment variables")

    return (username, password)

# get Weather API credentials


def get_bma_weather_api_auth():
    username = os.getenv("BMA_WEATHER_API_USERNAME")
    password = os.getenv("BMA_WEATHER_API_PASSWORD")

    if not all([username, password]):
        raise ValueError(
            "BMA Weather API credentials are not found in the environment variables")
    return (username, password)


def api_to_db_pipeline(api_url: str, table_name: str, headers: Optional[Dict[str, str]] = None,
                       params: Optional[Dict[str, Any]] = None, transform_func: Optional[callable] = None,
                       is_paginated: bool = False, data_key: Optional[str] = None, cookies: Optional[Dict[str, str]] = None,
                       auth: Optional[Any] = None, db_type: Optional[str] = None):
    """
    Complete pipeline to fetch data from an API and save it to a PostgreSQL database

    Args:
        api_url: URL of the API
        table_name: Name of the table to save data to
        headers: Optional request headers
        params: Optional query parameters
        transform_func: Optional function to transform the data before saving
        is_paginated: Whether the API is paginated
        data_key: Key in the response that contains the data array
        cookies: Optional cookies to send with the request
        auth: Optional authentication credentials (tuple of username, password)
        db_type: Optional type of database to connect
    """
    try:
        # Fetch data from API
        if is_paginated:
            data = paginated_api_request(
                api_url, params or {}, headers, data_key=data_key)
        else:
            # Add cookies and auth to the request if provided
            response = make_api_request(
                api_url,
                headers=headers,
                params=params,
                cookies=cookies,
                auth=auth
            )

            # Check for HTTP errors
            if response.status_code >= 400:
                error_msg = f"API request failed with status code {response.status_code}: {response.text}"
                logger.error(error_msg)
                return {
                    'status': 'error',
                    'message': error_msg,
                    'status_code': response.status_code
                }

            # Parse JSON response
            try:
                data = response.json()
                # logger.info(f"The fetched data is {data}")
                logger.info(
                    f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
            except Exception as json_err:
                error_msg = f"Failed to parse JSON response: {str(json_err)}. Response: {response.text[:200]}..."
                logger.error(error_msg)
                return {
                    'status': 'error',
                    'message': error_msg
                }

            # Extract data from response if data_key is provided
            if data_key and isinstance(data, dict) and data_key in data:
                data = data[data_key]
                logger.info(
                    f"Extracted {len(data) if isinstance(data, list) else 'non-list'} data using key '{data_key}'")

        # Validate data before saving
        if not data:
            error_msg = "API returned empty data"
            logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg
            }

        # Save data to database
        try:
            save_api_response_to_db(data, table_name, transform_func, db_type)
        except Exception as db_err:
            error_msg = f"Error saving data to database: {str(db_err)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg
            }

        return {
            'status': 'success',
            'records_processed': len(data) if isinstance(data, list) else 1,
            'table': table_name
        }

    except Exception as e:
        logger.error(f"Error in API to DB pipeline: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


