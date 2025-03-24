# Airflow Scrapers Repository

This repository contains Airflow DAGs for API data collection and data processing tasks with PostgreSQL storage.

## Project Structure

```
airflow-scrapers/
├── dags/           # Airflow DAG definitions
├── plugins/        # Custom Airflow plugins
│   └── operators/  # Custom operators for API tasks
├── include/        # Shared code and utilities
├── logs/           # Airflow logs directory
├── config/         # Configuration files
├── .env.example    # Example environment variables
├── requirements.txt # Python dependencies
├── Dockerfile      # Docker image definition
├── docker-compose.yaml # Docker Compose configuration
├── setup_airflow.sh # Script to set up Airflow (for non-Docker setup)
└── setup_postgres.sh # Script to set up PostgreSQL (for non-Docker setup)
```

## Setup with Docker (Recommended)

1. Configure environment variables:
   ```
   cp .env.example .env
   # Edit .env with your PostgreSQL and API credentials
   ```

2. Start the services using Docker Compose:
   ```
   docker-compose up -d
   ```
   This will start PostgreSQL, Airflow webserver, and Airflow scheduler in Docker containers.

3. Access the Airflow web UI at http://localhost:8080
   - Default username and password are defined in your .env file

4. To stop the services:
   ```
   docker-compose down
   ```

5. To view logs:
   ```
   docker-compose logs -f webserver  # For webserver logs
   docker-compose logs -f scheduler  # For scheduler logs
   ```

## Manual Setup (Without Docker)

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```
   cp .env.example .env
   # Edit .env with your PostgreSQL and API credentials
   ```

3. Set up PostgreSQL database:
   ```
   ./setup_postgres.sh
   ```
   This script creates the necessary PostgreSQL database for storing data.

4. Set up Airflow:
   ```
   ./setup_airflow.sh
   ```
   This script initializes Airflow and creates an admin user.

5. Start Airflow services:
   ```
   export AIRFLOW_HOME=$(pwd)
   airflow webserver -p 8080
   # In another terminal
   export AIRFLOW_HOME=$(pwd)
   airflow scheduler
   ```

## API Integration and PostgreSQL Storage

This repository includes tools for API-based data collection and PostgreSQL storage:

1. **API Utilities** (`include/api_utils.py`):
   - Authentication for various APIs
   - Data transformation utilities
   - Database connection helpers

2. **Custom Operators**:
   - `ApiToPostgresOperator`: For API data collection and PostgreSQL storage

3. **Current DAGs**:
   - `bangkok_budget_scraper.py`: Collects budget data from the Bangkok Metropolitan Administration API

## Adding New Scrapers

To add a new scraper:

1. Create a new DAG file in the `dags/` directory
2. Add any shared utilities to the `include/` directory
3. If needed, create custom operators in the `plugins/` directory

### Adding a New API Scraper

1. Create a new DAG file in the `dags/` directory
2. Use the `ApiToPostgresOperator` for simple API scraping:
   ```python
   fetch_data = ApiToPostgresOperator(
       task_id='fetch_data',
       api_url='https://api.example.com/data',
       table_name='my_data_table',
       transform_func=transform_data_function,
       schema=table_schema
   )
   ```
3. For more complex scenarios, create a custom transformation function

## Environment Variables

The following environment variables are required:

### Database Configuration
- `POSTGRES_USER`: PostgreSQL username for Airflow database
- `POSTGRES_PASSWORD`: PostgreSQL password for Airflow database
- `POSTGRES_HOST`: PostgreSQL host (default: postgres in Docker, localhost otherwise)
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_DB`: PostgreSQL database name for Airflow

### BMA Database Configuration
- `BMA_DB_USER`: PostgreSQL username for BMA data
- `BMA_DB_PASSWORD`: PostgreSQL password for BMA data
- `BMA_DB_HOST`: PostgreSQL host for BMA data
- `BMA_DB_PORT`: PostgreSQL port for BMA data
- `BMA_DB_NAME`: PostgreSQL database name for BMA data

### API Credentials
- `BMA_API_USERNAME`: Username for BMA API
- `BMA_API_PASSWORD`: Password for BMA API

### Airflow Configuration
- `AIRFLOW_ADMIN_USERNAME`: Airflow admin username
- `AIRFLOW_ADMIN_PASSWORD`: Airflow admin password
- `AIRFLOW_ADMIN_FIRSTNAME`: Airflow admin first name
- `AIRFLOW_ADMIN_LASTNAME`: Airflow admin last name
- `AIRFLOW_ADMIN_EMAIL`: Airflow admin email
- `FERNET_KEY`: Fernet key for Airflow (for encrypting connection passwords)

When using the manual setup, you'll also need:
- `AIRFLOW_HOME`: Path to the Airflow home directory

## Running DAGs

After setting up Airflow, you can trigger DAGs from the Airflow web UI at http://localhost:8080.

### Best Practices
- Document your DAGs and tasks thoroughly
- Use environment variables for database credentials
- Add appropriate indexes to PostgreSQL tables for query performance

### Working with Multiple Databases

This setup supports sending data to different databases by using the `db_type` parameter:

```python
fetch_data = ApiToPostgresOperator(
    task_id='fetch_data',
    api_url='https://api.example.com/data',
    table_name='my_data_table',
    transform_func=transform_data_function,
    schema=table_schema,
    db_type='BMA'  # This will use the BMA database configuration
)
```

### Troubleshooting

1. **Docker Issues**
   - Check container status: `docker-compose ps`
   - View container logs: `docker-compose logs -f <service_name>`
   - Restart services: `docker-compose restart <service_name>`

2. **Permission Issues**
   - If you encounter permission errors with `/opt/airflow` directories:
     - The logs directory uses a named volume (`airflow-logs`) to avoid permission issues
     - For Windows users: ensure Docker Desktop is up-to-date
     - For Linux/Ubuntu users: you may need to run `mkdir -p ./logs && chmod -R 777 ./logs` before starting containers

3. **Logging Configuration Issues**
   - If you see errors like `Unable to configure handler 'processor'`:
     - Make sure the `airflow.cfg` file has the correct logging configuration
     - Ensure the logs directory is properly mounted as a named volume in `docker-compose.yaml`
     - Try removing any existing logs with `docker-compose down -v` and then restart

4. **Database Connection Issues**
   - Ensure your database credentials are correct in the .env file
   - For Docker setup, make sure the PostgreSQL container is running
   - Check network connectivity between containers

5. **API Connection Issues**
   - Verify API credentials in the .env file
   - Check API endpoint URLs in your DAG files
