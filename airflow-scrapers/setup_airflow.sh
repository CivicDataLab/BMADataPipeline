#!/bin/bash

# Setup script for Airflow scrapers repository

# Set environment variables
export AIRFLOW_HOME=$(pwd)
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Create necessary directories if they don't exist
mkdir -p logs

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init

# Create admin user if it doesn't exist
echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname BMA \
    --lastname Admin \
    --role Admin \
    --email tech@civicdatalab.in

echo ""
echo "Setup complete!"
echo ""
echo "To start Airflow webserver:"
echo "airflow webserver -p 8080"
echo ""
echo "To start Airflow scheduler:"
echo "airflow scheduler"
echo ""
echo "Access the Airflow UI at: http://localhost:8080"
