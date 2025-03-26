#!/bin/bash

# Setup script for PostgreSQL database for Airflow scrapers

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo "No .env file found. Using default values."
    # Default values
    POSTGRES_USER="airflow"
    POSTGRES_PASSWORD="airflow"
    POSTGRES_HOST="localhost"
    POSTGRES_PORT="5432"
    POSTGRES_DB="airflow"
fi

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "PostgreSQL is not installed. Please install PostgreSQL first."
    echo "On macOS, you can use: brew install postgresql"
    exit 1
fi

# Check if PostgreSQL server is running
if ! pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT &> /dev/null; then
    echo "PostgreSQL server is not running. Please start the PostgreSQL server."
    echo "On macOS, you can use: brew services start postgresql"
    exit 1
fi

# Create database if it doesn't exist
echo "Creating database $POSTGRES_DB if it doesn't exist..."
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -c "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 || psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -c "CREATE DATABASE $POSTGRES_DB"

echo "PostgreSQL setup complete!"
echo "Database: $POSTGRES_DB"
echo "Host: $POSTGRES_HOST"
echo "Port: $POSTGRES_PORT"
echo "User: $POSTGRES_USER"
