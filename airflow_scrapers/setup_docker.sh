#!/bin/bash

set -e

# Generate a Fernet key if not already in .env
if ! grep -q "^FERNET_KEY=" .env 2>/dev/null; then
  echo "Generating Fernet key..."
  FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  echo "FERNET_KEY=$FERNET_KEY" >> .env
  echo "Fernet key added to .env"
fi

# Create .env file from example if it doesn't exist
if [ ! -f .env ]; then
  echo "Creating .env file from .env.example..."
  cp .env.example .env
  echo "Please update the .env file with your credentials before continuing."
  exit 1
fi

# Build and start the Docker containers
echo "Starting Docker containers..."
docker-compose up -d

echo "\nAirflow is now running!"
echo "Web UI: http://localhost:8080"
