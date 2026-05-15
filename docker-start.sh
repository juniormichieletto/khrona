#!/bin/bash
# Starts the Redis container in the background
echo "Starting Khrona local Redis for development..."
docker compose up -d redis
echo "Redis started. Connection string:"
echo "redis://:khrona_dev_password@localhost:6379/0"
