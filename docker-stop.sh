#!/bin/bash
# Stops the Redis container without removing data
echo "Stopping Khrona local Redis..."
docker compose stop redis
echo "Redis stopped. Use 'docker compose down -v' to completely remove data if needed."
