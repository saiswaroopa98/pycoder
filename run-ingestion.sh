#!/bin/bash
set -e

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    exit 1
fi

mkdir -p output

echo "Starting services..."
docker compose down -v 2>/dev/null || true
docker compose up -d --build

echo "Waiting for services to initialize..."
sleep 10

echo "Monitoring ingestion progress..."
echo "=============================================="

while true; do
    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null | tr -d ' ' || echo "0")
    
    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "=============================================="
        exit 0
    fi
    
    if ! docker ps | grep -q assignment-ingestion 2>/dev/null; then
        echo "ERROR: Container stopped"
        docker logs assignment-ingestion --tail 50
        exit 1
    fi
    
    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done