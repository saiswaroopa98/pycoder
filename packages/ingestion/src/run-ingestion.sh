#!/bin/bash
set -e

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create a .env file with your API_KEY"
    echo "Example: cp .env.example .env"
    exit 1
fi

# Create output directory
mkdir -p output

# Start the ingestion services
echo "Starting services..."
docker compose down -v 2>/dev/null || true
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 10

# Monitor progress
echo ""
echo "Monitoring ingestion progress..."
echo "(Press Ctrl+C to stop monitoring)"
echo "=============================================="

while true; do
    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null | tr -d ' ' || echo "0")
    
    # Check if ingestion is complete
    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "=============================================="
        echo ""
        echo "Event IDs have been exported to ./output/event_ids.txt"
        echo ""
        echo "To submit your results:"
        echo "curl -X POST \\"
        echo "  -H \"X-API-Key: ds_c073c1205c239fd07cbbe25b098dd224\" \\"
        echo "  -H \"Content-Type: text/plain\" \\"
        echo "  --data-binary @output/event_ids.txt \\"
        echo "  \"http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions?github_repo=https://github.com/yourusername/your-repo\""
        echo ""
        exit 0
    fi
    
    # Check if container is still running
    if ! docker ps | grep -q assignment-ingestion 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "ERROR: Ingestion container stopped unexpectedly"
        echo "=============================================="
        echo ""
        echo "Logs:"
        docker logs assignment-ingestion --tail 50
        exit 1
    fi
    
    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done