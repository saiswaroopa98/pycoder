#!/bin/bash

API_KEY="ds_c073c1205c239fd07cbbe25b098dd224"
BASE_URL="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"

echo "Testing API first request..."
curl -s -H "X-API-Key: $API_KEY" "${BASE_URL}/api/v1/events?limit=1000" | jq '{hasMore, nextCursor, dataLength: (.data | length)}'

echo ""
echo "If there was a cursor above, testing with cursor..."
CURSOR=$(curl -s -H "X-API-Key: $API_KEY" "${BASE_URL}/api/v1/events?limit=1000" | jq -r '.nextCursor // .cursor // empty')

if [ ! -z "$CURSOR" ]; then
    echo "Using cursor: $CURSOR"
    curl -s -H "X-API-Key: $API_KEY" "${BASE_URL}/api/v1/events?limit=1000&cursor=$CURSOR" | jq '{hasMore, nextCursor, dataLength: (.data | length)}'
else
    echo "No cursor returned!"
fi