#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-${DEMO_API_KEY:-}}"
VUS="${VUS:-10}"
DURATION="${DURATION:-30s}"
SCENARIO="${SCENARIO:-event_ingestion}"

if [ -z "$API_KEY" ]; then
    echo "Error: API_KEY or DEMO_API_KEY environment variable is required."
    exit 1
fi

SCENARIO_PATH="load_tests/k6/scenarios/${SCENARIO}.js"
if [ ! -f "$SCENARIO_PATH" ]; then
    echo "Error: Scenario file not found: $SCENARIO_PATH"
    exit 1
fi

echo "=========================================================="
echo " Running k6 Load Test Scenario: $SCENARIO"
echo " API URL:   $API_URL"
echo " VUs:       $VUS"
echo " Duration:  $DURATION"
echo "=========================================================="

if command -v k6 &> /dev/null; then
    k6 run \
        -e API_BASE_URL="$API_URL" \
        -e API_KEY="$API_KEY" \
        -e VUS="$VUS" \
        -e DURATION="$DURATION" \
        "$SCENARIO_PATH"
else
    echo "k6 binary not found on PATH. Running via Docker (grafana/k6)..."
    DOCKER_API_URL=$(echo "$API_URL" | sed 's/localhost/host.docker.internal/' | sed 's/127.0.0.1/host.docker.internal/')
    docker run --rm -i \
        --add-host=host.docker.internal:host-gateway \
        -v "$(pwd):/work" \
        -w /work \
        grafana/k6 run \
        -e "API_BASE_URL=$DOCKER_API_URL" \
        -e "API_KEY=$API_KEY" \
        -e "VUS=$VUS" \
        -e "DURATION=$DURATION" \
        "$SCENARIO_PATH"
fi
