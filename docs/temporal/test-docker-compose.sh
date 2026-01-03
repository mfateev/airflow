#!/bin/bash
# Test script for docker-compose-temporal.yaml
# This script builds and tests the Temporal integration docker-compose setup

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DIR="/tmp/airflow-temporal-test"

echo "=== Airflow Temporal Docker-Compose Test ==="
echo "Script dir: $SCRIPT_DIR"
echo "Repo root: $REPO_ROOT"
echo "Test dir: $TEST_DIR"
echo ""

# Cleanup function
cleanup() {
    echo "=== Cleaning up ==="
    cd "$TEST_DIR" 2>/dev/null && docker-compose -f docker-compose-temporal.yaml down -v 2>/dev/null || true
    rm -rf "$TEST_DIR" 2>/dev/null || true
    echo "Cleanup complete"
}

# Handle Ctrl+C
trap cleanup EXIT

# Step 1: Clean up any existing test
echo "=== Step 1: Cleaning up previous test ==="
cleanup

# Step 2: Build image from local sources using simple Dockerfile
echo "=== Step 2: Building Airflow image from local sources ==="
cd "$REPO_ROOT"
docker build -f docs/temporal/Dockerfile.temporal -t airflow-temporal:latest .

# Step 3: Setup test directory
echo "=== Step 3: Setting up test directory ==="
mkdir -p "$TEST_DIR"/{dags,logs,scripts}
cp "$SCRIPT_DIR/docker-compose-temporal.yaml" "$TEST_DIR/"
cp -r "$REPO_ROOT/scripts/temporal_airflow" "$TEST_DIR/scripts/"

# Create sample DAG
cat > "$TEST_DIR/dags/example_dag.py" << 'EOF'
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello from Temporal!")
    return "success"

def world():
    print("World!")
    return "done"

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="hello", python_callable=hello)
    t2 = PythonOperator(task_id="world", python_callable=world)
    t1 >> t2
EOF

# Step 4: Start services
echo "=== Step 4: Starting services ==="
cd "$TEST_DIR"
docker-compose -f docker-compose-temporal.yaml up -d

# Step 5: Wait for services
echo "=== Step 5: Waiting for services to be healthy (max 2 minutes) ==="
for i in {1..24}; do
    echo "Checking services... (attempt $i/24)"

    # Check all services
    HEALTHY=$(docker-compose -f docker-compose-temporal.yaml ps --format json 2>/dev/null | \
        jq -r 'select(.Health == "healthy" or .Health == "") | .Service' 2>/dev/null | wc -l || echo "0")

    if [ "$HEALTHY" -ge 4 ]; then
        echo "All services healthy!"
        break
    fi

    sleep 5
done

# Step 6: Show status
echo "=== Step 6: Service Status ==="
docker-compose -f docker-compose-temporal.yaml ps

# Step 7: Show logs if any service is not healthy
echo "=== Step 7: Checking logs ==="
for service in temporal-worker airflow-apiserver airflow-dag-processor; do
    STATUS=$(docker-compose -f docker-compose-temporal.yaml ps --format json 2>/dev/null | \
        jq -r "select(.Service == \"$service\") | .State" 2>/dev/null || echo "unknown")

    if [ "$STATUS" = "restarting" ]; then
        echo "=== Logs for $service (restarting) ==="
        docker-compose -f docker-compose-temporal.yaml logs "$service" 2>&1 | tail -20
    fi
done

echo ""
echo "=== Test Complete ==="
echo "Airflow UI: http://localhost:8080"
echo "Temporal UI: http://localhost:8233"
echo ""
echo "To clean up, run: cd $TEST_DIR && docker-compose -f docker-compose-temporal.yaml down -v"
