#!/bin/bash
# Helper script to run temporal_airflow tests in Breeze with workarounds for Docker mount caching

set -e

# Install dependencies
uv pip install temporalio structlog

# Add to PYTHONPATH
export PYTHONPATH=/opt/airflow/scripts/temporal_airflow:$PYTHONPATH

# Run tests
cd /opt/airflow
pytest scripts/temporal_airflow/tests/test_workflows.py "$@"
