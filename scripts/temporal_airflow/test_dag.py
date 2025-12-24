"""
Simple test DAG for Temporal integration testing.

This DAG is used to test the executor pattern where activities
load DAG files and execute tasks with real operators.
"""
from datetime import datetime

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


# Create DAG using SDK
with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test"],
) as dag:

    @task(task_id="task1")
    def simple_task():
        """Simple task that returns a value."""
        print("Executing simple_task")
        return "Hello from task1!"

    @task(task_id="task2")
    def dependent_task(upstream_value):
        """Task that depends on task1."""
        print(f"Received from upstream: {upstream_value}")
        return f"Processed: {upstream_value}"

    # Set dependencies
    result1 = simple_task()
    result2 = dependent_task(result1)
