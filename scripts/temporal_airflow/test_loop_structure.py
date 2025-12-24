"""
Test DAG for testing scheduling loop structure.

This DAG is used by test_scheduling_loop_structure to verify that
the workflow scheduling loop correctly handles task execution.
"""
from datetime import datetime

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


def loop_test_task():
    """Simple task that completes successfully."""
    return "loop iteration complete"


# Create DAG using SDK
with DAG(
    dag_id="test_loop_structure",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "loop"],
) as dag:

    @task(task_id="task1")
    def execute_task():
        """Execute the loop test task."""
        return loop_test_task()

    execute_task()
