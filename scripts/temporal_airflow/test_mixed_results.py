"""
Test DAG for testing mixed success and failure tasks.

This DAG is used by test_mixed_success_and_failure_tasks to verify
that the workflow correctly handles DAGs with both successful and
failing tasks.
"""
from datetime import datetime

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


# Create DAG using SDK
with DAG(
    dag_id="test_mixed_results",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "mixed"],
) as dag:

    @task(task_id="success_task")
    def success_task():
        """Task that succeeds."""
        print("Executing success_task")
        return "success"

    @task(task_id="failing_task")
    def failing_task():
        """Task that fails with RuntimeError."""
        print("Executing failing_task")
        raise RuntimeError("Task failure")

    # Both tasks are independent and run in parallel
    t1 = success_task()
    t2 = failing_task()
