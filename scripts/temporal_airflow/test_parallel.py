"""
Test DAG for testing parallel task execution.

This DAG is used by test_parallel_task_execution to verify that
multiple independent tasks can execute concurrently.
"""
from datetime import datetime

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


# Create DAG using SDK
with DAG(
    dag_id="test_parallel",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "parallel"],
) as dag:

    @task(task_id="task1")
    def task_a():
        """First parallel task."""
        print("Executing task1")
        return "a"

    @task(task_id="task2")
    def task_b():
        """Second parallel task."""
        print("Executing task2")
        return "b"

    @task(task_id="task3")
    def task_c():
        """Third parallel task."""
        print("Executing task3")
        return "c"

    # All tasks are independent and can run in parallel
    t1 = task_a()
    t2 = task_b()
    t3 = task_c()
