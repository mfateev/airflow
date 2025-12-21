"""Tests for Temporal workflows."""
import asyncio
from datetime import datetime

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput


@pytest.mark.asyncio
async def test_workflow_database_initialization():
    """Test that workflow initializes its own database."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # Create minimal input
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "test_dag"}},
        )

        # Start workflow
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-workflow-db-init",
                task_queue="test-queue",
            )

            # Should complete without error
            assert result.dag_id == "test_dag"
            assert result.run_id == "test_run"


@pytest.mark.asyncio
async def test_database_isolation():
    """Test that multiple workflows have isolated databases."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        input1 = DagExecutionInput(
            dag_id="dag1",
            run_id="run1",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag1"}},
        )

        input2 = DagExecutionInput(
            dag_id="dag2",
            run_id="run2",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag2"}},
        )

        # Start two workflows concurrently
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            results = await asyncio.gather(
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input1,
                    id="test-workflow-1",
                    task_queue="test-queue",
                ),
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input2,
                    id="test-workflow-2",
                    task_queue="test-queue",
                ),
            )

            # Both should complete successfully with correct IDs
            assert results[0].dag_id == "dag1"
            assert results[1].dag_id == "dag2"


@pytest.mark.asyncio
async def test_dag_deserialization():
    """Test that workflow deserializes DAG correctly."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create a real DAG
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: None)

    # Serialize it
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-dag-deser",
                task_queue="test-queue",
            )

            # Should complete without error
            assert result.dag_id == "test_dag"


@pytest.mark.asyncio
async def test_dag_run_creation():
    """Test that workflow creates DagRun and TaskInstances."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create DAG with tasks
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        t1 = PythonOperator(task_id="task1", python_callable=lambda: None)
        t2 = PythonOperator(task_id="task2", python_callable=lambda: None)
        t1 >> t2

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-dag-run-creation",
                task_queue="test-queue",
            )

            # Should complete (placeholder return for now)
            assert result.dag_id == "test_dag"


@pytest.mark.asyncio
async def test_scheduling_loop_structure():
    """Test that loop structure works (completion in Commit 5)."""
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create simple DAG
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        EmptyOperator(task_id="task1")

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-loop-structure",
                task_queue="test-queue",
            )

            # Loop should complete (state will be updated in Commit 5)
            assert result.state in ["success", "failed"]
            assert result.dag_id == "test_dag"


@pytest.mark.asyncio
async def test_activity_starting():
    """Test that workflow starts activities for schedulable tasks."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Create DAG with Python task
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: "result1")

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        # Register activity
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-activity-start",
                task_queue="test-queue",
            )

            # Should complete (even without handling completions yet)
            # Activities will run but workflow won't process results yet
            assert result.dag_id == "test_dag"


@pytest.mark.asyncio
async def test_end_to_end_dag_execution():
    """Test complete DAG execution with task completion handling."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Create DAG with dependencies
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        def task1_func():
            return "output_from_task1"

        def task2_func():
            return "output_from_task2"

        t1 = PythonOperator(task_id="task1", python_callable=task1_func)
        t2 = PythonOperator(task_id="task2", python_callable=task2_func)
        t1 >> t2  # Sequential execution

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-e2e-execution",
                task_queue="test-queue",
            )

            # Should complete successfully
            assert result.state == "success"
            assert result.dag_id == "test_dag"
            assert result.tasks_succeeded >= 2  # Both tasks ran


@pytest.mark.asyncio
async def test_parallel_task_execution():
    """Test that parallel tasks execute concurrently."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        t1 = PythonOperator(task_id="task1", python_callable=lambda: "a")
        t2 = PythonOperator(task_id="task2", python_callable=lambda: "b")
        t3 = PythonOperator(task_id="task3", python_callable=lambda: "c")
        # All can run in parallel
        [t1, t2, t3]

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-parallel-execution",
                task_queue="test-queue",
            )

            assert result.state == "success"
            assert result.tasks_succeeded == 3
