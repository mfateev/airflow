"""Tests for Temporal workflows."""
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from airflow._shared.timezones import timezone
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput


# Set DAGS_FOLDER to the directory containing test DAGs
# This is needed because the executor pattern loads DAGs from files
TEST_DAGS_FOLDER = Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def set_dags_folder():
    """Set AIRFLOW__CORE__DAGS_FOLDER to test DAGs directory for all tests."""
    old_value = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(TEST_DAGS_FOLDER)
    yield
    if old_value is None:
        os.environ.pop("AIRFLOW__CORE__DAGS_FOLDER", None)
    else:
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = old_value


# Configure sandbox to pass through problematic modules
# These modules use threading which is normally restricted, but they're only
# used for logging/observability which doesn't affect workflow determinism
PASSTHROUGH_MODULES = [
    "structlog",
    "rich",
    "airflow.sdk.observability",
    "airflow._shared.observability",
]


def create_worker(client, task_queue, workflows, activities):
    """Create a Worker with custom sandbox configuration."""
    return Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        # Thread pool for sync activities (blocking I/O operations)
        activity_executor=ThreadPoolExecutor(max_workers=5),
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                *PASSTHROUGH_MODULES
            )
        ),
    )


async def assert_no_workflow_task_failures(client, workflow_id: str) -> None:
    """
    Check workflow history for WorkflowTaskFailed events (indicates deadlock or other issues).

    Raises AssertionError if any WorkflowTaskFailed events are found.
    """
    from temporalio.api.enums.v1 import EventType

    handle = client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    failed_events = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            # Extract failure details
            failure = event.workflow_task_failed_event_attributes
            failed_events.append({
                "event_id": event.event_id,
                "cause": str(failure.cause) if failure.cause else "unknown",
                "failure": str(failure.failure.message) if failure.failure else "no message",
            })

    if failed_events:
        raise AssertionError(
            f"Workflow {workflow_id} had {len(failed_events)} WorkflowTaskFailed events "
            f"(possible deadlock): {failed_events}"
        )


@pytest.mark.asyncio
async def test_workflow_database_initialization():
    """Test that workflow initializes its own database without deadlocks."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError

    # Create a minimal real DAG with actual Python code
    def simple_task():
        return "task completed"

    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=simple_task)

    # Serialize it properly
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        # Create input with properly serialized DAG
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-workflow-db-init"

        # Start workflow
        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            # Test workflow completes successfully after Phase 2 implementation
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            # Verify successful completion
            assert result.state == "success"
            assert result.dag_id == "test_dag"
            assert result.run_id == "test_run"
            assert result.tasks_succeeded == 1
            assert result.tasks_failed == 0

            # CRITICAL: Check workflow history for deadlock events
            # Even if workflow succeeds, WorkflowTaskFailed events indicate problems
            await assert_no_workflow_task_failures(env.client, workflow_id)


# @pytest.mark.asyncio
# async def test_database_isolation():
#     """Test that multiple workflows have isolated databases."""
#     from airflow import DAG
#     from airflow.operators.empty import EmptyOperator
#     from airflow.serialization.serialized_objects import SerializedDAG
#     from temporal_airflow.activities import run_airflow_task
#
#     # Create two DAGs
#     with DAG(dag_id="dag1", start_date=timezone.datetime(2025, 1, 1)) as dag1:
#         EmptyOperator(task_id="task1")
#
#     with DAG(dag_id="dag2", start_date=timezone.datetime(2025, 1, 1)) as dag2:
#         EmptyOperator(task_id="task1")
#
#     # Serialize them
#     serialized1 = SerializedDAG.to_dict(dag1)
#     serialized2 = SerializedDAG.to_dict(dag2)
#
#     async with await WorkflowEnvironment.start_time_skipping() as env:
#         input1 = DagExecutionInput(
#             dag_id="dag1",
#             run_id="run1",
#             logical_date=timezone.datetime(2025, 1, 1),
#             serialized_dag=serialized1,
#         )
#
#         input2 = DagExecutionInput(
#             dag_id="dag2",
#             run_id="run2",
#             logical_date=timezone.datetime(2025, 1, 1),
#             serialized_dag=serialized2,
#         )
#
#         # Start two workflows concurrently
#         async with create_worker(
#             env.client,
#             task_queue="test-queue",
#             workflows=[ExecuteAirflowDagWorkflow],
#             activities=[run_airflow_task],
#         ):
#             results = await asyncio.gather(
#                 env.client.execute_workflow(
#                     ExecuteAirflowDagWorkflow.run,
#                     input1,
#                     id="test-workflow-1",
#                     task_queue="test-queue",
#                 ),
#                 env.client.execute_workflow(
#                     ExecuteAirflowDagWorkflow.run,
#                     input2,
#                     id="test-workflow-2",
#                     task_queue="test-queue",
#                 ),
#             )
#
#             # Both should complete successfully with correct IDs
#             assert results[0].dag_id == "dag1"
#             assert results[1].dag_id == "dag2"
#

@pytest.mark.asyncio
async def test_dag_deserialization():
    """Test that workflow deserializes DAG correctly."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Create a real DAG
    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: None)

    # Serialize it
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
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
    from temporal_airflow.activities import run_airflow_task

    # Create DAG with tasks
    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
        t1 = PythonOperator(task_id="task1", python_callable=lambda: None)
        t2 = PythonOperator(task_id="task2", python_callable=lambda: None)
        t1 >> t2

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
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
    import sys
    from pathlib import Path
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Load DAG from file (executor pattern)
    dag_file = Path(__file__).parent.parent / "test_loop_structure.py"

    import importlib.util
    spec = importlib.util.spec_from_file_location("test_loop_structure", dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find DAG in module
    from airflow.sdk.definitions.dag import DAG
    dag = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "test_loop_structure":
            dag = attr
            break

    assert dag is not None, "Could not find test_loop_structure DAG"
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_loop_structure",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            # After Phase 2, workflow should complete successfully
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-loop-structure",
                task_queue="test-queue",
            )

            # Verify successful completion
            assert result.state == "success"
            assert result.dag_id == "test_loop_structure"
            assert result.tasks_succeeded == 1
            assert result.tasks_failed == 0


@pytest.mark.asyncio
async def test_activity_starting():
    """Test that workflow starts activities for schedulable tasks."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Create DAG with Python task
    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: "result1")

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        # Register activity
        async with create_worker(
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
    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
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
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
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
    from pathlib import Path
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task

    # Load DAG from file (executor pattern)
    dag_file = Path(__file__).parent.parent / "test_parallel.py"

    import importlib.util
    spec = importlib.util.spec_from_file_location("test_parallel", dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find DAG in module
    from airflow.sdk.definitions.dag import DAG
    dag = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "test_parallel":
            dag = attr
            break

    assert dag is not None, "Could not find test_parallel DAG"
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_parallel",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
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

            # Verify successful parallel execution
            assert result.state == "success"
            assert result.dag_id == "test_parallel"
            assert result.tasks_succeeded == 3
            assert result.tasks_failed == 0


@pytest.mark.asyncio
async def test_task_failure_with_application_error():
    """Test that task failures raise ApplicationError with structured details."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError
    from temporal_airflow.models import DagExecutionFailureDetails

    # Create DAG with a task that will fail
    def failing_task():
        raise ValueError("Intentional test failure")

    with DAG(dag_id="test_dag", start_date=timezone.datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="failing_task", python_callable=failing_task)

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            # Workflow should fail with ApplicationError
            with pytest.raises(WorkflowFailureError) as exc_info:
                await env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input_data,
                    id="test-task-failure",
                    task_queue="test-queue",
                )

            # Verify it's a DagExecutionFailure ApplicationError with structured details
            assert isinstance(exc_info.value.cause, ApplicationError)
            assert exc_info.value.cause.type == "DagExecutionFailure"

            # Extract and validate failure details
            failure_data = exc_info.value.cause.details[0]
            failure_details = DagExecutionFailureDetails(**failure_data)
            assert failure_details.dag_id == "test_dag"
            assert failure_details.run_id == "test_run"
            assert failure_details.tasks_failed == 1
            assert failure_details.tasks_succeeded == 0


@pytest.mark.asyncio
async def test_mixed_success_and_failure_tasks():
    """Test DAG with both successful and failing tasks."""
    from pathlib import Path
    from airflow.serialization.serialized_objects import SerializedDAG
    from temporal_airflow.activities import run_airflow_task
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError
    from temporal_airflow.models import DagExecutionFailureDetails

    # Load DAG from file (executor pattern)
    dag_file = Path(__file__).parent.parent / "test_mixed_results.py"

    import importlib.util
    spec = importlib.util.spec_from_file_location("test_mixed_results", dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find DAG in module
    from airflow.sdk.definitions.dag import DAG
    dag = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "test_mixed_results":
            dag = attr
            break

    assert dag is not None, "Could not find test_mixed_results DAG"
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_mixed_results",
            run_id="test_run",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            # Workflow should fail with ApplicationError (overall DAG fails if any task fails)
            with pytest.raises(WorkflowFailureError) as exc_info:
                await env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input_data,
                    id="test-mixed-tasks",
                    task_queue="test-queue",
                )

            # Verify it's a DagExecutionFailure ApplicationError with mixed task results
            assert isinstance(exc_info.value.cause, ApplicationError)
            assert exc_info.value.cause.type == "DagExecutionFailure"

            # Extract and validate failure details show mixed results
            failure_data = exc_info.value.cause.details[0]
            failure_details = DagExecutionFailureDetails(**failure_data)
            assert failure_details.tasks_succeeded == 1
            assert failure_details.tasks_failed == 1
