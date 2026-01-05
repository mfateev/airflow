# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""End-to-end tests for deep integration workflow using real Airflow models.

These tests run the complete workflow with:
- Real Airflow DB for DAG storage and status sync
- Temporal test environment for workflow execution
- Real activities for task execution
- Actual DAG Python files (not dynamically created DAGs)
"""
from __future__ import annotations

import importlib.util
import os
from datetime import datetime, timezone
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporalio.api.enums.v1 import EventType

from temporal_airflow.activities import run_airflow_task
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.models import DeepDagExecutionInput, DagExecutionFailureDetails
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_task_status_batch,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
)


async def assert_no_workflow_task_failures(client, workflow_id: str) -> None:
    """Check workflow history for WorkflowTaskFailed events (indicates deadlock).

    Temporal's deadlock detector triggers WorkflowTaskFailed events when workflow
    code doesn't yield within 2 seconds. This helper checks the workflow history
    to ensure no such events occurred.

    Args:
        client: Temporal client
        workflow_id: The workflow ID to check

    Raises:
        AssertionError: If any WorkflowTaskFailed events are found
    """
    handle = client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    failed_events = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
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


# Set DAGS_FOLDER to the directory containing test DAGs
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


def _load_dag_from_file(dag_file: Path, dag_id: str):
    """Load a DAG from a Python file.

    This mimics how Airflow loads DAGs from files. The DAG file must define
    a DAG at module level with the specified dag_id.
    """
    from airflow.sdk.definitions.dag import DAG

    spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find DAG in module
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == dag_id:
            return attr

    raise ValueError(f"DAG '{dag_id}' not found in {dag_file}")


def _serialize_dag_to_db(dag):
    """Serialize and save a DAG to the database, returning cleanup function."""
    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagcode import DagCode
    from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG

    bundle_name = "test-bundle"
    dag_id = dag.dag_id

    with create_session() as session:
        # Clean up any existing data first (ensure fresh state)
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
        session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
        session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
        session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
        session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
        session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
        session.commit()

        # Create bundle first (required by foreign key constraint in Airflow 3.x)
        existing_bundle = session.query(DagBundleModel).filter(
            DagBundleModel.name == bundle_name
        ).first()
        if not existing_bundle:
            bundle = DagBundleModel(name=bundle_name)
            session.add(bundle)
            session.commit()

        # First create the DagModel entry (required for foreign key in dag_version)
        SerializedDAG.bulk_write_to_db(
            bundle_name=bundle_name,
            bundle_version=None,
            dags=[dag],
            session=session,
        )
        session.commit()

        # Convert to LazyDeserializedDAG and write the serialized DAG
        lazy_dag = LazyDeserializedDAG.from_dag(dag)
        SerializedDagModel.write_dag(
            lazy_dag,
            bundle_name=bundle_name,
            session=session,
        )
        session.commit()

    def cleanup():
        with create_session() as session:
            session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
            session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
            session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
            session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
            session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
            session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
            session.commit()

    return cleanup


# All sync activities that the deep workflow uses
SYNC_ACTIVITIES = [
    create_dagrun_record,
    sync_task_status,
    sync_task_status_batch,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
]

# Task execution activity
TASK_ACTIVITIES = [run_airflow_task]


class TestDeepWorkflowE2E:
    """End-to-end tests for deep integration workflow."""

    @pytest.mark.asyncio
    async def test_simple_dag_execution(self):
        """Test end-to-end execution of test_loop_structure DAG (1 task)."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with await WorkflowEnvironment.start_time_skipping() as env:
                input_data = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
                )

                async with Worker(
                    env.client,
                    task_queue="test-queue",
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=SYNC_ACTIVITIES + TASK_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    workflow_id = "test-deep-simple"
                    result = await env.client.execute_workflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        input_data,
                        id=workflow_id,
                        task_queue="test-queue",
                    )

                    # Check for deadlock events in workflow history
                    await assert_no_workflow_task_failures(env.client, workflow_id)

                    # Verify workflow result
                    assert result.state == "success"
                    assert result.dag_id == "test_loop_structure"
                    assert result.tasks_succeeded == 1
                    assert result.tasks_failed == 0

                    # Verify DagRun was synced to real DB
                    with create_session() as session:
                        dag_run = session.query(DagRun).filter(
                            DagRun.dag_id == "test_loop_structure"
                        ).first()
                        assert dag_run is not None
                        assert dag_run.state == DagRunState.SUCCESS
                        assert dag_run.run_type == DagRunType.EXTERNAL

                        # Verify TaskInstance was synced
                        ti = session.query(TaskInstance).filter(
                            TaskInstance.dag_id == "test_loop_structure",
                            TaskInstance.task_id == "task1",
                        ).first()
                        assert ti is not None
                        assert ti.state == TaskInstanceState.SUCCESS
        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_parallel_dag_execution(self):
        """Test execution of test_parallel DAG (3 tasks, 2 parallel + 1 join)."""
        dag_file = TEST_DAGS_FOLDER / "test_parallel.py"
        dag = _load_dag_from_file(dag_file, "test_parallel")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with await WorkflowEnvironment.start_time_skipping() as env:
                input_data = DeepDagExecutionInput(
                    dag_id="test_parallel",
                    logical_date=datetime(2025, 6, 2, tzinfo=timezone.utc),
                )

                async with Worker(
                    env.client,
                    task_queue="test-queue",
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=SYNC_ACTIVITIES + TASK_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    workflow_id = "test-deep-parallel"
                    result = await env.client.execute_workflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        input_data,
                        id=workflow_id,
                        task_queue="test-queue",
                    )

                    # Check for deadlock events in workflow history
                    await assert_no_workflow_task_failures(env.client, workflow_id)

                    # Verify workflow result
                    assert result.state == "success"
                    assert result.dag_id == "test_parallel"
                    assert result.tasks_succeeded == 3
                    assert result.tasks_failed == 0

                    # Verify all tasks were synced
                    with create_session() as session:
                        task_instances = session.query(TaskInstance).filter(
                            TaskInstance.dag_id == "test_parallel"
                        ).all()
                        assert len(task_instances) == 3
                        for ti in task_instances:
                            assert ti.state == TaskInstanceState.SUCCESS
        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_mixed_results_dag_execution(self):
        """Test execution of test_mixed_results DAG (1 success + 1 failure)."""
        dag_file = TEST_DAGS_FOLDER / "test_mixed_results.py"
        dag = _load_dag_from_file(dag_file, "test_mixed_results")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with await WorkflowEnvironment.start_time_skipping() as env:
                input_data = DeepDagExecutionInput(
                    dag_id="test_mixed_results",
                    logical_date=datetime(2025, 6, 3, tzinfo=timezone.utc),
                )

                async with Worker(
                    env.client,
                    task_queue="test-queue",
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=SYNC_ACTIVITIES + TASK_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    workflow_id = "test-deep-mixed"
                    # Workflow should fail with ApplicationError
                    with pytest.raises(WorkflowFailureError) as exc_info:
                        await env.client.execute_workflow(
                            ExecuteAirflowDagDeepWorkflow.run,
                            input_data,
                            id=workflow_id,
                            task_queue="test-queue",
                        )

                    # Check for deadlock events in workflow history
                    # (workflow failed due to task failure, not deadlock)
                    await assert_no_workflow_task_failures(env.client, workflow_id)

                    # Verify it's a DagExecutionFailure
                    assert isinstance(exc_info.value.cause, ApplicationError)
                    assert exc_info.value.cause.type == "DagExecutionFailure"

                    # Verify failure details
                    failure_data = exc_info.value.cause.details[0]
                    failure_details = DagExecutionFailureDetails(**failure_data)
                    assert failure_details.dag_id == "test_mixed_results"
                    assert failure_details.tasks_failed == 1
                    assert failure_details.tasks_succeeded == 1

                    # Verify DagRun was synced as failed
                    with create_session() as session:
                        dag_run = session.query(DagRun).filter(
                            DagRun.dag_id == "test_mixed_results"
                        ).first()
                        assert dag_run is not None
                        assert dag_run.state == DagRunState.FAILED
        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_existing_run_id(self):
        """Test workflow with pre-existing run_id (orchestrator-created DagRun)."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            # First create the DagRun manually (simulating orchestrator)
            logical_date = datetime(2025, 6, 5, tzinfo=timezone.utc)
            run_id = f"external__{logical_date.isoformat()}"

            with create_session() as session:
                from airflow.timetables.base import DataInterval

                serialized = SerializedDagModel.get("test_loop_structure", session=session)
                dag_run = DagRun(
                    dag_id="test_loop_structure",
                    run_id=run_id,
                    logical_date=logical_date,
                    run_type=DagRunType.EXTERNAL,
                    state=DagRunState.RUNNING,
                    data_interval=DataInterval(start=logical_date, end=logical_date),
                )
                session.add(dag_run)
                session.flush()

                # Create TaskInstances
                dag_run.dag = serialized.dag
                dag_run.verify_integrity(session=session, dag_version_id=serialized.dag_version_id)
                session.commit()

            async with await WorkflowEnvironment.start_time_skipping() as env:
                # Pass existing run_id to workflow
                input_data = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=logical_date,
                    run_id=run_id,  # Pre-existing run
                )

                async with Worker(
                    env.client,
                    task_queue="test-queue",
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=SYNC_ACTIVITIES + TASK_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    workflow_id = "test-deep-existing-run"
                    result = await env.client.execute_workflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        input_data,
                        id=workflow_id,
                        task_queue="test-queue",
                    )

                    # Check for deadlock events in workflow history
                    await assert_no_workflow_task_failures(env.client, workflow_id)

                    assert result.state == "success"
                    assert result.run_id == run_id

                    # Verify existing DagRun was updated (not duplicated)
                    with create_session() as session:
                        dag_runs = session.query(DagRun).filter(
                            DagRun.dag_id == "test_loop_structure"
                        ).all()
                        assert len(dag_runs) == 1  # Should reuse existing
                        assert dag_runs[0].state == DagRunState.SUCCESS
        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_idempotent_dagrun_creation(self):
        """Test that creating DagRun with same logical_date is idempotent."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with await WorkflowEnvironment.start_time_skipping() as env:
                logical_date = datetime(2025, 6, 6, tzinfo=timezone.utc)
                input_data = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=logical_date,
                )

                async with Worker(
                    env.client,
                    task_queue="test-queue",
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=SYNC_ACTIVITIES + TASK_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    # Run workflow first time
                    workflow_id_1 = "test-deep-idempotent-1"
                    result1 = await env.client.execute_workflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        input_data,
                        id=workflow_id_1,
                        task_queue="test-queue",
                    )

                    # Check for deadlock events in workflow history
                    await assert_no_workflow_task_failures(env.client, workflow_id_1)

                    assert result1.state == "success"

                    # Run workflow second time with same logical_date
                    workflow_id_2 = "test-deep-idempotent-2"
                    result2 = await env.client.execute_workflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        input_data,
                        id=workflow_id_2,
                        task_queue="test-queue",
                    )

                    # Check for deadlock events in workflow history
                    await assert_no_workflow_task_failures(env.client, workflow_id_2)

                    assert result2.state == "success"
                    # Should get same run_id due to idempotent creation
                    assert result2.run_id == result1.run_id
        finally:
            cleanup()
