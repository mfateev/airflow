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
"""End-to-end tests for TemporalOrchestrator using Temporal test environment.

These tests validate the complete flow:
  Orchestrator.start_dagrun() -> Temporal Workflow -> Activities -> Airflow DB

Using Temporal's WorkflowEnvironment, we can test the orchestrator without
requiring a real Temporal server.
"""
from __future__ import annotations

import importlib.util
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporalio.api.enums.v1 import EventType

from temporal_airflow.activities import run_airflow_task
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.orchestrator import TemporalOrchestrator
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
    """Load a DAG from a Python file."""
    from airflow.sdk.definitions.dag import DAG

    spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

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
        # Clean up any existing data first
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
        session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
        session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
        session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
        session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
        session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
        session.commit()

        # Create bundle
        existing_bundle = session.query(DagBundleModel).filter(
            DagBundleModel.name == bundle_name
        ).first()
        if not existing_bundle:
            bundle = DagBundleModel(name=bundle_name)
            session.add(bundle)
            session.commit()

        # Create DagModel entry
        SerializedDAG.bulk_write_to_db(
            bundle_name=bundle_name,
            bundle_version=None,
            dags=[dag],
            session=session,
        )
        session.commit()

        # Write serialized DAG
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


# All activities for the deep workflow
ALL_ACTIVITIES = [
    create_dagrun_record,
    sync_task_status,
    sync_task_status_batch,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
    run_airflow_task,
]


class OrchestratorWithTestClient(TemporalOrchestrator):
    """TemporalOrchestrator that uses an injected test client."""

    def __init__(self, test_client: Client, task_queue: str):
        super().__init__()
        self._test_client = test_client
        self._test_task_queue = task_queue

    async def _get_client(self) -> Client:
        """Return the injected test client."""
        return self._test_client

    async def _start_workflow_async(self, workflow_id, input):
        """Start workflow using test client and queue."""
        await self._test_client.start_workflow(
            ExecuteAirflowDagDeepWorkflow.run,
            input,
            id=workflow_id,
            task_queue=self._test_task_queue,
        )


@asynccontextmanager
async def create_test_orchestrator():
    """
    Create a TemporalOrchestrator with a test environment.

    Yields:
        tuple: (orchestrator, env) - orchestrator is ready to use with test client
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = "test-orchestrator-queue"

        # Create orchestrator with test client
        orchestrator = OrchestratorWithTestClient(env.client, task_queue)

        # Start worker
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ExecuteAirflowDagDeepWorkflow],
            activities=ALL_ACTIVITIES,
            activity_executor=ThreadPoolExecutor(max_workers=5),
        ):
            yield orchestrator, env


class TestOrchestratorE2E:
    """End-to-end tests for TemporalOrchestrator using test environment."""

    @pytest.mark.asyncio
    async def test_orchestrator_starts_and_completes_workflow(self):
        """Test that orchestrator starts a workflow that completes successfully."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                # Create a mock DagRun (simulating what the scheduler creates)
                logical_date = datetime(2025, 7, 1, tzinfo=timezone.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_loop_structure"
                mock_dag_run.run_id = "test-run-1"  # Just for workflow_id
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Start workflow - don't include run_id so workflow creates DagRun
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                # Wait for workflow to complete
                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                # Check for deadlock events in workflow history
                await assert_no_workflow_task_failures(env.client, workflow_id)

                # Verify workflow completed successfully (result is a dict)
                assert result["state"] == "success"
                assert result["dag_id"] == "test_loop_structure"
                assert result["tasks_succeeded"] == 1

                # Verify DagRun was synced to Airflow DB
                with create_session() as session:
                    dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == "test_loop_structure",
                        DagRun.logical_date == logical_date,
                    ).first()
                    assert dag_run is not None
                    assert dag_run.state == DagRunState.SUCCESS

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_orchestrator_with_existing_dagrun(self):
        """Test orchestrator when DagRun already exists in DB (normal flow)."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            # First, create a DagRun in the database (like scheduler would)
            logical_date = datetime(2025, 7, 2, tzinfo=timezone.utc)
            run_id = f"scheduled__{logical_date.isoformat()}"

            with create_session() as session:
                from airflow.timetables.base import DataInterval

                serialized = SerializedDagModel.get("test_loop_structure", session=session)

                # Create DagRun
                db_dag_run = DagRun(
                    dag_id="test_loop_structure",
                    run_id=run_id,
                    logical_date=logical_date,
                    run_type=DagRunType.SCHEDULED,
                    state=DagRunState.QUEUED,
                    data_interval=DataInterval(start=logical_date, end=logical_date),
                )
                session.add(db_dag_run)
                session.flush()

                # Create TaskInstances
                db_dag_run.dag = serialized.dag
                db_dag_run.verify_integrity(session=session, dag_version_id=serialized.dag_version_id)
                session.commit()

                dag_run_id = db_dag_run.id

            async with create_test_orchestrator() as (orchestrator, env):
                # Create mock DagRun with same values (simulating what scheduler passes)
                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_loop_structure"
                mock_dag_run.run_id = run_id
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Start workflow via orchestrator's async method
                # Pass include_run_id=True since DagRun exists in DB
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run, include_run_id=True),
                )

                # Wait for workflow completion
                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                # Check for deadlock events in workflow history
                await assert_no_workflow_task_failures(env.client, workflow_id)

                assert result["state"] == "success"

                # Verify existing DagRun was updated (not a new one created)
                with create_session() as session:
                    dag_runs = session.query(DagRun).filter(
                        DagRun.dag_id == "test_loop_structure",
                        DagRun.logical_date == logical_date,
                    ).all()

                    # Should only have one DagRun (the one we created)
                    assert len(dag_runs) == 1
                    assert dag_runs[0].id == dag_run_id
                    assert dag_runs[0].state == DagRunState.SUCCESS

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_orchestrator_marks_dagrun_external(self):
        """Test that orchestrator marks DagRun as EXTERNAL type."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime(2025, 7, 3, tzinfo=timezone.utc)

                # Create a mock DagRun
                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_loop_structure"
                mock_dag_run.run_id = "test-run-3"  # Just for workflow_id
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Start workflow - workflow will create DagRun with EXTERNAL type
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                # Wait for completion
                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                # Check for deadlock events in workflow history
                await assert_no_workflow_task_failures(env.client, workflow_id)

                # Verify DagRun in DB is EXTERNAL (created by workflow)
                with create_session() as session:
                    dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == "test_loop_structure",
                        DagRun.logical_date == logical_date,
                    ).first()
                    assert dag_run is not None
                    assert dag_run.run_type == DagRunType.EXTERNAL

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_orchestrator_parallel_dag_execution(self):
        """Test orchestrator with a DAG that has parallel tasks."""
        dag_file = TEST_DAGS_FOLDER / "test_parallel.py"
        dag = _load_dag_from_file(dag_file, "test_parallel")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime(2025, 7, 4, tzinfo=timezone.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_parallel"
                mock_dag_run.run_id = "test-run-4"  # Just for workflow_id
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                # Check for deadlock events in workflow history
                await assert_no_workflow_task_failures(env.client, workflow_id)

                assert result["state"] == "success"
                assert result["tasks_succeeded"] == 3  # 2 parallel + 1 join

                # Verify all task instances synced
                with create_session() as session:
                    task_instances = session.query(TaskInstance).filter(
                        TaskInstance.dag_id == "test_parallel"
                    ).all()
                    assert len(task_instances) == 3
                    for ti in task_instances:
                        assert ti.state == TaskInstanceState.SUCCESS

        finally:
            cleanup()


# Add helper method to OrchestratorWithTestClient
def _build_workflow_input(self, dag_run, include_run_id: bool = False):
    """Build DeepDagExecutionInput from DagRun.

    Args:
        dag_run: The DagRun (or mock) to build input from
        include_run_id: If True, include run_id (for existing DagRuns).
            If False, omit run_id so workflow creates a new DagRun.
    """
    from temporal_airflow.models import DeepDagExecutionInput

    return DeepDagExecutionInput(
        dag_id=dag_run.dag_id,
        logical_date=dag_run.logical_date,
        run_id=dag_run.run_id if include_run_id else None,
        conf=dag_run.conf or {},
    )


# Monkey-patch the helper method
OrchestratorWithTestClient._build_workflow_input = _build_workflow_input
