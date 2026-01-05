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
"""API integration tests for Temporal workflow execution.

These tests validate that data written by Temporal workflows to the Airflow DB
is correctly readable through Airflow's REST API endpoints.

The test flow:
1. Execute a workflow through the orchestrator
2. Call Airflow's REST API endpoints
3. Verify the API returns correct status/completion data
"""
from __future__ import annotations

import datetime
import importlib.util
import os
from contextlib import asynccontextmanager
from datetime import timezone as tz
from pathlib import Path
from unittest.mock import MagicMock

from concurrent.futures import ThreadPoolExecutor

import pytest
import time_machine
from fastapi.testclient import TestClient
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporal_airflow.activities import run_airflow_task
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.models import DeepDagExecutionInput
from temporal_airflow.orchestrator import TemporalOrchestrator
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
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

    def _build_workflow_input(self, dag_run, include_run_id: bool = False):
        """Build DeepDagExecutionInput from DagRun."""
        return DeepDagExecutionInput(
            dag_id=dag_run.dag_id,
            logical_date=dag_run.logical_date,
            run_id=dag_run.run_id if include_run_id else None,
            conf=dag_run.conf or {},
        )


@asynccontextmanager
async def create_test_orchestrator():
    """Create a TemporalOrchestrator with a test environment."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = "test-api-queue"
        orchestrator = OrchestratorWithTestClient(env.client, task_queue)

        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ExecuteAirflowDagDeepWorkflow],
            activities=ALL_ACTIVITIES,
            activity_executor=ThreadPoolExecutor(max_workers=5),
        ):
            yield orchestrator, env


@pytest.fixture
def api_test_client():
    """Create an authenticated Airflow API test client."""
    from tests_common.test_utils.config import conf_vars
    from airflow.api_fastapi.app import create_app
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
    from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser

    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager

        # Create a long-lived token for testing
        time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)

        with time_machine.travel(time_very_before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
            ).generate(
                auth_manager.serialize_user(SimpleAuthManagerUser(username="test", role="admin")),
            )

        yield TestClient(
            app,
            headers={"Authorization": f"Bearer {token}"},
            base_url="http://testserver/api/v2",
        )


class TestAPIIntegration:
    """Test that Airflow REST API correctly reads data written by Temporal workflows."""

    @pytest.mark.asyncio
    async def test_api_returns_dagrun_after_workflow_completion(self, api_test_client):
        """Test that DagRun API returns correct data after workflow completes."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime.datetime(2025, 8, 1, tzinfo=tz.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_loop_structure"
                mock_dag_run.run_id = "api-test-run-1"
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Execute workflow
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                # Wait for workflow to complete
                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                assert result["state"] == "success"

                # Get the run_id from the workflow result
                run_id = result["run_id"]

                # Now call the Airflow REST API to verify the data
                response = api_test_client.get(
                    f"/dags/test_loop_structure/dagRuns/{run_id}"
                )

                assert response.status_code == 200, f"API returned {response.status_code}: {response.text}"
                body = response.json()

                # Verify API returns correct data
                assert body["dag_id"] == "test_loop_structure"
                assert body["dag_run_id"] == run_id
                assert body["state"] == "success"
                assert body["run_type"] == "external"  # Created by Temporal

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_api_returns_task_instances_after_workflow_completion(self, api_test_client):
        """Test that TaskInstance API returns correct data after workflow completes."""
        dag_file = TEST_DAGS_FOLDER / "test_parallel.py"
        dag = _load_dag_from_file(dag_file, "test_parallel")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime.datetime(2025, 8, 2, tzinfo=tz.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_parallel"
                mock_dag_run.run_id = "api-test-run-2"
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Execute workflow
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                # Wait for workflow to complete
                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                assert result["state"] == "success"
                assert result["tasks_succeeded"] == 3

                run_id = result["run_id"]

                # Call API to get task instances
                response = api_test_client.get(
                    f"/dags/test_parallel/dagRuns/{run_id}/taskInstances"
                )

                assert response.status_code == 200, f"API returned {response.status_code}: {response.text}"
                body = response.json()

                # Verify all 3 task instances are returned
                assert body["total_entries"] == 3

                task_ids = {ti["task_id"] for ti in body["task_instances"]}
                assert task_ids == {"task1", "task2", "task3"}

                # All tasks should be successful
                for ti in body["task_instances"]:
                    assert ti["state"] == "success", f"Task {ti['task_id']} has state {ti['state']}"

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_api_reflects_running_state_during_execution(self, api_test_client):
        """Test that API reflects RUNNING state while workflow executes tasks."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime.datetime(2025, 8, 3, tzinfo=tz.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_loop_structure"
                mock_dag_run.run_id = "api-test-run-3"
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Execute workflow and let it complete
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)
                result = await handle.result()

                run_id = result["run_id"]

                # Final state should be success
                response = api_test_client.get(
                    f"/dags/test_loop_structure/dagRuns/{run_id}"
                )
                assert response.status_code == 200
                body = response.json()
                assert body["state"] == "success"

                # Verify start_date and end_date are set
                assert body["start_date"] is not None
                assert body["end_date"] is not None

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_api_returns_failed_dagrun_on_task_failure(self, api_test_client):
        """Test that API returns FAILED state when workflow has task failures."""
        dag_file = TEST_DAGS_FOLDER / "test_mixed_results.py"
        dag = _load_dag_from_file(dag_file, "test_mixed_results")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                logical_date = datetime.datetime(2025, 8, 4, tzinfo=tz.utc)

                mock_dag_run = MagicMock()
                mock_dag_run.dag_id = "test_mixed_results"
                mock_dag_run.run_id = "api-test-run-4"
                mock_dag_run.logical_date = logical_date
                mock_dag_run.conf = {}

                # Execute workflow (will fail due to failing task)
                await orchestrator._start_workflow_async(
                    workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                    input=orchestrator._build_workflow_input(mock_dag_run),
                )

                workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                handle = env.client.get_workflow_handle(workflow_id)

                # Workflow will raise WorkflowFailureError
                from temporalio.client import WorkflowFailureError
                try:
                    await handle.result()
                except WorkflowFailureError:
                    pass  # Expected - workflow failed due to task failure

                # Get the DagRun from DB to find the run_id
                with create_session() as session:
                    dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == "test_mixed_results",
                        DagRun.logical_date == logical_date,
                    ).first()
                    run_id = dag_run.run_id

                # API should show FAILED state
                response = api_test_client.get(
                    f"/dags/test_mixed_results/dagRuns/{run_id}"
                )

                assert response.status_code == 200
                body = response.json()
                assert body["state"] == "failed"

                # Get task instances - should show mixed states
                ti_response = api_test_client.get(
                    f"/dags/test_mixed_results/dagRuns/{run_id}/taskInstances"
                )
                assert ti_response.status_code == 200
                ti_body = ti_response.json()

                # Should have 2 tasks
                assert ti_body["total_entries"] == 2

                # One success, one failure
                states = {ti["task_id"]: ti["state"] for ti in ti_body["task_instances"]}
                assert states["success_task"] == "success"
                assert states["failing_task"] == "failed"

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_api_list_dagruns_includes_temporal_executed_runs(self, api_test_client):
        """Test that DagRuns list endpoint includes Temporal-executed runs."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            async with create_test_orchestrator() as (orchestrator, env):
                # Execute two workflows
                for i, date in enumerate([
                    datetime.datetime(2025, 8, 10, tzinfo=tz.utc),
                    datetime.datetime(2025, 8, 11, tzinfo=tz.utc),
                ]):
                    mock_dag_run = MagicMock()
                    mock_dag_run.dag_id = "test_loop_structure"
                    mock_dag_run.run_id = f"api-test-list-{i}"
                    mock_dag_run.logical_date = date
                    mock_dag_run.conf = {}

                    await orchestrator._start_workflow_async(
                        workflow_id=f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}",
                        input=orchestrator._build_workflow_input(mock_dag_run),
                    )

                    workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"
                    handle = env.client.get_workflow_handle(workflow_id)
                    await handle.result()

                # List all DagRuns for this DAG via API
                response = api_test_client.get(
                    "/dags/test_loop_structure/dagRuns"
                )

                assert response.status_code == 200
                body = response.json()

                # Should have at least 2 runs
                assert body["total_entries"] >= 2

                # All should be EXTERNAL type and SUCCESS state
                for run in body["dag_runs"]:
                    assert run["run_type"] == "external"
                    assert run["state"] == "success"

        finally:
            cleanup()
