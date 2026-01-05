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
"""End-to-end tests using real Temporal service and Airflow API.

These tests require:
1. A running Temporal server (default: localhost:7233)
2. Airflow database configured and accessible

The tests validate the complete production flow:
- Start Temporal worker in background
- Submit workflow via orchestrator
- Monitor execution via Airflow REST API
- Verify final state via API

Run with:
    # Start Temporal server first (in separate terminal):
    temporal server start-dev

    # Run E2E tests:
    pytest scripts/temporal_airflow/tests/test_e2e_real_temporal.py -v -s

    # Or via breeze:
    breeze shell -c "pytest /opt/airflow/scripts/temporal_airflow/tests/test_e2e_real_temporal.py -v -s"

Environment variables:
    TEMPORAL_ADDRESS: Temporal server address (default: localhost:7233)
    TEMPORAL_NAMESPACE: Temporal namespace (default: default)
    SKIP_TEMPORAL_E2E: Set to "true" to skip these tests
"""
from __future__ import annotations

import asyncio
import datetime
import importlib.util
import os
import time
from contextlib import asynccontextmanager
from datetime import timezone as tz
from pathlib import Path
from typing import AsyncGenerator

from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporal_airflow.activities import run_airflow_task
from temporal_airflow.client_config import create_temporal_client, get_task_queue
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.models import DeepDagExecutionInput
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
)


# Test DAGs folder
TEST_DAGS_FOLDER = Path(__file__).parent.parent

# All activities for the deep workflow
ALL_ACTIVITIES = [
    create_dagrun_record,
    sync_task_status,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
    run_airflow_task,
]


def is_temporal_available() -> bool:
    """Check if Temporal server is available.

    Tries multiple addresses in order:
    1. TEMPORAL_ADDRESS environment variable
    2. host.docker.internal:7233 (for Docker containers accessing host)
    3. localhost:7233 (for direct host access)
    """
    if os.environ.get("SKIP_TEMPORAL_E2E", "").lower() == "true":
        return False

    # List of addresses to try
    addresses_to_try = []

    # First, try env var if set
    env_addr = os.environ.get("TEMPORAL_ADDRESS")
    if env_addr:
        addresses_to_try.append(env_addr)

    # Then try Docker host access and localhost
    addresses_to_try.extend([
        "host.docker.internal:7233",  # Docker Desktop for Mac/Windows
        "172.17.0.1:7233",  # Docker bridge network gateway (Linux)
        "localhost:7233",
    ])

    import socket
    for addr in addresses_to_try:
        try:
            if ":" in addr:
                hostname, port = addr.rsplit(":", 1)
                port = int(port)
            else:
                hostname = addr
                port = 7233

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((hostname, port))
            sock.close()

            if result == 0:
                # Set TEMPORAL_ADDRESS so the client uses this address
                os.environ["TEMPORAL_ADDRESS"] = addr
                return True
        except Exception:
            continue

    return False


# Skip all tests in this module if Temporal is not available
pytestmark = [
    pytest.mark.skipif(
        not is_temporal_available(),
        reason="Temporal server not available. Start with: temporal server start-dev"
    ),
    pytest.mark.e2e,
]


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


@asynccontextmanager
async def run_worker_in_background(
    client: Client,
    task_queue: str,
) -> AsyncGenerator[Worker, None]:
    """Run a Temporal worker in background for the duration of the context."""
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ExecuteAirflowDagDeepWorkflow],
        activities=ALL_ACTIVITIES,
        activity_executor=ThreadPoolExecutor(max_workers=5),
    )

    # Start worker in background task
    worker_task = asyncio.create_task(worker.run())

    try:
        # Give worker time to start
        await asyncio.sleep(0.5)
        yield worker
    finally:
        # Shutdown worker
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


@pytest.fixture
def api_test_client():
    """Create an authenticated Airflow API test client."""
    from airflow.api_fastapi.app import create_app
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
    from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
    from airflow.configuration import conf
    import time_machine
    from fastapi.testclient import TestClient

    # Set auth manager config
    old_value = conf.get("core", "auth_manager", fallback=None)
    conf.set(
        "core",
        "auth_manager",
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
    )

    try:
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
    finally:
        # Restore old value
        if old_value is not None:
            conf.set("core", "auth_manager", old_value)


class TestE2ERealTemporal:
    """End-to-end tests using real Temporal service."""

    @pytest.mark.asyncio
    async def test_workflow_execution_with_real_temporal(self, api_test_client):
        """Test complete workflow execution with real Temporal server."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            # Connect to real Temporal server
            client = await create_temporal_client()
            task_queue = get_task_queue()

            print(f"\nConnected to Temporal at {client.service_client.config.target_host}")
            print(f"Task queue: {task_queue}")

            async with run_worker_in_background(client, task_queue):
                # Create workflow input
                logical_date = datetime.datetime(2025, 9, 1, tzinfo=tz.utc)
                workflow_input = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=logical_date,
                    conf={},
                )

                workflow_id = f"e2e-test-{int(time.time())}"

                print(f"Starting workflow: {workflow_id}")

                # Start workflow
                handle = await client.start_workflow(
                    ExecuteAirflowDagDeepWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                print(f"Workflow started, waiting for completion...")

                # Wait for completion
                result = await handle.result()

                print(f"Workflow completed: {result}")

                assert result.state == "success"
                assert result.dag_id == "test_loop_structure"
                assert result.tasks_succeeded == 1

                run_id = result.run_id

                # Verify via Airflow REST API
                response = api_test_client.get(
                    f"/dags/test_loop_structure/dagRuns/{run_id}"
                )

                assert response.status_code == 200, f"API error: {response.text}"
                body = response.json()

                print(f"API response: {body}")

                assert body["state"] == "success"
                assert body["run_type"] == "external"
                assert body["dag_id"] == "test_loop_structure"

                # Verify task instances via API
                ti_response = api_test_client.get(
                    f"/dags/test_loop_structure/dagRuns/{run_id}/taskInstances"
                )

                assert ti_response.status_code == 200
                ti_body = ti_response.json()

                assert ti_body["total_entries"] == 1
                assert ti_body["task_instances"][0]["state"] == "success"

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_parallel_execution_with_real_temporal(self, api_test_client):
        """Test parallel task execution with real Temporal server."""
        dag_file = TEST_DAGS_FOLDER / "test_parallel.py"
        dag = _load_dag_from_file(dag_file, "test_parallel")
        cleanup = _serialize_dag_to_db(dag)

        try:
            client = await create_temporal_client()
            task_queue = get_task_queue()

            async with run_worker_in_background(client, task_queue):
                logical_date = datetime.datetime(2025, 9, 2, tzinfo=tz.utc)
                workflow_input = DeepDagExecutionInput(
                    dag_id="test_parallel",
                    logical_date=logical_date,
                    conf={},
                )

                workflow_id = f"e2e-parallel-{int(time.time())}"

                print(f"\nStarting parallel workflow: {workflow_id}")

                handle = await client.start_workflow(
                    ExecuteAirflowDagDeepWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                result = await handle.result()

                print(f"Workflow completed: {result}")

                assert result.state == "success"
                assert result.tasks_succeeded == 3

                run_id = result.run_id

                # Verify all tasks via API
                ti_response = api_test_client.get(
                    f"/dags/test_parallel/dagRuns/{run_id}/taskInstances"
                )

                assert ti_response.status_code == 200
                ti_body = ti_response.json()

                assert ti_body["total_entries"] == 3

                task_states = {ti["task_id"]: ti["state"] for ti in ti_body["task_instances"]}
                assert all(state == "success" for state in task_states.values())

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_workflow_failure_with_real_temporal(self, api_test_client):
        """Test workflow failure handling with real Temporal server."""
        dag_file = TEST_DAGS_FOLDER / "test_mixed_results.py"
        dag = _load_dag_from_file(dag_file, "test_mixed_results")
        cleanup = _serialize_dag_to_db(dag)

        try:
            client = await create_temporal_client()
            task_queue = get_task_queue()

            async with run_worker_in_background(client, task_queue):
                logical_date = datetime.datetime(2025, 9, 3, tzinfo=tz.utc)
                workflow_input = DeepDagExecutionInput(
                    dag_id="test_mixed_results",
                    logical_date=logical_date,
                    conf={},
                )

                workflow_id = f"e2e-failure-{int(time.time())}"

                print(f"\nStarting failing workflow: {workflow_id}")

                handle = await client.start_workflow(
                    ExecuteAirflowDagDeepWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                # Workflow will fail due to task failure
                from temporalio.client import WorkflowFailureError
                try:
                    await handle.result()
                    pytest.fail("Expected workflow to fail")
                except WorkflowFailureError as e:
                    print(f"Workflow failed as expected: {e}")

                # Get run_id from DB
                with create_session() as session:
                    dag_run = session.query(DagRun).filter(
                        DagRun.dag_id == "test_mixed_results",
                        DagRun.logical_date == logical_date,
                    ).first()
                    run_id = dag_run.run_id

                # Verify failure via API
                response = api_test_client.get(
                    f"/dags/test_mixed_results/dagRuns/{run_id}"
                )

                assert response.status_code == 200
                body = response.json()

                assert body["state"] == "failed"

                # Verify mixed task states
                ti_response = api_test_client.get(
                    f"/dags/test_mixed_results/dagRuns/{run_id}/taskInstances"
                )

                assert ti_response.status_code == 200
                ti_body = ti_response.json()

                task_states = {ti["task_id"]: ti["state"] for ti in ti_body["task_instances"]}
                assert task_states["success_task"] == "success"
                assert task_states["failing_task"] == "failed"

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_workflow_monitoring_during_execution(self, api_test_client):
        """Test that we can monitor workflow execution via API during execution."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            client = await create_temporal_client()
            task_queue = get_task_queue()

            async with run_worker_in_background(client, task_queue):
                logical_date = datetime.datetime(2025, 9, 4, tzinfo=tz.utc)
                workflow_input = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=logical_date,
                    conf={},
                )

                workflow_id = f"e2e-monitor-{int(time.time())}"

                print(f"\nStarting workflow for monitoring: {workflow_id}")

                # Start workflow (don't wait for result yet)
                handle = await client.start_workflow(
                    ExecuteAirflowDagDeepWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                # Query Temporal workflow status
                desc = await handle.describe()
                print(f"Workflow status: {desc.status}")

                # Wait for completion
                result = await handle.result()

                # Verify final status
                desc_final = await handle.describe()
                print(f"Final workflow status: {desc_final.status}")

                run_id = result.run_id

                # Verify via Airflow API
                response = api_test_client.get(
                    f"/dags/test_loop_structure/dagRuns/{run_id}"
                )

                assert response.status_code == 200
                body = response.json()

                assert body["state"] == "success"
                assert body["start_date"] is not None
                assert body["end_date"] is not None

        finally:
            cleanup()

    @pytest.mark.asyncio
    async def test_temporal_cli_workflow_query(self):
        """Test that workflows can be queried via Temporal CLI."""
        dag_file = TEST_DAGS_FOLDER / "test_loop_structure.py"
        dag = _load_dag_from_file(dag_file, "test_loop_structure")
        cleanup = _serialize_dag_to_db(dag)

        try:
            client = await create_temporal_client()
            task_queue = get_task_queue()

            async with run_worker_in_background(client, task_queue):
                logical_date = datetime.datetime(2025, 9, 5, tzinfo=tz.utc)
                workflow_input = DeepDagExecutionInput(
                    dag_id="test_loop_structure",
                    logical_date=logical_date,
                    conf={},
                )

                workflow_id = f"e2e-cli-{int(time.time())}"

                handle = await client.start_workflow(
                    ExecuteAirflowDagDeepWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                result = await handle.result()

                assert result.state == "success"

                # Verify workflow can be queried via client (similar to CLI)
                desc = await handle.describe()

                print(f"\nWorkflow query result:")
                print(f"  Workflow ID: {desc.id}")
                print(f"  Run ID: {desc.run_id}")
                print(f"  Status: {desc.status}")
                print(f"  Start time: {desc.start_time}")
                print(f"  Close time: {desc.close_time}")

                # Verify workflow is listed in client queries
                workflows = []
                async for wf in client.list_workflows(
                    query=f'WorkflowId = "{workflow_id}"'
                ):
                    workflows.append(wf)

                assert len(workflows) == 1
                assert workflows[0].id == workflow_id

        finally:
            cleanup()
