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
"""Integration tests for Temporal orchestrator with real Temporal server.

These tests require the Temporal integration to be enabled:
    breeze --integration temporal

Run with:
    pytest --integration temporal tests/integration/executors/test_temporal_orchestrator.py
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.mark.integration("temporal")
class TestTemporalIntegration:
    """Integration tests for Temporal server connectivity."""

    def test_temporal_server_is_running(self):
        """Verify Temporal server is accessible."""
        from temporalio.client import Client

        async def check_connection():
            # Default address when running in Breeze with temporal integration
            temporal_address = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
            client = await Client.connect(temporal_address)
            # Simple check - if we can connect, the server is running
            assert client is not None
            return True

        result = asyncio.run(check_connection())
        assert result is True

    def test_temporal_client_config(self):
        """Test that temporal_airflow client config works."""
        from temporal_airflow.client_config import create_temporal_client, get_task_queue

        async def check_client():
            client = await create_temporal_client()
            assert client is not None
            return True

        result = asyncio.run(check_client())
        assert result is True

        # Task queue should be configured
        task_queue = get_task_queue()
        assert task_queue is not None
        assert isinstance(task_queue, str)


@pytest.mark.integration("temporal")
@pytest.mark.backend("postgres")
class TestTemporalOrchestratorIntegration:
    """Integration tests for TemporalOrchestrator with real Temporal server."""

    @pytest.fixture
    def orchestrator(self):
        """Create a TemporalOrchestrator instance."""
        from temporal_airflow.orchestrator import TemporalOrchestrator

        return TemporalOrchestrator()

    @pytest.fixture
    def mock_dag_run(self):
        """Create a mock DagRun for testing."""
        dag_run = MagicMock(spec=DagRun)
        dag_run.dag_id = "test_dag_temporal_integration"
        dag_run.run_id = f"test_run_{int(time.time())}"
        dag_run.logical_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        dag_run.conf = {"test_key": "test_value"}
        dag_run.run_type = DagRunType.MANUAL
        return dag_run

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        return MagicMock()

    def test_orchestrator_starts_workflow(
        self, orchestrator, mock_dag_run, mock_session
    ):
        """Test that orchestrator successfully starts a Temporal workflow."""
        # Start the workflow - this should not raise
        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Verify run_type was changed to EXTERNAL
        assert mock_dag_run.run_type == DagRunType.EXTERNAL

        # Verify session.merge was called
        mock_session.merge.assert_called_once_with(mock_dag_run)

    def test_orchestrator_workflow_appears_in_temporal(
        self, orchestrator, mock_dag_run, mock_session
    ):
        """Test that started workflow appears in Temporal server."""
        from temporalio.client import Client

        # Start the workflow
        orchestrator.start_dagrun(mock_dag_run, mock_session)

        workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"

        async def check_workflow():
            temporal_address = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
            client = await Client.connect(temporal_address)
            handle = client.get_workflow_handle(workflow_id)
            # This will raise if workflow doesn't exist
            description = await handle.describe()
            return description.status.name

        status = asyncio.run(check_workflow())
        # Workflow should be running (or may have failed quickly if no worker)
        assert status in ("RUNNING", "FAILED", "TERMINATED")

    def test_orchestrator_cancel_workflow(
        self, orchestrator, mock_dag_run, mock_session
    ):
        """Test that orchestrator can cancel a running workflow."""
        # First start the workflow
        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Reset mock for cancel check
        mock_session.reset_mock()

        # Now cancel it
        orchestrator.cancel_dagrun(mock_dag_run, mock_session)

        # Check that workflow was cancelled in Temporal
        from temporalio.client import Client

        workflow_id = f"airflow-{mock_dag_run.dag_id}-{mock_dag_run.run_id}"

        async def check_cancelled():
            temporal_address = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
            client = await Client.connect(temporal_address)
            handle = client.get_workflow_handle(workflow_id)
            description = await handle.describe()
            return description.status.name

        status = asyncio.run(check_cancelled())
        # Workflow should be cancelled or terminated
        assert status in ("CANCELED", "TERMINATED", "FAILED")


@pytest.mark.integration("temporal")
@pytest.mark.backend("postgres")
class TestTemporalEndToEndExecution:
    """End-to-end tests for DAG execution through Temporal.

    These tests verify the complete flow:
    Orchestrator -> Temporal Workflow -> Sync Activities -> Airflow DB
    """

    @pytest.fixture
    def temporal_worker_running(self):
        """
        Check if a Temporal worker is running.

        Note: For full e2e tests, a worker must be running that handles
        the deep workflow and sync activities. This fixture checks if
        one is available.
        """
        # In CI, we would start a worker as part of the test setup
        # For now, we just check if the environment suggests a worker is running
        worker_expected = os.environ.get("TEMPORAL_WORKER_RUNNING", "false")
        return worker_expected.lower() == "true"

    def test_workflow_input_serialization(self):
        """Test that DeepDagExecutionInput serializes correctly for Temporal."""
        from temporal_airflow.models import DeepDagExecutionInput

        input_data = DeepDagExecutionInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            run_id="test_run_123",
            conf={"key": "value", "nested": {"a": 1}},
        )

        # Serialize and deserialize
        json_data = input_data.model_dump_json()
        restored = DeepDagExecutionInput.model_validate_json(json_data)

        assert restored.dag_id == input_data.dag_id
        assert restored.run_id == input_data.run_id
        assert restored.conf == input_data.conf

    @pytest.mark.skipif(
        os.environ.get("TEMPORAL_WORKER_RUNNING", "false").lower() != "true",
        reason="Temporal worker not running - set TEMPORAL_WORKER_RUNNING=true",
    )
    def test_full_dag_execution_via_temporal(self, session):
        """
        Test complete DAG execution through Temporal with status sync.

        This test requires:
        1. Temporal server running (via --integration temporal)
        2. Temporal worker running with deep workflow registered
        3. Test DAG available in Airflow
        """
        from airflow.models.dag import DagModel
        from temporal_airflow.orchestrator import TemporalOrchestrator

        # Create a simple test DagRun
        dag_id = "test_dag_e2e_temporal"
        run_id = f"e2e_test_{int(time.time())}"
        logical_date = datetime(2025, 1, 1, tzinfo=timezone.utc)

        # Ensure DAG exists in DB (would be created by scheduler in real scenario)
        dag_model = DagModel(dag_id=dag_id, is_active=True, is_paused=False)
        session.merge(dag_model)

        # Create DagRun
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
            state=DagRunState.QUEUED,
        )
        session.add(dag_run)
        session.commit()

        # Use orchestrator to start execution
        orchestrator = TemporalOrchestrator()
        orchestrator.start_dagrun(dag_run, session)

        # Verify run_type changed to EXTERNAL
        session.refresh(dag_run)
        assert dag_run.run_type == DagRunType.EXTERNAL

        # Wait for workflow to complete (with timeout)
        max_wait = 60  # seconds
        poll_interval = 2

        from temporalio.client import Client

        workflow_id = f"airflow-{dag_id}-{run_id}"

        async def wait_for_completion():
            temporal_address = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
            client = await Client.connect(temporal_address)
            handle = client.get_workflow_handle(workflow_id)

            waited = 0
            while waited < max_wait:
                desc = await handle.describe()
                if desc.status.name in ("COMPLETED", "FAILED", "CANCELED", "TERMINATED"):
                    return desc.status.name
                await asyncio.sleep(poll_interval)
                waited += poll_interval

            return "TIMEOUT"

        final_status = asyncio.run(wait_for_completion())

        # Verify workflow completed
        assert final_status == "COMPLETED", f"Workflow ended with status: {final_status}"

        # Verify DagRun state was synced back to Airflow DB
        session.refresh(dag_run)
        assert dag_run.state == DagRunState.SUCCESS
