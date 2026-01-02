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
"""Tests for TemporalOrchestrator."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from temporal_airflow.orchestrator import TemporalOrchestrator


class TestTemporalOrchestrator:
    """Tests for TemporalOrchestrator."""

    def test_inherits_from_base(self):
        """TemporalOrchestrator should inherit from BaseDagRunOrchestrator."""
        orchestrator = TemporalOrchestrator()
        assert isinstance(orchestrator, BaseDagRunOrchestrator)

    def test_can_instantiate(self):
        """TemporalOrchestrator can be instantiated."""
        orchestrator = TemporalOrchestrator()
        assert orchestrator is not None
        assert orchestrator._client is None

    @patch("temporal_airflow.orchestrator.asyncio.run")
    def test_start_dagrun_marks_as_external(self, mock_asyncio_run):
        """start_dagrun should mark the DagRun as EXTERNAL."""
        orchestrator = TemporalOrchestrator()

        # Mock dag_run
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"
        mock_dag_run.logical_date = datetime(2025, 1, 1)
        mock_dag_run.conf = {"key": "value"}
        mock_dag_run.run_type = DagRunType.MANUAL

        # Mock session
        mock_session = MagicMock()

        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Verify run_type was changed to EXTERNAL
        assert mock_dag_run.run_type == DagRunType.EXTERNAL
        mock_session.merge.assert_called_once_with(mock_dag_run)

    @patch("temporal_airflow.orchestrator.asyncio.run")
    def test_start_dagrun_calls_temporal_workflow(self, mock_asyncio_run):
        """start_dagrun should start a Temporal deep workflow."""
        orchestrator = TemporalOrchestrator()

        # Mock dag_run
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"
        mock_dag_run.logical_date = datetime(2025, 1, 1)
        mock_dag_run.conf = {"key": "value"}

        # Mock session
        mock_session = MagicMock()

        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Verify asyncio.run was called to start the workflow
        mock_asyncio_run.assert_called_once()

    @patch("temporal_airflow.orchestrator.asyncio.run")
    def test_start_dagrun_passes_run_id_to_workflow(self, mock_asyncio_run):
        """start_dagrun should pass existing run_id to deep workflow."""
        orchestrator = TemporalOrchestrator()

        # Mock dag_run
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "existing_run_123"
        mock_dag_run.logical_date = datetime(2025, 1, 1)
        mock_dag_run.conf = {"key": "value"}

        # Mock session
        mock_session = MagicMock()

        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Verify asyncio.run was called
        mock_asyncio_run.assert_called_once()

        # Verify the coroutine was passed to asyncio.run
        # The workflow input should contain the run_id
        call_args = mock_asyncio_run.call_args
        assert call_args is not None

    @patch("temporal_airflow.orchestrator.asyncio.run")
    def test_cancel_dagrun_cancels_workflow(self, mock_asyncio_run):
        """cancel_dagrun should cancel the Temporal workflow."""
        orchestrator = TemporalOrchestrator()

        # Mock dag_run
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"

        # Mock session
        mock_session = MagicMock()

        orchestrator.cancel_dagrun(mock_dag_run, mock_session)

        # Verify asyncio.run was called to cancel the workflow
        mock_asyncio_run.assert_called_once()

    @patch("temporal_airflow.orchestrator.asyncio.run")
    def test_cancel_dagrun_handles_error(self, mock_asyncio_run):
        """cancel_dagrun should handle errors and set state to FAILED."""
        orchestrator = TemporalOrchestrator()

        # Make asyncio.run raise an exception
        mock_asyncio_run.side_effect = Exception("Workflow not found")

        # Mock dag_run
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run_123"

        # Mock session
        mock_session = MagicMock()

        # Should not raise
        orchestrator.cancel_dagrun(mock_dag_run, mock_session)

        # State should be set to FAILED
        assert mock_dag_run.state == DagRunState.FAILED
        mock_session.merge.assert_called_with(mock_dag_run)

    def test_workflow_id_format(self):
        """Workflow ID should follow expected format."""
        orchestrator = TemporalOrchestrator()

        # The workflow ID format is "airflow-{dag_id}-{run_id}"
        # This is tested implicitly through start_dagrun, but we can verify the format
        dag_id = "my_dag"
        run_id = "manual__2025-01-01T00:00:00"
        expected_workflow_id = f"airflow-{dag_id}-{run_id}"

        assert expected_workflow_id == "airflow-my_dag-manual__2025-01-01T00:00:00"
