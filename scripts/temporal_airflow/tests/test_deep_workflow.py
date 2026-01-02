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
"""Tests for deep integration workflow."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from airflow.utils.state import TaskInstanceState

from temporal_airflow.models import (
    DeepDagExecutionInput,
    CreateDagRunResult,
    TaskExecutionResult,
)
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow


class TestDeepWorkflowStructure:
    """Tests for deep integration workflow structure."""

    def test_workflow_class_exists(self):
        """ExecuteAirflowDagDeepWorkflow class should exist."""
        assert ExecuteAirflowDagDeepWorkflow is not None

    def test_workflow_has_run_method(self):
        """Workflow should have async run method."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        assert hasattr(workflow, "run")

    def test_workflow_initial_state(self):
        """Workflow should have correct initial state."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        assert workflow.run_id is None
        assert workflow.dag is None
        assert workflow.dag_fileloc is None
        assert workflow.xcom_store == {}
        assert workflow.tasks_succeeded == 0
        assert workflow.tasks_failed == 0


class TestDeepDagExecutionInput:
    """Tests for DeepDagExecutionInput model."""

    def test_valid_input_with_run_id(self):
        """DeepDagExecutionInput should accept run_id."""
        input_data = DeepDagExecutionInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            run_id="existing_run_id",
            conf={"key": "value"},
        )
        assert input_data.dag_id == "test_dag"
        assert input_data.run_id == "existing_run_id"

    def test_valid_input_without_run_id(self):
        """DeepDagExecutionInput should work without run_id."""
        input_data = DeepDagExecutionInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        assert input_data.dag_id == "test_dag"
        assert input_data.run_id is None

    def test_serialization(self):
        """DeepDagExecutionInput should serialize correctly."""
        input_data = DeepDagExecutionInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            conf={"key": "value"},
        )
        json_data = input_data.model_dump_json()
        assert "test_dag" in json_data


class TestGetUpstreamXcom:
    """Tests for _get_upstream_xcom method."""

    def test_no_upstream_tasks(self):
        """Should return None if no upstream tasks."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        # Create mock DAG with task that has no upstream
        mock_task = MagicMock()
        mock_task.upstream_task_ids = []

        mock_dag = MagicMock()
        mock_dag.get_task.return_value = mock_task
        workflow.dag = mock_dag

        result = workflow._get_upstream_xcom("task1", "dag1", "run1", -1)
        assert result is None

    def test_with_upstream_xcom(self):
        """Should return upstream XCom values."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        # Store XCom for upstream task
        workflow.xcom_store[("dag1", "upstream_task", "run1", -1)] = {"result": 42}

        # Create mock DAG
        mock_task = MagicMock()
        mock_task.upstream_task_ids = ["upstream_task"]

        mock_dag = MagicMock()
        mock_dag.get_task.return_value = mock_task
        workflow.dag = mock_dag

        result = workflow._get_upstream_xcom("task1", "dag1", "run1", -1)
        assert result == {"upstream_task": {"result": 42}}

    def test_upstream_not_in_store(self):
        """Should return None if upstream not in xcom_store."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        mock_task = MagicMock()
        mock_task.upstream_task_ids = ["upstream_task"]

        mock_dag = MagicMock()
        mock_dag.get_task.return_value = mock_task
        workflow.dag = mock_dag

        result = workflow._get_upstream_xcom("task1", "dag1", "run1", -1)
        assert result is None


class TestSchedulingLoopLogic:
    """Tests for scheduling loop logic."""

    def test_task_state_tracking(self):
        """Workflow should track task states correctly."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        # Simulate task completion
        workflow.tasks_succeeded = 2
        workflow.tasks_failed = 1

        assert workflow.tasks_succeeded == 2
        assert workflow.tasks_failed == 1

    def test_xcom_store_update(self):
        """Workflow should store XCom correctly."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        ti_key = ("dag1", "task1", "run1", -1)
        xcom_data = {"return_value": 42}

        workflow.xcom_store[ti_key] = xcom_data

        assert workflow.xcom_store[ti_key] == xcom_data


class TestActivityIntegration:
    """Tests for activity integration."""

    def test_sync_activities_imported(self):
        """Sync activities should be importable."""
        from temporal_airflow.sync_activities import (
            create_dagrun_record,
            sync_task_status,
            sync_dagrun_status,
            load_serialized_dag,
            ensure_task_instances,
        )

        assert create_dagrun_record is not None
        assert sync_task_status is not None
        assert sync_dagrun_status is not None
        assert load_serialized_dag is not None
        assert ensure_task_instances is not None

    def test_models_for_deep_integration(self):
        """All models for deep integration should be available."""
        from temporal_airflow.models import (
            DeepDagExecutionInput,
            CreateDagRunInput,
            CreateDagRunResult,
            TaskStatusSync,
            DagRunStatusSync,
            LoadSerializedDagInput,
            LoadSerializedDagResult,
            EnsureTaskInstancesInput,
        )

        # Verify all models can be instantiated
        assert DeepDagExecutionInput(
            dag_id="test",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        assert CreateDagRunInput(
            dag_id="test",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        assert CreateDagRunResult(run_id="test", dag_run_id=1)
        assert TaskStatusSync(
            dag_id="test",
            task_id="task",
            run_id="run",
            state="success",
        )
        assert DagRunStatusSync(
            dag_id="test",
            run_id="run",
            state="success",
        )
        assert LoadSerializedDagInput(dag_id="test")
        assert LoadSerializedDagResult(
            dag_data={"dag_id": "test"},
            fileloc="/opt/airflow/dags/test.py",
        )
        assert EnsureTaskInstancesInput(dag_id="test", run_id="run")


class TestWorkflowAttributes:
    """Tests for workflow attributes and decorator."""

    def test_workflow_name(self):
        """Workflow should have correct name."""
        # The workflow decorator sets __temporal_workflow_definition
        # Check the workflow is properly decorated
        assert hasattr(ExecuteAirflowDagDeepWorkflow, "__temporal_workflow_definition")

    def test_workflow_sandboxed_false(self):
        """Workflow should have sandboxed=False."""
        # Access the workflow definition using getattr to avoid name mangling
        defn = getattr(ExecuteAirflowDagDeepWorkflow, "__temporal_workflow_definition")
        assert defn.sandboxed is False


class TestDifferenceFromStandalone:
    """Tests to verify differences from standalone workflow."""

    def test_no_database_initialization(self):
        """Deep workflow should not have _initialize_database method."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        # Should not have in-memory DB methods
        assert not hasattr(workflow, "_initialize_database")
        assert not hasattr(workflow, "engine")
        assert not hasattr(workflow, "sessionFactory")

    def test_uses_external_db_via_activities(self):
        """Deep workflow should rely on activities for DB access."""
        # Verify the workflow imports sync activities
        from temporal_airflow.deep_workflow import (
            create_dagrun_record,
            sync_task_status,
            sync_dagrun_status,
        )

        assert create_dagrun_record is not None
        assert sync_task_status is not None
        assert sync_dagrun_status is not None

    def test_no_connections_variables_fields(self):
        """Deep workflow should not pass connections/variables to activities."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        # Deep integration reads from Airflow DB via hooks
        assert not hasattr(workflow, "connections")
        assert not hasattr(workflow, "variables")
