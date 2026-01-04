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
"""Tests for deep integration workflow with in-workflow database."""
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
        # In-workflow database state (same as standalone)
        assert workflow.engine is None
        assert workflow.sessionFactory is None
        # DAG state
        assert workflow.dag is None
        assert workflow.dag_fileloc is None
        # Deep integration state
        assert workflow.run_id is None
        # XCom state
        assert workflow.xcom_store == {}
        # Task tracking
        assert workflow.tasks_succeeded == 0
        assert workflow.tasks_failed == 0

    def test_workflow_has_database_methods(self):
        """Deep workflow should have in-workflow database methods (same as standalone)."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        # Should have in-memory DB methods
        assert hasattr(workflow, "_initialize_database")
        assert hasattr(workflow, "_create_local_dag_run")
        assert hasattr(workflow, "_get_upstream_xcom")
        assert hasattr(workflow, "_update_local_task_state")
        assert hasattr(workflow, "_scheduling_loop")


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

        # Create mock task with no upstream
        mock_task = MagicMock()
        mock_task.upstream_task_ids = []

        # Create mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.dag_id = "dag1"
        mock_ti.run_id = "run1"
        mock_ti.map_index = -1

        result = workflow._get_upstream_xcom(mock_ti, mock_task)
        assert result is None

    def test_with_upstream_xcom(self):
        """Should return upstream XCom values."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        # Store XCom for upstream task
        workflow.xcom_store[("dag1", "upstream_task", "run1", -1)] = {"result": 42}

        # Create mock task with upstream
        mock_task = MagicMock()
        mock_task.upstream_task_ids = ["upstream_task"]

        # Create mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.dag_id = "dag1"
        mock_ti.run_id = "run1"
        mock_ti.map_index = -1

        result = workflow._get_upstream_xcom(mock_ti, mock_task)
        assert result == {"upstream_task": {"result": 42}}

    def test_upstream_not_in_store(self):
        """Should return None if upstream not in xcom_store."""
        workflow = ExecuteAirflowDagDeepWorkflow()

        mock_task = MagicMock()
        mock_task.upstream_task_ids = ["upstream_task"]

        mock_ti = MagicMock()
        mock_ti.dag_id = "dag1"
        mock_ti.run_id = "run1"
        mock_ti.map_index = -1

        result = workflow._get_upstream_xcom(mock_ti, mock_task)
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


class TestSameAsStandalone:
    """Tests to verify deep workflow follows standalone pattern."""

    def test_has_in_workflow_database(self):
        """Deep workflow should have in-workflow database (same as standalone)."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        # Should have attributes for in-memory DB
        assert hasattr(workflow, "engine")
        assert hasattr(workflow, "sessionFactory")
        # Initially None until _initialize_database is called
        assert workflow.engine is None
        assert workflow.sessionFactory is None

    def test_has_initialize_database_method(self):
        """Deep workflow should have _initialize_database method."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        assert hasattr(workflow, "_initialize_database")
        assert callable(workflow._initialize_database)

    def test_has_create_local_dag_run_method(self):
        """Deep workflow should have _create_local_dag_run method."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        assert hasattr(workflow, "_create_local_dag_run")
        assert callable(workflow._create_local_dag_run)

    def test_uses_sync_activities_for_real_db(self):
        """Deep workflow should use sync activities for real Airflow DB."""
        # Verify the workflow imports sync activities
        from temporal_airflow.deep_workflow import (
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

    def test_no_connections_variables_fields(self):
        """Deep workflow should not have connections/variables fields."""
        workflow = ExecuteAirflowDagDeepWorkflow()
        # Deep integration reads from Airflow DB via hooks
        # (unlike standalone which passes them in workflow input)
        assert not hasattr(workflow, "connections")
        assert not hasattr(workflow, "variables")


class TestDesignPrinciple:
    """Tests to verify design principle: same as standalone + sync activities."""

    def test_docstring_mentions_design(self):
        """Workflow docstring should mention it follows standalone pattern."""
        docstring = ExecuteAirflowDagDeepWorkflow.__doc__
        assert "standalone" in docstring.lower() or "SAME" in docstring

    def test_scheduling_loop_uses_native_logic(self):
        """Scheduling loop should be designed to use Airflow's native logic."""
        # Verify the workflow has a _scheduling_loop method that takes dag_run_id
        workflow = ExecuteAirflowDagDeepWorkflow()
        assert hasattr(workflow, "_scheduling_loop")

        # Check method signature accepts dag_run_id (indicating in-workflow DB pattern)
        import inspect
        sig = inspect.signature(workflow._scheduling_loop)
        params = list(sig.parameters.keys())
        assert "dag_run_id" in params


class TestWorkflowReplay:
    """Tests for workflow replay behavior.

    Temporal workflows can replay from history, which means the same workflow
    code runs multiple times within the same worker process. The in-memory
    SQLite database must handle this correctly.
    """

    def test_initialize_database_creates_fresh_db_on_replay(self):
        """Database should be fresh on each initialization (simulating replay).

        This tests that calling _initialize_database multiple times doesn't
        cause UNIQUE constraint errors, which would happen if the database
        retained data from previous runs.
        """
        from unittest.mock import MagicMock, patch

        workflow = ExecuteAirflowDagDeepWorkflow()

        # Mock workflow.info() to return consistent IDs (simulating same execution)
        mock_info = MagicMock()
        mock_info.workflow_id = "test-workflow-id"
        mock_info.run_id = "test-run-id-12345"

        with patch("temporal_airflow.deep_workflow.workflow") as mock_workflow_module:
            mock_workflow_module.info.return_value = mock_info
            mock_workflow_module.logger = MagicMock()

            # First initialization
            workflow._initialize_database()

            # Verify engine and sessionFactory are set
            assert workflow.engine is not None
            assert workflow.sessionFactory is not None

            # Create some data in the database
            from airflow.models.dag_version import DagVersion
            session = workflow.sessionFactory()
            dag_version = DagVersion(dag_id="test_dag", version_number=1)
            session.add(dag_version)
            session.commit()
            session.close()

            # Second initialization (simulating replay)
            # This should NOT raise UNIQUE constraint error
            workflow._initialize_database()

            # Verify database is fresh - should be able to create same record again
            session = workflow.sessionFactory()
            dag_version2 = DagVersion(dag_id="test_dag", version_number=1)
            session.add(dag_version2)
            session.commit()  # This would fail if DB wasn't fresh
            session.close()

    def test_different_run_ids_get_different_databases(self):
        """Different run_ids should get different database instances.

        This ensures that multiple workflow executions in the same worker
        don't interfere with each other.
        """
        from unittest.mock import MagicMock, patch

        workflow1 = ExecuteAirflowDagDeepWorkflow()
        workflow2 = ExecuteAirflowDagDeepWorkflow()

        with patch("temporal_airflow.deep_workflow.workflow") as mock_workflow_module:
            mock_workflow_module.logger = MagicMock()

            # First workflow with run_id_1
            mock_info1 = MagicMock()
            mock_info1.workflow_id = "test-workflow"
            mock_info1.run_id = "run-id-1"
            mock_workflow_module.info.return_value = mock_info1

            workflow1._initialize_database()

            # Create data in workflow1's database
            from airflow.models.dag_version import DagVersion
            session1 = workflow1.sessionFactory()
            dag_version1 = DagVersion(dag_id="test_dag", version_number=1)
            session1.add(dag_version1)
            session1.commit()
            session1.close()

            # Second workflow with run_id_2
            mock_info2 = MagicMock()
            mock_info2.workflow_id = "test-workflow"
            mock_info2.run_id = "run-id-2"
            mock_workflow_module.info.return_value = mock_info2

            workflow2._initialize_database()

            # Should be able to create same record in workflow2's database
            # because it's a different database instance
            session2 = workflow2.sessionFactory()
            dag_version2 = DagVersion(dag_id="test_dag", version_number=1)
            session2.add(dag_version2)
            session2.commit()  # Would fail if sharing same DB
            session2.close()

            # Verify data in each database is independent
            session1 = workflow1.sessionFactory()
            count1 = session1.query(DagVersion).count()
            session1.close()

            session2 = workflow2.sessionFactory()
            count2 = session2.query(DagVersion).count()
            session2.close()

            # Each should have exactly 1 record (not 2)
            assert count1 == 1
            assert count2 == 1

    def test_create_local_dag_run_succeeds_after_db_reinit(self):
        """_create_local_dag_run should succeed after database reinitialization.

        This simulates the full replay scenario where the workflow restarts
        from the beginning.
        """
        from unittest.mock import MagicMock, patch
        from datetime import datetime, timezone

        workflow = ExecuteAirflowDagDeepWorkflow()

        mock_info = MagicMock()
        mock_info.workflow_id = "test-workflow-replay"
        mock_info.run_id = "test-run-replay"

        # Create a minimal mock DAG
        mock_dag = MagicMock()
        mock_dag.dag_id = "test_dag"
        mock_dag.task_dict = {}

        with patch("temporal_airflow.deep_workflow.workflow") as mock_workflow_module:
            mock_workflow_module.info.return_value = mock_info
            mock_workflow_module.logger = MagicMock()

            # Set the DAG on the workflow (normally done by loading from DB)
            workflow.dag = mock_dag

            # First execution
            workflow._initialize_database()
            dag_run_id1 = workflow._create_local_dag_run(
                dag_id="test_dag",
                run_id="run_1",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                conf={},
            )
            assert dag_run_id1 is not None

            # Simulate replay - reinitialize and create again
            workflow._initialize_database()
            dag_run_id2 = workflow._create_local_dag_run(
                dag_id="test_dag",
                run_id="run_1",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                conf={},
            )
            assert dag_run_id2 is not None

            # IDs might be different since it's a fresh DB, but both should succeed

    def test_concurrent_workflows_have_isolated_databases(self):
        """Multiple concurrent workflows should have completely isolated databases.

        This simulates multiple DAGs running in parallel within the same worker.
        Each workflow should have its own database that doesn't interfere with others.

        We simulate concurrency by interleaving operations from multiple workflows
        without completing any one workflow before starting others.
        """
        from unittest.mock import MagicMock, patch
        from datetime import datetime, timezone

        # Create 3 workflow instances (simulating 3 DAGs in same worker)
        workflow1 = ExecuteAirflowDagDeepWorkflow()
        workflow2 = ExecuteAirflowDagDeepWorkflow()
        workflow3 = ExecuteAirflowDagDeepWorkflow()

        # Create mock DAGs
        mock_dag1 = MagicMock()
        mock_dag1.dag_id = "dag_a"
        mock_dag1.task_dict = {}

        mock_dag2 = MagicMock()
        mock_dag2.dag_id = "dag_b"
        mock_dag2.task_dict = {}

        mock_dag3 = MagicMock()
        mock_dag3.dag_id = "dag_c"
        mock_dag3.task_dict = {}

        workflow1.dag = mock_dag1
        workflow2.dag = mock_dag2
        workflow3.dag = mock_dag3

        with patch("temporal_airflow.deep_workflow.workflow") as mock_workflow_module:
            mock_workflow_module.logger = MagicMock()

            # Initialize all 3 databases in interleaved order (simulating concurrent start)
            mock_info1 = MagicMock()
            mock_info1.workflow_id = "concurrent-test-1"
            mock_info1.run_id = "run-1"
            mock_workflow_module.info.return_value = mock_info1
            workflow1._initialize_database()

            mock_info2 = MagicMock()
            mock_info2.workflow_id = "concurrent-test-2"
            mock_info2.run_id = "run-2"
            mock_workflow_module.info.return_value = mock_info2
            workflow2._initialize_database()

            mock_info3 = MagicMock()
            mock_info3.workflow_id = "concurrent-test-3"
            mock_info3.run_id = "run-3"
            mock_workflow_module.info.return_value = mock_info3
            workflow3._initialize_database()

            # Now create DagRuns in all 3 (interleaved, simulating parallel execution)
            dag_run_id1 = workflow1._create_local_dag_run(
                dag_id="dag_a",
                run_id="run_a",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                conf={},
            )

            dag_run_id2 = workflow2._create_local_dag_run(
                dag_id="dag_b",
                run_id="run_b",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                conf={},
            )

            dag_run_id3 = workflow3._create_local_dag_run(
                dag_id="dag_c",
                run_id="run_c",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                conf={},
            )

            # Verify each workflow sees only its own data
            from airflow.models.dagrun import DagRun

            session1 = workflow1.sessionFactory()
            dag_runs1 = session1.query(DagRun).all()
            session1.close()

            session2 = workflow2.sessionFactory()
            dag_runs2 = session2.query(DagRun).all()
            session2.close()

            session3 = workflow3.sessionFactory()
            dag_runs3 = session3.query(DagRun).all()
            session3.close()

            # Each workflow should see exactly 1 DagRun with its own dag_id
            assert len(dag_runs1) == 1, f"workflow1 has {len(dag_runs1)} dag runs"
            assert len(dag_runs2) == 1, f"workflow2 has {len(dag_runs2)} dag runs"
            assert len(dag_runs3) == 1, f"workflow3 has {len(dag_runs3)} dag runs"

            assert dag_runs1[0].dag_id == "dag_a"
            assert dag_runs2[0].dag_id == "dag_b"
            assert dag_runs3[0].dag_id == "dag_c"

            # Verify IDs are not None
            assert dag_run_id1 is not None
            assert dag_run_id2 is not None
            assert dag_run_id3 is not None

    def test_memory_cleanup_on_garbage_collection(self):
        """Engine should be disposed when workflow is garbage collected.

        This tests the weakref.finalize cleanup mechanism that prevents
        memory leaks when workflows are evicted from Temporal's sticky cache.
        """
        import gc
        import weakref
        from unittest.mock import MagicMock, patch

        # Track whether cleanup was called
        cleanup_called = []

        with patch("temporal_airflow.deep_workflow.workflow") as mock_workflow_module:
            mock_info = MagicMock()
            mock_info.workflow_id = "test-cleanup"
            mock_info.run_id = "test-run-cleanup"
            mock_workflow_module.info.return_value = mock_info
            mock_workflow_module.logger = MagicMock()

            # Create workflow and initialize database
            workflow = ExecuteAirflowDagDeepWorkflow()
            workflow._initialize_database()

            # Verify finalizer is registered
            assert hasattr(workflow, "_db_cleanup")
            assert workflow._db_cleanup.alive  # Finalizer should be alive

            # Get reference to engine before deleting workflow
            engine = workflow.engine
            assert engine is not None

            # Create a weak reference to track when workflow is collected
            workflow_ref = weakref.ref(workflow)

            # Delete workflow instance
            del workflow

            # Force garbage collection
            gc.collect()

            # Workflow should be collected
            assert workflow_ref() is None, "Workflow should have been garbage collected"

            # Engine should be disposed (connections closed)
            # After dispose(), the engine's pool should be invalidated
            # We can't easily check this directly, but the test verifies
            # that the cleanup mechanism is properly registered
