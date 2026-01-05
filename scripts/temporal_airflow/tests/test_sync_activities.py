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
"""Tests for sync_activities module using real Airflow models."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from temporalio.exceptions import ApplicationError

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporal_airflow.models import (
    CreateDagRunInput,
    DagRunStatusSync,
    EnsureTaskInstancesInput,
    LoadSerializedDagInput,
    TaskStatusSync,
)
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    ensure_task_instances,
    load_serialized_dag,
    sync_dagrun_status,
    sync_task_status,
)


@pytest.fixture
def test_dag():
    """Create a test DAG for testing."""
    with DAG(
        dag_id="test_sync_activities_dag",
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        schedule=None,
        catchup=False,
    ) as dag:
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")
        task1 >> task2
    # Set fileloc to this test file (DagCode.write_code needs a real file)
    dag.fileloc = __file__
    return dag


@pytest.fixture
def serialized_dag(test_dag):
    """Serialize and save the test DAG to the database."""
    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagcode import DagCode
    from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG

    bundle_name = "test-bundle"
    dag_id = test_dag.dag_id

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
            dags=[test_dag],
            session=session,
        )
        session.commit()

        # Convert to LazyDeserializedDAG and write the serialized DAG
        lazy_dag = LazyDeserializedDAG.from_dag(test_dag)
        SerializedDagModel.write_dag(
            lazy_dag,
            bundle_name=bundle_name,
            session=session,
        )
        session.commit()

        # Fetch it back to get the ID
        serialized = SerializedDagModel.get(dag_id, session=session)
        yield serialized

        # Cleanup - delete in reverse order of foreign key dependencies
        session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
        session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
        session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
        session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
        session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
        session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
        session.commit()


@pytest.fixture
def cleanup_dagruns():
    """Cleanup any DagRuns created during tests."""
    yield
    with create_session() as session:
        # Delete TaskInstances first (foreign key constraint)
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == "test_sync_activities_dag"
        ).delete()
        # Delete DagRuns
        session.query(DagRun).filter(
            DagRun.dag_id == "test_sync_activities_dag"
        ).delete()
        session.commit()


class TestCreateDagRunRecordReal:
    """Tests for create_dagrun_record activity with real models."""

    def test_creates_new_dagrun(self, serialized_dag, cleanup_dagruns):
        """create_dagrun_record should create a new DagRun with EXTERNAL type."""
        input_data = CreateDagRunInput(
            dag_id="test_sync_activities_dag",
            logical_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
            conf={"key": "value"},
        )

        result = create_dagrun_record(input_data)

        # Verify the DagRun was created
        assert result.run_id is not None
        assert "external__" in result.run_id
        assert result.dag_run_id is not None

        # Verify in database
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.id == result.dag_run_id)
                .first()
            )
            assert dag_run is not None
            assert dag_run.dag_id == "test_sync_activities_dag"
            assert dag_run.run_type == DagRunType.EXTERNAL
            assert dag_run.state == DagRunState.RUNNING

            # Verify TaskInstances were created
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.run_id == dag_run.run_id)
                .all()
            )
            task_ids = {ti.task_id for ti in task_instances}
            assert "task1" in task_ids
            assert "task2" in task_ids

    def test_returns_existing_dagrun(self, serialized_dag, cleanup_dagruns):
        """create_dagrun_record should return existing DagRun if found."""
        logical_date = datetime(2025, 6, 2, tzinfo=timezone.utc)

        # Create first time
        input_data = CreateDagRunInput(
            dag_id="test_sync_activities_dag",
            logical_date=logical_date,
        )
        first_result = create_dagrun_record(input_data)

        # Call again with same logical_date
        second_result = create_dagrun_record(input_data)

        # Should return the same DagRun
        assert second_result.dag_run_id == first_result.dag_run_id
        assert second_result.run_id == first_result.run_id

    def test_raises_error_for_missing_serialized_dag(self):
        """create_dagrun_record should raise ApplicationError if DAG not found."""
        input_data = CreateDagRunInput(
            dag_id="nonexistent_dag_12345",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

        with pytest.raises(ApplicationError) as exc_info:
            create_dagrun_record(input_data)

        assert "not found" in str(exc_info.value)


class TestSyncTaskStatusReal:
    """Tests for sync_task_status activity with real models."""

    def test_updates_task_state(self, serialized_dag, cleanup_dagruns):
        """sync_task_status should update TaskInstance state."""
        # First create a DagRun to get TaskInstances
        create_input = CreateDagRunInput(
            dag_id="test_sync_activities_dag",
            logical_date=datetime(2025, 6, 3, tzinfo=timezone.utc),
        )
        dag_run_result = create_dagrun_record(create_input)

        # Sync task status
        sync_input = TaskStatusSync(
            dag_id="test_sync_activities_dag",
            task_id="task1",
            run_id=dag_run_result.run_id,
            map_index=-1,
            state="success",
            start_date=datetime(2025, 6, 3, 12, 0, 0, tzinfo=timezone.utc),
            end_date=datetime(2025, 6, 3, 12, 1, 0, tzinfo=timezone.utc),
        )

        sync_task_status(sync_input)

        # Verify in database
        with create_session() as session:
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == "test_sync_activities_dag",
                    TaskInstance.task_id == "task1",
                    TaskInstance.run_id == dag_run_result.run_id,
                )
                .first()
            )
            assert ti is not None
            assert ti.state == TaskInstanceState.SUCCESS
            assert ti.start_date == sync_input.start_date
            assert ti.end_date == sync_input.end_date

    def test_handles_missing_task_instance(self, serialized_dag, cleanup_dagruns):
        """sync_task_status should handle missing TaskInstance gracefully."""
        input_data = TaskStatusSync(
            dag_id="test_sync_activities_dag",
            task_id="nonexistent_task",
            run_id="nonexistent_run",
            map_index=-1,
            state="success",
        )

        # Should not raise, just log warning
        sync_task_status(input_data)


class TestSyncDagRunStatusReal:
    """Tests for sync_dagrun_status activity with real models."""

    def test_updates_dagrun_state(self, serialized_dag, cleanup_dagruns):
        """sync_dagrun_status should update DagRun state."""
        # First create a DagRun
        create_input = CreateDagRunInput(
            dag_id="test_sync_activities_dag",
            logical_date=datetime(2025, 6, 4, tzinfo=timezone.utc),
        )
        dag_run_result = create_dagrun_record(create_input)

        # Sync DagRun status to success
        sync_input = DagRunStatusSync(
            dag_id="test_sync_activities_dag",
            run_id=dag_run_result.run_id,
            state="success",
            end_date=datetime(2025, 6, 4, 12, 5, 0, tzinfo=timezone.utc),
        )

        sync_dagrun_status(sync_input)

        # Verify in database
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.run_id == dag_run_result.run_id)
                .first()
            )
            assert dag_run is not None
            assert dag_run.state == DagRunState.SUCCESS
            assert dag_run.end_date == sync_input.end_date

    def test_handles_missing_dagrun(self):
        """sync_dagrun_status should handle missing DagRun gracefully."""
        input_data = DagRunStatusSync(
            dag_id="test_sync_activities_dag",
            run_id="nonexistent_run",
            state="success",
        )

        # Should not raise, just log warning
        sync_dagrun_status(input_data)


class TestLoadSerializedDagReal:
    """Tests for load_serialized_dag activity with real models."""

    def test_loads_serialized_dag(self, serialized_dag):
        """load_serialized_dag should return serialized DAG data and fileloc."""
        input_data = LoadSerializedDagInput(dag_id="test_sync_activities_dag")

        result = load_serialized_dag(input_data)

        # Verify the data structure
        assert result.dag_data is not None
        assert "dag" in result.dag_data
        dag_dict = result.dag_data.get("dag", {})
        assert dag_dict.get("dag_id") == "test_sync_activities_dag"

        # Verify fileloc is returned
        assert result.fileloc is not None
        assert isinstance(result.fileloc, str)

    def test_raises_error_for_missing_dag(self):
        """load_serialized_dag should raise ApplicationError if DAG not found."""
        input_data = LoadSerializedDagInput(dag_id="nonexistent_dag_67890")

        with pytest.raises(ApplicationError) as exc_info:
            load_serialized_dag(input_data)

        assert "not found" in str(exc_info.value)


class TestEnsureTaskInstancesReal:
    """Tests for ensure_task_instances activity with real models."""

    def test_ensures_task_instances_exist(self, serialized_dag, cleanup_dagruns):
        """ensure_task_instances should create TaskInstances if missing."""
        # First create a DagRun (which already creates TaskInstances)
        create_input = CreateDagRunInput(
            dag_id="test_sync_activities_dag",
            logical_date=datetime(2025, 6, 5, tzinfo=timezone.utc),
        )
        dag_run_result = create_dagrun_record(create_input)

        # Call ensure_task_instances (should be idempotent)
        input_data = EnsureTaskInstancesInput(
            dag_id="test_sync_activities_dag",
            run_id=dag_run_result.run_id,
        )

        ensure_task_instances(input_data)

        # Verify TaskInstances still exist
        with create_session() as session:
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.run_id == dag_run_result.run_id)
                .all()
            )
            assert len(task_instances) == 2

    def test_raises_error_for_missing_dagrun(self):
        """ensure_task_instances should raise ApplicationError if DagRun not found."""
        input_data = EnsureTaskInstancesInput(
            dag_id="test_sync_activities_dag",
            run_id="nonexistent_run",
        )

        with pytest.raises(ApplicationError) as exc_info:
            ensure_task_instances(input_data)

        assert "not found" in str(exc_info.value)


class TestModels:
    """Tests for the sync activity models."""

    def test_create_dagrun_input_fields(self):
        """CreateDagRunInput should have required fields."""
        input_data = CreateDagRunInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            conf={"key": "value"},
        )
        assert input_data.dag_id == "test_dag"
        assert input_data.logical_date == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert input_data.conf == {"key": "value"}

    def test_task_status_sync_fields(self):
        """TaskStatusSync should have required fields."""
        input_data = TaskStatusSync(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            state="success",
            start_date=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
            xcom_value={"result": 42},
        )
        assert input_data.dag_id == "test_dag"
        assert input_data.task_id == "test_task"
        assert input_data.state == "success"
        assert input_data.xcom_value == {"result": 42}

    def test_dagrun_status_sync_fields(self):
        """DagRunStatusSync should have required fields."""
        input_data = DagRunStatusSync(
            dag_id="test_dag",
            run_id="test_run",
            state="success",
            end_date=datetime(2025, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
        )
        assert input_data.dag_id == "test_dag"
        assert input_data.run_id == "test_run"
        assert input_data.state == "success"
