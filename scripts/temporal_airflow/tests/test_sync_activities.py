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
"""Tests for sync_activities module."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from temporalio.exceptions import ApplicationError

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


class TestCreateDagRunRecord:
    """Tests for create_dagrun_record activity."""

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_creates_new_dagrun(self, mock_create_session):
        """create_dagrun_record should create a new DagRun with EXTERNAL type."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock query to return None (no existing DagRun)
        mock_session.query.return_value.filter.return_value.first.return_value = None

        # Mock SerializedDagModel
        mock_serialized = MagicMock()
        mock_serialized.data = {"dag_id": "test_dag", "tasks": []}

        with patch(
            "temporal_airflow.sync_activities.SerializedDagModel.get",
            return_value=mock_serialized,
        ):
            # Mock the DagRun being created
            mock_dag_run = MagicMock()
            mock_dag_run.id = 123
            mock_dag_run.run_id = "external__2025-01-01T00:00:00"

            # Patch DagRun class
            with patch(
                "temporal_airflow.sync_activities.DagRun",
                return_value=mock_dag_run,
            ) as mock_dag_run_class:
                input_data = CreateDagRunInput(
                    dag_id="test_dag",
                    logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                    conf={"key": "value"},
                )

                result = await create_dagrun_record(input_data)

                # Verify DagRun was created with EXTERNAL type
                mock_dag_run_class.assert_called_once()
                call_kwargs = mock_dag_run_class.call_args.kwargs
                assert call_kwargs["dag_id"] == "test_dag"
                assert call_kwargs["run_type"] == DagRunType.EXTERNAL
                assert call_kwargs["state"] == DagRunState.RUNNING

                # Verify result
                assert result.dag_run_id == 123

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_returns_existing_dagrun(self, mock_create_session):
        """create_dagrun_record should return existing DagRun if found."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock existing DagRun
        mock_existing = MagicMock()
        mock_existing.id = 456
        mock_existing.run_id = "existing_run_id"
        mock_session.query.return_value.filter.return_value.first.return_value = mock_existing

        input_data = CreateDagRunInput(
            dag_id="test_dag",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

        result = await create_dagrun_record(input_data)

        assert result.run_id == "existing_run_id"
        assert result.dag_run_id == 456

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_raises_error_for_missing_serialized_dag(self, mock_create_session):
        """create_dagrun_record should raise ApplicationError if DAG not found."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock query to return None (no existing DagRun)
        mock_session.query.return_value.filter.return_value.first.return_value = None

        # Mock SerializedDagModel.get to return None
        with patch(
            "temporal_airflow.sync_activities.SerializedDagModel.get",
            return_value=None,
        ):
            input_data = CreateDagRunInput(
                dag_id="nonexistent_dag",
                logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )

            with pytest.raises(ApplicationError) as exc_info:
                await create_dagrun_record(input_data)

            assert "not found" in str(exc_info.value)


class TestSyncTaskStatus:
    """Tests for sync_task_status activity."""

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_updates_task_state(self, mock_create_session):
        """sync_task_status should update TaskInstance state."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock TaskInstance
        mock_ti = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_ti

        input_data = TaskStatusSync(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            state="success",
            start_date=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            end_date=datetime(2025, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
        )

        await sync_task_status(input_data)

        # Verify state was updated
        assert mock_ti.state == TaskInstanceState.SUCCESS
        assert mock_ti.start_date == input_data.start_date
        assert mock_ti.end_date == input_data.end_date
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_writes_xcom_value(self, mock_create_session):
        """sync_task_status should write XCom if provided."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock TaskInstance
        mock_ti = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_ti

        with patch("temporal_airflow.sync_activities.XComModel.set") as mock_xcom_set:
            input_data = TaskStatusSync(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=-1,
                state="success",
                xcom_value={"result": 42},
            )

            await sync_task_status(input_data)

            # Verify XComModel.set was called
            mock_xcom_set.assert_called_once_with(
                key="return_value",
                value={"result": 42},
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                map_index=-1,
                session=mock_session,
            )

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_handles_missing_task_instance(self, mock_create_session):
        """sync_task_status should handle missing TaskInstance gracefully."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock query to return None
        mock_session.query.return_value.filter.return_value.first.return_value = None

        input_data = TaskStatusSync(
            dag_id="test_dag",
            task_id="nonexistent_task",
            run_id="test_run",
            map_index=-1,
            state="success",
        )

        # Should not raise
        await sync_task_status(input_data)


class TestSyncDagRunStatus:
    """Tests for sync_dagrun_status activity."""

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_updates_dagrun_state(self, mock_create_session):
        """sync_dagrun_status should update DagRun state."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock DagRun
        mock_dag_run = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_dag_run

        input_data = DagRunStatusSync(
            dag_id="test_dag",
            run_id="test_run",
            state="success",
            end_date=datetime(2025, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
        )

        await sync_dagrun_status(input_data)

        # Verify state was updated
        assert mock_dag_run.state == DagRunState.SUCCESS
        assert mock_dag_run.end_date == input_data.end_date
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_handles_missing_dagrun(self, mock_create_session):
        """sync_dagrun_status should handle missing DagRun gracefully."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock query to return None
        mock_session.query.return_value.filter.return_value.first.return_value = None

        input_data = DagRunStatusSync(
            dag_id="test_dag",
            run_id="nonexistent_run",
            state="success",
        )

        # Should not raise
        await sync_dagrun_status(input_data)


class TestLoadSerializedDag:
    """Tests for load_serialized_dag activity."""

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_loads_serialized_dag(self, mock_create_session):
        """load_serialized_dag should return serialized DAG data and fileloc."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock SerializedDagModel
        mock_serialized = MagicMock()
        mock_serialized.data = {
            "dag_id": "test_dag",
            "tasks": [{"task_id": "task1"}, {"task_id": "task2"}],
        }
        mock_serialized.fileloc = "/opt/airflow/dags/subdirectory/my_dag_file.py"

        with patch(
            "temporal_airflow.sync_activities.SerializedDagModel.get",
            return_value=mock_serialized,
        ):
            input_data = LoadSerializedDagInput(dag_id="test_dag")

            result = await load_serialized_dag(input_data)

            # Verify LoadSerializedDagResult structure
            assert result.dag_data == mock_serialized.data
            assert result.dag_data["dag_id"] == "test_dag"
            assert len(result.dag_data["tasks"]) == 2
            assert result.fileloc == "/opt/airflow/dags/subdirectory/my_dag_file.py"

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_raises_error_for_missing_dag(self, mock_create_session):
        """load_serialized_dag should raise ApplicationError if DAG not found."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        with patch(
            "temporal_airflow.sync_activities.SerializedDagModel.get",
            return_value=None,
        ):
            input_data = LoadSerializedDagInput(dag_id="nonexistent_dag")

            with pytest.raises(ApplicationError) as exc_info:
                await load_serialized_dag(input_data)

            assert "not found" in str(exc_info.value)


class TestEnsureTaskInstances:
    """Tests for ensure_task_instances activity."""

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_calls_verify_integrity(self, mock_create_session):
        """ensure_task_instances should call verify_integrity on DagRun."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock DagRun
        mock_dag_run = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = mock_dag_run

        input_data = EnsureTaskInstancesInput(
            dag_id="test_dag",
            run_id="test_run",
        )

        await ensure_task_instances(input_data)

        # Verify verify_integrity was called
        mock_dag_run.verify_integrity.assert_called_once_with(session=mock_session)
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    @patch("temporal_airflow.sync_activities.create_session")
    async def test_raises_error_for_missing_dagrun(self, mock_create_session):
        """ensure_task_instances should raise ApplicationError if DagRun not found."""
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_create_session.return_value.__exit__ = MagicMock(return_value=None)

        # Mock query to return None
        mock_session.query.return_value.filter.return_value.first.return_value = None

        input_data = EnsureTaskInstancesInput(
            dag_id="test_dag",
            run_id="nonexistent_run",
        )

        with pytest.raises(ApplicationError) as exc_info:
            await ensure_task_instances(input_data)

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
