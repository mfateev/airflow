from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from airflow.utils.state import TaskInstanceState

from temporal_airflow.models import (
    DagExecutionInput,
    DagExecutionResult,
    ActivityTaskInput,  # New executor pattern model
    TaskExecutionResult,
)


class TestActivityTaskInput:
    """Test ActivityTaskInput model validation (Executor Pattern)."""

    def test_valid_input(self):
        """Test valid ActivityTaskInput creation."""
        input = ActivityTaskInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            dag_rel_path="dags/test_dag.py",
        )
        assert input.dag_id == "test_dag"
        assert input.task_id == "test_task"
        assert input.run_id == "test_run"
        assert input.dag_rel_path == "dags/test_dag.py"
        assert input.try_number == 1  # Default
        assert input.map_index == -1  # Default
        assert input.queue is None  # Default
        assert input.pool_slots == 1  # Default

    def test_missing_required_field(self):
        """Test validation fails when required field missing."""
        with pytest.raises(ValidationError):
            ActivityTaskInput(dag_id="test")

    def test_with_optional_fields(self):
        """Test ActivityTaskInput with optional fields."""
        input = ActivityTaskInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            dag_rel_path="dags/test_dag.py",
            try_number=2,
            map_index=5,
            upstream_results={"task1": "value1"},
            queue="high_priority",
            pool_slots=2,
        )
        assert input.try_number == 2
        assert input.map_index == 5
        assert input.upstream_results == {"task1": "value1"}
        assert input.queue == "high_priority"
        assert input.pool_slots == 2

    def test_serialization(self):
        """Test model serialization to dict (for Temporal JSON serialization)."""
        input = ActivityTaskInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1, 12, 0, 0),
            dag_rel_path="dags/test_dag.py",
        )

        data = input.model_dump()
        assert data["dag_id"] == "test_dag"
        assert data["task_id"] == "test_task"
        assert data["dag_rel_path"] == "dags/test_dag.py"
        assert isinstance(data["logical_date"], datetime)

    def test_deserialization(self):
        """Test creating model from dict (for Temporal JSON deserialization)."""
        data = {
            "dag_id": "test_dag",
            "task_id": "test_task",
            "run_id": "test_run",
            "logical_date": datetime(2025, 1, 1),
            "dag_rel_path": "dags/test_dag.py",
        }

        input = ActivityTaskInput(**data)
        assert input.dag_id == "test_dag"
        assert input.dag_rel_path == "dags/test_dag.py"

    def test_json_serialization(self):
        """Test model can be serialized to JSON (required for Temporal)."""
        input = ActivityTaskInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1, 12, 0, 0),
            dag_rel_path="dags/test_dag.py",
            upstream_results={"task1": {"result": "value1"}},
        )

        # Should be JSON-serializable
        json_str = input.model_dump_json()
        assert isinstance(json_str, str)
        assert "test_dag" in json_str
        assert "dags/test_dag.py" in json_str

        # Should be able to deserialize from JSON
        input2 = ActivityTaskInput.model_validate_json(json_str)
        assert input2.dag_id == input.dag_id
        assert input2.dag_rel_path == input.dag_rel_path
        assert input2.upstream_results == input.upstream_results


class TestTaskExecutionResult:
    """Test TaskExecutionResult model validation."""

    def test_valid_result_success(self):
        """Test valid TaskExecutionResult with SUCCESS state."""
        result = TaskExecutionResult(
            dag_id="test",
            task_id="task1",
            run_id="run1",
            try_number=1,
            state=TaskInstanceState.SUCCESS,
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 1, 0),
        )
        assert result.dag_id == "test"
        assert result.state == TaskInstanceState.SUCCESS
        assert result.return_value is None  # Default
        assert result.xcom_data is None  # Default
        assert result.error_message is None  # Default

    def test_valid_result_failed(self):
        """Test valid TaskExecutionResult with FAILED state."""
        result = TaskExecutionResult(
            dag_id="test",
            task_id="task1",
            run_id="run1",
            try_number=1,
            state=TaskInstanceState.FAILED,
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 1, 0),
            error_message="Task failed: ValueError",
        )
        assert result.state == TaskInstanceState.FAILED
        assert result.error_message == "Task failed: ValueError"

    def test_with_xcom_data(self):
        """Test TaskExecutionResult with XCom data."""
        result = TaskExecutionResult(
            dag_id="test",
            task_id="task1",
            run_id="run1",
            try_number=1,
            state=TaskInstanceState.SUCCESS,
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 1, 0),
            return_value="task output",
            xcom_data={"return_value": "task output", "custom_key": "custom_value"},
        )
        assert result.return_value == "task output"
        assert result.xcom_data["custom_key"] == "custom_value"

    def test_native_enum_serialization(self):
        """Test that native TaskInstanceState enum serializes correctly."""
        result = TaskExecutionResult(
            dag_id="test",
            task_id="task1",
            run_id="run1",
            try_number=1,
            state=TaskInstanceState.SUCCESS,
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 1, 0),
        )

        # Serialize to dict
        data = result.model_dump()
        assert data["state"] == "success"  # Enum value

        # Should be able to recreate from dict
        result2 = TaskExecutionResult(**data)
        assert result2.state == TaskInstanceState.SUCCESS

    def test_missing_required_field(self):
        """Test validation fails when required field missing."""
        with pytest.raises(ValidationError):
            TaskExecutionResult(
                dag_id="test",
                task_id="task1",
                # Missing run_id, try_number, state, etc.
            )


class TestDagExecutionInput:
    """Test DagExecutionInput model validation."""

    def test_valid_input(self):
        """Test valid DagExecutionInput creation."""
        input = DagExecutionInput(
            dag_id="test_dag",
            run_id="manual__2025-01-01T00:00:00",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"tasks": [], "dag_id": "test_dag"},
        )
        assert input.dag_id == "test_dag"
        assert input.run_id == "manual__2025-01-01T00:00:00"
        assert input.conf is None  # Default
        assert "tasks" in input.serialized_dag

    def test_with_conf(self):
        """Test DagExecutionInput with DAG configuration."""
        input = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={},
            conf={"param1": "value1", "param2": 42},
        )
        assert input.conf == {"param1": "value1", "param2": 42}

    def test_serialization(self):
        """Test DagExecutionInput serialization."""
        input = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1, 12, 0, 0),
            serialized_dag={"tasks": []},
        )

        data = input.model_dump()
        assert data["dag_id"] == "test_dag"
        assert isinstance(data["logical_date"], datetime)

        # Should deserialize back
        input2 = DagExecutionInput(**data)
        assert input2.dag_id == "test_dag"

    def test_missing_required_field(self):
        """Test validation fails when required field missing."""
        with pytest.raises(ValidationError):
            DagExecutionInput(dag_id="test")


class TestDagExecutionResult:
    """Test DagExecutionResult model validation."""

    def test_valid_result(self):
        """Test valid DagExecutionResult creation."""
        result = DagExecutionResult(
            state="success",
            dag_id="test_dag",
            run_id="test_run",
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 5, 0),
            tasks_succeeded=5,
            tasks_failed=0,
        )
        assert result.state == "success"
        assert result.tasks_succeeded == 5
        assert result.tasks_failed == 0

    def test_defaults(self):
        """Test DagExecutionResult default values."""
        result = DagExecutionResult(
            state="success",
            dag_id="test_dag",
            run_id="test_run",
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 5, 0),
        )
        assert result.tasks_succeeded == 0  # Default
        assert result.tasks_failed == 0  # Default

    def test_failed_dag(self):
        """Test DagExecutionResult for failed DAG."""
        result = DagExecutionResult(
            state="failed",
            dag_id="test_dag",
            run_id="test_run",
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 5, 0),
            tasks_succeeded=3,
            tasks_failed=2,
        )
        assert result.state == "failed"
        assert result.tasks_succeeded == 3
        assert result.tasks_failed == 2

    def test_serialization(self):
        """Test DagExecutionResult serialization."""
        result = DagExecutionResult(
            state="success",
            dag_id="test_dag",
            run_id="test_run",
            start_date=datetime(2025, 1, 1, 10, 0, 0),
            end_date=datetime(2025, 1, 1, 10, 5, 0),
            tasks_succeeded=5,
        )

        data = result.model_dump()
        assert data["state"] == "success"
        assert data["tasks_succeeded"] == 5

        # Should deserialize back
        result2 = DagExecutionResult(**data)
        assert result2.state == "success"
