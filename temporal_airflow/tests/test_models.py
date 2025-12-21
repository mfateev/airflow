from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from temporal_airflow.models import TaskExecutionInput


class TestTaskExecutionInput:
    """Test TaskExecutionInput model validation."""

    def test_valid_input(self):
        """Test valid TaskExecutionInput creation."""
        input = TaskExecutionInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_task={"_task_type": "PythonOperator"},
        )
        assert input.dag_id == "test_dag"
        assert input.task_id == "test_task"
        assert input.run_id == "test_run"
        assert input.try_number == 1  # Default
        assert input.map_index == -1  # Default
        assert input.queue is None  # Default

    def test_missing_required_field(self):
        """Test validation fails when required field missing."""
        with pytest.raises(ValidationError):
            TaskExecutionInput(dag_id="test")

    def test_with_optional_fields(self):
        """Test TaskExecutionInput with optional fields."""
        input = TaskExecutionInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_task={},
            try_number=2,
            map_index=5,
            upstream_results={"task1": "value1"},
            queue="high_priority",
        )
        assert input.try_number == 2
        assert input.map_index == 5
        assert input.upstream_results == {"task1": "value1"}
        assert input.queue == "high_priority"

    def test_serialization(self):
        """Test model serialization to dict."""
        input = TaskExecutionInput(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1, 12, 0, 0),
            serialized_task={"_task_type": "PythonOperator"},
        )

        data = input.model_dump()
        assert data["dag_id"] == "test_dag"
        assert data["task_id"] == "test_task"
        assert isinstance(data["logical_date"], datetime)

    def test_deserialization(self):
        """Test creating model from dict."""
        data = {
            "dag_id": "test_dag",
            "task_id": "test_task",
            "run_id": "test_run",
            "logical_date": datetime(2025, 1, 1),
            "serialized_task": {},
        }

        input = TaskExecutionInput(**data)
        assert input.dag_id == "test_dag"
