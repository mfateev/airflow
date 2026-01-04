from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from airflow.utils.state import TaskInstanceState

# ============================================================================
# Activity Models (Executor Pattern)
# ============================================================================


class TaskExecutionFailureDetails(BaseModel):
    """
    Details passed in ApplicationError when task execution fails.

    This provides structured error information that can be parsed
    by the workflow to update TaskInstance state.
    """

    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    try_number: int = Field(..., description="Retry attempt number")
    start_date: datetime = Field(..., description="Task start time")
    end_date: datetime = Field(..., description="Task end time")
    error_message: str = Field(..., description="Error message from exception")


class ActivityTaskInput(BaseModel):
    """
    Input model for task execution activity (Executor Pattern).

    This model uses the executor pattern: activities load DAG from file
    instead of receiving serialized operators. This provides:
    - Real operators with real callables (no serialization issues)
    - Full feature support (callbacks, context, XCom, etc.)
    - Simpler code and easier debugging

    Architecture:
    - Activities receive only JSON-serializable metadata
    - Activities load DAG file and extract task operator
    - Activities execute task and return JSON result
    - Workflow updates in-memory DB based on result

    Standalone Mode Support:
    - connections: Passed from workflow, set as AIRFLOW_CONN_* env vars
    - variables: Passed from workflow, set as AIRFLOW_VAR_* env vars
    """

    # Task identification (metadata only)
    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")

    # Execution metadata
    try_number: int = Field(default=1, description="Retry attempt number")
    map_index: int = Field(default=-1, description="Mapped task index (-1 for non-mapped)")

    # DAG file location (instead of serialized operator)
    dag_rel_path: str = Field(..., description="Relative path to DAG file from DAGS_FOLDER")

    # Execution context (passed from workflow)
    upstream_results: dict[str, Any] | None = Field(
        default=None,
        description="XCom values from upstream tasks",
    )

    # Additional metadata
    queue: str | None = Field(default=None, description="Task queue for routing")
    pool_slots: int = Field(default=1, description="Number of pool slots required")

    # Standalone mode support: connections/variables passed from workflow
    connections: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Connection definitions (set as AIRFLOW_CONN_* env vars)",
    )
    variables: dict[str, str] | None = Field(
        default=None,
        description="Variable definitions (set as AIRFLOW_VAR_* env vars)",
    )


class TaskExecutionResult(BaseModel):
    """
    Result model for task execution activity.

    Decision 4: Uses native TaskInstanceState enum.
    Pydantic handles serialization automatically.
    """

    # Task identification (echo back)
    dag_id: str
    task_id: str
    run_id: str
    try_number: int

    # Execution result (Decision 4: native enum)
    state: TaskInstanceState = Field(..., description="Final task state")

    # Timing information
    start_date: datetime = Field(..., description="Task start time")
    end_date: datetime = Field(..., description="Task end time")

    # Task output (Decision 7: XCom handling)
    return_value: Any | None = Field(default=None, description="Task return value")
    xcom_data: dict[str, Any] | None = Field(
        default=None,
        description="XCom values pushed by this task",
    )

    # Error information
    error_message: str | None = Field(default=None, description="Error message if failed")


# ============================================================================
# Workflow Models
# ============================================================================


class DagExecutionInput(BaseModel):
    """
    Input model for DAG execution workflow.

    Decision 3: Full serialized_dag passed to workflow (once),
    then workflow extracts individual tasks for activities.

    Standalone Mode Support:
    - connections: Pass connection definitions directly (no Airflow DB needed)
    - variables: Pass variable definitions directly (no Airflow DB needed)
    """

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")
    conf: dict[str, Any] | None = Field(default=None, description="DAG run configuration")
    serialized_dag: dict[str, Any] = Field(..., description="Serialized DAG definition")

    # Standalone mode support: pass connections/variables without Airflow DB
    connections: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Connection definitions keyed by connection ID. "
            "Example: {'postgres_default': {'conn_type': 'postgres', 'host': 'localhost', ...}}"
        ),
    )
    variables: dict[str, str] | None = Field(
        default=None,
        description=(
            "Variable definitions keyed by variable name. "
            "Example: {'api_key': 'secret123', 'environment': 'prod'}"
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "dag_id": "example_dag",
                "run_id": "manual__2025-01-01T00:00:00",
                "logical_date": "2025-01-01T00:00:00Z",
                "conf": {},
                "serialized_dag": {"tasks": []},
                "connections": {
                    "postgres_default": {
                        "conn_type": "postgres",
                        "host": "localhost",
                        "port": 5432,
                        "login": "airflow",
                        "password": "airflow",
                        "schema": "airflow",
                    }
                },
                "variables": {"environment": "dev", "api_key": "test_key"},
            }
        }
    )


class DagExecutionResult(BaseModel):
    """Result model for DAG execution workflow."""

    state: str = Field(..., description="Final DAG run state")
    dag_id: str
    run_id: str
    start_date: datetime
    end_date: datetime
    tasks_succeeded: int = Field(default=0, description="Number of successful tasks")
    tasks_failed: int = Field(default=0, description="Number of failed tasks")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "state": "success",
                "dag_id": "example_dag",
                "run_id": "manual__2025-01-01T00:00:00",
                "start_date": "2025-01-01T00:00:00Z",
                "end_date": "2025-01-01T00:01:30Z",
                "tasks_succeeded": 5,
                "tasks_failed": 0,
            }
        }
    )


class DagExecutionFailureDetails(BaseModel):
    """
    Details passed in ApplicationError when DAG execution fails.

    This provides structured error information about the failed DAG run.
    """

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")
    start_date: datetime = Field(..., description="DAG run start time")
    end_date: datetime = Field(..., description="DAG run end time")
    tasks_succeeded: int = Field(..., description="Number of successful tasks")
    tasks_failed: int = Field(..., description="Number of failed tasks")
    error_message: str = Field(..., description="Error summary")


# ============================================================================
# Deep Integration Models (DB Sync)
# ============================================================================


class CreateDagRunInput(BaseModel):
    """Input for create_dagrun_record activity."""

    dag_id: str = Field(..., description="DAG identifier")
    logical_date: datetime = Field(..., description="Logical execution date")
    conf: dict[str, Any] | None = Field(default=None, description="DAG run configuration")


class CreateDagRunResult(BaseModel):
    """Result from create_dagrun_record activity."""

    run_id: str = Field(..., description="Generated run_id for the DagRun")
    dag_run_id: int = Field(..., description="Database primary key ID")


class TaskStatusSync(BaseModel):
    """Input for sync_task_status activity."""

    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    map_index: int = Field(default=-1, description="Mapped task index (-1 for non-mapped)")
    state: str = Field(..., description="TaskInstanceState value as string")
    start_date: datetime | None = Field(default=None, description="Task start time")
    end_date: datetime | None = Field(default=None, description="Task end time")
    xcom_value: Any | None = Field(default=None, description="XCom return value to store")


class DagRunStatusSync(BaseModel):
    """Input for sync_dagrun_status activity."""

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")
    state: str = Field(..., description="DagRunState value as string")
    end_date: datetime | None = Field(default=None, description="DAG run end time")


class BatchTaskStatusSync(BaseModel):
    """Input for batch sync_task_status activity."""

    syncs: list[TaskStatusSync] = Field(..., description="List of task status syncs to apply")


class LoadSerializedDagInput(BaseModel):
    """Input for load_serialized_dag activity."""

    dag_id: str = Field(..., description="DAG identifier to load from SerializedDagModel")


class LoadSerializedDagResult(BaseModel):
    """Result from load_serialized_dag activity."""

    dag_data: dict[str, Any] = Field(..., description="Serialized DAG data dict")
    fileloc: str = Field(..., description="DAG file location (relative to DAGS_FOLDER)")


class EnsureTaskInstancesInput(BaseModel):
    """Input for ensure_task_instances activity."""

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")


class DeepDagExecutionInput(BaseModel):
    """
    Input model for deep integration workflow.

    Unlike DagExecutionInput, this workflow:
    - Uses real Airflow DB (not in-memory SQLite)
    - Creates DagRun/TaskInstance records via activities
    - Syncs status back to Airflow DB for UI visibility
    - Reads connections/variables from Airflow DB
    """

    dag_id: str = Field(..., description="DAG identifier")
    logical_date: datetime = Field(..., description="Logical execution date")
    run_id: str | None = Field(
        default=None,
        description="Existing run_id if DagRun already created (e.g., by orchestrator)",
    )
    conf: dict[str, Any] | None = Field(default=None, description="DAG run configuration")
