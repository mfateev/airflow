from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from airflow.utils.state import TaskInstanceState

# ============================================================================
# Activity Models
# ============================================================================


class TaskExecutionInput(BaseModel):
    """
    Input model for task execution activity.

    Decision 7: Passes only serialized_task, not entire DAG.
    This reduces Temporal history size by ~100x.
    """

    # Task identification
    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")

    # Execution metadata
    try_number: int = Field(default=1, description="Retry attempt number")
    map_index: int = Field(default=-1, description="Mapped task index (-1 for non-mapped)")

    # Task definition (NOT full DAG - Decision 7)
    serialized_task: dict[str, Any] = Field(..., description="Serialized task operator")

    # Execution context
    upstream_results: dict[str, Any] | None = Field(
        default=None,
        description="XCom values from upstream tasks",
    )

    # Queue for task routing (Decision 9)
    queue: str | None = Field(default=None, description="Task queue for routing")


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
