from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

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
