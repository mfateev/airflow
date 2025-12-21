from __future__ import annotations

from datetime import datetime

import structlog
from temporalio import activity

from airflow.utils.state import TaskInstanceState
from temporal_airflow.models import TaskExecutionInput, TaskExecutionResult

logger = structlog.get_logger()


@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: TaskExecutionInput) -> TaskExecutionResult:
    """
    Execute an Airflow task.

    Design Notes:
    - Decision 7: Receives only serialized_task, not full DAG
    - Decision 4: Uses Pydantic models with native enums
    """
    activity.logger.info(
        f"Starting task execution: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, try={input.try_number})"
    )

    start_time = datetime.utcnow()
    end_time = datetime.utcnow()

    # TODO: Implement actual task execution
    # For now, return success

    return TaskExecutionResult(
        dag_id=input.dag_id,
        task_id=input.task_id,
        run_id=input.run_id,
        try_number=input.try_number,
        state=TaskInstanceState.SUCCESS,
        start_date=start_time,
        end_date=end_time,
    )
