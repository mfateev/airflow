from __future__ import annotations

from datetime import datetime

import structlog
from temporalio import activity

from airflow.serialization.serialized_objects import SerializedBaseOperator
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

    try:
        # Deserialize just this task (Decision 7)
        task = SerializedBaseOperator.deserialize_operator(input.serialized_task)

        activity.logger.info(f"Deserialized task: {task.__class__.__name__}")

        # Build minimal execution context
        context = {
            "dag_id": input.dag_id,
            "task_id": input.task_id,
            "run_id": input.run_id,
            "logical_date": input.logical_date,
            "try_number": input.try_number,
            # Simple XCom pull from upstream results
            "task_instance": type("TI", (), {
                "xcom_pull": lambda task_ids=None, key="return_value":
                    input.upstream_results.get(task_ids) if input.upstream_results else None
            })(),
        }

        activity.logger.info(f"Built execution context for {input.task_id}")

        # Execute task ✨
        result = task.execute(context=context)

        # Capture XCom pushes
        xcom_data = {"return_value": result} if result is not None else None

        end_time = datetime.utcnow()

        activity.logger.info(
            f"Task completed successfully: {input.dag_id}.{input.task_id} "
            f"(duration: {(end_time - start_time).total_seconds()}s)"
        )

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.SUCCESS,
            start_date=start_time,
            end_date=end_time,
            return_value=result,
            xcom_data=xcom_data,
        )

    except Exception as e:
        end_time = datetime.utcnow()

        activity.logger.error(f"Task failed: {input.dag_id}.{input.task_id}", exc_info=e)

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.FAILED,
            start_date=start_time,
            end_date=end_time,
            error_message=str(e),
        )
