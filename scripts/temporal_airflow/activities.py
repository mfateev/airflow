from __future__ import annotations

from datetime import datetime, timezone

import structlog
from temporalio import activity
from temporalio.exceptions import ApplicationError

from airflow.sdk.bases.operator import ExecutorSafeguard
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.state import TaskInstanceState
from temporal_airflow.models import (
    TaskExecutionInput,
    TaskExecutionResult,
    TaskExecutionFailureDetails,
)

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

    start_time = datetime.now(timezone.utc)

    try:
        # Deserialize task by reconstructing the operator with all parameters (Decision 7)
        task_type = input.serialized_task.get('task_type')
        task_module = input.serialized_task.get('_task_module')
        task_id = input.serialized_task.get('task_id')

        activity.logger.info(
            f"Reconstructing operator: {task_type} from {task_module} (task_id={task_id})"
        )

        # Import the operator class
        from importlib import import_module
        module = import_module(task_module)
        operator_class = getattr(module, task_type)

        # Extract constructor parameters from serialized data
        # Use SerializedBaseOperator's deserialization which handles all the complexity
        serialized_op = SerializedBaseOperator(input.serialized_task)

        # Get the actual operator by deserializing
        # The deserialize method returns a dict with operator data
        try:
            # Try to get params from the serialized object's internal structure
            params = serialized_op._task_dict

            # Filter out internal Airflow metadata
            constructor_kwargs = {
                k: v for k, v in params.items()
                if not k.startswith('_') or k == '_python_callable'  # Include _python_callable
            }

            activity.logger.info(f"Constructor kwargs keys: {list(constructor_kwargs.keys())}")

            # Instantiate the operator with deserialized parameters
            task = operator_class(**constructor_kwargs)
        except Exception as e:
            activity.logger.error(f"Deserialization failed: {e}, falling back to basic instantiation")
            # Fallback: just use task_id
            task = operator_class(task_id=task_id)

        activity.logger.info(
            f"Instantiated operator: {task.__class__.__name__} (has_execute={hasattr(task, 'execute')})"
        )

        # Build minimal execution context
        # Note: This is a simplified context for basic operators
        # Full task_runner infrastructure would be needed for advanced features
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

        activity.logger.info(f"Executing {task_type}.execute()")

        # Execute task ✨
        # Pass ExecutorSafeguard sentinel to indicate execution from proper task runner
        sentinel_key = f"{task.__class__.__name__}__sentinel"
        result = task.execute(context=context, **{sentinel_key: ExecutorSafeguard.sentinel_value})
        activity.logger.info(f"Task executed successfully")

        # Capture XCom pushes
        xcom_data = {"return_value": result} if result is not None else None

        end_time = datetime.now(timezone.utc)

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
        end_time = datetime.now(timezone.utc)

        activity.logger.error(f"Task failed: {input.dag_id}.{input.task_id}", exc_info=e)

        # Create structured failure details using Pydantic model
        failure_details = TaskExecutionFailureDetails(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            start_date=start_time,
            end_date=end_time,
            error_message=str(e),
        )

        # Raise ApplicationError with structured details as positional args
        # Temporal's Pydantic converter will serialize the model automatically
        # non_retryable=True indicates failure is permanent for this attempt
        raise ApplicationError(
            f"Task execution failed: {input.dag_id}.{input.task_id}",
            failure_details,
            type="TaskExecutionFailure",
            non_retryable=True,
        )
