from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import structlog
from temporalio import activity
from temporalio.exceptions import ApplicationError

from airflow.utils.state import TaskInstanceState
from airflow.sdk.bases.operator import ExecutorSafeguard
from temporal_airflow.models import (
    ActivityTaskInput,
    TaskExecutionResult,
    TaskExecutionFailureDetails,
)

logger = structlog.get_logger()


def load_dag_from_file(dag_rel_path: str, dag_id: str):
    """
    Load DAG from Python file.

    This is the core of the executor pattern: activities load DAG files
    and get real operators with real callables (no serialization).

    Args:
        dag_rel_path: Relative path from DAGS_FOLDER (e.g., "dags/my_dag.py")
        dag_id: DAG identifier to extract from the file

    Returns:
        DAG object with all tasks

    Raises:
        FileNotFoundError: If DAG file doesn't exist
        ValueError: If DAG not found in file
    """
    # Get DAGS_FOLDER from environment or use default
    dags_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")

    # Construct full path
    dag_file = Path(dags_folder) / dag_rel_path

    if not dag_file.exists():
        raise FileNotFoundError(
            f"DAG file not found: {dag_file} "
            f"(dag_rel_path={dag_rel_path}, dags_folder={dags_folder})"
        )

    activity.logger.info(f"Loading DAG from {dag_file}")

    # Execute Python file to load DAG
    # This gets us real operators with real callables!
    import importlib.util
    spec = importlib.util.spec_from_file_location("temp_dag_module", dag_file)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module spec from {dag_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find DAG in module
    from airflow.sdk.definitions.dag import DAG

    dag = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == dag_id:
            dag = attr
            break

    if dag is None:
        raise ValueError(
            f"DAG '{dag_id}' not found in {dag_file}. "
            f"Available DAGs: {[getattr(module, name).dag_id for name in dir(module) if isinstance(getattr(module, name), DAG)]}"
        )

    activity.logger.info(
        f"Loaded DAG '{dag.dag_id}' with {len(dag.task_dict)} tasks from {dag_file}"
    )

    return dag


@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: ActivityTaskInput) -> TaskExecutionResult:
    """
    Execute an Airflow task using executor pattern.

    Architecture (Executor Pattern - Phase 2):
    - NO database access (activities can run on remote machines)
    - Loads DAG from file (gets real operators with real callables)
    - Executes task with minimal context (no DB queries)
    - Returns JSON result to workflow
    - Workflow updates in-memory DB based on result

    This is how LocalExecutor and all production executors work!
    """
    activity.logger.info(
        f"Starting task execution: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, try={input.try_number}, dag_path={input.dag_rel_path})"
    )

    start_time = datetime.now(timezone.utc)

    try:
        # Step 1: Load DAG from file (executor pattern)
        dag = load_dag_from_file(input.dag_rel_path, input.dag_id)

        # Step 2: Extract task operator from DAG
        if input.task_id not in dag.task_dict:
            raise ValueError(
                f"Task '{input.task_id}' not found in DAG '{dag.dag_id}'. "
                f"Available tasks: {list(dag.task_dict.keys())}"
            )

        task = dag.task_dict[input.task_id]
        activity.logger.info(
            f"Extracted task '{task.task_id}' ({task.__class__.__name__}) from DAG"
        )

        # Step 3: Create minimal execution context (no DB access)
        # This is a simplified context for basic operators
        context = {
            "dag": dag,
            "task": task,
            "dag_id": input.dag_id,
            "task_id": input.task_id,
            "run_id": input.run_id,
            "logical_date": input.logical_date,
            "try_number": input.try_number,
            "map_index": input.map_index,
        }

        # Add XCom pull function if upstream results provided
        if input.upstream_results:
            def xcom_pull(task_ids=None, key="return_value"):
                """Simple XCom pull from upstream_results dict."""
                if task_ids is None:
                    return None
                return input.upstream_results.get(task_ids)

            # Create minimal task_instance object for context
            class MinimalTI:
                def __init__(self, xcom_pull_fn):
                    self.xcom_pull = xcom_pull_fn

            context["task_instance"] = MinimalTI(xcom_pull)
            context["ti"] = context["task_instance"]

        activity.logger.info(f"Executing {task.__class__.__name__}.execute()")

        # Step 4: Execute task with real operator (NO serialization!)
        # This is the key benefit: we have real callables, not string representations
        # Add ExecutorSafeguard sentinel to allow direct execute() calls outside Task Runner
        sentinel_key = f"{task.__class__.__name__}__sentinel"
        context[sentinel_key] = ExecutorSafeguard.sentinel_value
        result = task.execute(context)

        activity.logger.info(
            f"Task executed successfully, result type: {type(result).__name__}"
        )

        # Step 5: Package result as JSON (must be JSON-serializable for Temporal)
        # Workflow will store this in XCom table in its in-memory DB
        xcom_data = {"return_value": result} if result is not None else None

        end_time = datetime.now(timezone.utc)

        activity.logger.info(
            f"Task completed successfully: {input.dag_id}.{input.task_id} "
            f"(duration: {(end_time - start_time).total_seconds()}s)"
        )

        # Step 6: Return JSON result to workflow (NO DB access)
        # Workflow will update TaskInstance state in its in-memory DB
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
