from __future__ import annotations

import os
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

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


def _build_connection_uri(conn_data: dict[str, Any]) -> str:
    """
    Build Airflow connection URI from connection data dict.

    Airflow connection URI format:
    {conn_type}://[{login}[:{password}]@]{host}[:{port}][/{schema}][?{extra}]

    Args:
        conn_data: Connection definition dict with keys:
            - conn_type: Connection type (e.g., 'postgres', 'mysql', 'http')
            - host: Hostname
            - port: Port number (optional)
            - login: Username (optional)
            - password: Password (optional)
            - schema: Database/schema name (optional)
            - extra: Extra parameters as dict (optional)

    Returns:
        Airflow-compatible connection URI string
    """
    conn_type = conn_data.get("conn_type", "")
    login = conn_data.get("login", "")
    password = conn_data.get("password", "")
    host = conn_data.get("host", "")
    port = conn_data.get("port", "")
    schema = conn_data.get("schema", "")
    extra = conn_data.get("extra", {})

    # URL-encode login and password to handle special characters
    if login:
        login = urllib.parse.quote(str(login), safe="")
    if password:
        password = urllib.parse.quote(str(password), safe="")

    # Build URI
    uri = f"{conn_type}://"

    if login:
        uri += login
        if password:
            uri += f":{password}"
        uri += "@"

    if host:
        uri += host
        if port:
            uri += f":{port}"

    if schema:
        uri += f"/{schema}"

    if extra and isinstance(extra, dict):
        extra_str = urllib.parse.urlencode(extra)
        uri += f"?{extra_str}"

    return uri


@contextmanager
def _setup_airflow_env(
    connections: dict[str, dict[str, Any]] | None,
    variables: dict[str, str] | None,
) -> Generator[None, None, None]:
    """
    Context manager to set up Airflow environment variables for connections and variables.

    Sets AIRFLOW_CONN_* and AIRFLOW_VAR_* environment variables before task execution
    and cleans them up afterward.

    Args:
        connections: Connection definitions keyed by connection ID
        variables: Variable definitions keyed by variable name

    Yields:
        None (environment is configured)
    """
    set_conn_vars: list[str] = []
    set_var_vars: list[str] = []

    try:
        # Set connection environment variables
        if connections:
            for conn_id, conn_data in connections.items():
                env_var = f"AIRFLOW_CONN_{conn_id.upper()}"
                conn_uri = _build_connection_uri(conn_data)
                os.environ[env_var] = conn_uri
                set_conn_vars.append(env_var)
                activity.logger.debug(f"Set connection env var: {env_var}")

        # Set variable environment variables
        if variables:
            for var_name, var_value in variables.items():
                env_var = f"AIRFLOW_VAR_{var_name.upper()}"
                os.environ[env_var] = str(var_value)
                set_var_vars.append(env_var)
                activity.logger.debug(f"Set variable env var: {env_var}")

        yield

    finally:
        # Clean up connection environment variables
        for env_var in set_conn_vars:
            os.environ.pop(env_var, None)

        # Clean up variable environment variables
        for env_var in set_var_vars:
            os.environ.pop(env_var, None)


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
        # Step 0: Set up Airflow environment variables for connections/variables
        # This context manager ensures cleanup even if task fails
        with _setup_airflow_env(input.connections, input.variables):
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
            # NOTE: Must pass as kwarg, not in context dict (ExecutorSafeguard checks kwargs)
            sentinel_key = f"{task.__class__.__name__}__sentinel"
            result = task.execute(context, **{sentinel_key: ExecutorSafeguard.sentinel_value})

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
