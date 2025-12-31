#!/usr/bin/env python
"""Submit Airflow DAG to Temporal for execution.

This script submits a DAG file to Temporal for execution without requiring
any Airflow infrastructure (scheduler, webserver, database).

Configuration is loaded using Temporal's standard environment configuration system.
See: https://docs.temporal.io/develop/environment-configuration

Usage:
    # Basic submission
    python submit_dag.py my_dag --dag-file dags/my_dag.py

    # Wait for completion
    python submit_dag.py my_dag --dag-file dags/my_dag.py --wait

    # With DAG configuration
    python submit_dag.py my_dag --dag-file dags/my_dag.py --conf '{"key": "value"}'

    # With connections (JSON file)
    python submit_dag.py my_dag --dag-file dags/my_dag.py --connections connections.json

    # With variables (JSON file)
    python submit_dag.py my_dag --dag-file dags/my_dag.py --variables variables.json

Examples:
    # connections.json format:
    {
        "postgres_default": {
            "conn_type": "postgres",
            "host": "localhost",
            "port": 5432,
            "login": "airflow",
            "password": "airflow",
            "schema": "airflow"
        }
    }

    # variables.json format:
    {
        "environment": "production",
        "api_key": "secret123"
    }
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Add scripts directory to path for temporal_airflow imports
_scripts_dir = Path(__file__).parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from temporalio.client import WorkflowHandle

from temporal_airflow.client_config import create_temporal_client, get_task_queue, get_dags_folder
from temporal_airflow.activities import load_dag_from_file
from temporal_airflow.models import DagExecutionInput, DagExecutionResult
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def serialize_dag(dag) -> dict[str, Any]:
    """Serialize DAG using Airflow's serialization."""
    from airflow.serialization.serialized_objects import SerializedDAG
    return SerializedDAG.to_dict(dag)


async def submit_dag(
    dag_id: str,
    dag_file: str,
    connections: dict[str, dict[str, Any]] | None = None,
    variables: dict[str, str] | None = None,
    conf: dict[str, Any] | None = None,
    wait: bool = False,
    run_id: str | None = None,
) -> DagExecutionResult | WorkflowHandle:
    """
    Submit DAG for execution on Temporal.

    Args:
        dag_id: DAG identifier
        dag_file: Relative path to DAG file (from DAGS_FOLDER)
        connections: Connection definitions (optional)
        variables: Variable definitions (optional)
        conf: DAG run configuration (optional)
        wait: Wait for workflow to complete
        run_id: Custom run ID (optional, auto-generated if not provided)

    Returns:
        DagExecutionResult if wait=True, else WorkflowHandle
    """
    task_queue = get_task_queue()

    # Load and serialize DAG
    logger.info(f"Loading DAG '{dag_id}' from {dag_file}...")
    dag = load_dag_from_file(dag_file, dag_id)
    serialized_dag = serialize_dag(dag)
    logger.info(f"DAG loaded successfully with {len(dag.task_dict)} tasks")

    # Generate run ID if not provided
    if run_id is None:
        run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # Create workflow input
    workflow_input = DagExecutionInput(
        dag_id=dag_id,
        run_id=run_id,
        logical_date=datetime.now(timezone.utc),
        conf=conf,
        serialized_dag=serialized_dag,
        connections=connections,
        variables=variables,
    )

    # Connect to Temporal
    logger.info("Connecting to Temporal...")
    client = await create_temporal_client()
    logger.info(f"Connected to {client.service_client.config.target_host}")

    # Start workflow
    workflow_id = f"dag-{dag_id}-{run_id}"
    logger.info(f"Starting workflow: {workflow_id}")

    handle = await client.start_workflow(
        ExecuteAirflowDagWorkflow.run,
        workflow_input,
        id=workflow_id,
        task_queue=task_queue,
    )

    logger.info("")
    logger.info("Workflow started!")
    logger.info(f"  Workflow ID: {handle.id}")
    logger.info(f"  Run ID: {handle.result_run_id}")
    logger.info(f"  Task Queue: {task_queue}")

    if wait:
        logger.info("")
        logger.info("Waiting for workflow to complete...")
        try:
            result = await handle.result()
            logger.info("")
            logger.info("Workflow completed successfully!")
            logger.info(f"  State: {result.state}")
            logger.info(f"  Tasks succeeded: {result.tasks_succeeded}")
            logger.info(f"  Tasks failed: {result.tasks_failed}")
            logger.info(f"  Duration: {result.end_date - result.start_date}")
            return result
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            raise

    return handle


def load_json_file(path: str) -> dict:
    """Load JSON from file."""
    with open(path) as f:
        return json.load(f)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Submit Airflow DAG to Temporal for execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "dag_id",
        help="DAG identifier",
    )
    parser.add_argument(
        "--dag-file",
        required=True,
        help="Relative path to DAG file (from DAGS_FOLDER)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for workflow to complete",
    )
    parser.add_argument(
        "--conf",
        help="DAG configuration as JSON string",
    )
    parser.add_argument(
        "--connections",
        help="Path to connections JSON file",
    )
    parser.add_argument(
        "--variables",
        help="Path to variables JSON file",
    )
    parser.add_argument(
        "--run-id",
        help="Custom run ID (auto-generated if not provided)",
    )

    args = parser.parse_args()

    # Parse conf
    conf = None
    if args.conf:
        try:
            conf = json.loads(args.conf)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON for --conf: {e}")
            sys.exit(1)

    # Load connections
    connections = None
    if args.connections:
        try:
            connections = load_json_file(args.connections)
            logger.info(f"Loaded {len(connections)} connections from {args.connections}")
        except Exception as e:
            logger.error(f"Failed to load connections file: {e}")
            sys.exit(1)

    # Load variables
    variables = None
    if args.variables:
        try:
            variables = load_json_file(args.variables)
            logger.info(f"Loaded {len(variables)} variables from {args.variables}")
        except Exception as e:
            logger.error(f"Failed to load variables file: {e}")
            sys.exit(1)

    # Submit DAG
    try:
        result = asyncio.run(
            submit_dag(
                dag_id=args.dag_id,
                dag_file=args.dag_file,
                connections=connections,
                variables=variables,
                conf=conf,
                wait=args.wait,
                run_id=args.run_id,
            )
        )

        if not args.wait:
            logger.info("")
            logger.info("Use --wait to wait for completion, or check Temporal UI")

    except Exception as e:
        logger.error(f"Failed to submit DAG: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
