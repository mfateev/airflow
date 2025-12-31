#!/usr/bin/env python
"""Start Temporal worker for Airflow DAG execution.

This worker hosts the ExecuteAirflowDagWorkflow and run_airflow_task activity.

Configuration is loaded using Temporal's standard environment configuration system.
See: https://docs.temporal.io/develop/environment-configuration

Environment variables:
- TEMPORAL_ADDRESS: Host and port (default: localhost:7233)
- TEMPORAL_NAMESPACE: Namespace (default: default)
- TEMPORAL_API_KEY: API key (auto-enables TLS)
- TEMPORAL_TLS: Enable TLS (true/false)
- TEMPORAL_PROFILE: Profile name from config file
- TEMPORAL_TASK_QUEUE: Task queue name (default: airflow-tasks)
- AIRFLOW__CORE__DAGS_FOLDER: DAG files location (default: /opt/airflow/dags)

Usage:
    # Basic usage with defaults (localhost:7233)
    python worker.py

    # With environment variables
    export TEMPORAL_ADDRESS="localhost:7233"
    export TEMPORAL_NAMESPACE="default"
    export TEMPORAL_TASK_QUEUE="airflow-tasks"
    python worker.py

    # With TOML config file (~/.config/temporalio/temporal.toml)
    python worker.py

    # With specific profile
    TEMPORAL_PROFILE=prod python worker.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Add scripts directory to path for temporal_airflow imports
_scripts_dir = Path(__file__).parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from temporalio.worker import Worker

from temporal_airflow.client_config import create_temporal_client, get_task_queue, get_dags_folder
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.activities import run_airflow_task

# Configure logging - must be before pre-warming imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _prewarm_imports() -> None:
    """
    Pre-import heavy modules before starting workflows.

    This prevents Temporal's deadlock detector from triggering when workflows
    deserialize DAGs, since the imports will already be cached.

    The deadlock detector triggers if a workflow doesn't yield within 2 seconds.
    Heavy imports like pandas/numpy can exceed this limit on first import.
    """
    logger.info("Pre-warming imports (this may take a few seconds)...")
    import time
    start = time.time()

    # Pre-import heavy data processing libraries
    try:
        import pandas  # noqa: F401
        import numpy  # noqa: F401
    except ImportError:
        pass  # Not required, may not be installed

    # Pre-import Airflow serialization (triggers plugin loading)
    from airflow.serialization.serialized_objects import SerializedDAG  # noqa: F401

    # Pre-initialize plugin manager (loads plugins from plugins folder)
    from airflow import plugins_manager
    plugins_manager.ensure_plugins_loaded()

    elapsed = time.time() - start
    logger.info(f"Pre-warming complete in {elapsed:.2f}s")


async def main() -> None:
    """Start Temporal worker for Airflow DAG execution."""
    # Pre-warm imports to avoid deadlock detection during workflow execution
    _prewarm_imports()

    task_queue = get_task_queue()
    dags_folder = get_dags_folder()

    logger.info("=" * 60)
    logger.info("Temporal Airflow Worker")
    logger.info("=" * 60)
    logger.info(f"Task queue: {task_queue}")
    logger.info(f"DAGs folder: {dags_folder}")
    logger.info("")

    # Verify DAGs folder exists
    if not dags_folder.exists():
        logger.warning(f"DAGs folder does not exist: {dags_folder}")
        logger.warning("Set AIRFLOW__CORE__DAGS_FOLDER to your DAGs directory")

    logger.info("Connecting to Temporal...")
    try:
        client = await create_temporal_client()
        logger.info(f"Connected to Temporal at {client.service_client.config.target_host}")
        logger.info(f"Namespace: {client.namespace}")
    except Exception as e:
        logger.error(f"Failed to connect to Temporal: {e}")
        logger.error("Check your Temporal configuration (env vars or ~/.config/temporalio/temporal.toml)")
        sys.exit(1)

    # Create worker
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ExecuteAirflowDagWorkflow],
        activities=[run_airflow_task],
    )

    logger.info("")
    logger.info("Worker started successfully!")
    logger.info(f"  Workflow: ExecuteAirflowDagWorkflow")
    logger.info(f"  Activity: run_airflow_task")
    logger.info("")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    # Run worker (blocks until interrupted)
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("")
        logger.info("Worker stopped by user")
