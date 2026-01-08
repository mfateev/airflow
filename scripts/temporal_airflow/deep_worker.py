#!/usr/bin/env python
"""Start Temporal worker for Airflow deep integration.

This worker hosts the ExecuteAirflowDagDeepWorkflow and all required activities
for deep integration with Airflow DB.

Deep integration means:
- DAG metadata is loaded from Airflow's serialized_dag table
- Task/DagRun state is synced back to Airflow DB for UI visibility
- Connections/Variables are read from Airflow DB via hooks

Configuration uses Temporal's standard environment configuration system.
See: https://docs.temporal.io/develop/environment-configuration

Environment variables:
- TEMPORAL_ADDRESS: Host and port (default: localhost:7233)
- TEMPORAL_NAMESPACE: Namespace (default: default)
- TEMPORAL_API_KEY: API key (auto-enables TLS)
- TEMPORAL_TLS: Enable TLS (true/false)
- TEMPORAL_TASK_QUEUE: Task queue name (default: airflow-tasks)
- AIRFLOW__CORE__DAGS_FOLDER: DAG files location (default: /opt/airflow/dags)
- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: Airflow DB connection string

Usage:
    # Basic usage (requires Airflow DB connection)
    python deep_worker.py

    # With environment variables
    export TEMPORAL_ADDRESS="localhost:7233"
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@postgres/airflow"
    python deep_worker.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Add scripts directory to path for temporal_airflow imports
_scripts_dir = Path(__file__).parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from temporalio.worker import Worker

from temporal_airflow.client_config import create_temporal_client, get_task_queue, get_dags_folder
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.activities import run_airflow_task
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_task_status_batch,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _prewarm_imports() -> None:
    """
    Pre-import heavy modules and warm up DAG deserialization.

    This prevents Temporal's deadlock detector from triggering when workflows
    deserialize DAGs. The key insight: cold deserialization takes ~1.4s due to
    operator imports, but warm deserialization is <1ms. By deserializing one DAG
    at startup, we warm up all the common operator imports.
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
    from airflow.serialization.serialized_objects import SerializedDAG
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.session import create_session

    # Pre-initialize plugin manager
    from airflow import plugins_manager
    plugins_manager.ensure_plugins_loaded()

    # CRITICAL: Actually deserialize a DAG to warm up operator imports
    # Cold deserialization: ~1.4s, Warm deserialization: <1ms
    try:
        with create_session() as session:
            # Get any serialized DAG
            serialized = session.query(SerializedDagModel).first()
            if serialized:
                logger.info(f"Warming up deserialization with DAG: {serialized.dag_id}")
                deser_start = time.time()
                _ = SerializedDAG.from_dict(serialized.data)
                deser_time = time.time() - deser_start
                logger.info(f"DAG deserialization warmup: {deser_time*1000:.0f}ms")
            else:
                logger.warning("No DAGs found for deserialization warmup")
    except Exception as e:
        logger.warning(f"Could not warm up deserialization: {e}")

    # Pre-import scheduling/trigger rule modules (used in dag_run.update_state)
    # These modules can cause slow lazy imports during workflow execution
    try:
        logger.info("Warming up scheduling modules...")
        sched_start = time.time()
        from airflow.models.dagrun import DagRun  # noqa: F401
        from airflow.models.taskinstance import TaskInstance  # noqa: F401
        from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep  # noqa: F401
        from airflow.ti_deps.dep_context import DepContext  # noqa: F401
        from airflow.utils.state import DagRunState, TaskInstanceState  # noqa: F401
        from airflow.models.dagversion import DagVersion  # noqa: F401
        # Import task instance dependency checking
        from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS  # noqa: F401
        sched_time = time.time() - sched_start
        logger.info(f"Scheduling modules warmup: {sched_time*1000:.0f}ms")
    except Exception as e:
        logger.warning(f"Could not warm up scheduling modules: {e}")

    elapsed = time.time() - start
    logger.info(f"Pre-warming complete in {elapsed:.2f}s")


async def main() -> None:
    """Start Temporal worker for Airflow deep integration."""
    # Pre-warm imports
    _prewarm_imports()

    task_queue = get_task_queue()
    dags_folder = get_dags_folder()

    logger.info("=" * 60)
    logger.info("Temporal Airflow Deep Integration Worker")
    logger.info("=" * 60)
    logger.info(f"Task queue: {task_queue}")
    logger.info(f"DAGs folder: {dags_folder}")
    logger.info("")

    # Verify DAGs folder exists
    if not dags_folder.exists():
        logger.warning(f"DAGs folder does not exist: {dags_folder}")
        logger.warning("Set AIRFLOW__CORE__DAGS_FOLDER to your DAGs directory")

    # Verify Airflow DB connection
    try:
        from airflow.configuration import conf
        db_conn = conf.get("database", "sql_alchemy_conn")
        logger.info(f"Airflow DB: {db_conn.split('@')[-1] if '@' in db_conn else db_conn}")
    except Exception as e:
        logger.warning(f"Could not read Airflow DB config: {e}")

    logger.info("")
    logger.info("Connecting to Temporal...")
    try:
        client = await create_temporal_client()
        logger.info(f"Connected to Temporal at {client.service_client.config.target_host}")
        logger.info(f"Namespace: {client.namespace}")
    except Exception as e:
        logger.error(f"Failed to connect to Temporal: {e}")
        logger.error("Check your Temporal configuration")
        sys.exit(1)

    # Create thread pool for sync activities
    # Sync activities (def, not async def) run blocking I/O operations
    # (SQLAlchemy queries, file I/O) and must run in a thread pool
    activity_executor = ThreadPoolExecutor(max_workers=10)

    # Create worker with deep workflow and all activities
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ExecuteAirflowDagDeepWorkflow],
        activities=[
            # Task execution
            run_airflow_task,
            # Sync activities for Airflow DB
            create_dagrun_record,
            sync_task_status,
            sync_task_status_batch,
            sync_dagrun_status,
            load_serialized_dag,
            ensure_task_instances,
        ],
        activity_executor=activity_executor,
    )

    logger.info("")
    logger.info("Worker started successfully!")
    logger.info("")
    logger.info("  Workflow:")
    logger.info("    - ExecuteAirflowDagDeepWorkflow")
    logger.info("")
    logger.info("  Activities:")
    logger.info("    - run_airflow_task (task execution)")
    logger.info("    - create_dagrun_record (sync to Airflow DB)")
    logger.info("    - sync_task_status (sync single task)")
    logger.info("    - sync_task_status_batch (sync multiple tasks)")
    logger.info("    - sync_dagrun_status (sync to Airflow DB)")
    logger.info("    - load_serialized_dag (load from Airflow DB)")
    logger.info("    - ensure_task_instances (sync to Airflow DB)")
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
