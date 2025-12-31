# Temporal Standalone Mode: Airflow DAGs Without Airflow Infrastructure

**Date**: 2025-12-31 (Updated)
**Status**: ✅ Implemented
**Purpose**: Execute Airflow DAGs using pure Temporal, eliminating Airflow scheduler, webserver, database, and CLI

---

## Executive Summary

This document describes **Standalone Mode** - a deployment model where Temporal fully replaces Airflow's infrastructure while still executing standard Airflow DAG files. Users interact with Temporal directly, avoiding Airflow UI, CLI, and database entirely.

**Key Principle**: DAG files remain standard Airflow code, but execution is 100% Temporal.

---

## Quick Start

### Prerequisites
- Temporal server running (localhost:7233 by default)
- Python 3.10+
- `uv` package manager (or pip)

### 1. Start the Worker

```bash
cd /path/to/airflow-root/temporal-scheduler

# Increase file descriptor limit (macOS/Linux)
ulimit -n 2048

# Start worker with DAGs folder
AIRFLOW__CORE__DAGS_FOLDER="/path/to/your/dags" \
  uv run python scripts/temporal_airflow/worker.py
```

### 2. Submit a DAG

In a separate terminal:

```bash
# Basic submission (returns immediately)
AIRFLOW__CORE__DAGS_FOLDER="/path/to/your/dags" \
  uv run python scripts/temporal_airflow/submit_dag.py <dag_id> --dag-file <filename.py>

# Wait for completion
AIRFLOW__CORE__DAGS_FOLDER="/path/to/your/dags" \
  uv run python scripts/temporal_airflow/submit_dag.py <dag_id> --dag-file <filename.py> --wait

# With connections and variables
uv run python scripts/temporal_airflow/submit_dag.py my_dag \
  --dag-file my_dag.py \
  --connections connections.json \
  --variables variables.json \
  --wait
```

### 3. Monitor with Temporal CLI

```bash
# List workflows
temporal workflow list

# Show workflow details
temporal workflow describe --workflow-id <workflow_id>

# View event history
temporal workflow show --workflow-id <workflow_id>
```

### Configuration (Environment Variables)

Uses [Temporal's standard environment configuration](https://docs.temporal.io/develop/environment-configuration):

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal server address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_API_KEY` | (none) | API key (auto-enables TLS) |
| `TEMPORAL_TLS` | `false` | Enable TLS |
| `TEMPORAL_TASK_QUEUE` | `airflow-tasks` | Task queue name |
| `AIRFLOW__CORE__DAGS_FOLDER` | `/opt/airflow/dags` | DAG files location |

---

## Architecture Comparison

### Traditional Airflow Architecture

```
┌─────────────────────────────────────────────────┐
│  Airflow Webserver (Flask)                      │
│  - Web UI                                        │
│  - REST API                                      │
│  - User authentication                           │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Airflow Database (Postgres/MySQL)              │
│  - DAG metadata                                  │
│  - DagRun/TaskInstance state                    │
│  - Connections, Variables                        │
│  - Logs metadata                                 │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Airflow Scheduler                               │
│  - DAG file parsing                              │
│  - DagRun creation (schedules)                   │
│  - Task dependency resolution                    │
│  - Task queueing                                 │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Airflow Executor (Local/Celery/Kubernetes)     │
│  - Task distribution                             │
│  - Worker management                             │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Airflow Workers                                 │
│  - Execute tasks                                 │
│  - Write logs                                    │
│  - Update task state                             │
└─────────────────────────────────────────────────┘
```

**Components**: 5 processes + database
**Complexity**: High (configuration, networking, scaling)
**State Storage**: Centralized database

---

### Standalone Temporal Architecture

```
┌─────────────────────────────────────────────────┐
│  Submit Script (submit_dag.py)                   │
│  - Load DAG file                                 │
│  - Serialize DAG                                 │
│  - Pass connections, variables, config           │
│  - Submit to Temporal                            │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Temporal Server (single binary or cloud)        │
│  - Workflow orchestration                        │
│  - Durable state storage                         │
│  - Retry/timeout management                      │
│  - Web UI (monitoring)                           │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  Temporal Worker (Python process)                │
│  - ExecuteAirflowDagWorkflow (workflows.py)      │
│    • In-memory SQLite per workflow               │
│    • Task dependency resolution                  │
│    • Scheduling loop                             │
│  - run_airflow_task Activity (activities.py)     │
│    • Load DAG from file                          │
│    • Execute task operator                       │
│    • Return result                               │
└─────────────────────────────────────────────────┘
```

**Components**: 2 processes (Temporal server + worker)
**Complexity**: Low (single config file)
**State Storage**: Temporal's durable state + in-memory SQLite per workflow

---

## What You DON'T Need (Eliminated)

### ❌ Airflow Database
- **Traditional**: Postgres/MySQL storing all state
- **Standalone**: Each workflow has ephemeral SQLite (in-memory)
- **Impact**: No database setup, no migrations, no connection pooling issues

### ❌ Airflow Scheduler
- **Traditional**: Continuous process parsing DAGs, creating runs
- **Standalone**: Temporal workflow IS the scheduler
- **Impact**: No scheduler downtime, no DAG parsing overhead

### ❌ Airflow Webserver
- **Traditional**: Flask app serving UI/API
- **Standalone**: Temporal UI for monitoring
- **Impact**: No webserver configuration, no RBAC setup

### ❌ Airflow CLI
- **Traditional**: `airflow dags trigger`, `airflow tasks test`, etc.
- **Standalone**: Python script submits workflows to Temporal
- **Impact**: Simpler deployment, no CLI installation

### ❌ Airflow Executor
- **Traditional**: LocalExecutor, CeleryExecutor, KubernetesExecutor
- **Standalone**: Temporal activities ARE the executor
- **Impact**: Native distributed execution, built-in retries

---

## What You DO Need (Required)

### ✅ 1. Temporal Server

**Options**:

**A) Local Development (Docker Compose)**
```bash
# Start Temporal server with UI
curl -L https://github.com/temporalio/docker-compose/archive/main.zip -o temporal.zip
unzip temporal.zip
cd docker-compose-main
docker-compose up
```

**B) Temporal Cloud (Managed)**
```bash
# No infrastructure management
# Production-ready SLA
# $0.000025 per workflow execution
```

**C) Self-Hosted (Kubernetes)**
```bash
# Helm chart available
helm install temporal temporalio/temporal
```

**What it provides**:
- Durable workflow state
- Activity task queues
- Retry/timeout management
- Web UI at http://localhost:8233

---

### ✅ 2. Temporal Worker (Your Python Code)

**File Structure**:
```
temporal_airflow/
├── workflows.py         # ExecuteAirflowDagWorkflow (orchestration)
├── activities.py        # run_airflow_task (execution)
├── models.py            # Pydantic models for I/O
├── time_provider.py     # Deterministic time injection
├── client_config.py     # Temporal client config using SDK envconfig
├── worker.py            # Worker startup with import pre-warming
└── submit_dag.py        # CLI to submit DAGs
```

**Worker Startup** (`worker.py`):
```python
"""Start Temporal worker for Airflow DAG execution."""

import asyncio
import logging
from temporalio.worker import Worker

from temporal_airflow.client_config import create_temporal_client, get_task_queue
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.activities import run_airflow_task

logger = logging.getLogger(__name__)

def _prewarm_imports():
    """Pre-import heavy modules to avoid Temporal's deadlock detection."""
    logger.info("Pre-warming imports...")
    # Pre-import heavy data processing libraries
    try:
        import pandas, numpy  # noqa: F401
    except ImportError:
        pass
    # Pre-import Airflow serialization (triggers plugin loading)
    from airflow.serialization.serialized_objects import SerializedDAG  # noqa: F401
    from airflow import plugins_manager
    plugins_manager.ensure_plugins_loaded()
    logger.info("Pre-warming complete")

async def main():
    _prewarm_imports()

    # Connect using SDK's envconfig (reads TEMPORAL_ADDRESS, etc.)
    client = await create_temporal_client()
    task_queue = get_task_queue()

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ExecuteAirflowDagWorkflow],
        activities=[run_airflow_task],
    )

    logger.info(f"Worker started on queue: {task_queue}")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

**Run Worker**:
```bash
# Set DAGs folder and start worker
AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags uv run python scripts/temporal_airflow/worker.py

# Or with Temporal Cloud
TEMPORAL_ADDRESS=my-namespace.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=my-namespace \
TEMPORAL_API_KEY=your-api-key \
AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags \
  uv run python scripts/temporal_airflow/worker.py
```

---

### ✅ 3. DAG Files (Standard Airflow)

**Location**: Accessible to Temporal workers (filesystem, S3, git)

**Example DAG** (`dags/my_data_pipeline.py`):
```python
"""Standard Airflow DAG - works in both Airflow and Standalone mode."""
from datetime import datetime
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task

with DAG(
    dag_id="my_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task(task_id="extract")
    def extract():
        """Extract data from source."""
        return [1, 2, 3, 4, 5]

    @task(task_id="transform")
    def transform(data):
        """Transform data."""
        return [x * 2 for x in data]

    @task(task_id="load")
    def load(data):
        """Load data to destination."""
        print(f"Loading: {data}")
        return len(data)

    # Define dependencies
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)
```

**Key Point**: DAG file is unchanged from standard Airflow!

---

### ✅ 4. Submit Script (Replaces Airflow CLI)

**File**: `submit_dag.py` (NEW)

```python
"""Submit Airflow DAG to Temporal for execution."""

import asyncio
from datetime import datetime
from pathlib import Path
from temporalio.client import Client
from airflow.serialization.serialized_objects import SerializedDAG

from temporal_airflow.models import DagExecutionInput
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.activities import load_dag_from_file


async def submit_dag(
    dag_id: str,
    dag_file: str,
    connections: dict[str, dict] | None = None,
    variables: dict[str, str] | None = None,
    conf: dict | None = None,
    wait: bool = False,
):
    """
    Submit DAG for execution on Temporal.

    Args:
        dag_id: DAG identifier
        dag_file: Relative path to DAG file (from DAGS_FOLDER)
        connections: Connection definitions (optional)
        variables: Variable definitions (optional)
        conf: DAG run configuration (optional)
        wait: Wait for workflow to complete

    Returns:
        Workflow handle
    """
    # Load and serialize DAG
    print(f"Loading DAG from {dag_file}...")
    dag = load_dag_from_file(dag_file, dag_id)
    serialized = SerializedDAG.to_dict(dag)

    # Create workflow input
    run_id = f"manual_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    input_data = DagExecutionInput(
        dag_id=dag_id,
        run_id=run_id,
        logical_date=datetime.utcnow(),
        serialized_dag=serialized,
        conf=conf,
        connections=connections,
        variables=variables,
    )

    # Connect to Temporal
    print("Connecting to Temporal...")
    client = await Client.connect("localhost:7233")

    # Start workflow
    workflow_id = f"dag-{dag_id}-{run_id}"
    print(f"Starting workflow: {workflow_id}")

    handle = await client.start_workflow(
        ExecuteAirflowDagWorkflow.run,
        input_data,
        id=workflow_id,
        task_queue="airflow-tasks",
    )

    print(f"✓ Workflow started!")
    print(f"  Workflow ID: {handle.id}")
    print(f"  Temporal UI: http://localhost:8233/namespaces/default/workflows/{handle.id}")

    if wait:
        print("\nWaiting for workflow to complete...")
        result = await handle.result()
        print(f"\n✓ Workflow completed!")
        print(f"  State: {result.state}")
        print(f"  Tasks succeeded: {result.tasks_succeeded}")
        print(f"  Tasks failed: {result.tasks_failed}")
        print(f"  Duration: {result.end_date - result.start_date}")
        return result

    return handle


# CLI Interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Submit Airflow DAG to Temporal")
    parser.add_argument("dag_id", help="DAG ID to execute")
    parser.add_argument("--dag-file", required=True, help="DAG file path")
    parser.add_argument("--wait", action="store_true", help="Wait for completion")
    parser.add_argument("--conf", help="JSON configuration")

    args = parser.parse_args()

    # Parse conf if provided
    conf = None
    if args.conf:
        import json
        conf = json.loads(args.conf)

    # Submit DAG
    asyncio.run(submit_dag(
        dag_id=args.dag_id,
        dag_file=args.dag_file,
        conf=conf,
        wait=args.wait,
    ))
```

**Usage**:
```bash
# Submit DAG (async)
python submit_dag.py my_data_pipeline --dag-file dags/my_data_pipeline.py

# Submit and wait for result
python submit_dag.py my_data_pipeline --dag-file dags/my_data_pipeline.py --wait

# With configuration
python submit_dag.py my_data_pipeline \
  --dag-file dags/my_data_pipeline.py \
  --conf '{"retries": 3}'
```

---

## Implementation Details (Completed)

### Change 1: Update Models (Add Connections/Variables)

**File**: `temporal_airflow/models.py`

**Changes**:
```python
from dataclasses import dataclass
from datetime import datetime
from typing import Any

@dataclass
class DagExecutionInput:
    """Input for ExecuteAirflowDagWorkflow."""
    dag_id: str
    run_id: str
    logical_date: datetime
    serialized_dag: dict
    conf: dict | None = None

    # NEW: Standalone mode support
    connections: dict[str, dict] | None = None
    """
    Connection definitions, keyed by connection ID.
    Example: {
        'postgres_default': {
            'conn_type': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'login': 'user',
            'password': 'secret',
            'schema': 'mydb',
            'extra': {}
        }
    }
    """

    variables: dict[str, str] | None = None
    """
    Variable definitions, keyed by variable name.
    Example: {'api_key': 'secret123', 'environment': 'prod'}
    """


@dataclass
class ActivityTaskInput:
    """Input for run_airflow_task activity."""
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    dag_rel_path: str
    upstream_results: dict[str, Any] | None = None

    # NEW: Standalone mode support
    variables: dict[str, str] | None = None
    connections: dict[str, dict] | None = None
```

**Testing**:
```python
# Test with connections
input_data = DagExecutionInput(
    dag_id="test_dag",
    run_id="test_run_1",
    logical_date=datetime(2025, 1, 1),
    serialized_dag=serialized,
    connections={
        'postgres_default': {
            'conn_type': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'login': 'airflow',
            'password': 'airflow',
            'schema': 'airflow',
        }
    },
    variables={
        'environment': 'test',
        'api_key': 'test_key_123',
    }
)
```

---

### Change 2: Update Workflow (Pass to Activities)

**File**: `temporal_airflow/workflows.py`

**Changes in `_scheduling_loop()` method**:
```python
# Around line 363 in workflows.py
activity_input = ActivityTaskInput(
    dag_id=ti.dag_id,
    task_id=ti.task_id,
    run_id=ti.run_id,
    try_number=ti.try_number,
    dag_rel_path=dag_rel_path,
    upstream_results=upstream_xcom,
    # NEW: Pass connections and variables
    variables=input.variables,      # From workflow input
    connections=input.connections,  # From workflow input
)
```

**Changes in `run()` method**:
```python
# Store input for access in scheduling loop
self.input = input  # NEW: Store input reference

# ... existing code ...
```

---

### Change 3: Update Activities (Make Available to Operators)

**File**: `temporal_airflow/activities.py`

**Add environment variable setup**:
```python
@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: ActivityTaskInput) -> TaskExecutionResult:
    """Execute a single Airflow task."""

    activity.logger.info(
        f"Starting task execution: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, try={input.try_number}, dag_path={input.dag_rel_path})"
    )

    try:
        # NEW: Set up connections as environment variables
        if input.connections:
            for conn_id, conn_data in input.connections.items():
                # Build connection URI
                conn_uri = _build_connection_uri(conn_data)
                # Set Airflow standard env var
                env_var = f"AIRFLOW_CONN_{conn_id.upper()}"
                os.environ[env_var] = conn_uri
                activity.logger.info(f"Set connection: {conn_id}")

        # NEW: Set up variables as environment variables
        if input.variables:
            for var_name, var_value in input.variables.items():
                # Set Airflow standard env var
                env_var = f"AIRFLOW_VAR_{var_name.upper()}"
                os.environ[env_var] = var_value
                activity.logger.info(f"Set variable: {var_name}")

        # ... existing DAG loading code ...

        # ... existing task execution code ...

    finally:
        # NEW: Clean up environment variables
        if input.connections:
            for conn_id in input.connections.keys():
                env_var = f"AIRFLOW_CONN_{conn_id.upper()}"
                os.environ.pop(env_var, None)

        if input.variables:
            for var_name in input.variables.keys():
                env_var = f"AIRFLOW_VAR_{var_name.upper()}"
                os.environ.pop(env_var, None)


def _build_connection_uri(conn_data: dict) -> str:
    """
    Build Airflow connection URI from connection data.

    Format: {conn_type}://[{login}[:{password}]@]{host}[:{port}][/{schema}][?{extra}]
    """
    conn_type = conn_data.get('conn_type', '')
    login = conn_data.get('login', '')
    password = conn_data.get('password', '')
    host = conn_data.get('host', '')
    port = conn_data.get('port', '')
    schema = conn_data.get('schema', '')
    extra = conn_data.get('extra', {})

    # Build URI parts
    uri_parts = [f"{conn_type}://"]

    if login:
        uri_parts.append(login)
        if password:
            uri_parts.append(f":{password}")
        uri_parts.append("@")

    if host:
        uri_parts.append(host)
        if port:
            uri_parts.append(f":{port}")

    if schema:
        uri_parts.append(f"/{schema}")

    if extra:
        import urllib.parse
        extra_str = urllib.parse.urlencode(extra)
        uri_parts.append(f"?{extra_str}")

    return "".join(uri_parts)
```

---

### Change 4: Create Worker Startup Script

**File**: `temporal_airflow/worker.py` (NEW)

```python
"""Start Temporal worker for Airflow DAG execution."""

import asyncio
import logging
import os
from temporalio.client import Client
from temporalio.worker import Worker

from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.activities import run_airflow_task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Start Temporal worker."""
    # Get configuration from environment
    temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "airflow-tasks")
    dags_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/files/dags")

    logger.info(f"Connecting to Temporal at {temporal_host}")
    logger.info(f"Task queue: {task_queue}")
    logger.info(f"DAGs folder: {dags_folder}")

    # Connect to Temporal
    client = await Client.connect(temporal_host)

    # Create worker
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ExecuteAirflowDagWorkflow],
        activities=[run_airflow_task],
    )

    logger.info("✓ Worker started successfully")
    logger.info(f"  Listening on task queue: {task_queue}")
    logger.info(f"  Workflow: ExecuteAirflowDagWorkflow")
    logger.info(f"  Activity: run_airflow_task")
    logger.info("")
    logger.info("Press Ctrl+C to stop")

    # Run worker (blocks until interrupted)
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n✓ Worker stopped")
```

---

## Usage Examples

### Example 1: Simple DAG Execution

**DAG File** (`dags/hello_world.py`):
```python
from datetime import datetime
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule=None,
) as dag:

    @task
    def say_hello():
        print("Hello from Temporal!")
        return "success"

    say_hello()
```

**Submit**:
```bash
python submit_dag.py hello_world --dag-file dags/hello_world.py --wait
```

**Output**:
```
Loading DAG from dags/hello_world.py...
Connecting to Temporal...
Starting workflow: dag-hello_world-manual_20251223_120000
✓ Workflow started!
  Workflow ID: dag-hello_world-manual_20251223_120000
  Temporal UI: http://localhost:8233/namespaces/default/workflows/dag-hello_world-manual_20251223_120000

Waiting for workflow to complete...

✓ Workflow completed!
  State: success
  Tasks succeeded: 1
  Tasks failed: 0
  Duration: 0:00:02.534
```

---

### Example 2: DAG with Database Connection

**DAG File** (`dags/postgres_etl.py`):
```python
from datetime import datetime
from airflow.sdk.definitions.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="postgres_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO users (name) VALUES
            ('Alice'), ('Bob'), ('Charlie');
        """
    )

    create_table >> insert_data
```

**Submit with Connection**:
```python
# submit_postgres_dag.py
import asyncio
from submit_dag import submit_dag

asyncio.run(submit_dag(
    dag_id="postgres_etl",
    dag_file="dags/postgres_etl.py",
    connections={
        'postgres_default': {
            'conn_type': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'login': 'airflow',
            'password': 'airflow',
            'schema': 'airflow',
        }
    },
    wait=True,
))
```

---

### Example 3: DAG with Variables

**DAG File** (`dags/api_pipeline.py`):
```python
from datetime import datetime
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task
from airflow.models import Variable

with DAG(
    dag_id="api_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
) as dag:

    @task
    def fetch_data():
        api_key = Variable.get('api_key')
        environment = Variable.get('environment')
        print(f"Fetching from {environment} with key: {api_key[:5]}...")
        return {"records": 100}

    @task
    def process_data(data):
        print(f"Processing {data['records']} records")
        return {"processed": data['records']}

    data = fetch_data()
    process_data(data)
```

**Submit with Variables**:
```python
# submit_api_dag.py
import asyncio
from submit_dag import submit_dag

asyncio.run(submit_dag(
    dag_id="api_pipeline",
    dag_file="dags/api_pipeline.py",
    variables={
        'api_key': 'sk_live_abc123xyz789',
        'environment': 'production',
    },
    wait=True,
))
```

---

## Deployment Guide

### Local Development Setup

**1. Start Temporal Server**:
```bash
# Using Docker Compose
git clone https://github.com/temporalio/docker-compose.git
cd docker-compose
docker-compose up
```

**2. Install Dependencies**:
```bash
pip install temporalio apache-airflow
```

**3. Set Environment**:
```bash
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags
export TEMPORAL_HOST=localhost:7233
```

**4. Start Worker**:
```bash
python temporal_airflow/worker.py
```

**5. Submit DAG**:
```bash
python submit_dag.py my_dag --dag-file dags/my_dag.py --wait
```

**6. Monitor**:
- Open Temporal UI: http://localhost:8233
- View workflow execution, logs, history

---

### Production Deployment

#### Option A: Kubernetes

**temporal-worker-deployment.yaml**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: temporal-airflow-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: temporal-airflow-worker
  template:
    metadata:
      labels:
        app: temporal-airflow-worker
    spec:
      containers:
      - name: worker
        image: my-org/temporal-airflow-worker:latest
        env:
        - name: TEMPORAL_HOST
          value: "temporal-frontend.temporal.svc.cluster.local:7233"
        - name: TEMPORAL_TASK_QUEUE
          value: "airflow-tasks"
        - name: AIRFLOW__CORE__DAGS_FOLDER
          value: "/dags"
        volumeMounts:
        - name: dags
          mountPath: /dags
          readOnly: true
      volumes:
      - name: dags
        configMap:
          name: airflow-dags
```

#### Option B: Temporal Cloud

```python
# worker.py with Temporal Cloud
client = await Client.connect(
    target_host="my-namespace.tmprl.cloud:7233",
    namespace="my-namespace",
    tls=TLSConfig(
        client_cert=open("client.pem", "rb").read(),
        client_private_key=open("client-key.pem", "rb").read(),
    ),
)
```

---

## Benefits of Standalone Mode

### ✅ Simplified Infrastructure
- 2 processes instead of 5+
- No database setup/migrations
- No webserver configuration
- Single configuration file

### ✅ Better Reliability
- Temporal's proven durability
- Automatic retries and timeouts
- No scheduler downtime
- Worker failure recovery

### ✅ Superior Scalability
- Workers scale independently
- No scheduler bottleneck
- No database contention
- Temporal handles millions of workflows

### ✅ Improved Observability
- Temporal UI shows complete workflow history
- Activity logs captured automatically
- Replay capability for debugging
- Built-in metrics and tracing

### ✅ Cost Efficiency
- Fewer processes to run
- No database hosting costs
- Temporal Cloud pay-per-use
- Less operational overhead

---

## Tradeoffs and Limitations

### ❌ No Airflow UI
- **Impact**: Can't use familiar Airflow UI
- **Mitigation**: Temporal UI provides similar functionality
- **Alternative**: Build custom UI querying Temporal API

### ❌ No DAG File Auto-Discovery
- **Impact**: Must explicitly submit DAGs
- **Mitigation**: Build DAG watcher service (simple Python script)
- **Alternative**: Submit on schedule via cron or Temporal Schedules

### ❌ No Built-in Scheduler
- **Impact**: No cron schedules out of box
- **Mitigation**: Use Temporal Schedules feature
- **Example**:
  ```python
  # Create schedule for daily execution
  await client.create_schedule(
      "daily-etl",
      Schedule(
          action=ScheduleActionStartWorkflow(
              ExecuteAirflowDagWorkflow.run,
              input_data,
              task_queue="airflow-tasks",
          ),
          spec=ScheduleSpec(
              cron_expressions=["0 2 * * *"],  # Daily at 2am
          ),
      ),
  )
  ```

### ❌ Variables/Connections Not Persistent
- **Impact**: Must pass on each submission
- **Mitigation**: Use external secret store (AWS Secrets Manager, Vault)
- **Pattern**: Activities fetch from secret store, not from workflow input

### ❌ Limited Airflow Ecosystem Integration
- **Impact**: Some provider features may not work
- **Examples**: Airflow plugins, custom auth managers, dataset triggers
- **Mitigation**: Most operators work fine; test before migration

---

## Migration Path

### From Airflow to Standalone

**Step 1: Validate DAGs**
```bash
# Test DAG loads correctly
python -c "from temporal_airflow.activities import load_dag_from_file; \
           dag = load_dag_from_file('my_dag.py', 'my_dag'); \
           print(f'✓ DAG loaded: {dag.dag_id}')"
```

**Step 2: Extract Connections/Variables**
```bash
# Export from Airflow DB
airflow connections export connections.json
airflow variables export variables.json

# Convert to submit script format
python convert_connections.py connections.json > connections.py
```

**Step 3: Test in Standalone**
```bash
# Submit one DAG
python submit_dag.py my_dag --dag-file dags/my_dag.py --wait
```

**Step 4: Gradual Migration**
- Run both Airflow and Standalone in parallel
- Migrate DAGs one by one
- Validate results match
- Decommission Airflow when done

---

## Implementation Status

All phases of Standalone Mode are now complete:

1. ✅ **Phase 1-3 Complete** (Time provider, executor pattern, tests)
2. ✅ **Phase 4: Connections/Variables Support**
   - Added `connections` and `variables` fields to `DagExecutionInput` and `ActivityTaskInput`
   - Workflow stores and passes connections/variables to activities
   - Activities set `AIRFLOW_CONN_*` and `AIRFLOW_VAR_*` environment variables
3. ✅ **Phase 5: Submission Interface**
   - Created `submit_dag.py` CLI script
   - Created `worker.py` startup script with import pre-warming
   - Created `client_config.py` using Temporal SDK's envconfig
4. ✅ **Phase 6: Testing**
   - All 37 tests pass
   - Tested with parallel DAGs, sequential DAGs, mixed success/failure
5. ✅ **Phase 7: Documentation**
   - This document updated with actual usage

---

## Conclusion

Standalone Mode offers a **simpler, more reliable, and more scalable** way to execute Airflow DAGs by leveraging Temporal's proven orchestration engine. While it trades off Airflow's UI and built-in scheduling for Temporal's superior durability and observability, it's an excellent choice for teams that:

- Want simpler infrastructure
- Value reliability over familiarity
- Need better scalability
- Are comfortable with code-first workflows
- Want to avoid database management

The implementation is straightforward, requires minimal changes to existing code, and provides a clear migration path from traditional Airflow.
