# Getting Started: Airflow with Temporal Orchestrator

This guide shows how to run Airflow DAGs using Temporal for orchestration instead of the traditional Airflow scheduler.

## Prerequisites

- Docker and Docker Compose
- 4GB+ RAM available for Docker
- Clone of the Apache Airflow repository (this is Airflow 3.x with Temporal support)

## Quick Start

### 1. Build the Airflow-Temporal Image

From the Airflow repository root, build the Docker image:

```bash
cd /path/to/airflow
docker build -f docs/temporal/Dockerfile.temporal -t airflow-temporal:latest .
```

This builds an Airflow 3.x image with Temporal SDK pre-installed.

### 2. Create Project Directory

```bash
mkdir -p airflow-temporal/dags airflow-temporal/logs airflow-temporal/scripts
cd airflow-temporal
```

### 3. Copy Required Files

```bash
# Copy docker-compose file
cp /path/to/airflow/docs/temporal/docker-compose-temporal.yaml .

# Copy Temporal worker scripts
cp -r /path/to/airflow/scripts/temporal_airflow scripts/
```

### 4. Create a Sample DAG

Create `dags/example_dag.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello from Temporal!")
    return "success"

def world():
    print("World!")
    return "done"

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="hello", python_callable=hello)
    t2 = PythonOperator(task_id="world", python_callable=world)
    t1 >> t2
```

### 5. Start Services

```bash
docker-compose -f docker-compose-temporal.yaml up -d
```

Wait for all services to be healthy:

```bash
docker-compose -f docker-compose-temporal.yaml ps
```

### 6. Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Temporal UI | http://localhost:8233 | (none required) |

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         User                                 │
│                          │                                   │
│                          ▼                                   │
│                   ┌─────────────┐                           │
│                   │ Airflow UI  │ (trigger DAG)             │
│                   │ :8080       │                           │
│                   └──────┬──────┘                           │
│                          │                                   │
│                          ▼                                   │
│              ┌───────────────────────┐                      │
│              │      PostgreSQL       │                      │
│              │   (Airflow metadata)  │                      │
│              │  - serialized_dag     │                      │
│              │  - dag_run state      │                      │
│              │  - connections        │                      │
│              └───────────┬───────────┘                      │
│                          │                                   │
│         ┌────────────────┴────────────────┐                 │
│         │                                 │                 │
│         ▼                                 ▼                 │
│  ┌──────────────┐                ┌──────────────────┐      │
│  │ DAG Processor│                │ Temporal Server  │      │
│  │ (parse DAGs) │                │ :7233 (API)      │      │
│  └──────────────┘                │ :8233 (UI)       │      │
│                                  └────────┬─────────┘      │
│                                           │                 │
│                                           ▼                 │
│                                  ┌──────────────────┐      │
│                                  │ Temporal Worker  │      │
│                                  │ - Deep Workflow  │      │
│                                  │ - Sync Activities│      │
│                                  │ - Task Execution │      │
│                                  └──────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Components

| Component | Role |
|-----------|------|
| **Temporal Server** | Orchestrates workflow execution, handles retries, maintains durable state |
| **PostgreSQL** | Stores Airflow metadata (serialized DAGs, connections, dag_run state for UI) |
| **Airflow Webserver** | Provides Airflow UI for monitoring and triggering DAGs |
| **DAG Processor** | Parses DAG files and stores serialized DAGs in database |
| **Temporal Worker** | Executes DAGs via `ExecuteAirflowDagDeepWorkflow` |

### What's Different from Standard Airflow

| Standard Airflow | Temporal Airflow |
|-----------------|------------------|
| Scheduler polls DB | Temporal orchestrates workflows |
| Celery/K8s executor | Temporal activities execute tasks |
| Redis message broker | Temporal server handles task queuing |
| Scheduler bottleneck | Horizontal worker scaling |

## Triggering DAGs

### Via Airflow UI

1. Go to http://localhost:8080
2. Find your DAG in the list
3. Click the "Play" button to trigger

### Via Airflow CLI

```bash
docker-compose -f docker-compose-temporal.yaml exec airflow-webserver \
  airflow dags trigger example_dag
```

### Via Temporal CLI

```bash
# Install Temporal CLI
brew install temporal  # macOS
# or download from https://github.com/temporalio/cli

# Trigger DAG via workflow
temporal workflow start \
  --task-queue airflow-tasks \
  --type ExecuteAirflowDagDeepWorkflow \
  --input '{"dag_id": "example_dag", "logical_date": "2025-01-01T00:00:00Z"}'
```

## Monitoring

### Airflow UI (http://localhost:8080)

- View DAG runs and task states
- See logs (synced from Temporal)
- Trigger new runs

### Temporal UI (http://localhost:8233)

- View workflow execution history
- See detailed activity logs
- Debug failed workflows with full event history
- Replay workflows for debugging

## Configuration

### Environment Variables

Set these in your `.env` file or docker-compose:

```bash
# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Temporal
TEMPORAL_ADDRESS=temporal:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=airflow-tasks
```

### Adding Connections

```bash
docker-compose -f docker-compose-temporal.yaml exec airflow-webserver \
  airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'airflow'
```

### Adding Variables

```bash
docker-compose -f docker-compose-temporal.yaml exec airflow-webserver \
  airflow variables set my_variable "my_value"
```

## Scaling Workers

Scale Temporal workers for more parallelism:

```bash
docker-compose -f docker-compose-temporal.yaml up -d --scale temporal-worker=3
```

## Stopping Services

```bash
# Stop all services
docker-compose -f docker-compose-temporal.yaml down

# Stop and remove volumes (clean slate)
docker-compose -f docker-compose-temporal.yaml down -v
```

## Troubleshooting

### Check Service Logs

```bash
# All services
docker-compose -f docker-compose-temporal.yaml logs -f

# Specific service
docker-compose -f docker-compose-temporal.yaml logs -f temporal-worker
```

### Common Issues

**Worker can't connect to Temporal:**
```bash
# Check Temporal is healthy
docker-compose -f docker-compose-temporal.yaml exec temporal temporal operator cluster health
```

**DAG not appearing in UI:**
```bash
# Check DAG processor logs
docker-compose -f docker-compose-temporal.yaml logs airflow-dag-processor

# Verify DAG syntax
docker-compose -f docker-compose-temporal.yaml exec airflow-webserver \
  python -c "from airflow.models import DagBag; db = DagBag('/opt/airflow/dags'); print(db.import_errors)"
```

**Workflow failed:**
1. Check Temporal UI at http://localhost:8233
2. Find the failed workflow
3. Click to see detailed event history and error messages

## Next Steps

- [Deep Integration Design](DEEP_INTEGRATION_DESIGN.md) - Architecture details
- [Standalone Mode](STANDALONE_MODE.md) - Run without Airflow infrastructure
- [Testing Procedure](TESTING_PROCEDURE.md) - How to test changes
