# Airflow with Temporal Orchestration

This guide explains how to run Apache Airflow with Temporal as the execution engine. Instead of Airflow's traditional scheduler and executors, Temporal workflows orchestrate DAG execution while preserving the familiar Airflow UI experience.

## Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        STANDARD AIRFLOW                               │
│   User → Airflow UI/API → Scheduler → Executor → Worker → Tasks      │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW + TEMPORAL                                 │
│   User → Airflow API Server ──→ Temporal Workflow                    │
│              (creates DagRun)       ↓                                │
│                    Airflow DB ← Temporal Worker → Tasks              │
└──────────────────────────────────────────────────────────────────────┘
```

**Key difference:** When the API server (or scheduler) creates a DagRun, the configured `TemporalOrchestrator` routes it directly to Temporal for execution.

**Benefits:**
- Temporal handles retries, timeouts, and failure recovery
- Full visibility in Temporal UI for debugging workflows
- Airflow UI shows familiar DAG status (synced from Temporal in real-time)
- Connections and Variables work from Airflow DB
- Concurrent DAG execution tested and verified

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git (to clone the repository)
- ~4GB available memory for containers

### Step 1: Clone and Build

```bash
# Clone the Airflow repository with Temporal support
git clone https://github.com/mfateev/airflow.git
cd airflow
git checkout temporal-deep-scheduler

# Build the Airflow image with Temporal support
docker build -f docs/temporal/Dockerfile.temporal -t airflow-temporal:latest .
```

### Step 2: Set Up Project Directory

```bash
# Create project directory
mkdir -p ~/airflow-temporal/{dags,logs,scripts}
cd ~/airflow-temporal

# Copy required files
cp /path/to/airflow/docs/temporal/docker-compose-temporal.yaml .
cp -r /path/to/airflow/scripts/temporal_airflow scripts/
```

**Note:** Do NOT set `AIRFLOW_UID` to your local user ID. The container must run as user 50000 (the airflow user) for the Python installation to work correctly.

### Step 3: Create a Sample DAG

```bash
cat > dags/example_dag.py << 'EOF'
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello from Temporal!")
    return "hello_result"

def world():
    print("World!")
    return "world_result"

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="hello", python_callable=hello)
    t2 = PythonOperator(task_id="world", python_callable=world)
    t1 >> t2
EOF
```

### Step 4: Start Services

```bash
docker-compose -f docker-compose-temporal.yaml up -d
```

Wait for services to be healthy (~30-60 seconds):
```bash
docker-compose -f docker-compose-temporal.yaml ps
```

### Step 5: Access the UIs

- **Airflow UI**: http://localhost:8080
- **Temporal UI**: http://localhost:8233

**Login credentials:** The Simple Auth Manager auto-generates passwords on first startup.
```bash
# Get the admin password
docker exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```
Username: `admin`, password: value from the JSON output above.

## Triggering DAGs

### Option A: Via Airflow UI (Recommended)

The docker-compose includes the Temporal orchestrator configured on both the API server and scheduler. DAGs triggered from the UI are automatically executed via Temporal.

1. Navigate to http://localhost:8080
2. Find your DAG (e.g., `example_dag`)
3. Toggle the DAG to "Active" (unpause it)
4. Click the "Play" button → "Trigger DAG"

**What happens behind the scenes:**
1. Airflow creates a DagRun in the database
2. The `TemporalOrchestrator` intercepts the DagRun
3. A Temporal workflow (`execute_airflow_dag_deep`) is started
4. Task status is synced back to Airflow DB for UI visibility

### Option B: Via Airflow CLI

```bash
# Trigger via CLI
docker exec airflow-apiserver airflow dags trigger example_dag

# Check status
docker exec airflow-apiserver airflow dags list-runs -d example_dag
```

### Option C: Via Temporal CLI (Direct Submission)

You can bypass Airflow entirely and submit DAGs directly to Temporal:

```bash
# Enter the worker container
docker exec -it temporal-worker bash

# Submit DAG directly to Temporal
python /opt/airflow/scripts/temporal_airflow/submit_dag.py \
    example_dag \
    --dag-file dags/example_dag.py \
    --wait
```

This method:
- Doesn't require the Airflow scheduler
- Creates DagRun records in Airflow DB for UI visibility
- Good for testing and development

### Option D: Via Airflow REST API

```bash
# Trigger DAG via API
curl -X POST "http://localhost:8080/api/v2/dags/example_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'

# Check status
curl "http://localhost:8080/api/v2/dags/example_dag/dagRuns"
```

## Monitoring Execution

### In Airflow UI

1. Go to http://localhost:8080
2. Click on your DAG name
3. View the Grid or Graph tab to see task status
4. Click on a task instance for details

The status is synced from Temporal in real-time via sync activities.

### In Temporal UI

1. Go to http://localhost:8233
2. Click "Workflows" in the sidebar
3. Find your workflow (format: `airflow-{dag_id}-{run_id}`)
4. View:
   - Workflow history and events
   - Activity execution details
   - Retry attempts and failures
   - Input/output payloads

## Architecture

### Services

| Service | Port | Description |
|---------|------|-------------|
| `temporal` | 7233 (gRPC), 8233 (UI) | Temporal server (dev mode with SQLite) |
| `postgres` | 5432 | Airflow metadata database |
| `airflow-apiserver` | 8080 | Airflow UI/API - routes DagRuns to Temporal |
| `airflow-dag-processor` | - | Parses DAG files, serializes to DB |
| `airflow-scheduler` | - | Evaluates timetables, creates scheduled DagRuns |
| `temporal-worker` | - | Executes Temporal workflows/activities |

### Components

```
scripts/temporal_airflow/
├── orchestrator.py      # TemporalOrchestrator - routes DagRuns to Temporal
├── deep_workflow.py     # ExecuteAirflowDagDeepWorkflow - main workflow
├── deep_worker.py       # Worker startup with import pre-warming
├── activities.py        # run_airflow_task - executes individual tasks
├── sync_activities.py   # DB sync activities for Airflow UI visibility
├── models.py            # Pydantic models for workflow/activity I/O
├── client_config.py     # Temporal client configuration
└── submit_dag.py        # CLI for direct DAG submission
```

### Data Flow

```
1. DAG Trigger (UI/CLI/API)
   └─→ API Server/Scheduler calls dag.create_dagrun()
       └─→ TemporalOrchestrator.start_dagrun()
           └─→ Starts Temporal workflow (async)
               └─→ Returns immediately with DagRun info

2. Temporal Workflow Execution (async, in background)
   ├─→ load_serialized_dag (reads DAG structure from Airflow DB)
   ├─→ Creates in-workflow SQLite DB for Airflow's scheduling logic
   ├─→ ensure_task_instances (creates TaskInstance records in DB)
   ├─→ Scheduling loop using dag_run.update_state():
   │   ├─→ Airflow's TriggerRuleDep evaluates which tasks are ready
   │   ├─→ run_airflow_task (execute task in ThreadPoolExecutor)
   │   └─→ sync_task_status_batch (batch sync to Airflow DB)
   └─→ sync_dagrun_status (mark DagRun complete in Airflow DB)

3. Airflow UI reads status from DB (continuously updated by workflow)
```

### Key Design Decisions

1. **In-Workflow Database**: Each workflow creates an in-memory SQLite database to run Airflow's native scheduling logic (`dag_run.update_state()`, `TriggerRuleDep`). This ensures trigger rules work correctly.

2. **Sandboxed=False**: The workflow runs with `sandboxed=False` to allow Airflow's complex module system to work without serialization issues.

3. **ThreadPoolExecutor for Activities**: Sync activities (database operations) run in a ThreadPoolExecutor to avoid blocking Temporal's event loop.

4. **Import Pre-warming**: The worker pre-warms Python imports and deserializes a sample DAG at startup to avoid Temporal's deadlock detector triggering on cold imports (~1.4s).

5. **Batched Syncs**: Task status updates are batched into single DB transactions to reduce round-trips.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `temporal:7233` | Temporal server address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `airflow-tasks` | Task queue name |
| `AIRFLOW__CORE__ORCHESTRATOR` | `DefaultOrchestrator` | Set to `temporal_airflow.orchestrator.TemporalOrchestrator` |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | - | PostgreSQL connection string |

### Airflow Configuration

To enable Temporal orchestration for UI-triggered DAGs, add to `airflow.cfg`:

```ini
[core]
orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator
```

Or via environment variable:
```bash
export AIRFLOW__CORE__ORCHESTRATOR="temporal_airflow.orchestrator.TemporalOrchestrator"
```

## Common Tasks

### View Logs

```bash
# All services
docker-compose -f docker-compose-temporal.yaml logs -f

# Specific service
docker-compose -f docker-compose-temporal.yaml logs -f temporal-worker
```

### Check Service Health

```bash
docker-compose -f docker-compose-temporal.yaml ps
```

### Restart Services

```bash
docker-compose -f docker-compose-temporal.yaml restart
```

### Stop Everything

```bash
docker-compose -f docker-compose-temporal.yaml down
```

### Stop and Remove Data

```bash
docker-compose -f docker-compose-temporal.yaml down -v
```

## Troubleshooting

### Container Name Conflict

If you see an error like:
```
Error response from daemon: Conflict. The container name "/temporal" is already in use
```

Stop and remove existing containers first:
```bash
docker-compose -f docker-compose-temporal.yaml down
```

Or remove all related containers:
```bash
docker rm -f temporal postgres airflow-apiserver airflow-dag-processor airflow-scheduler temporal-worker airflow-init 2>/dev/null; docker-compose -f docker-compose-temporal.yaml up -d
```

### DAG Not Appearing in UI

1. Check the DAG file is in `./dags/` directory
2. Check DAG processor logs:
   ```bash
   docker-compose -f docker-compose-temporal.yaml logs airflow-dag-processor
   ```
3. Ensure there are no Python syntax errors in the DAG file

### Workflow Not Starting

1. Check Temporal worker is running:
   ```bash
   docker-compose -f docker-compose-temporal.yaml logs temporal-worker
   ```
2. Verify Temporal server is healthy:
   ```bash
   docker exec temporal temporal operator cluster health
   ```

### Task Failing

1. Check Temporal UI for detailed error messages
2. Look at workflow history for activity failures
3. Check worker logs:
   ```bash
   docker-compose -f docker-compose-temporal.yaml logs temporal-worker | grep -i error
   ```

### Deadlock Errors (TMPRL1101)

If you see deadlock warnings like "Potential deadlock detected", this is usually caused by:
1. Cold imports during first execution (resolved by pre-warming)
2. Stale worker state from previous runs

**Fix:** Restart the worker container:
```bash
docker restart temporal-worker
```

### Connection/Variable Not Found

Connections and Variables are read from Airflow DB during task execution. Add them via:

```bash
# Via CLI
docker exec airflow-apiserver airflow connections add my_conn --conn-type postgres --conn-host localhost

# Or via UI
# Go to Admin → Connections
```

## Development

### Running Tests

```bash
# Run temporal_airflow tests
docker exec temporal-worker python -m pytest \
    /opt/airflow/scripts/temporal_airflow/tests/ -v

# Run E2E tests with real Temporal server
docker exec airflow-apiserver python -m pytest \
    /opt/airflow/scripts/temporal_airflow/tests/test_airflow_user_experience.py -v
```

### Modifying the Worker

After making changes to `scripts/temporal_airflow/`:

```bash
# Copy updated files
docker cp scripts/temporal_airflow/. temporal-worker:/opt/airflow/scripts/temporal_airflow/

# Restart worker
docker restart temporal-worker
```

### Stress Testing

The integration supports concurrent DAG execution:

```bash
# Run multiple DAGs concurrently via Temporal CLI
for i in {1..5}; do
  docker exec temporal-worker python /opt/airflow/scripts/temporal_airflow/submit_dag.py \
      example_dag --dag-file dags/example_dag.py &
done
wait
```

## Current Limitations

1. **Development Mode Only**: The docker-compose uses Temporal's dev server with SQLite. For production, use Temporal Cloud or a production Temporal deployment with PostgreSQL.

2. **No Sensors/Triggers**: Deferrable operators and sensors are not yet implemented in the Temporal integration.

3. **Experimental**: This is a prototype integration. Some Airflow features may not work as expected. Report issues at the GitHub repository.

## Further Reading

- [Deep Integration Design](DEEP_INTEGRATION_DESIGN.md) - Architecture details
- [Standalone Mode](STANDALONE_MODE.md) - Running without Airflow DB
- [Implementation Plan](DEEP_INTEGRATION_IMPLEMENTATION_PLAN.md) - Development roadmap
- [Temporal Documentation](https://docs.temporal.io/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
