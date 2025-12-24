# Temporal-Airflow Integration Refactoring Plan

## Executive Summary

Refactor the Temporal integration from **operator serialization pattern** (broken) to **executor worker pattern** (standard Airflow approach). This aligns with how LocalExecutor, KubernetesExecutor, and all production executors work.

**ARCHITECTURE NOTE:** This integration uses a **per-workflow in-memory database** (not shared external database). Each workflow instance maintains its own SQLite/Postgres in-memory DB. State persists through Temporal replay, not external storage. Synchronous DB operations are acceptable as they're not external IO.

## Current Architecture (Broken)

```
Workflow → serialize_operator() → SerializedBaseOperator dict
    ↓
Activity → deserialize_operator() → ❌ No execute() method
    ↓
Manual reconstruction → ❌ Missing attributes (op_kwargs, etc.)
```

**Problems:**
- SerializedBaseOperator is scheduler metadata, not executable
- Callables stored as strings: `"<function at 0x...>"`
- Complex deserialization with missing attributes
- Not using standard Airflow execution path

## Target Architecture (Executor Pattern with In-Memory Database)

```
Workflow (Scheduler Role + In-Memory DB)
    ↓
Creates per-workflow in-memory database
    ↓
Creates TaskInstance records in DB
    ↓
Creates ExecuteTask workload (JSON-serializable metadata)
    ↓
Sends workload to Activity (could be remote machine)
    ↓
Activity (Executor Worker Role - NO DB ACCESS)
    ↓
Loads DAG file → gets real operator → execute()
    ↓
Returns result as JSON (state, output, etc.)
    ↓
Workflow receives result
    ↓
Workflow updates TaskInstance state in DB (synchronous)
```

**Benefits:**
- ✅ Uses standard Airflow execution path (DAG loading + task execution)
- ✅ No operator serialization complexity
- ✅ Real operators with real callables
- ✅ Full feature support (callbacks, context, XCom, etc.)
- ✅ Simpler code, easier debugging
- ✅ Per-workflow isolation (no external DB contention)
- ✅ State persists through Temporal replay (no durable storage needed)
- ✅ Activities can run on remote workers (only JSON parameters)
- ✅ Clean separation: workflow manages state, activities execute tasks

## Key Changes

### 1. Data Flow

**Before:**
```python
# Workflow
serialized_task = SerializedBaseOperator.serialize_operator(task)
await workflow.execute_activity(run_task, TaskExecutionInput(
    serialized_task=serialized_task
))

# Activity
task = deserialize_operator(input.serialized_task)  # Broken!
result = task.execute(context)
```

**After (In-Memory Database Pattern):**
```python
# Workflow (has DB access)
class DagRunWorkflow:
    def __init__(self):
        # Create per-workflow in-memory database
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    async def run_task(self, task_id: str):
        session = self.Session()

        # Create TaskInstance in in-memory DB (workflow manages state)
        ti = TaskInstance(
            dag_id=self.dag_id,
            task_id=task_id,
            run_id=self.run_id,
            state="queued",
        )
        session.add(ti)
        session.commit()

        # Create workload (JSON-serializable metadata only)
        workload = ExecuteTask(
            ti=ti,  # TaskInstance is serializable
            dag_rel_path=Path("dags/my_dag.py"),
            bundle_info=BundleInfo(name="dags"),
            token="",  # Not needed
            log_path="logs/..."
        )

        # Update state before sending to activity
        ti.state = "running"
        session.commit()

        # Send workload to activity (could be remote machine)
        result = await workflow.execute_activity(
            execute_task,
            args=[workload]  # ONLY JSON-serializable data
        )

        # Workflow updates DB based on activity result
        ti.state = result["state"]
        if "xcom_value" in result:
            xcom = XCom(
                task_id=ti.task_id,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                value=result["xcom_value"],
            )
            session.add(xcom)
        session.commit()

        return ti.state

# Activity (NO DB access - can run on remote machine)
@activity.defn
async def execute_task(workload: ExecuteTask) -> dict:
    """Execute task and return JSON result."""
    # Load DAG from file (workers have access to DAG directory)
    dag = load_dag_from_file(workload.dag_rel_path)
    task = dag.task_dict[workload.ti.task_id]

    # Create minimal context (no DB required)
    context = {
        "dag": dag,
        "task": task,
        "dag_run": {"run_id": workload.ti.run_id},
        "ti": workload.ti,
        # Add other context as needed
    }

    try:
        # Execute task (pure computation, no DB)
        result = task.execute(context)

        # Return JSON result to workflow
        return {
            "state": "success",
            "xcom_value": result,  # Must be JSON-serializable
        }
    except Exception as e:
        # Return failure state
        return {
            "state": "failed",
            "error": str(e),
        }
```

### 2. Component Changes

#### A. Models (`models.py`)

**Current:**
```python
class TaskExecutionInput(BaseModel):
    dag_id: str
    task_id: str
    run_id: str
    serialized_task: dict  # ❌ Broken approach
```

**Target:**
```python
from airflow.executors.workloads import ExecuteTask

# Use standard Airflow workload (no custom model needed!)
# ExecuteTask already contains:
#   - ti: TaskInstance (metadata)
#   - dag_rel_path: Path
#   - bundle_info: BundleInfo
#   - token: str
#   - log_path: str
```

#### B. Workflow (`workflows.py`)

**Changes:**
1. **Remove:** `SerializedBaseOperator.serialize_operator()`
2. **Add:** Create in-memory database in workflow `__init__`
3. **Add:** Create `ExecuteTask` workloads (JSON-serializable)
4. **Add:** Update DB state based on activity results
5. **Remove:** Any attempt to pass DB session to activities

**New responsibilities:**
- Initialize per-workflow in-memory database
- Load DAG from file (or create programmatically)
- Create TaskInstance records in in-memory database
- Generate ExecuteTask workloads (JSON-serializable metadata)
- Send workloads to activities (remote execution possible)
- Receive JSON results from activities
- Update TaskInstance state in DB based on results
- Store XCom values returned from activities

**Key architecture points:**
- Workflow has EXCLUSIVE access to in-memory DB
- Activities receive ONLY JSON-serializable parameters
- Activities return ONLY JSON-serializable results
- Workflow is source of truth for all state
- No polling needed - workflow updates state immediately upon activity completion

#### C. Activities (`activities.py`)

**Changes:**
1. **Remove:** All deserialization code (~100 lines)
2. **Replace with:** DAG loading + task execution
3. **Remove:** Any DB access code
4. **Add:** Return JSON results to workflow

**New implementation:**
```python
from airflow.sdk.definitions.dag import DAG
from airflow.executors.workloads import ExecuteTask
from pathlib import Path

@activity.defn
async def execute_airflow_task(workload: ExecuteTask) -> dict:
    """Execute task and return JSON result.

    This activity can run on a remote machine. It receives only
    JSON-serializable parameters and returns only JSON-serializable results.
    NO DATABASE ACCESS - workflow manages all state.
    """
    activity.logger.info(
        f"Executing {workload.ti.dag_id}.{workload.ti.task_id}"
    )

    try:
        # Load DAG from file (workers have access to DAG directory)
        dag = load_dag_from_file(workload.dag_rel_path)
        task = dag.task_dict[workload.ti.task_id]

        # Create minimal execution context (no DB required)
        context = {
            "dag": dag,
            "task": task,
            "dag_run": {
                "dag_id": workload.ti.dag_id,
                "run_id": workload.ti.run_id,
            },
            "ti": workload.ti,
            # TODO: Add other context fields as needed
        }

        # Execute task (pure computation)
        result = task.execute(context)

        # Return JSON-serializable result to workflow
        return {
            "state": "success",
            "xcom_value": result,  # Must be JSON-serializable
            "task_id": workload.ti.task_id,
        }

    except Exception as e:
        activity.logger.error(f"Task execution failed: {e}", exc_info=True)

        # Return failure state (workflow will update DB)
        return {
            "state": "failed",
            "error": str(e),
            "error_type": type(e).__name__,
            "task_id": workload.ti.task_id,
        }
```

**Key architecture points:**
- Activities have NO database access
- Activities receive ONLY ExecuteTask workload (JSON-serializable)
- Activities return ONLY JSON dict (state, result, error)
- Activities can run on remote machines
- Workflow updates DB state based on returned results
- Clean separation: activities do computation, workflow manages state

### 3. Infrastructure Requirements

#### A. DAG File Access

Activities need access to DAG files. Options:

**Option 1: Shared Volume (Simplest for local development)**
```python
# docker-compose.yml
services:
  temporal-worker:
    volumes:
      - ./dags:/opt/airflow/dags:ro  # Read-only DAG files
```

**Option 2: S3/GCS (Production)**
```python
# Activities download DAG files from S3
dag_content = s3.get_object(Bucket="dags", Key=workload.dag_rel_path)
```

**Option 3: DAG Bundle (Airflow 3.0 feature)**
```python
# Use bundle_info to fetch versioned DAGs
dag = load_dag_from_bundle(workload.bundle_info, workload.dag_rel_path)
```

#### B. Execution API & State Management

**ARCHITECTURE NOTE:** This integration uses **per-workflow in-memory database** (not shared external database). State persists through Temporal replay, not external storage. **Activities have NO database access** - they can only return JSON results to workflow.

**State Management Pattern:**

Since activities cannot access the workflow's in-memory database:

1. **Workflow manages ALL state** in its in-memory DB
2. **Activities execute tasks** and return JSON results
3. **Workflow updates state** based on activity results

**Implementation (Direct Task Execution):**
```python
# Activity: Execute task, return result (NO DB access)
@activity.defn
async def execute_airflow_task(workload: ExecuteTask) -> dict:
    """Execute task and return JSON result."""
    # Load DAG from file
    dag = load_dag_from_file(workload.dag_rel_path)
    task = dag.task_dict[workload.ti.task_id]

    # Create context (no DB required)
    context = {
        "dag": dag,
        "task": task,
        "ti": workload.ti,
        # Add other context fields as needed
    }

    # Execute task (pure computation)
    result = task.execute(context)

    # Return JSON result (workflow will update DB)
    return {
        "state": "success",
        "xcom_value": result,
    }

# Workflow: Update DB based on activity result
async def run_task(self, task_id: str):
    session = self.Session()

    # Create TaskInstance in DB
    ti = TaskInstance(...)
    session.add(ti)
    session.commit()

    # Update state before activity
    ti.state = "running"
    session.commit()

    # Execute activity (remote, no DB access)
    result = await workflow.execute_activity(execute_airflow_task, workload)

    # Update state based on result
    ti.state = result["state"]
    if "xcom_value" in result:
        xcom = XCom(value=result["xcom_value"], ...)
        session.add(xcom)
    session.commit()
```

**Why NOT use supervise():**
- `supervise()` expects HTTP Execution API to report state
- Activities cannot access workflow's in-memory DB
- Simpler to execute task directly and return result
- Workflow has exclusive control over state management

#### C. Database Access

**IMPLEMENTED PATTERN: Per-Workflow In-Memory Database (Workflow Only)**

This integration uses a **local in-memory database per workflow** with the following characteristics:

**Architecture:**
- Each workflow instance has its own in-memory SQLite/Postgres database
- Database is NOT durable - state recovered through Temporal replay
- **ONLY workflow code** can access the database
- **Activities have NO database access** (they run on remote workers)
- Activities receive JSON parameters and return JSON results
- Workflow updates DB based on activity results

**Benefits:**
- ✅ No external database dependency during execution
- ✅ Fast synchronous operations (in-process, workflow only)
- ✅ State automatically recovers via Temporal replay
- ✅ Isolated per-workflow (no contention)
- ✅ Activities can run on remote machines (no DB connection needed)
- ✅ Clean separation: workflow = state manager, activities = computation

**Implementation:**
```python
# Workflow (EXCLUSIVE DB access)
class DagRunWorkflow:
    def __init__(self):
        # Create in-memory database for this workflow instance
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    async def run_task(self, task_id: str):
        session = self.Session()

        # Create TaskInstance in in-memory DB
        ti = TaskInstance(
            dag_id=self.dag_id,
            task_id=task_id,
            run_id=self.run_id,
            state="queued",
        )
        session.add(ti)
        session.commit()

        # Update state before activity
        ti.state = "running"
        session.commit()

        # Send JSON workload to activity (NO DB session)
        workload = ExecuteTask(ti=ti, ...)
        result = await workflow.execute_activity(
            execute_airflow_task,
            args=[workload]  # ONLY JSON data
        )

        # Workflow updates DB based on result
        ti.state = result["state"]
        if "xcom_value" in result:
            xcom = XCom(value=result["xcom_value"], ...)
            session.add(xcom)
        session.commit()

        return ti.state

# Activity (NO DB access - can run remotely)
@activity.defn
async def execute_airflow_task(workload: ExecuteTask) -> dict:
    # Execute task (pure computation, no DB)
    dag = load_dag_from_file(workload.dag_rel_path)
    task = dag.task_dict[workload.ti.task_id]
    result = task.execute(context)

    # Return JSON result (workflow will update DB)
    return {"state": "success", "xcom_value": result}
```

**Data Flow:**
```
Workflow creates in-memory DB (workflow code only)
    ↓
Workflow creates TaskInstance records
    ↓
Workflow sends JSON workload to activity (remote execution)
    ↓
Activity loads DAG, executes task (NO DB access)
    ↓
Activity returns JSON result
    ↓
Workflow updates TaskInstance state in DB
    ↓
On replay: DB recreated, all state restored
```

**Critical Constraints:**
- Activities can run on REMOTE machines (different process/host)
- DB session CANNOT be serialized or passed to activities
- All activity parameters must be JSON-serializable
- All activity return values must be JSON-serializable
- Workflow has EXCLUSIVE control over state management

#### D. JWT Token Generation

**NOTE:** With in-memory database architecture, JWT tokens may not be needed if bypassing Execution API.

**If using supervise() with direct DB access:**
- Token field in ExecuteTask can be empty/dummy value
- No HTTP authentication needed for in-memory DB writes

**If using standard supervise() with Execution API:**
```python
from airflow.api_fastapi.auth.tokens import JWTGenerator

# In workflow
self.jwt_generator = JWTGenerator(
    valid_for=3600,
    audience=["execution-api"],
    issuer="temporal-scheduler",
    secret_key=conf.get("api_auth", "secret_key")
)

token = self.jwt_generator.generate({"sub": str(ti.id)})
```

**Recommendation:** Start without JWT tokens (Option 1 or 3 from section B), add only if needed.

## Implementation Phases

### Phase 1: Data Models & Workload Creation

**Goal:** Replace custom serialization with ExecuteTask workload

**Tasks:**
1. Update imports to use `airflow.executors.workloads.ExecuteTask`
2. Remove `TaskExecutionInput` model (use `ExecuteTask` directly)
3. Add JWT generation utilities
4. Update workflow to create ExecuteTask workloads instead of serializing

**Files:**
- `models.py`: Remove TaskExecutionInput, add workload helpers
- `workflows.py`: Update task scheduling logic

**Test:** Verify ExecuteTask workloads serialize/deserialize correctly

### Phase 2: Activity Refactoring

**Goal:** Replace deserialization with supervise()

**Tasks:**
1. Remove all operator deserialization code from activities.py
2. Implement `execute_airflow_task()` using `supervise()`
3. Configure Execution API URL
4. Add error handling and logging

**Files:**
- `activities.py`: Complete rewrite (~10 lines instead of ~150)

**Test:** Activity can call supervise() successfully (may fail on API call initially)

### Phase 3: Execution API Integration

**Goal:** Enable state reporting from activities

**Options (choose one):**

**Option A: Mock API (fastest for testing)**
```python
# Create minimal FastAPI mock
# Activities can report state, workflow can query
```

**Option B: Use real Airflow API**
```bash
# Run Airflow API server
# Configure activities to connect
```

**Test:** Activity can report task completion, workflow sees updated state

### Phase 4: DAG File Access

**Goal:** Activities can load DAG files

**Tasks:**
1. Set up shared volume or DAG storage
2. Configure DAGS_FOLDER path in activities
3. Test DAG loading from activities

**Files:**
- `docker-compose.yml` or deployment config
- Environment variables for DAG path

**Test:** Activity can load and parse DAG file

### Phase 5: End-to-End Integration

**Goal:** Complete workflow → activity → supervise() → task execution

**Tasks:**
1. Create simple test DAG with PythonOperator
2. Workflow creates DagRun and TaskInstance records
3. Workflow generates ExecuteTask workload
4. Activity executes task via supervise()
5. Workflow polls for completion

**Files:**
- `tests/test_workflows.py`: Update tests
- Create sample DAG file for testing

**Test:** Full DAG execution succeeds

### Phase 6: State Management & Error Handling

**Goal:** Robust state tracking and error handling

**Tasks:**
1. Implement state polling in workflow
2. Handle task failures, retries
3. Implement timeout handling
4. Add comprehensive logging

**Test:** Handle various task states (success, failure, retry, timeout)

### Phase 7: Cleanup & Documentation

**Goal:** Remove old code, document new architecture

**Tasks:**
1. Delete OPERATOR_SERIALIZATION.md (no longer needed)
2. Remove unused deserialization code
3. Update documentation
4. Add architecture diagram

## Code Deletion Plan

Files/sections to remove:

### `activities.py` (lines 37-97)
```python
# DELETE: All operator deserialization code
# - SerializedBaseOperator usage
# - Operator reconstruction logic
# - Attribute copying
# - Manual initialization
```

### `models.py`
```python
# DELETE: TaskExecutionInput model
# REPLACE WITH: Use ExecuteTask directly from airflow.executors.workloads
```

### `workflows.py`
```python
# DELETE: serialize_operator() calls
# DELETE: SerializedBaseOperator imports
# ADD: ExecuteTask.make() calls
```

## Risk Mitigation

### Risk 1: Activity Result Serialization
**Risk:** Task results may not be JSON-serializable (e.g., custom objects, file handles)
**Mitigation:**
- Document that operators must return JSON-serializable values
- Convert complex results to JSON-friendly format in activity before returning
- Store large results in shared storage, return reference/path
- Add validation in activity to ensure return value is JSON-serializable

### Risk 2: DAG File Synchronization
**Risk:** Activities on remote workers need access to same DAG files as workflow
**Mitigation:**
- Use read-only shared volume (NFS, EFS) for development
- Use S3/GCS with versioning for production
- Ensure DAG files are distributed to all workers

### Risk 3: Context Creation Without DB
**Risk:** Task context may require database queries (XCom, Variables, Connections)
**Mitigation:**
- Pass needed context data in ExecuteTask workload (e.g., XCom values, Variables)
- Or allow activities to query external DB/storage for context data
- Implement lazy context loading in activities

### Risk 4: TaskInstance Relationships
**Risk:** TaskInstance from workload may not have task/DAG object references
**Mitigation:**
- Activities load DAG from file and assign: `ti.task = dag.task_dict[ti.task_id]`
- Create minimal TaskInstance object in activity if needed
- Ensure context has all necessary fields without DB queries

## Testing Strategy

### Unit Tests
- ExecuteTask workload creation
- JWT token generation
- Activity supervise() call

### Integration Tests
- Load DAG file from activity
- Execute simple PythonOperator
- State reporting to API/database

### End-to-End Tests
- Full DAG execution
- Multi-task DAG with dependencies
- Error handling and retries

## Success Criteria

✅ Activities use `supervise()` (same as LocalExecutor)
✅ No operator serialization/deserialization
✅ Tasks execute with real callables
✅ State tracked correctly
✅ All existing tests pass
✅ Code reduced by ~100 lines

## Timeline Estimate

**Phase 1-2:** 2-4 hours (core refactoring)
**Phase 3:** 1-2 hours (API integration)
**Phase 4:** 1 hour (DAG access)
**Phase 5:** 2-3 hours (E2E testing)
**Phase 6-7:** 2-3 hours (polish)

**Total:** 8-13 hours

## Open Questions

1. **Context Data Requirements:** What context data do tasks actually need?
   - Variables, Connections, XComs from previous tasks?
   - **Option A:** Pass all context data in ExecuteTask workload
   - **Option B:** Allow activities to query external storage for context
   - **Recommendation:** Start with Option A (pass in workload), add Option B if needed

2. **DAG Storage:** Local volume or remote (S3)?
   - **Recommendation:** Local volume for development, S3 for production

3. **Result Serialization:** How to handle non-JSON-serializable task results?
   - **Challenge:** Some operators return complex objects
   - **Solution:** Convert to JSON-friendly format or store in shared storage
   - **Recommendation:** Document JSON-serialization requirement for operators

4. **TaskInstance Object:** Should activities create their own TaskInstance object?
   - **Challenge:** ExecuteTask.ti is serialized metadata, not full object
   - **Solution:** Activities reconstruct TaskInstance from metadata if needed
   - **Recommendation:** Use minimal context dict, avoid full TaskInstance

5. **Scheduling:** Should workflow handle scheduling logic or just execution?
   - **Recommendation:** Start with just execution, add scheduling later

6. **XCom Storage:** Where should XCom data be stored?
   - **Workflow DB:** For workflow-internal XComs (passed between tasks in same DAG run)
   - **External storage:** For XComs accessed across workflow instances
   - **Recommendation:** Store in workflow's in-memory DB, workflow passes to next task

7. **Error Handling:** How should activity failures be handled?
   - **Option A:** Activity returns error state, workflow updates DB
   - **Option B:** Activity raises exception, workflow catches and updates DB
   - **Recommendation:** Option A (return error state) for cleaner workflow code

## References

- LocalExecutor implementation: `airflow-core/src/airflow/executors/local_executor.py`
- supervise() function: `task-sdk/src/airflow/sdk/execution_time/supervisor.py`
- ExecuteTask workload: `airflow-core/src/airflow/executors/workloads.py`
- Execution API: `airflow-core/src/airflow/api_fastapi/execution_api/`

## Next Steps

1. Review and approve this plan
2. Choose options for Execution API, database, DAG storage
3. Begin Phase 1 implementation
4. Iterative development and testing
