# Temporal-Airflow Integration: Strategic Questions

**Date**: 2025-12-23
**Status**: Awaiting Answers
**Purpose**: Define scope and direction for next implementation phases

---

## Executive Summary

Phases 1-3 are complete with 37 tests passing. The current implementation has:
- ✅ Workflow-specific SQLite databases (isolated)
- ✅ Executor pattern with DAG file loading
- ✅ Basic task orchestration with dependencies
- ✅ XCom passing and parallel execution
- ✅ Task failure handling

Before continuing, we need strategic decisions about integration strategy, database sync, feature priorities, and deployment model.

---

## Current Implementation Status

### ✅ What's Working (Phases 1-3 Complete)

1. **Time Provider**: Deterministic time injection via `get_current_time()` with ContextVar
2. **Database Isolation**: Each workflow has workflow-specific SQLite in-memory DB (Decision 1)
3. **Executor Pattern**: Activities load DAG files from disk, get real operators with callables
4. **Basic Orchestration**:
   - Workflow creates DagRun and TaskInstances
   - Scheduling loop with dependency resolution
   - Activity execution with XCom passing
   - Parallel task execution
   - Task failure handling with ApplicationError
5. **Test Coverage**: 37 tests passing
   - Time provider tests (4)
   - Model tests (18)
   - Workflow tests (9)
   - Mount visibility tests (2)
   - Empty/minimal workflow tests (4)

### 🔍 What's NOT Implemented

- Integration with Airflow scheduler/database/UI
- Deferrable operators (triggers)
- Sensors with reschedule mode
- Dynamic task mapping
- Pools/queues (marked as TODO Phase 5)
- Callbacks (on_success, on_failure, on_retry, on_execute)
- SLA monitoring
- Continue-as-new for long-running workflows
- Connection to actual Airflow infrastructure
- Variables/Connections from Airflow backend
- Task groups
- Branching operators

---

## Strategic Questions

### 1. Integration Strategy (Critical)

**Question**: How should Temporal fit into the Airflow ecosystem?

**Options**:
- **A)** Replace Airflow scheduler entirely - Temporal IS the scheduler
  - Temporal becomes the core orchestration engine
  - Airflow scheduler process not needed
  - Requires deep integration with Airflow webserver/API

- **B)** Hybrid mode - Some DAGs run on Airflow scheduler, some on Temporal
  - Coexistence model
  - Users choose per-DAG which engine to use
  - Both schedulers running simultaneously

- **C)** New executor type - Users configure `executor = TemporalExecutor`
  - Airflow scheduler still manages DAGs
  - Temporal executes tasks (like CeleryExecutor/KubernetesExecutor)
  - Standard Airflow architecture

- **D)** Standalone mode - Completely separate from Airflow (proof-of-concept)
  - No integration with Airflow infrastructure
  - Users submit directly to Temporal
  - Pure prototype/learning project

- **E)** Other? (please specify)

**Your Answer**: _______________

**Impact**: Determines if we need to integrate with Airflow's database, API, UI, and how users adopt this technology.

---

### 2. Database Strategy (Critical)

**Question**: What should we do with DagRun/TaskInstance state?

**Options**:
- **A)** Keep isolated SQLite forever (current) - No sync with Airflow DB
  - Workflow state lives only in Temporal
  - No Airflow UI visibility
  - Simplest implementation

- **B)** Write-through to Airflow DB - Every state change syncs to Postgres/MySQL
  - Real-time updates to Airflow database
  - Airflow UI always current
  - Higher complexity, potential performance impact

- **C)** Write-back at end - Sync final state when workflow completes
  - Batch update at workflow completion
  - Minimal performance impact
  - UI shows stale data during execution

- **D)** Dual-write - Update both in-memory SQLite and Airflow DB
  - Similar to write-through but more explicit
  - Can handle failures independently

- **E)** Other? (please specify)

**Your Answer**: _______________

**Impact**: Affects UI visibility, historical queries, monitoring, debugging, and operational workflows.

---

### 3. UI/Monitoring Strategy

**Question**: How should users monitor Temporal-executed DAGs?

**Options**:
- **A)** Only Temporal UI (no Airflow UI integration)
  - Use Temporal Web UI to view workflow execution
  - No Airflow UI updates
  - Requires users to learn Temporal UI

- **B)** Airflow UI shows DAG runs (requires DB sync)
  - Standard Airflow UI experience
  - Depends on database sync strategy
  - Best user experience

- **C)** Custom integration (webhook/API to update Airflow)
  - Build custom API/webhook to update Airflow
  - Can be selective about what syncs
  - More control but more complexity

- **D)** Don't care about monitoring for now
  - Focus on core functionality first
  - Defer monitoring to later phase

- **E)** Other? (please specify)

**Your Answer**: _______________

**Impact**: User experience, operational visibility, debugging capabilities.

---

### 4. Deployment Model

**Question**: How do users opt-in to Temporal execution?

**Options**:
- **A)** DAG-level decorator: `@temporal_dag` or `use_temporal=True` in DAG args
  - Explicit opt-in per DAG
  - Easy migration path
  - Example: `DAG(dag_id="my_dag", use_temporal=True)`

- **B)** Instance-level: Configure Airflow to use Temporal for ALL DAGs
  - Global configuration
  - All-or-nothing approach
  - Example: `executor = TemporalExecutor` in airflow.cfg

- **C)** Queue-based: DAGs with certain tags/queues go to Temporal
  - Flexible routing
  - Example: DAGs tagged "temporal" go to Temporal

- **D)** Explicit submission: User calls Temporal client directly
  - No Airflow integration needed
  - Programmatic control
  - Example: `temporal_client.execute_dag(dag_id="my_dag")`

- **E)** Other? (please specify)

**Your Answer**: _______________

**Impact**: User adoption, migration path, ease of use.

---

### 5. Feature Priority

**Question**: Which features are MUST-HAVE for MVP?

Check all that apply:

- [ ] Basic task dependencies (✅ already works)
- [ ] XCom passing (✅ already works)
- [ ] Task retries with exponential backoff
- [ ] Deferrable operators (sensors waiting on external events)
- [ ] Dynamic task mapping (`.expand()`)
- [ ] Pools for resource management
- [ ] Callbacks (on_success, on_failure, on_retry)
- [ ] SLA monitoring
- [ ] Task groups
- [ ] Branching operators (BranchPythonOperator, etc.)
- [ ] Connections management (reading from Airflow DB)
- [ ] Variables management (reading from Airflow DB)
- [ ] Continue-as-new for long DAGs
- [ ] SubDAGs or trigger_dagrun support
- [ ] Email notifications
- [ ] Other? (please specify): _______________

**Your Answer**: _______________

**Impact**: Development priorities, testing focus, MVP definition.

---

### 6. Scale/Performance Target

**Question**: What's the primary use case?

**Options**:
- **A)** Many small DAGs running frequently (high throughput)
  - Example: 1000s of 5-task DAGs per minute
  - Focus: Minimize overhead per workflow

- **B)** Few large DAGs with complex dependencies (deep graphs)
  - Example: DAGs with 100+ tasks, complex dependencies
  - Focus: Handle large task graphs efficiently

- **C)** Long-running workflows (hours to days)
  - Example: ML training pipelines, data processing
  - Focus: Continue-as-new strategy, durability

- **D)** Mix of all above
  - Need to handle all scenarios
  - Balanced optimization

- **E)** Other? (please specify)

**Your Answer**: _______________

**Impact**: Continue-as-new strategy, history limits, optimization focus.

---

### 7. Temporal History Limits

**Question**: Temporal workflows have history size limits (~50K events). When should we continue-as-new?

**Context**: Each task execution creates ~10-20 history events. A 1000-task DAG could hit limits.

**Options**:
- **A)** Never (assume DAGs are small enough)
  - Max ~2000 tasks per DAG
  - Simple implementation

- **B)** After N tasks executed (specify N: ___)
  - Automatic continuation based on task count
  - Example: Continue after every 500 tasks

- **C)** After N scheduling loop iterations (specify N: ___)
  - Based on scheduler loops, not tasks
  - Example: Continue after every 100 loops

- **D)** Based on history size (monitor and continue at X KB)
  - Most accurate but complex
  - Requires Temporal API calls to check history

- **E)** Let user configure per-DAG
  - DAG parameter: `max_tasks_per_workflow=1000`
  - Flexible but requires user knowledge

- **F)** Defer to later phase
  - Focus on small DAGs first
  - Handle large DAGs in Phase 6+

**Your Answer**: _______________

**Impact**: Support for large/long-running DAGs, reliability.

---

### 8. Airflow Core Integration

**Question**: Do you want to modify Airflow core code or keep Temporal integration external?

**Options**:
- **A)** Modify core - Add TemporalExecutor to airflow-core
  - Become part of official Airflow
  - Requires contribution process
  - Long-term maintenance by Airflow community

- **B)** External provider - Keep as separate provider package
  - Provider package: `apache-airflow-providers-temporal`
  - Easier to iterate independently
  - Standard provider pattern

- **C)** Prototype only - No production integration needed
  - Research/POC code
  - Not intended for real use
  - Focus on validation, not polish

**Your Answer**: _______________

**Impact**: Contribution path, maintenance, compatibility with Airflow versions.

---

### 9. Testing Strategy

**Question**: What level of testing do you want?

**Options**:
- **A)** Unit tests only (current - 37 tests)
  - Fast, deterministic
  - No external dependencies
  - Current state

- **B)** Integration tests with real Temporal server
  - Test against actual Temporal
  - Catch integration issues
  - Slower but more realistic

- **C)** End-to-end tests with Airflow + Temporal
  - Full stack testing
  - Database sync, UI updates, etc.
  - Most comprehensive

- **D)** Performance/load testing
  - Measure throughput, latency
  - Find bottlenecks
  - Validate scale targets

- **E)** All of above
  - Comprehensive test suite
  - High confidence
  - More development time

**Your Answer**: _______________

**Impact**: Reliability, confidence, development time, operational readiness.

---

### 10. Timeline/Scope

**Question**: What's your goal?

**Options**:
- **A)** Production-ready implementation
  - Battle-tested, documented
  - Ready for real workloads
  - High quality bar

- **B)** Proof-of-concept to validate approach
  - Demonstrate feasibility
  - Not production-grade
  - Quick iterations

- **C)** Research/learning project
  - Educational purpose
  - Explore possibilities
  - No production intent

- **D)** Foundation for future work
  - Solid base to build on
  - Room for iteration
  - Balance quality and speed

**Your Answer**: _______________

**Impact**: Thoroughness, documentation, edge cases, polish.

---

## Additional Context Questions

### 11. Airflow Pain Points

**Question**: Are there specific Airflow pain points you're trying to solve with Temporal?

Examples:
- Scheduler performance/scalability issues
- Reliability (scheduler crashes, lost tasks)
- Observability (debugging complex DAGs)
- Long-running task handling
- Resource management (pools, queues)
- State management complexity
- Other? (please specify)

**Your Answer**: _______________

---

### 12. Existing DAGs

**Question**: Do you have existing Airflow DAGs that need to work with this, or starting fresh?

**Options**:
- **A)** Must support existing DAGs (backward compatibility critical)
- **B)** Starting fresh (can design for Temporal from scratch)
- **C)** Mix (some existing, some new)
- **D)** Specific DAG patterns to support (please specify)

**Your Answer**: _______________

**Impact**: Backward compatibility requirements, operator support, migration path.

---

### 13. Temporal Experience

**Question**: What's your experience level with Temporal?

**Options**:
- **A)** Expert (use Temporal in production)
- **B)** Intermediate (built some workflows)
- **C)** Beginner (just learning)
- **D)** None (new to Temporal)

**Your Answer**: _______________

**Impact**: How much we rely on advanced Temporal features, documentation detail.

---

### 14. Constraints

**Question**: Are there any constraints?

Examples:
- Can't modify Airflow core (must be external)
- Must work with specific Airflow version (e.g., 2.9.x)
- Deployment environment restrictions (cloud, on-prem, k8s)
- Can't add new dependencies
- Must maintain API compatibility
- Performance requirements (latency, throughput)
- Other? (please specify)

**Your Answer**: _______________

**Impact**: Architecture decisions, implementation approach, technology choices.

---

## Instructions

Please answer all questions by filling in the "Your Answer" sections. You can:
1. Select option letters (A, B, C, etc.)
2. Check boxes with [x]
3. Fill in blanks with specific values
4. Add free-form text for "Other" options
5. Add any additional context or clarifications

Once you provide answers, I'll create a comprehensive implementation plan that aligns with your goals and constraints.

---

## Next Steps After Answers

1. Review and validate answers for consistency
2. Identify any ambiguities or conflicts
3. Create Phase 4+ implementation plan with:
   - Specific tasks and subtasks
   - File changes with line numbers
   - Test requirements
   - Integration points
   - Risk assessment
4. Define success criteria for each phase
5. Estimate complexity (no time estimates per Decision 10)
