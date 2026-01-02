"""Temporal workflow for executing Airflow DAGs with deep integration."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError, ActivityError

# Pass through Airflow imports to avoid sandbox reloading issues
with workflow.unsafe.imports_passed_through():
    import pendulum  # Must be imported first to avoid metaclass conflicts
    from airflow.models.dagrun import DagRunState
    from airflow.models.taskinstance import TaskInstanceState
    from airflow.serialization.serialized_objects import SerializedDAG
    from airflow.utils.time_provider import set_time_provider, clear_time_provider

from temporal_airflow.models import (
    DeepDagExecutionInput,
    DagExecutionResult,
    DagExecutionFailureDetails,
    ActivityTaskInput,
    TaskExecutionResult,
    TaskExecutionFailureDetails,
    CreateDagRunInput,
    TaskStatusSync,
    DagRunStatusSync,
    LoadSerializedDagInput,
    EnsureTaskInstancesInput,
)
from temporal_airflow.activities import run_airflow_task
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
)


@workflow.defn(name="execute_airflow_dag_deep", sandboxed=False)
class ExecuteAirflowDagDeepWorkflow:
    """
    Temporal workflow for deep integration mode.

    Unlike the standalone workflow, this workflow:
    - Uses real Airflow DB (not in-memory SQLite)
    - Creates DagRun/TaskInstance records via activities
    - Syncs status back to Airflow DB for UI visibility
    - Reads connections/variables from Airflow DB via hooks
    - Loads serialized DAG from Airflow DB

    This enables full Airflow UI integration while letting
    Temporal drive the execution.
    """

    def __init__(self):
        """Initialize workflow state."""
        # Deep integration state
        self.run_id: str | None = None
        self.dag: Any = None  # SerializedDAG

        # XCom state in workflow (for passing to activities)
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # Task tracking
        self.tasks_succeeded: int = 0
        self.tasks_failed: int = 0

    @workflow.run
    async def run(self, input: DeepDagExecutionInput) -> DagExecutionResult:
        """
        Execute the DAG with deep integration to Airflow DB.

        Args:
            input: Workflow input with dag_id, logical_date, optional run_id

        Returns:
            DagExecutionResult with final state and statistics
        """
        workflow.logger.info(
            f"Starting deep integration workflow: {input.dag_id} "
            f"(run_id={input.run_id or 'will be created'})"
        )

        # Set Temporal's deterministic time provider for Airflow code
        set_time_provider(workflow.now)

        start_time = workflow.now()

        try:
            # Phase 1: Create or use existing DagRun record
            if input.run_id:
                # DagRun already exists (e.g., created by orchestrator)
                self.run_id = input.run_id
                workflow.logger.info(f"Using existing DagRun: {self.run_id}")

                # Ensure TaskInstances exist
                await workflow.execute_activity(
                    ensure_task_instances,
                    EnsureTaskInstancesInput(
                        dag_id=input.dag_id,
                        run_id=self.run_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
            else:
                # Create new DagRun in Airflow DB
                result = await workflow.execute_activity(
                    create_dagrun_record,
                    CreateDagRunInput(
                        dag_id=input.dag_id,
                        logical_date=input.logical_date,
                        conf=input.conf,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self.run_id = result.run_id
                workflow.logger.info(f"Created DagRun: {self.run_id}")

            # Phase 2: Load serialized DAG from Airflow DB
            dag_data = await workflow.execute_activity(
                load_serialized_dag,
                LoadSerializedDagInput(dag_id=input.dag_id),
                start_to_close_timeout=timedelta(seconds=30),
            )
            self.dag = SerializedDAG.from_dict(dag_data)
            workflow.logger.info(f"Loaded DAG: {self.dag.dag_id} with {len(self.dag.task_dict)} tasks")

            # Phase 3: Execute scheduling loop with status sync
            final_state = await self._scheduling_loop(input.dag_id)

            end_time = workflow.now()

            # Phase 4: Sync final state to Airflow DB
            await workflow.execute_activity(
                sync_dagrun_status,
                DagRunStatusSync(
                    dag_id=input.dag_id,
                    run_id=self.run_id,
                    state=final_state,
                    end_date=end_time,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )

            # If DAG failed, raise ApplicationError
            if final_state == "failed":
                failure_details = DagExecutionFailureDetails(
                    dag_id=input.dag_id,
                    run_id=self.run_id,
                    start_date=start_time,
                    end_date=end_time,
                    tasks_succeeded=self.tasks_succeeded,
                    tasks_failed=self.tasks_failed,
                    error_message=f"DAG execution failed: {self.tasks_failed} task(s) failed",
                )
                raise ApplicationError(
                    f"DAG execution failed: {input.dag_id} / {self.run_id}",
                    failure_details,
                    type="DagExecutionFailure",
                    non_retryable=True,
                )

            return DagExecutionResult(
                state=final_state,
                dag_id=input.dag_id,
                run_id=self.run_id,
                start_date=start_time,
                end_date=end_time,
                tasks_succeeded=self.tasks_succeeded,
                tasks_failed=self.tasks_failed,
            )

        finally:
            clear_time_provider()

    def _get_upstream_xcom(self, task_id: str, dag_id: str, run_id: str, map_index: int) -> dict[str, Any] | None:
        """
        Gather XCom values from upstream tasks.

        XCom is stored in workflow state and passed to activities.
        """
        task = self.dag.get_task(task_id)
        if not task.upstream_task_ids:
            return None

        upstream_results = {}
        for upstream_task_id in task.upstream_task_ids:
            upstream_key = (dag_id, upstream_task_id, run_id, map_index)
            if upstream_key in self.xcom_store:
                upstream_results[upstream_task_id] = self.xcom_store[upstream_key]

        return upstream_results if upstream_results else None

    async def _scheduling_loop(self, dag_id: str) -> str:
        """
        Main scheduling loop for deep integration.

        Unlike the standalone workflow, this:
        - Doesn't maintain local DB state
        - Syncs task status to Airflow DB after each completion
        - Uses topological order from SerializedDAG

        Returns:
            Final DAG run state ("success" or "failed")
        """
        # Get topological order of tasks from DAG
        task_ids = list(self.dag.task_dict.keys())
        workflow.logger.info(f"Tasks to execute: {task_ids}")

        # Track state: task_id -> state
        task_states: dict[str, TaskInstanceState] = {
            task_id: TaskInstanceState.SCHEDULED for task_id in task_ids
        }

        # Track running activities: task_id -> ActivityHandle
        running_activities: dict[str, Any] = {}

        max_iterations = 100
        for iteration in range(max_iterations):
            workflow.logger.info(
                f"Scheduling loop iteration {iteration + 1}: "
                f"{len(running_activities)} running, "
                f"succeeded={self.tasks_succeeded}, failed={self.tasks_failed}"
            )

            # Check if all tasks are complete
            incomplete_tasks = [
                tid for tid, state in task_states.items()
                if state not in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.SKIPPED)
            ]

            if not incomplete_tasks:
                workflow.logger.info("All tasks complete")
                # Determine final state
                if self.tasks_failed > 0:
                    return "failed"
                return "success"

            # Find tasks that are ready to run (dependencies satisfied)
            ready_tasks = []
            for task_id in incomplete_tasks:
                if task_id in running_activities:
                    continue  # Already running

                task = self.dag.get_task(task_id)

                # Check if upstream tasks are complete
                upstream_complete = True
                upstream_failed = False
                for upstream_id in task.upstream_task_ids:
                    upstream_state = task_states.get(upstream_id)
                    if upstream_state not in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.SKIPPED):
                        upstream_complete = False
                        break
                    if upstream_state == TaskInstanceState.FAILED:
                        upstream_failed = True

                if not upstream_complete:
                    continue

                # If upstream failed and trigger_rule is "all_success" (default), skip this task
                trigger_rule = getattr(task, 'trigger_rule', 'all_success')
                if upstream_failed and trigger_rule == 'all_success':
                    workflow.logger.info(f"Skipping {task_id} due to upstream failure")
                    task_states[task_id] = TaskInstanceState.SKIPPED

                    # Sync skip status to Airflow DB
                    await workflow.execute_activity(
                        sync_task_status,
                        TaskStatusSync(
                            dag_id=dag_id,
                            task_id=task_id,
                            run_id=self.run_id,
                            map_index=-1,
                            state=TaskInstanceState.SKIPPED.value,
                            end_date=workflow.now(),
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    continue

                ready_tasks.append(task_id)

            # Start activities for ready tasks
            for task_id in ready_tasks:
                task_states[task_id] = TaskInstanceState.RUNNING

                # Sync running status to Airflow DB
                await workflow.execute_activity(
                    sync_task_status,
                    TaskStatusSync(
                        dag_id=dag_id,
                        task_id=task_id,
                        run_id=self.run_id,
                        map_index=-1,
                        state=TaskInstanceState.RUNNING.value,
                        start_date=workflow.now(),
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )

                # Gather upstream XCom
                upstream_results = self._get_upstream_xcom(task_id, dag_id, self.run_id, -1)

                # DAG file path (relative to DAGS_FOLDER)
                dag_rel_path = f"{dag_id}.py"

                activity_queue = workflow.info().task_queue

                workflow.logger.info(
                    f"Starting activity for {task_id} on queue '{activity_queue}'"
                )

                # Deep integration: No connections/variables passed
                # Activities read from Airflow DB via hooks
                handle = workflow.start_activity(
                    run_airflow_task,
                    arg=ActivityTaskInput(
                        dag_id=dag_id,
                        task_id=task_id,
                        run_id=self.run_id,
                        logical_date=workflow.now(),  # Use workflow time
                        try_number=1,
                        map_index=-1,
                        dag_rel_path=dag_rel_path,
                        upstream_results=upstream_results,
                        # Deep integration: connections/variables from Airflow DB
                        connections=None,
                        variables=None,
                    ),
                    task_queue=activity_queue,
                    start_to_close_timeout=timedelta(hours=2),
                    heartbeat_timeout=timedelta(minutes=5),
                )

                running_activities[task_id] = handle

            # Wait for any activities to complete
            if running_activities:
                workflow.logger.info(
                    f"Waiting for {len(running_activities)} activities..."
                )

                done, pending = await asyncio.wait(
                    running_activities.values(),
                    timeout=5,
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Process completed activities
                for completed in done:
                    task_id = next(k for k, v in running_activities.items() if v == completed)
                    ti_key = (dag_id, task_id, self.run_id, -1)

                    try:
                        result: TaskExecutionResult = completed.result()

                        workflow.logger.info(
                            f"Activity completed for {task_id}: state={result.state}"
                        )

                        # Update local state
                        task_states[task_id] = result.state

                        if result.state == TaskInstanceState.SUCCESS:
                            self.tasks_succeeded += 1
                        elif result.state == TaskInstanceState.FAILED:
                            self.tasks_failed += 1

                        # Store XCom
                        if result.xcom_data:
                            self.xcom_store[ti_key] = result.xcom_data

                        # Sync status to Airflow DB
                        await workflow.execute_activity(
                            sync_task_status,
                            TaskStatusSync(
                                dag_id=dag_id,
                                task_id=task_id,
                                run_id=self.run_id,
                                map_index=-1,
                                state=result.state.value,
                                start_date=result.start_date,
                                end_date=result.end_date,
                                xcom_value=result.return_value,
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                        )

                    except ActivityError as e:
                        workflow.logger.error(
                            f"ActivityError for {task_id}: {e.message}"
                        )

                        task_states[task_id] = TaskInstanceState.FAILED
                        self.tasks_failed += 1

                        # Sync failure to Airflow DB
                        await workflow.execute_activity(
                            sync_task_status,
                            TaskStatusSync(
                                dag_id=dag_id,
                                task_id=task_id,
                                run_id=self.run_id,
                                map_index=-1,
                                state=TaskInstanceState.FAILED.value,
                                end_date=workflow.now(),
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                        )

                    except Exception as e:
                        workflow.logger.error(
                            f"Unexpected error for {task_id}: {type(e).__name__}: {e}"
                        )
                        task_states[task_id] = TaskInstanceState.FAILED
                        self.tasks_failed += 1

                    del running_activities[task_id]

            else:
                # No running activities, sleep before checking for new work
                await asyncio.sleep(1)

        workflow.logger.error("Max iterations reached!")
        return "failed"
