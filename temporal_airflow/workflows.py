"""Temporal workflow for executing Airflow DAGs."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow

from airflow.models.dagrun import DagRun, DagRunState
from airflow.serialization.serialized_objects import SerializedDAG
from temporal_airflow.time_provider import set_workflow_time
from temporal_airflow.models import (
    DagExecutionInput,
    DagExecutionResult,
    TaskExecutionInput,
    TaskExecutionResult,
)


@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    """
    Temporal workflow that executes a single Airflow DAG.

    Design Notes:
    - Decision 1: Workflow-specific database (no global configure_orm)
    - Decision 2: Direct activity management (no executor)
    - Decision 6: Accepts sync calls (documented as acceptable)
    - Decision 7: Stores XCom in workflow state, passes to activities
    """

    def __init__(self):
        """Initialize workflow state."""
        # Decision 1: Workflow-specific database state
        self.engine = None
        self.SessionFactory = None

        # Decision 3 & 7: DAG state management
        self.serialized_dag = None  # Full DAG (from input)
        self.dag = None  # Deserialized DAG

        # Decision 7: XCom state in workflow
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # TODO Phase 5: Add pool_usage tracking

    @workflow.run
    async def run(self, input: DagExecutionInput) -> DagExecutionResult:
        """
        Execute the DAG.

        Args:
            input: Typed workflow input (Decision 4: Pydantic model)

        Returns:
            DagExecutionResult with final state and statistics
        """
        workflow.logger.info(f"Starting DAG execution: {input.dag_id} / {input.run_id}")

        start_time = workflow.now()

        try:
            # Phase 1: Setup (Decision 1: workflow-specific DB)
            self._initialize_database()

            # Commit 2: Store and deserialize DAG (Decision 3)
            self.serialized_dag = input.serialized_dag
            self.dag = SerializedDAG.from_dict(self.serialized_dag)

            workflow.logger.info(f"Deserialized DAG: {self.dag.dag_id}")

            # Commit 3: Create DAG run
            dag_run_id = self._create_dag_run(
                dag_id=input.dag_id,
                run_id=input.run_id,
                logical_date=input.logical_date,
                conf=input.conf,
            )

            workflow.logger.info(f"Created DAG run: {dag_run_id}")

            # Commit 4-7: Main scheduling loop
            final_state = await self._scheduling_loop(dag_run_id)

            end_time = workflow.now()

            # Return result
            return DagExecutionResult(
                state=final_state,
                dag_id=input.dag_id,
                run_id=input.run_id,
                start_date=start_time,
                end_date=end_time,
                tasks_succeeded=0,  # TODO: Track in later commits
                tasks_failed=0,
            )

        finally:
            from temporal_airflow.time_provider import clear_workflow_time
            clear_workflow_time()

    def _initialize_database(self):
        """
        Initialize workflow-specific in-memory database.

        Design Note (Decision 1):
        - Each workflow gets unique in-memory DB
        - Never calls global configure_orm()
        - Uses SQLite URI with workflow-specific identifier
        """
        # Import at runtime to avoid sandbox restrictions
        from sqlalchemy import create_engine
        from sqlalchemy.pool import StaticPool
        from sqlalchemy.orm import sessionmaker

        workflow_id = workflow.info().workflow_id
        conn_str = f"sqlite:///file:memdb_{workflow_id}?mode=memory&cache=shared&uri=true"

        # Create workflow-specific engine (no global state!)
        self.engine = create_engine(
            conn_str,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )

        # Create workflow-specific session factory
        self.SessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Create schema
        from airflow.models import Base
        Base.metadata.create_all(self.engine)

        workflow.logger.info(f"Database initialized for workflow {workflow_id}")

    def _create_dag_run(
        self,
        dag_id: str,
        run_id: str,
        logical_date: datetime,
        conf: dict | None,
    ) -> int:
        """
        Create DagRun and TaskInstances.

        Design Note (Decision 1):
        - Uses workflow-specific SessionFactory
        - Never uses global create_session()
        """
        set_workflow_time(workflow.now())

        # Use workflow-specific session (Decision 1)
        session = self.SessionFactory()
        try:
            # Create DagRun
            dag_run = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,
                run_type="manual",
                state=DagRunState.RUNNING,
                conf=conf,
            )
            dag_run.dag = self.dag  # Set DAG reference (required for update_state)

            session.add(dag_run)
            session.flush()

            # Create TaskInstances
            dag_run.verify_integrity(session=session)

            session.commit()

            workflow.logger.info(f"Created DagRun: {dag_run.id} with {len(dag_run.task_instances)} tasks")
            return dag_run.id
        finally:
            session.close()

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        """
        Main scheduling loop.

        Design Notes:
        - Decision 2: Direct activity management (no executor)
        - Decision 6: Accepts sync calls (ORM queries, update_state)

        Returns:
            Final DAG run state
        """
        # Track running activities: ti_key -> ActivityHandle
        running_activities: dict[tuple, Any] = {}

        max_iterations = 10000  # Safety limit

        for iteration in range(max_iterations):
            # Update workflow time (deterministic)
            set_workflow_time(workflow.now())

            # Decision 6: Sync calls acceptable (fast, in-memory DB)
            session = self.SessionFactory()
            try:
                dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
                dag_run.dag = self.dag  # Restore DAG reference

                # Check if complete
                if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    workflow.logger.info(f"DAG completed: {dag_run.state}")
                    return dag_run.state.value

                # TODO Commit 5: Update state and get schedulable tasks
                # TODO Commit 6: Start activities for schedulable tasks
                # TODO Commit 7: Handle activity completions

            finally:
                session.close()

            # No running activities yet, just sleep
            await asyncio.sleep(5)

        workflow.logger.error("Max iterations reached!")
        return "failed"
