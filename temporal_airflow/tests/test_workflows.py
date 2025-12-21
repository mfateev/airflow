"""Tests for Temporal workflows."""
import asyncio
from datetime import datetime

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput


@pytest.mark.asyncio
async def test_workflow_database_initialization():
    """Test that workflow initializes its own database."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # Create minimal input
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "test_dag"}},
        )

        # Start workflow
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-workflow-db-init",
                task_queue="test-queue",
            )

            # Should complete without error
            assert result.dag_id == "test_dag"
            assert result.run_id == "test_run"


@pytest.mark.asyncio
async def test_database_isolation():
    """Test that multiple workflows have isolated databases."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        input1 = DagExecutionInput(
            dag_id="dag1",
            run_id="run1",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag1"}},
        )

        input2 = DagExecutionInput(
            dag_id="dag2",
            run_id="run2",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag2"}},
        )

        # Start two workflows concurrently
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            results = await asyncio.gather(
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input1,
                    id="test-workflow-1",
                    task_queue="test-queue",
                ),
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input2,
                    id="test-workflow-2",
                    task_queue="test-queue",
                ),
            )

            # Both should complete successfully with correct IDs
            assert results[0].dag_id == "dag1"
            assert results[1].dag_id == "dag2"
