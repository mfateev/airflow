"""Self-contained test for executing an empty Temporal workflow."""
from __future__ import annotations

import pytest
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker


@workflow.defn(name="empty_workflow")
class EmptyWorkflow:
    """
    An empty workflow that does nothing except return a simple value.

    This is the most minimal Temporal workflow possible - it has no activities,
    no state, no logic, just accepts an input and returns a value.
    """

    @workflow.run
    async def run(self) -> str:
        """Execute empty workflow - just return success."""
        workflow.logger.info("Empty workflow executed")
        return "success"


@pytest.mark.asyncio
async def test_empty_workflow_execution():
    """
    Test that an empty workflow can be executed successfully.

    This test validates the basic Temporal infrastructure:
    - WorkflowEnvironment starts correctly
    - Worker can register and run workflows
    - Workflow execution completes successfully
    - Result is returned correctly
    """
    # Start Temporal test environment with time skipping
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # Create worker with our empty workflow
        async with Worker(
            env.client,
            task_queue="empty-test-queue",
            workflows=[EmptyWorkflow],
        ):
            # Execute the workflow
            result = await env.client.execute_workflow(
                EmptyWorkflow.run,
                id="test-empty-workflow",
                task_queue="empty-test-queue",
            )

            # Verify result
            assert result == "success"


@pytest.mark.asyncio
async def test_empty_workflow_with_multiple_executions():
    """Test that multiple empty workflows can execute in sequence."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="empty-test-queue",
            workflows=[EmptyWorkflow],
        ):
            # Execute workflow multiple times
            results = []
            for i in range(3):
                result = await env.client.execute_workflow(
                    EmptyWorkflow.run,
                    id=f"test-empty-workflow-{i}",
                    task_queue="empty-test-queue",
                )
                results.append(result)

            # Verify all executions succeeded
            assert results == ["success", "success", "success"]
