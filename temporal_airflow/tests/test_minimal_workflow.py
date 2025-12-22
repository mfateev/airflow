"""Minimal workflow test to verify Temporal test infrastructure."""
import pytest
from datetime import datetime
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions


# Configure sandbox passthrough
PASSTHROUGH_MODULES = [
    "structlog",
    "rich",
    "airflow.sdk.observability",
    "airflow._shared.observability",
]


@workflow.defn(name="minimal_test_workflow")
class MinimalTestWorkflow:
    """Minimal workflow that just returns a value."""

    @workflow.run
    async def run(self, value: str) -> str:
        """Return the input value."""
        workflow.logger.info(f"Minimal workflow received: {value}")
        return f"processed: {value}"


def create_worker(client, task_queue, workflows, activities=None):
    """Create a Worker with custom sandbox configuration."""
    return Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities or [],
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                *PASSTHROUGH_MODULES
            )
        ),
    )


@pytest.mark.asyncio
async def test_minimal_workflow_execution():
    """Test that a minimal workflow can execute."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[MinimalTestWorkflow],
        ):
            result = await env.client.execute_workflow(
                MinimalTestWorkflow.run,
                "test_value",
                id="test-minimal",
                task_queue="test-queue",
            )

            assert result == "processed: test_value"
