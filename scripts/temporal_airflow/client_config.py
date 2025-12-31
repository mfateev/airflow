"""Temporal client configuration using SDK's built-in envconfig.

Configuration is loaded using Temporal's standard environment configuration system.
See: https://docs.temporal.io/develop/environment-configuration

Configuration precedence (highest to lowest):
1. Environment variables (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, etc.)
2. TOML config file (~/.config/temporalio/temporal.toml by default)
3. Defaults

Supported environment variables:
- TEMPORAL_ADDRESS: Host and port (default: localhost:7233)
- TEMPORAL_NAMESPACE: Namespace (default: default)
- TEMPORAL_API_KEY: API key (auto-enables TLS)
- TEMPORAL_TLS: Enable TLS (true/false)
- TEMPORAL_TLS_CLIENT_CERT_PATH: Client certificate path
- TEMPORAL_TLS_CLIENT_KEY_PATH: Client key path
- TEMPORAL_TLS_SERVER_CA_CERT_PATH: CA certificate path
- TEMPORAL_PROFILE: Profile name from config file

Additional project-specific variables:
- TEMPORAL_TASK_QUEUE: Task queue name (default: airflow-tasks)
- AIRFLOW__CORE__DAGS_FOLDER: DAG files location (default: /opt/airflow/dags)

Requires temporalio >= 1.9.0 for envconfig support.
"""
from __future__ import annotations

import os
from pathlib import Path

from temporalio.client import Client
from temporalio.envconfig import ClientConfig


async def create_temporal_client(
    profile: str | None = None,
    config_file: str | None = None,
) -> Client:
    """
    Create Temporal client using SDK's environment configuration.

    Args:
        profile: Profile name from config file (default: uses TEMPORAL_PROFILE env var or "default")
        config_file: Path to TOML config file (default: ~/.config/temporalio/temporal.toml)

    Returns:
        Connected Temporal client
    """
    connect_config = ClientConfig.load_client_connect_config(
        profile=profile,
        config_file=config_file,
    )

    # Ensure defaults if not set by config file or env vars
    if "target_host" not in connect_config:
        connect_config["target_host"] = "localhost:7233"
    if "namespace" not in connect_config:
        connect_config["namespace"] = "default"

    return await Client.connect(**connect_config)


def get_task_queue() -> str:
    """Get task queue from environment or default."""
    return os.environ.get("TEMPORAL_TASK_QUEUE", "airflow-tasks")


def get_dags_folder() -> Path:
    """Get DAGs folder from environment or default."""
    return Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))
