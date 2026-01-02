# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Orchestrator loader and singleton management."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from airflow._shared.module_loading import import_string
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator

log = structlog.get_logger(__name__)

# Singleton instance
_orchestrator: BaseDagRunOrchestrator | None = None

# Built-in orchestrator classes (can be extended via config)
ORCHESTRATOR_CLASSES: dict[str, str] = {
    "DefaultOrchestrator": "airflow.orchestrators.base_orchestrator.DefaultOrchestrator",
}


def get_orchestrator() -> BaseDagRunOrchestrator:
    """
    Get the configured orchestrator (singleton).

    The orchestrator is configured via the [core] orchestrator setting in airflow.cfg.

    Returns the DefaultOrchestrator if not configured or if configuration is invalid.

    :return: The configured orchestrator instance
    """
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = _load_orchestrator()
    return _orchestrator


def _load_orchestrator() -> BaseDagRunOrchestrator:
    """
    Load the orchestrator based on configuration.

    :return: An orchestrator instance
    """
    orchestrator_name = conf.get("core", "orchestrator", fallback="DefaultOrchestrator")

    # Check if it's a known orchestrator alias
    if orchestrator_name in ORCHESTRATOR_CLASSES:
        orchestrator_path = ORCHESTRATOR_CLASSES[orchestrator_name]
    else:
        # Assume it's a full module path
        orchestrator_path = orchestrator_name

    try:
        orchestrator_cls = import_string(orchestrator_path)
        orchestrator = orchestrator_cls()
        log.info("Loaded orchestrator: %s", orchestrator_name)
        return orchestrator
    except ImportError as e:
        log.warning(
            "Failed to load orchestrator '%s' (error: %s). Using DefaultOrchestrator.",
            orchestrator_name,
            e,
        )
        from airflow.orchestrators.base_orchestrator import DefaultOrchestrator

        return DefaultOrchestrator()
    except Exception as e:
        log.exception(
            "Unexpected error loading orchestrator '%s'. Using DefaultOrchestrator.",
            orchestrator_name,
        )
        from airflow.orchestrators.base_orchestrator import DefaultOrchestrator

        return DefaultOrchestrator()


def reset_orchestrator() -> None:
    """
    Reset the orchestrator singleton.

    This is primarily useful for testing.
    """
    global _orchestrator
    _orchestrator = None
