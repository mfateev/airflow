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
"""
Orchestrators module.

Orchestrators control how DagRuns are executed. The default orchestrator
uses Airflow's scheduler + executor pattern. Alternative orchestrators
(like TemporalOrchestrator) route execution to external systems.

Configuration:
    Set [core] orchestrator in airflow.cfg:

    [core]
    orchestrator = DefaultOrchestrator  # Default - uses scheduler/executor
    # orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator

Usage:
    from airflow.orchestrators import get_orchestrator

    orchestrator = get_orchestrator()
    orchestrator.start_dagrun(dag_run, session)
"""

from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator, DefaultOrchestrator
from airflow.orchestrators.orchestrator_loader import get_orchestrator, reset_orchestrator

__all__ = [
    "BaseDagRunOrchestrator",
    "DefaultOrchestrator",
    "get_orchestrator",
    "reset_orchestrator",
]
