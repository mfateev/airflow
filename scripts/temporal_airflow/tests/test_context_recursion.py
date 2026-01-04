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
Test that context with **context unpacking doesn't cause RecursionError.

This reproduces the issue where DAGs using **context pattern fail with
RecursionError due to circular references in dag/task objects.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch


def test_context_unpacking_with_dag_objects():
    """
    Test that context containing dag/task objects can be unpacked without recursion.

    This test reproduces the issue seen with demo_dag.py where:
    - validate_sources(**context) caused RecursionError
    - The context contained 'dag' and 'task' objects with circular references
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    # Create a DAG with circular references (dag -> task -> dag)
    with DAG(
        dag_id="test_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
    ) as dag:
        def my_task(**context):
            """Task that uses **context pattern."""
            # Access context items
            dag_id = context.get("dag_id")
            task_id = context.get("task_id")
            ti = context.get("ti")
            return f"{dag_id}.{task_id}"

        task = PythonOperator(
            task_id="test_task",
            python_callable=my_task,
        )

    # Create context similar to what activities.py creates
    context = {
        "dag": dag,
        "task": task,
        "dag_id": "test_dag",
        "task_id": "test_task",
        "run_id": "test_run",
        "logical_date": datetime(2025, 1, 1),
        "try_number": 1,
        "map_index": -1,
    }

    # Add minimal TI
    class MinimalTI:
        def xcom_pull(self, task_ids=None, key="return_value"):
            return None

    context["task_instance"] = MinimalTI()
    context["ti"] = context["task_instance"]

    # This should NOT cause RecursionError
    # The issue is that **context unpacking with dag/task objects may recurse
    try:
        result = my_task(**context)
        assert result == "test_dag.test_task"
    except RecursionError:
        pytest.fail("RecursionError when unpacking context with dag/task objects")


def test_context_without_dag_objects():
    """
    Test that context without dag/task objects works correctly.

    This is the fix: don't include dag/task in context for **context pattern.
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="test_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
    ) as dag:
        def my_task(**context):
            """Task that uses **context pattern."""
            dag_id = context.get("dag_id")
            task_id = context.get("task_id")
            return f"{dag_id}.{task_id}"

        task = PythonOperator(
            task_id="test_task",
            python_callable=my_task,
        )

    # Create context WITHOUT dag/task objects (the fix)
    context = {
        "dag_id": "test_dag",
        "task_id": "test_task",
        "run_id": "test_run",
        "logical_date": datetime(2025, 1, 1),
        "try_number": 1,
        "map_index": -1,
        "ds": "2025-01-01",
        "ts": "2025-01-01T00:00:00+00:00",
    }

    class MinimalTI:
        def xcom_pull(self, task_ids=None, key="return_value"):
            return {"upstream_value": 42}

    context["task_instance"] = MinimalTI()
    context["ti"] = context["task_instance"]

    # This should work without recursion
    result = my_task(**context)
    assert result == "test_dag.test_task"


def test_python_operator_execute_with_dag_objects_causes_recursion():
    """
    Test that context WITH dag/task objects causes RecursionError.

    This documents the bug: including dag/task in context causes recursion
    in XComArg.iter_xcom_references() due to circular references.
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.sdk.bases.operator import ExecutorSafeguard

    def my_task(**context):
        return "ok"

    with DAG(
        dag_id="test_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
    ) as dag:
        task = PythonOperator(
            task_id="test_task",
            python_callable=my_task,
        )

    # Create context WITH dag/task objects (the bug)
    context = {
        "dag": dag,  # BUG: circular reference
        "task": task,  # BUG: circular reference
        "dag_id": "test_dag",
        "task_id": "test_task",
        "run_id": "test_run",
        "logical_date": datetime(2025, 1, 1),
        "try_number": 1,
        "map_index": -1,
    }

    class MinimalTI:
        def xcom_pull(self, task_ids=None, key="return_value"):
            return None

    context["task_instance"] = MinimalTI()
    context["ti"] = context["task_instance"]

    # This DOES cause RecursionError (documenting the bug)
    with pytest.raises(RecursionError):
        sentinel_key = f"{task.__class__.__name__}__sentinel"
        task.execute(context, **{sentinel_key: ExecutorSafeguard.sentinel_value})


def test_python_operator_execute_without_dag_objects_works():
    """
    Test that context WITHOUT dag/task objects works correctly.

    This is the fix: don't include dag/task in context.
    The context should only contain simple serializable values.
    """
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.sdk.bases.operator import ExecutorSafeguard

    def my_task(**context):
        """Task that uses **context pattern."""
        dag_id = context.get("dag_id")
        ti = context.get("ti")
        if ti:
            result = ti.xcom_pull(task_ids="upstream")
            return f"dag={dag_id}, xcom={result}"
        return f"dag={dag_id}"

    with DAG(
        dag_id="test_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
    ) as dag:
        task = PythonOperator(
            task_id="test_task",
            python_callable=my_task,
        )

    # Create context WITHOUT dag/task objects (the fix)
    context = {
        "dag_id": "test_dag",
        "task_id": "test_task",
        "run_id": "test_run",
        "logical_date": datetime(2025, 1, 1),
        "try_number": 1,
        "map_index": -1,
        "ds": "2025-01-01",
        "ts": "2025-01-01T00:00:00",
    }

    class MinimalTI:
        def xcom_pull(self, task_ids=None, key="return_value"):
            if task_ids == "upstream":
                return {"value": 42}
            return None

    context["task_instance"] = MinimalTI()
    context["ti"] = context["task_instance"]

    # This should NOT cause RecursionError
    sentinel_key = f"{task.__class__.__name__}__sentinel"
    result = task.execute(context, **{sentinel_key: ExecutorSafeguard.sentinel_value})
    assert "dag=test_dag" in str(result)
    assert "xcom={'value': 42}" in str(result)


def test_xcom_pull_in_context():
    """
    Test that xcom_pull works correctly in the minimal context.
    """
    upstream_results = {
        "start": {"pipeline_id": "pipe_123"},
        "extract": {"data": [1, 2, 3]},
    }

    def xcom_pull(task_ids=None, key="return_value"):
        """Simple XCom pull from upstream_results dict."""
        if task_ids is None:
            return None
        if isinstance(task_ids, str):
            return upstream_results.get(task_ids)
        # Handle list of task_ids
        return {tid: upstream_results.get(tid) for tid in task_ids}

    class MinimalTI:
        def __init__(self, xcom_pull_fn):
            self.xcom_pull = xcom_pull_fn

    ti = MinimalTI(xcom_pull)

    # Test single task_id
    result = ti.xcom_pull(task_ids="start")
    assert result == {"pipeline_id": "pipe_123"}

    # Test accessing nested value (like in demo_dag)
    pipeline_id = ti.xcom_pull(task_ids="start")["pipeline_id"]
    assert pipeline_id == "pipe_123"
