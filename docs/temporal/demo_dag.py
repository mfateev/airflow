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
Demo DAG for Airflow + Temporal Integration

This DAG demonstrates:
- Parallel task execution (Temporal handles concurrent activities)
- Task dependencies and diamond patterns
- Task groups for organization
- XCom for passing data between tasks
- Different task durations to visualize progress

DAG Structure:
                        ┌─→ extract_users ──┐
    start ──→ validate ─┼─→ extract_orders ─┼─→ transform ──→ load ──→ notify
                        └─→ extract_products┘
"""
from __future__ import annotations

import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def start_pipeline():
    """Initialize the data pipeline."""
    print("=" * 50)
    print("Starting Data Pipeline")
    print("=" * 50)
    return {"pipeline_id": f"pipe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}


def validate_sources(**context):
    """Validate that all data sources are available."""
    pipeline_id = context["ti"].xcom_pull(task_ids="start")["pipeline_id"]
    print(f"Pipeline: {pipeline_id}")
    print("Validating data sources...")
    time.sleep(2)  # Simulate validation

    sources = ["users_db", "orders_db", "products_api"]
    for source in sources:
        print(f"  ✓ {source} is available")

    return {"validated_sources": sources}


def extract_users(**context):
    """Extract user data (runs in parallel with other extracts)."""
    print("Extracting user data...")
    time.sleep(random.randint(3, 5))  # Simulate varying extraction time

    users = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]
    print(f"  Extracted {len(users)} users")
    return {"users": users, "count": len(users)}


def extract_orders(**context):
    """Extract order data (runs in parallel with other extracts)."""
    print("Extracting order data...")
    time.sleep(random.randint(4, 6))  # Simulate varying extraction time

    orders = [
        {"id": 101, "user_id": 1, "amount": 150.00, "status": "completed"},
        {"id": 102, "user_id": 2, "amount": 250.00, "status": "pending"},
        {"id": 103, "user_id": 1, "amount": 75.50, "status": "completed"},
        {"id": 104, "user_id": 3, "amount": 320.00, "status": "completed"},
    ]
    print(f"  Extracted {len(orders)} orders")
    return {"orders": orders, "count": len(orders)}


def extract_products(**context):
    """Extract product data (runs in parallel with other extracts)."""
    print("Extracting product data...")
    time.sleep(random.randint(2, 4))  # Simulate varying extraction time

    products = [
        {"id": "P001", "name": "Widget", "price": 29.99},
        {"id": "P002", "name": "Gadget", "price": 49.99},
        {"id": "P003", "name": "Gizmo", "price": 19.99},
    ]
    print(f"  Extracted {len(products)} products")
    return {"products": products, "count": len(products)}


def transform_data(**context):
    """Transform and join all extracted data."""
    ti = context["ti"]

    users_data = ti.xcom_pull(task_ids="extract.extract_users")
    orders_data = ti.xcom_pull(task_ids="extract.extract_orders")
    products_data = ti.xcom_pull(task_ids="extract.extract_products")

    print("Transforming data...")
    print(f"  - Users: {users_data['count']}")
    print(f"  - Orders: {orders_data['count']}")
    print(f"  - Products: {products_data['count']}")

    time.sleep(3)  # Simulate transformation

    # Calculate some metrics
    total_revenue = sum(o["amount"] for o in orders_data["orders"])
    completed_orders = sum(1 for o in orders_data["orders"] if o["status"] == "completed")

    result = {
        "total_users": users_data["count"],
        "total_orders": orders_data["count"],
        "total_products": products_data["count"],
        "total_revenue": total_revenue,
        "completed_orders": completed_orders,
    }
    print(f"  ✓ Transformation complete: {result}")
    return result


def load_to_warehouse(**context):
    """Load transformed data to the data warehouse."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="transform")

    print("Loading to data warehouse...")
    time.sleep(2)  # Simulate loading

    print(f"  ✓ Loaded metrics: {metrics}")
    return {"status": "loaded", "rows_affected": sum(metrics.values())}


def send_notification(**context):
    """Send completion notification."""
    ti = context["ti"]
    pipeline_id = ti.xcom_pull(task_ids="start")["pipeline_id"]
    load_result = ti.xcom_pull(task_ids="load")

    print("=" * 50)
    print(f"Pipeline {pipeline_id} completed successfully!")
    print(f"Load status: {load_result['status']}")
    print(f"Total rows processed: {load_result['rows_affected']}")
    print("=" * 50)

    return {"notification_sent": True}


with DAG(
    dag_id="temporal_demo_pipeline",
    description="Demo DAG showcasing Temporal orchestration with parallel tasks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "temporal", "etl"],
    default_args={
        "retries": 1,
    },
) as dag:

    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=start_pipeline,
    )

    # Validation
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_sources,
    )

    # Extract tasks in a TaskGroup (run in parallel)
    with TaskGroup(group_id="extract") as extract_group:
        extract_users_task = PythonOperator(
            task_id="extract_users",
            python_callable=extract_users,
        )

        extract_orders_task = PythonOperator(
            task_id="extract_orders",
            python_callable=extract_orders,
        )

        extract_products_task = PythonOperator(
            task_id="extract_products",
            python_callable=extract_products,
        )

    # Transform (waits for all extracts)
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    # Load
    load = PythonOperator(
        task_id="load",
        python_callable=load_to_warehouse,
    )

    # Notify
    notify = PythonOperator(
        task_id="notify",
        python_callable=send_notification,
    )

    # Define dependencies
    start >> validate >> extract_group >> transform >> load >> notify
