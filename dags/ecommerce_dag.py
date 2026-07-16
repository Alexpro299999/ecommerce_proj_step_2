"""Airflow DAG for e-commerce sales reporting pipeline.

This module is a lightweight orchestrator — task implementations
are defined in `dags.ecommerce_tasks` to keep DAG parsing fast and
the orchestration clear.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

from pipeline.tasks import (
    cleanup_task,
    extract_db_task,
    extract_files_task,
    load_task,
    transform_task,
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ecommerce_daily_report",
    default_args=default_args,
    description="Daily ETL pipeline for e-commerce sales reporting",
    schedule="@daily",
    start_date=datetime(2026, 5, 28),
    catchup=False,
    tags=["ecommerce", "etl"],
)
def ecommerce_daily_report():
    events_path = extract_files_task()
    db_outputs = extract_db_task()
    customers_path = db_outputs["customers_path"]
    products_path = db_outputs["products_path"]
    report_path = transform_task(events_path, customers_path, products_path)
    load_op = load_task(report_path)
    cleanup_op = cleanup_task()
    load_op >> cleanup_op


dag = ecommerce_daily_report()
