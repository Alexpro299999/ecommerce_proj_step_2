"""Airflow DAG for e-commerce sales reporting pipeline.

Orchestrates daily ETL operations: extract events from nested archives,
enrich with product and customer data from database, transform and
aggregate into a sales report, load to CSV, and clean up temporary files.
"""

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.utils.context import Context

from pipeline.airflow_tasks import extract_db, extract_files, load, transform

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
    """Daily ETL pipeline for e-commerce sales reporting.

    Orchestrated tasks:
    1. Extract events from nested ZIP archives.
    2. Extract customer and product catalogues from PostgreSQL.
    3. Transform and aggregate sales data.
    4. Load report to CSV files.
    5. Clean up temporary artifacts.
    """

    @task(task_id="extract_files_task")
    def extract_files_task(**context: Context) -> str:
        """Extract events from nested archives and persist to CSV.

        Retrieves the logical run date (``ds``) from Airflow context
        and passes it to the adapter function to ensure isolation
        between concurrent runs.

        :param context: Airflow task execution context.
        :type context: airflow.utils.context.Context
        :return: Path to the extracted events CSV file.
        :rtype: str
        """
        current_run_date = context["ds"]
        return extract_files(run_date=current_run_date)

    @task(task_id="extract_db_task", multiple_outputs=True)
    def extract_db_task(**context: Context) -> Dict[str, str]:
        """Extract customer and product catalogues from PostgreSQL.

        Loads both tables from the ecommerce database and persists
        them as CSV files. Returns a dict with keys ``customers_path``
        and ``products_path`` for use by downstream tasks.

        :param context: Airflow task execution context.
        :type context: airflow.utils.context.Context
        :return: Dictionary with ``customers_path`` and ``products_path``.
        :rtype: dict
        """
        current_run_date = context["ds"]
        return extract_db(run_date=current_run_date)

    @task(task_id="transform_task")
    def transform_task(
        events_path: str,
        customers_path: str,
        products_path: str,
        **context: Context,
    ) -> str:
        """Transform and aggregate sales data.

        Loads extracted events and catalogues, joins them, calculates
        revenue metrics, and aggregates by product category and
        customer segment. Returns path to the final report CSV.

        :param events_path: Path to the events CSV file.
        :type events_path: str
        :param customers_path: Path to the customers CSV file.
        :type customers_path: str
        :param products_path: Path to the products CSV file.
        :type products_path: str
        :param context: Airflow task execution context.
        :type context: airflow.utils.context.Context
        :return: Path to the transformed report CSV file.
        :rtype: str
        """
        current_run_date = context["ds"]
        return transform(
            run_date=current_run_date,
            events_path=events_path,
            customers_path=customers_path,
            products_path=products_path,
        )

    @task(task_id="load_task")
    def load_task(report_path: str, **context: Context) -> None:
        """Load the generated report to CSV files.

        Writes the aggregated sales report to ``reports/sales_report.csv``
        and per-category breakdown files to ``reports/{category}.csv``.

        :param report_path: Path to the transformed report CSV file.
        :type report_path: str
        :param context: Airflow task execution context.
        :type context: airflow.utils.context.Context
        :return: None
        :rtype: None
        """
        load(report_path)

    @task(task_id="cleanup_task", trigger_rule="all_done")
    def cleanup_task(**context: Context) -> None:
        """Remove per-run temporary directory after DAG completes.

        This task uses the logical date (``ds``) to identify the run
        directory. It will remove the directory even if previous tasks
        failed.

        :param context: Airflow task context (injected automatically).
        :type context: airflow.utils.context.Context
        :return: None
        :rtype: None
        """
        import shutil
        from pathlib import Path

        from pipeline.config import settings

        PROJECT_ROOT = Path(__file__).resolve().parents[1]
        run_date: str = context["ds"]
        safe = run_date.replace(":", "_").replace("/", "_").replace(" ", "_")
        run_dir = PROJECT_ROOT / settings.tmp_dir / safe
        if run_dir.exists():
            shutil.rmtree(run_dir)

    events_path = extract_files_task()
    db_outputs = extract_db_task()
    customers_path = db_outputs["customers_path"]
    products_path = db_outputs["products_path"]
    report_path = transform_task(events_path, customers_path, products_path)
    load_op = load_task(report_path)
    cleanup_op = cleanup_task()
    load_op >> cleanup_op


dag = ecommerce_daily_report()
