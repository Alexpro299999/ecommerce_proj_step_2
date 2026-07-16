"""Airflow task wrappers for the e-commerce ETL pipeline.

Each function is an Airflow `@task` that delegates to the existing
adapter functions in `pipeline.etl_adapters`. Dependencies are obtained
via the `etl_container` provider to keep wiring in one place.
"""

from typing import Dict

from airflow.decorators import task
from airflow.utils.context import Context

from pipeline.etl_container import etl_container
from pipeline.etl_adapters import extract_db, extract_files, load, transform
from pipeline.utils.logging_setup import setup_logging


@task(task_id="extract_files_task")
def extract_files_task(**context: Context) -> str:
    setup_logging(clear_handlers=False, add_console=False)
    current_run_date = context["ds"]
    return extract_files(
        run_date=current_run_date,
        file_extractor=etl_container.file_extractor(),
    )


@task(task_id="extract_db_task", multiple_outputs=True)
def extract_db_task(**context: Context) -> Dict[str, str]:
    setup_logging(clear_handlers=False, add_console=False)
    current_run_date = context["ds"]
    return extract_db(
        run_date=current_run_date,
        db_extractor=etl_container.db_extractor(),
    )


@task(task_id="transform_task")
def transform_task(
    events_path: str,
    customers_path: str,
    products_path: str,
    **context: Context,
) -> str:
    setup_logging(clear_handlers=False, add_console=False)
    current_run_date = context["ds"]
    return transform(
        run_date=current_run_date,
        events_path=events_path,
        customers_path=customers_path,
        products_path=products_path,
        transformer=etl_container.transformer(),
    )


@task(task_id="load_task")
def load_task(report_path: str, **context: Context) -> None:
    setup_logging(clear_handlers=False, add_console=False)
    load(report_path, loader=etl_container.loader())


@task(task_id="cleanup_task", trigger_rule="all_done")
def cleanup_task(**context: Context) -> None:
    setup_logging(clear_handlers=False, add_console=False)
    import shutil
    from pathlib import Path

    from pipeline.config import settings

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    run_date: str = context["ds"]
    safe = run_date.replace(":", "_").replace("/", "_").replace(" ", "_")
    run_dir = PROJECT_ROOT / settings.tmp_dir / safe
    if run_dir.exists():
        shutil.rmtree(run_dir)
