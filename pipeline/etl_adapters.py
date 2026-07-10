"""Airflow task wrappers for the e-commerce ETL pipeline.

This module provides a file-based interface for Airflow tasks. Task
functions execute existing pipeline classes and persist intermediate
datasets to local files so that state is passed through the
filesystem rather than in-memory objects.

The functions intentionally obtain the Airflow execution context at
runtime and create per-run storage directories, avoiding race
conditions between concurrent DAG runs.
"""

from pathlib import Path
from typing import Dict

from pipeline.config import settings
from pipeline.extractors.interfaces import IEnrichmentExtractor, IEventsExtractor
from pipeline.loaders.interfaces import ILoader

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TMP_DIR_BASE = PROJECT_ROOT / settings.tmp_dir


def _sanitize_run_id(run_key: str) -> str:
    """Sanitize a run identifier so it is safe for filenames.

    :param run_key: Original run identifier (logical date or run id).
    :type run_key: str
    :return: Sanitized run identifier suitable for a filesystem path.
    :rtype: str
    """
    return run_key.replace(":", "_").replace("/", "_").replace(" ", "_")


def _run_dir_for(run_key: str) -> Path:
    """Return the path to the per-run temporary directory.

    :param run_key: Run key (logical date or run id).
    :type run_key: str
    :return: Path to the run-specific temporary directory.
    :rtype: pathlib.Path
    """
    safe = _sanitize_run_id(run_key)
    run_dir = TMP_DIR_BASE / safe
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def extract_files(run_date: str, file_extractor: IEventsExtractor | None = None) -> str:
    """Extract events from nested archives and persist them to a CSV file.

    :param run_date: The logical Airflow execution date (``ds``) in
        ``YYYY-MM-DD`` format used to isolate artifacts.
    :type run_date: str
    :param file_extractor: Optional file extractor instance for dependency injection.
    :type file_extractor: pipeline.extractors.interfaces.IEventsExtractor | None
    :return: Path to the persisted events CSV file.
    :rtype: str
    """
    if file_extractor is None:
        # Local imports keep DAG parsing lightweight.
        from pipeline.extractors import FileExtractor

        file_extractor = FileExtractor()

    run_dir = _run_dir_for(run_date)
    events_path = run_dir / "events.csv"

    events_df = file_extractor.extract()
    events_df.to_csv(events_path, index=False)
    return str(events_path)


def extract_db(
    run_date: str, db_extractor: IEnrichmentExtractor | None = None
) -> Dict[str, str]:
    """Extract customer and product catalogues from the database.

    :param run_date: The logical Airflow execution date (``ds``) in
        ``YYYY-MM-DD`` format used to isolate artifacts.
    :type run_date: str
    :param db_extractor: Optional database extractor instance for dependency injection.
    :type db_extractor: pipeline.extractors.interfaces.IEnrichmentExtractor | None
    :return: Dictionary with keys ``customers_path`` and ``products_path``.
    :rtype: dict
    """
    if db_extractor is None:
        # Local imports keep DAG parsing lightweight.
        from pipeline.extractors import DbExtractor
        from pipeline.utils.db_connection import DBConnectionManager

        db_extractor = DbExtractor(db_manager=DBConnectionManager())

    run_dir = _run_dir_for(run_date)
    customers_path = run_dir / "customers.csv"
    products_path = run_dir / "products.csv"

    customers_df = db_extractor.extract_customers()
    products_df = db_extractor.extract_products()
    customers_df.to_csv(customers_path, index=False)
    products_df.to_csv(products_path, index=False)

    return {"customers_path": str(customers_path), "products_path": str(products_path)}


def transform(
    run_date: str,
    events_path: str,
    customers_path: str,
    products_path: str,
    transformer: "SalesTransformer" | None = None,
) -> str:
    """Load extracted datasets, run transformation, and persist the report.

    :param run_date: The logical Airflow run date (``ds``) in ``YYYY-MM-DD``.
    :type run_date: str
    :param events_path: Path to the CSV file with extracted events.
    :type events_path: str
    :param customers_path: Path to the CSV file with customers.
    :type customers_path: str
    :param products_path: Path to the CSV file with products.
    :type products_path: str
    :param transformer: Optional transformer instance for dependency injection.
    :type transformer: pipeline.transformers.SalesTransformer | None
    :return: Path to the persisted report CSV file.
    :rtype: str
    """
    if transformer is None:
        # Local imports keep DAG parsing lightweight.
        import pandas as pd

        from pipeline.transformers import SalesTransformer

        transformer = SalesTransformer()
    else:
        import pandas as pd

    run_dir = _run_dir_for(run_date)
    report_path = run_dir / "sales_report.csv"

    events_df = pd.read_csv(events_path)
    customers_df = pd.read_csv(customers_path)
    products_df = pd.read_csv(products_path)
    report_df = transformer.transform(events_df, customers_df, products_df)
    report_df.to_csv(report_path, index=False)
    return str(report_path)


def load(report_path: str, loader: ILoader | None = None) -> None:
    """Load the transformed report and persist final CSV files.

    :param report_path: Path to the transformed report CSV file.
    :type report_path: str
    :param loader: Optional report loader for dependency injection.
    :type loader: pipeline.loaders.interfaces.ILoader | None
    :return: None
    :rtype: None
    """
    # Local imports keep module import lightweight for DAG parsing.
    import pandas as pd

    from pipeline.loaders import FileLoader

    if loader is None:
        loader = FileLoader()

    report_df = pd.read_csv(report_path)
    loader.load(report_df)
