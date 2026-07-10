"""Dependency injection container for the e-commerce ETL pipeline.

This module centralizes construction of concrete pipeline dependencies
so the Airflow DAG can remain declarative and easy to test.
"""

from pipeline.extractors import DbExtractor, FileExtractor
from pipeline.loaders import FileLoader
from pipeline.transformers import SalesTransformer
from pipeline.utils.db_connection import DBConnectionManager


def create_file_extractor() -> FileExtractor:
    """Create the default file extractor."""
    return FileExtractor()


def create_db_extractor() -> DbExtractor:
    """Create the default database extractor with a connection manager."""
    return DbExtractor(db_manager=DBConnectionManager())


def create_transformer() -> SalesTransformer:
    """Create the default sales transformer."""
    return SalesTransformer()


def create_loader() -> FileLoader:
    """Create the default file loader."""
    return FileLoader()
