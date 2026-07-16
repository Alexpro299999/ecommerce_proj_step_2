"""Compatibility shim for dependency helpers.

This module keeps the historical `pipeline.di` import path working by
re-exporting convenience functions from the dedicated
`pipeline.etl_container` module.
"""

from pipeline.etl_container import etl_container
from pipeline.extractors import FileExtractor
from pipeline.extractors import DbExtractor
from pipeline.transformers import SalesTransformer
from pipeline.loaders import FileLoader


def create_file_extractor() -> FileExtractor:
    """Return the default file extractor (compat shim)."""
    return etl_container.file_extractor()


def create_db_extractor() -> DbExtractor:
    """Return the default DB extractor (compat shim)."""
    return etl_container.db_extractor()


def create_transformer() -> SalesTransformer:
    """Return the default transformer (compat shim)."""
    return etl_container.transformer()


def create_loader() -> FileLoader:
    """Return the default loader (compat shim)."""
    return etl_container.loader()
