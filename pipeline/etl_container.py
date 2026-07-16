"""ETL dependency container.

Houses the `etl_container` provider with static factory methods. This
keeps the concrete wiring in its own module for discoverability and
clarity while `pipeline.di` remains a compatibility shim.
"""

from pipeline.extractors import DbExtractor, FileExtractor
from pipeline.loaders import FileLoader
from pipeline.transformers import SalesTransformer
from pipeline.utils.db_connection import DBConnectionManager


class etl_container:
    """Container of factory methods for pipeline components.

    Use static methods to obtain configured instances of extractors,
    transformers and loaders. Keeping the provider in a dedicated
    module makes it easy to locate and import directly when desired.
    """

    @staticmethod
    def file_extractor() -> FileExtractor:
        """Return a default FileExtractor instance."""
        return FileExtractor()

    @staticmethod
    def db_extractor() -> DbExtractor:
        """Return a default DbExtractor configured with a DBConnectionManager."""
        return DbExtractor(db_manager=DBConnectionManager())

    @staticmethod
    def transformer() -> SalesTransformer:
        """Return a default SalesTransformer instance."""
        return SalesTransformer()

    @staticmethod
    def loader() -> FileLoader:
        """Return a default FileLoader instance."""
        return FileLoader()
