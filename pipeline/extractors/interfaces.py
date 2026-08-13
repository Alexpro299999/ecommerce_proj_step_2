"""
Abstract interfaces for data extractors.

Defines contracts for event log extraction (file-based) and
enrichment data extraction (database-based). All concrete
extractors must implement these interfaces.
"""

from abc import ABC, abstractmethod

import pandas as pd


class IEventsExtractor(ABC):
    """
    Interface for extracting raw event logs from file-based sources.

    Concrete implementations handle the specifics of file format,
    compression, and batching strategy.
    """

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Reads all event logs and returns them as a single DataFrame.

        :return: DataFrame containing all raw event records.
        :rtype: pd.DataFrame
        """
        pass


class IEnrichmentExtractor(ABC):
    """
    Interface for extracting enrichment data from a database.
    """

    @abstractmethod
    def extract_table(self, table_name: str, columns: list[str]) -> pd.DataFrame:
        """
        Load a table from the database with specified columns.

        :param table_name: Name of the table to extract.
        :type table_name: str
        :param columns: List of column names to select.
        :type columns: list[str]
        :return: DataFrame with the selected columns.
        :rtype: pd.DataFrame
        """
        pass