"""
Database extractor for enrichment data (customers, products).

Reads catalogue tables from a PostgreSQL database and returns
them as Pandas DataFrames for downstream joins in the pipeline.
"""

import pandas as pd

from pipeline.extractors.interfaces import IEnrichmentExtractor
from pipeline.utils.db_connection import DBConnectionManager


class DbExtractor(IEnrichmentExtractor):
    """
    Extracts catalogue tables from PostgreSQL.

    Uses :class:`DBConnectionManager` to obtain database connections
    via a context manager, ensuring proper cleanup.

    :param db_manager: Connection manager providing database access.
    :type db_manager: DBConnectionManager
    """

    def __init__(self, db_manager: DBConnectionManager):
        self.db_manager = db_manager

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
        cols_str = ", ".join(columns)
        query = f"SELECT {cols_str} FROM {table_name};"
        with self.db_manager.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def extract_customers(self) -> pd.DataFrame:
        """
        Load the customers table from the database.

        :return: DataFrame with columns customer_id, join_date, segment.
        :rtype: pd.DataFrame
        """
        return self.extract_table(
            table_name="customers",
            columns=["customer_id", "join_date", "segment"]
        )

    def extract_products(self) -> pd.DataFrame:
        """
        Load the products table from the database.

        :return: DataFrame with columns product_id, product_name, category, price.
        :rtype: pd.DataFrame
        """
        return self.extract_table(
            table_name="products",
            columns=["product_id", "product_name", "category", "price"]
        )