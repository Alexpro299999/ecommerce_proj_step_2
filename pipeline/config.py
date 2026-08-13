"""
Application configuration via Pydantic Settings.

Loads environment variables from the ``.env`` file at project root.
Falls back to default values if variables are not set.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central configuration for the ETL pipeline.

    Fields are populated from environment variables (case-insensitive).
    The ``.env`` file is read automatically on instantiation.

    :ivar db_user: PostgreSQL username.
    :ivar db_pass: PostgreSQL password.
    :ivar db_host: Database server hostname.
    :ivar db_port: Database server port.
    :ivar db_name: Target database name.
    :ivar batch_size: Number of events per processing batch.
    :ivar log_dir: Directory for storing application logs.
    :ivar data_dir: Directory containing raw event ZIP archives.
    :ivar tmp_dir: Directory for temporary files between Airflow tasks.
    :ivar reports_dir: Directory for generated CSV reports.
    """

    db_user: str = "myuser"
    db_pass: str = "mypassword"
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "ecommerce_db"
    batch_size: int = 50000

    log_dir: str = "logs"
    data_dir: str = "data"
    tmp_dir: str = "data/tmp"
    reports_dir: str = "reports"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


# Singleton-style instance - import this directly across the project.
settings = Settings()
