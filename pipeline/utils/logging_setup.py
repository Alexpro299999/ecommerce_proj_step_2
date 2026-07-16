"""
Logging configuration for the ETL pipeline.

Sets up a dual-output logging system:
- Console handler for real-time feedback.
- Rotating file handler for persistent log storage.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pipeline.config import settings


def setup_logging(
    log_level=logging.INFO,
    log_dir=settings.log_dir,
    log_file="pipeline.log",
    clear_handlers=True,
    add_console=True,
):
    """
    Configures the root logger with console and file handlers.

    Clears existing handlers if clear_handlers is True to prevent duplicate log
    entries on repeated calls. Checks for existing handlers of the same type
    when clear_handlers is False to remain idempotent.

    :param log_level: Minimum severity level to capture.
    :type log_level: int
    :param log_dir: Directory for log files (created if missing).
    :type log_dir: str
    :param log_file: Name of the rotating log file.
    :type log_file: str
    :param clear_handlers: Whether to clear pre-existing handlers from the root logger.
    :type clear_handlers: bool
    :param add_console: Whether to attach a console log stream.
    :type add_console: bool
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear any pre-existing handlers to avoid duplicates if requested.
    if clear_handlers and root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Console handler - prints logs to stdout if requested.
    if add_console:
        has_console_handler = any(
            isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler)
            for h in root_logger.handlers
        )
        if not has_console_handler:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(log_level)
            root_logger.addHandler(console_handler)

    # File handler - rotates at 5 MB, keeps 3 backups.
    file_path = os.path.abspath(os.path.join(log_dir, log_file))
    has_file_handler = False
    for handler in root_logger.handlers:
        if isinstance(handler, RotatingFileHandler):
            if os.path.abspath(handler.baseFilename) == file_path:
                has_file_handler = True
                break

    if not has_file_handler:
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        root_logger.addHandler(file_handler)

    logging.getLogger("logger_setup").info("Logging system initialized successfully")