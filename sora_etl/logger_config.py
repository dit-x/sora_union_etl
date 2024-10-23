import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(
    name: str,
    log_file: str,
    level: int = logging.INFO,
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    max_bytes: int = 5 * 1024 * 1024,  # 5 MB
    backup_count: int = 5  # Number of backup files to keep
) -> logging.Logger:
    """
    Sets up a logger for the application with file and console handlers, including log rotation.

    Args:
        name (str): The name of the logger.
        log_file (str): The file path for logging output.
        level (int): The logging level (default is logging.INFO).
        log_format (str): The format for log messages.
        max_bytes (int): The maximum size of the log file before rotating (default is 5 MB).
        backup_count (int): The number of backup log files to keep (default is 5).

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Create log directory if it does not exist
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Rotating file handler for logging to file with rotation
    rotating_file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    rotating_file_handler.setLevel(level)

    # Console handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Logging format
    formatter = logging.Formatter(log_format)
    rotating_file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger if they are not already added
    if not logger.handlers:
        logger.addHandler(rotating_file_handler)
        logger.addHandler(console_handler)

    return logger