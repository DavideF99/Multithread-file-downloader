# src/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(log_file='logs/downloader.log', log_level=logging.INFO):
    """
    Set up logging with both file and console output.
    
    Args:
        log_file: Path to log file
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir:  # Only create if there's actually a directory path
        os.makedirs(log_dir, exist_ok=True)
    
    # Create logger (use root logger for simplicity)
    logger = logging.getLogger('ml_downloader')
    logger.setLevel(logging.DEBUG)  # Capture everything, handlers filter
    
    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger
    
    # Format: Include timestamp, level, thread ID, and message
    log_format = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - [Thread-%(thread)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler - shows INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    
    # File handler - shows DEBUG and above, rotates at 10MB
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5  # Keep 5 old log files
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    
    return logger


def get_logger():
    """Get the configured logger instance."""
    return logging.getLogger('ml_downloader')