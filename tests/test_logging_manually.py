# Create test_logging_manual.py
from src.logger import setup_logging

logger = setup_logging()

logger.debug("This won't show in console, only file")
logger.info("This shows in both")
logger.warning("Warning message")
logger.error("Error message")