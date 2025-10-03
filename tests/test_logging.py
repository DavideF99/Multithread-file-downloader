import logging
import pytest
import threading
from src.logger import setup_logging, get_logger


@pytest.fixture(autouse=True)
def cleanup_logger():
    """Clean up logger handlers before and after each test."""
    # Clean up before test
    logger = logging.getLogger('ml_downloader')
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    
    yield  # Run the test
    
    # Clean up after test
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_logger_creates_log_file(tmp_path):
    """Test that logger creates a log file."""
    log_file = tmp_path / "test.log"
    
    logger = setup_logging(log_file=str(log_file))
    logger.info("Test message")
    
    # Flush before reading
    for handler in logger.handlers:
        handler.flush()
    
    assert log_file.exists(), f"Log file {log_file} was not created"
    content = log_file.read_text()
    assert "Test message" in content


def test_logger_writes_correct_format(tmp_path):
    """Test log message format includes required fields."""
    log_file = tmp_path / "test.log"
    logger = setup_logging(log_file=str(log_file))
    
    logger.info("Test message")
    
    # Flush handlers
    for handler in logger.handlers:
        handler.flush()
    
    # Check file content
    content = log_file.read_text()
    assert "[INFO]" in content
    assert "Thread-" in content
    assert "Test message" in content


def test_file_contains_debug_messages(tmp_path):
    """Test that file handler captures DEBUG level."""
    log_file = tmp_path / "test.log"
    logger = setup_logging(log_file=str(log_file))
    
    logger.debug("Debug message")
    logger.info("Info message")
    
    # Flush handlers
    for handler in logger.handlers:
        handler.flush()
    
    # File should have both
    content = log_file.read_text()
    assert "Debug message" in content
    assert "[DEBUG]" in content
    assert "Info message" in content
    assert "[INFO]" in content


def test_multiple_threads_log_safely(tmp_path):
    """Test thread-safe logging with concurrent writes."""
    log_file = tmp_path / "test.log"
    logger = setup_logging(log_file=str(log_file))
    
    def log_messages(thread_id):
        for i in range(10):
            logger.info(f"Thread {thread_id} message {i}")
    
    # Spawn 5 threads
    threads = []
    for i in range(5):
        t = threading.Thread(target=log_messages, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    # Flush handlers
    for handler in logger.handlers:
        handler.flush()
    
    # Verify file exists and has 50 messages
    assert log_file.exists(), "Log file was not created"
    
    content = log_file.read_text()
    lines = [line for line in content.split('\n') if line.strip()]
    assert len(lines) == 50, f"Expected 50 log lines, got {len(lines)}"


def test_logger_singleton_behavior(tmp_path):
    """Test that calling setup_logging multiple times doesn't duplicate handlers."""
    log_file = tmp_path / "test.log"
    
    logger1 = setup_logging(log_file=str(log_file))
    logger2 = setup_logging(log_file=str(log_file))
    
    # Should be same logger
    assert logger1 is logger2
    
    # Log one message
    logger1.info("Single message")
    
    # Flush handlers
    for handler in logger1.handlers:
        handler.flush()
    
    # Should only appear once
    content = log_file.read_text()
    assert content.count("Single message") == 1, "Message logged multiple times!"