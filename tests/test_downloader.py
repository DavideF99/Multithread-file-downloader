# tests/test_downloader.py
import pytest
import os
import requests
from unittest.mock import Mock, patch, mock_open
from src.downloader import download_file


# ==================== Successful Download Tests ====================

def test_successful_download(tmp_path):
    """Test downloading a file successfully."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    expected_content = b"This is test content"
    
    # Mock the requests.get() call
    with patch('src.downloader.requests.get') as mock_get:
        # Create mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(expected_content))}
        mock_response.iter_content = Mock(return_value=[expected_content])
        mock_get.return_value = mock_response
        
        # Call function
        download_file(url, str(destination), expected_size=len(expected_content))
        
        # Verify file was created and content is correct
        assert destination.exists()
        assert destination.read_bytes() == expected_content
        
        # Verify requests.get was called correctly
        mock_get.assert_called_once_with(url, stream=True, timeout=30)


def test_download_without_content_length(tmp_path):
    """Test download succeeds even without Content-Length header."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Test content"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}  # No Content-Length
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should not raise exception
        download_file(url, str(destination))
        
        assert destination.exists()
        assert destination.read_bytes() == content


def test_download_creates_nested_directories(tmp_path):
    """Test that nested directories are created if they don't exist."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "subdir1" / "subdir2" / "test.txt"
    content = b"Test"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': '4'}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        download_file(url, str(destination))
        
        assert destination.exists()
        assert destination.parent.exists()  # subdir2 created
        assert destination.parent.parent.exists()  # subdir1 created


# ==================== Retry Logic Tests ====================

def test_download_retries_on_timeout(tmp_path):
    """Test that timeout triggers retry and eventually succeeds."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Success"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.time.sleep') as mock_sleep:
        
        # First call: Timeout
        # Second call: Success
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {'Content-Length': '7'}
        mock_response_success.iter_content = Mock(return_value=[content])
        
        mock_get.side_effect = [
            requests.exceptions.Timeout("Connection timeout"),
            mock_response_success
        ]
        
        # Should succeed after retry
        download_file(url, str(destination), max_retries=3)
        
        # Verify retry happened
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()  # Waited between retries
        
        # Verify file was created
        assert destination.exists()


def test_download_retries_on_500_error(tmp_path):
    """Test that 5xx server errors trigger retry."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Success"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.time.sleep') as mock_sleep:
        
        # First call: 500 error
        mock_response_error = Mock()
        mock_response_error.status_code = 500
        
        # Create HTTPError with response attribute
        error = requests.exceptions.HTTPError("500 Server Error")
        error.response = mock_response_error  # ← ADD THIS
        mock_response_error.raise_for_status.side_effect = error
        
        # Second call: Success
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {'Content-Length': '7'}
        mock_response_success.iter_content = Mock(return_value=[content])
        
        mock_get.side_effect = [mock_response_error, mock_response_success]
        
        download_file(url, str(destination), max_retries=3)
        
        assert mock_get.call_count == 2
        assert destination.exists()


def test_download_exponential_backoff(tmp_path):
    """Test that backoff delay increases exponentially."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.time.sleep') as mock_sleep:
        
        # All attempts timeout
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")
        
        # Should raise after max_retries
        with pytest.raises(Exception, match="Failed to download"):
            download_file(url, str(destination), max_retries=3, base_delay=1)
        
        # Verify exponential backoff: 2, 4 seconds (attempts 1, 2)
        assert mock_sleep.call_count == 2
        calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert calls[0] == 2  # 1 * 2^1
        assert calls[1] == 4  # 1 * 2^2


def test_download_respects_max_delay(tmp_path):
    """Test that backoff is capped at max_delay."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.time.sleep') as mock_sleep:
        
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")
        
        with pytest.raises(Exception):
            download_file(url, str(destination), max_retries=5, 
                         base_delay=1, max_delay=5)
        
        # Check that no delay exceeds max_delay
        for call in mock_sleep.call_args_list:
            delay = call[0][0]
            assert delay <= 5


# ==================== Error Handling Tests ====================

def test_download_fails_immediately_on_404(tmp_path):
    """Test that 404 doesn't retry."""
    url = "http://example.com/notfound.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Should raise immediately without retry
        with pytest.raises(ValueError, match="URL not found.*404"):
            download_file(url, str(destination), max_retries=3)
        
        # Verify only called once (no retries)
        assert mock_get.call_count == 1


def test_download_fails_immediately_on_403(tmp_path):
    """Test that 403 Forbidden doesn't retry."""
    url = "http://example.com/forbidden.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="Access forbidden.*403"):
            download_file(url, str(destination))
        
        assert mock_get.call_count == 1


def test_download_size_mismatch_raises_error(tmp_path):
    """Test that size mismatch between expected and Content-Length raises error."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': '1000'}
        mock_response.iter_content = Mock(return_value=[b"test"])
        mock_get.return_value = mock_response
        
        # Expected 500 bytes, but server says 1000
        with pytest.raises(ValueError, match="Size mismatch.*expected 500.*got 1000"):
            download_file(url, str(destination), expected_size=500)


def test_download_validates_final_file_size(tmp_path):
    """Test that final downloaded file size is validated."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"short"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': '100'}  # Says 100
        mock_response.iter_content = Mock(return_value=[content])  # Only 5 bytes
        mock_get.return_value = mock_response
        
        # Should raise because downloaded size != expected size
        with pytest.raises(ValueError, match="Downloaded file size mismatch"):
            download_file(url, str(destination), expected_size=100)


def test_download_exhausts_retries(tmp_path):
    """Test that after max_retries, function raises exception."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.time.sleep'):
        
        # All attempts fail
        mock_get.side_effect = requests.exceptions.Timeout("Always timeout")
        
        with pytest.raises(Exception, match="Failed to download.*after 3 attempts"):
            download_file(url, str(destination), max_retries=3)
        
        # Verify it tried max_retries times
        assert mock_get.call_count == 3


# ==================== Edge Cases ====================

def test_download_with_chunked_response(tmp_path):
    """Test downloading file that comes in multiple chunks."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Simulate chunked response
    chunks = [b"chunk1", b"chunk2", b"chunk3"]
    total_size = sum(len(c) for c in chunks)
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(total_size)}
        mock_response.iter_content = Mock(return_value=chunks)
        mock_get.return_value = mock_response
        
        download_file(url, str(destination))
        
        # Verify all chunks were written
        assert destination.read_bytes() == b"chunk1chunk2chunk3"


def test_download_filters_empty_chunks(tmp_path):
    """Test that empty keep-alive chunks are filtered."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Mix of real chunks and empty keep-alive chunks
    chunks = [b"data1", b"", b"data2", None, b"data3"]
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': '15'}
        mock_response.iter_content = Mock(return_value=chunks)
        mock_get.return_value = mock_response
        
        download_file(url, str(destination))
        
        # Should only write non-empty chunks
        assert destination.read_bytes() == b"data1data2data3"


def test_download_with_no_expected_size(tmp_path):
    """Test that download works when expected_size is None."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Test content"
    
    with patch('src.downloader.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': '12'}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # No expected_size provided - should not validate
        download_file(url, str(destination), expected_size=None)
        
        assert destination.exists()