# tests/test_resume.py
import pytest
import os
import json
import requests
from unittest.mock import Mock, patch, mock_open
from src.downloader import (
    download_with_resume,
    load_progress,
    save_progress,
    get_progress_file_path,
    validate_partial_file,
    cleanup_progress_file
)


# ==================== Helper Function Tests ====================

def test_get_progress_file_path():
    """Test progress file path generation."""
    # Simple file
    assert get_progress_file_path("data.txt") == ".progress/data.txt.progress"
    
    # Nested path
    assert get_progress_file_path("downloads/cifar10/data.tar.gz") == \
           ".progress/downloads/cifar10/data.tar.gz.progress"


def test_save_and_load_progress(tmp_path):
    """Test saving and loading progress data."""
    progress_file = tmp_path / "test.progress"
    
    progress_data = {
        'url': 'http://example.com/file.txt',
        'downloaded_bytes': 5000,
        'total_size': 10000
    }
    
    # Save
    save_progress(str(progress_file), progress_data)
    
    # Verify file exists
    assert progress_file.exists()
    
    # Load
    loaded = load_progress(str(progress_file))
    
    # Verify data (note: last_updated added by save_progress)
    assert loaded['url'] == 'http://example.com/file.txt'
    assert loaded['downloaded_bytes'] == 5000
    assert loaded['total_size'] == 10000
    assert 'last_updated' in loaded


def test_load_progress_nonexistent_file():
    """Test loading progress from non-existent file returns None."""
    result = load_progress("/nonexistent/path/file.progress")
    assert result is None


def test_load_progress_corrupted_json(tmp_path):
    """Test loading corrupted JSON returns None."""
    progress_file = tmp_path / "corrupted.progress"
    progress_file.write_text("{ invalid json }")
    
    result = load_progress(str(progress_file))
    assert result is None


def test_validate_partial_file(tmp_path):
    """Test partial file validation."""
    partial_file = tmp_path / "partial.txt"
    partial_file.write_bytes(b"12345")  # 5 bytes
    
    # Correct size
    assert validate_partial_file(str(partial_file), 5) is True
    
    # Wrong size
    assert validate_partial_file(str(partial_file), 10) is False
    
    # Non-existent file
    assert validate_partial_file(str(tmp_path / "missing.txt"), 5) is False


def test_cleanup_progress_file(tmp_path):
    """Test progress file cleanup."""
    destination = tmp_path / "downloads" / "file.txt"
    progress_file = tmp_path / ".progress" / "downloads" / "file.txt.progress"
    
    # Create progress file
    os.makedirs(progress_file.parent, exist_ok=True)
    progress_file.write_text("{}")
    
    # Cleanup with mocked path resolution
    with patch('src.downloader.get_progress_file_path', return_value=str(progress_file)):
        cleanup_progress_file(str(destination))
    
    # Verify deleted
    assert not progress_file.exists()


# ==================== Resume Download Tests ====================

def test_download_from_scratch_no_progress(tmp_path):
    """Test fresh download when no progress file exists."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Test content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress') as mock_save:
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination), expected_size=len(content))
        
        # Verify file created
        assert destination.exists()
        assert destination.read_bytes() == content
        
        # Verify no Range header used
        call_kwargs = mock_get.call_args[1]
        assert 'headers' not in call_kwargs or 'Range' not in call_kwargs.get('headers', {})


def test_resume_from_partial_download(tmp_path):
    """Test resuming from partial download with valid progress."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Create partial file (first 5 bytes)
    destination.write_bytes(b"Hello")
    
    # Remaining content
    remaining = b" World"
    total_content = b"Hello World"
    
    # Mock progress data
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': 5,
        'total_size': len(total_content),
        'status': 'in_progress'
    }
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress') as mock_save:
        
        mock_response = Mock()
        mock_response.status_code = 206  # Partial Content
        mock_response.headers = {'Content-Length': str(len(remaining))}
        mock_response.iter_content = Mock(return_value=[remaining])
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination), expected_size=len(total_content))
        
        # Verify complete file
        assert destination.read_bytes() == total_content
        
        # Verify Range header was used
        call_kwargs = mock_get.call_args[1]
        assert 'headers' in call_kwargs
        assert call_kwargs['headers']['Range'] == 'bytes=5-'


def test_resume_with_size_mismatch_starts_fresh(tmp_path):
    """Test that file size mismatch triggers fresh download."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Create partial file with wrong size
    destination.write_bytes(b"123")  # 3 bytes
    
    # Progress says 10 bytes downloaded (mismatch!)
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': 10,  # Wrong!
        'total_size': 100
    }
    
    content = b"Fresh content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination))
        
        # Verify started fresh (no Range header)
        call_kwargs = mock_get.call_args[1]
        assert 'headers' not in call_kwargs or 'Range' not in call_kwargs.get('headers', {})
        
        # Verify new content
        assert destination.read_bytes() == content


def test_resume_with_url_mismatch_starts_fresh(tmp_path):
    """Test that URL mismatch in progress triggers fresh download."""
    url = "http://example.com/new_file.txt"
    destination = tmp_path / "test.txt"
    
    # Progress has different URL
    progress_data = {
        'url': 'http://example.com/old_file.txt',  # Different!
        'destination': str(destination),
        'downloaded_bytes': 50,
        'total_size': 100
    }
    
    content = b"New file content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination))
        
        # Verify no Range header (fresh download)
        call_kwargs = mock_get.call_args[1]
        assert 'headers' not in call_kwargs or 'Range' not in call_kwargs.get('headers', {})


def test_server_doesnt_support_resume_starts_fresh(tmp_path):
    """Test fallback to fresh download when server doesn't support ranges."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Create partial file
    destination.write_bytes(b"Partial")
    
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': 7,
        'total_size': 20
    }
    
    full_content = b"Complete file content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'):
        
        # Server returns 200 instead of 206 (doesn't support ranges)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(full_content))}
        mock_response.iter_content = Mock(return_value=[full_content])
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination))
        
        # Verify complete file (overwrote partial)
        assert destination.read_bytes() == full_content


def test_resume_with_416_range_not_satisfiable(tmp_path):
    """Test handling of 416 status (file already complete)."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    content = b"Complete file"
    destination.write_bytes(content)
    
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': len(content),
        'total_size': len(content)
    }
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress') as mock_save:
        
        # Server returns 416 (requested range not satisfiable)
        mock_response = Mock()
        mock_response.status_code = 416
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination), expected_size=len(content))
        
        # Should detect file is complete and return
        # Verify progress marked as complete
        save_calls = [call[0][1] for call in mock_save.call_args_list]
        assert any(call.get('status') == 'complete' for call in save_calls)


def test_progress_saved_periodically(tmp_path):
    """Test that progress is saved periodically during download."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Create large chunks to trigger multiple saves
    chunk_size = 512 * 1024  # 512 KB chunks
    chunks = [b'x' * chunk_size for _ in range(5)]  # 2.5 MB total
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress') as mock_save:
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(chunks) * chunk_size)}
        mock_response.iter_content = Mock(return_value=chunks)
        mock_get.return_value = mock_response
        
        download_with_resume(url, str(destination))
        
        # Verify save_progress called multiple times (not just at start/end)
        # Should be called at: init, ~every 1MB, and completion
        assert mock_save.call_count >= 3


def test_resume_retries_on_timeout(tmp_path):
    """Test that resume capability works with retry logic."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    # Create partial file
    partial = b"Partial"
    destination.write_bytes(partial)
    
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': len(partial),
        'total_size': 20
    }
    
    remaining = b" content here"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'), \
         patch('src.downloader.time.sleep'):
        
        # First attempt: Timeout
        # Second attempt: Success with resume
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.headers = {'Content-Length': str(len(remaining))}
        mock_response.iter_content = Mock(return_value=[remaining])
        
        mock_get.side_effect = [
            requests.exceptions.Timeout("Network timeout"),
            mock_response
        ]
        
        download_with_resume(url, str(destination), max_retries=3)
        
        # Verify retried and resumed
        assert mock_get.call_count == 2
        
        # Second call should have Range header
        second_call_kwargs = mock_get.call_args_list[1][1]
        assert second_call_kwargs['headers']['Range'] == f'bytes={len(partial)}-'


def test_resume_validates_final_size(tmp_path):
    """Test that final file size is validated after resume."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    expected_size = 100
    
    # Create partial
    destination.write_bytes(b"x" * 50)
    
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': 50,
        'total_size': expected_size
    }
    
    # Server sends less than expected
    remaining = b"x" * 30  # Total will be 80, not 100
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'):
        
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.headers = {'Content-Length': '30'}
        mock_response.iter_content = Mock(return_value=[remaining])
        mock_get.return_value = mock_response
        
        # Should raise size mismatch error
        with pytest.raises(ValueError, match="Size mismatch: expected 100, got 80"):
            download_with_resume(url, str(destination), expected_size=expected_size)


# ==================== Edge Cases ====================

def test_resume_with_no_content_length_header(tmp_path):
    """Test resume when server doesn't send Content-Length."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    
    content = b"Content without length header"
    
    progress_data = {
        'url': url,
        'destination': str(destination),
        'downloaded_bytes': 0,
        'total_size': None
    }
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=progress_data), \
         patch('src.downloader.save_progress'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}  # No Content-Length
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should still work
        download_with_resume(url, str(destination))
        
        assert destination.exists()
        assert destination.read_bytes() == content


def test_atomic_progress_save(tmp_path):
    """Test that progress saves are atomic (use temp file)."""
    progress_file = tmp_path / "test.progress"
    
    progress_data = {'test': 'data'}
    
    with patch('os.replace') as mock_replace:
        save_progress(str(progress_file), progress_data)
        
        # Verify os.replace was called (atomic rename)
        mock_replace.assert_called_once()
        args = mock_replace.call_args[0]
        assert args[0].endswith('.tmp')  # Source is temp file
        assert args[1] == str(progress_file)  # Destination is final file