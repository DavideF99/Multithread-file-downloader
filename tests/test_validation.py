# tests/test_validation.py
import pytest
import os
import hashlib
from unittest.mock import Mock, patch, mock_open
from src.downloader import (
    calculate_checksum,
    validate_checksum,
    download_and_validate
)


# ==================== Checksum Calculation Tests ====================

def test_calculate_md5_checksum(tmp_path):
    """Test MD5 checksum calculation."""
    test_file = tmp_path / "test.txt"
    content = b"Hello, World!"
    test_file.write_bytes(content)
    
    # Calculate expected MD5
    expected = hashlib.md5(content).hexdigest()
    
    # Test function
    actual = calculate_checksum(str(test_file), checksum_type='md5')
    
    assert actual == expected


def test_calculate_sha256_checksum(tmp_path):
    """Test SHA256 checksum calculation."""
    test_file = tmp_path / "test.txt"
    content = b"Hello, World!"
    test_file.write_bytes(content)
    
    # Calculate expected SHA256
    expected = hashlib.sha256(content).hexdigest()
    
    # Test function
    actual = calculate_checksum(str(test_file), checksum_type='sha256')
    
    assert actual == expected


def test_calculate_checksum_large_file(tmp_path):
    """Test checksum calculation with large file (multiple chunks)."""
    test_file = tmp_path / "large.bin"
    
    # Create 1MB file
    content = b'x' * (1024 * 1024)
    test_file.write_bytes(content)
    
    # Calculate expected
    expected = hashlib.md5(content).hexdigest()
    
    # Test function with small chunk size
    actual = calculate_checksum(str(test_file), checksum_type='md5', chunk_size=8192)
    
    assert actual == expected


def test_calculate_checksum_invalid_type(tmp_path):
    """Test that invalid checksum type raises error."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"test")
    
    with pytest.raises(ValueError, match="Unsupported checksum type"):
        calculate_checksum(str(test_file), checksum_type='sha512')


def test_calculate_checksum_nonexistent_file():
    """Test that missing file raises IOError."""
    with pytest.raises(IOError):
        calculate_checksum("/nonexistent/file.txt")


def test_calculate_checksum_empty_file(tmp_path):
    """Test checksum of empty file."""
    test_file = tmp_path / "empty.txt"
    test_file.write_bytes(b"")
    
    # MD5 of empty string
    expected = hashlib.md5(b"").hexdigest()
    
    actual = calculate_checksum(str(test_file), checksum_type='md5')
    
    assert actual == expected


def test_checksum_deterministic(tmp_path):
    """Test that same file always produces same checksum."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"Deterministic content")
    
    # Calculate twice
    checksum1 = calculate_checksum(str(test_file))
    checksum2 = calculate_checksum(str(test_file))
    
    assert checksum1 == checksum2


def test_checksum_different_for_different_content(tmp_path):
    """Test that different files produce different checksums."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    
    file1.write_bytes(b"Content A")
    file2.write_bytes(b"Content B")
    
    checksum1 = calculate_checksum(str(file1))
    checksum2 = calculate_checksum(str(file2))
    
    assert checksum1 != checksum2


# ==================== Checksum Validation Tests ====================

def test_validate_checksum_success(tmp_path):
    """Test successful checksum validation."""
    test_file = tmp_path / "test.txt"
    content = b"Test content"
    test_file.write_bytes(content)
    
    expected_checksum = hashlib.md5(content).hexdigest()
    
    # Should not raise exception
    result = validate_checksum(str(test_file), expected_checksum, checksum_type='md5')
    
    assert result is True


def test_validate_checksum_failure(tmp_path):
    """Test checksum validation failure."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"Actual content")
    
    wrong_checksum = "0" * 32  # Invalid MD5
    
    with pytest.raises(ValueError, match="Checksum mismatch"):
        validate_checksum(str(test_file), wrong_checksum, checksum_type='md5')


def test_validate_checksum_case_insensitive(tmp_path):
    """Test that checksum validation is case-insensitive."""
    test_file = tmp_path / "test.txt"
    content = b"Test"
    test_file.write_bytes(content)
    
    checksum = hashlib.md5(content).hexdigest()
    
    # Test uppercase
    result = validate_checksum(str(test_file), checksum.upper(), checksum_type='md5')
    assert result is True
    
    # Test lowercase
    result = validate_checksum(str(test_file), checksum.lower(), checksum_type='md5')
    assert result is True
    
    # Test mixed case
    mixed = checksum[:10].upper() + checksum[10:].lower()
    result = validate_checksum(str(test_file), mixed, checksum_type='md5')
    assert result is True


def test_validate_checksum_skip(tmp_path):
    """Test that 'skip' bypasses validation."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"Any content")
    
    # Should pass even with wrong content
    result = validate_checksum(str(test_file), 'skip', checksum_type='md5')
    
    assert result is True


def test_validate_checksum_skip_case_insensitive(tmp_path):
    """Test that 'SKIP', 'Skip', etc. all work."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"Content")
    
    for skip_variant in ['skip', 'SKIP', 'Skip', 'sKiP']:
        result = validate_checksum(str(test_file), skip_variant, checksum_type='md5')
        assert result is True


def test_validate_sha256_checksum(tmp_path):
    """Test SHA256 validation."""
    test_file = tmp_path / "test.txt"
    content = b"SHA256 test"
    test_file.write_bytes(content)
    
    expected_checksum = hashlib.sha256(content).hexdigest()
    
    result = validate_checksum(str(test_file), expected_checksum, checksum_type='sha256')
    
    assert result is True


# ==================== Integration Tests ====================

def test_download_and_validate_success(tmp_path):
    """Test successful download with validation."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Test content"
    expected_checksum = hashlib.md5(content).hexdigest()
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress'), \
         patch('src.downloader.cleanup_progress_file'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should succeed
        download_and_validate(
            url,
            str(destination),
            expected_size=len(content),
            checksum=expected_checksum,
            checksum_type='md5'
        )
        
        # Verify file exists and content is correct
        assert destination.exists()
        assert destination.read_bytes() == content


def test_download_and_validate_checksum_failure_deletes_file(tmp_path):
    """Test that failed validation deletes the file."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Downloaded content"
    wrong_checksum = "0" * 32  # Wrong checksum
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should raise validation error
        with pytest.raises(ValueError, match="Checksum mismatch"):
            download_and_validate(
                url,
                str(destination),
                checksum=wrong_checksum,
                checksum_type='md5'
            )
        
        # Verify file was deleted
        assert not destination.exists()


def test_download_and_validate_no_checksum(tmp_path):
    """Test download without checksum validation."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress'), \
         patch('src.downloader.cleanup_progress_file'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should succeed without validation
        download_and_validate(url, str(destination), checksum=None)
        
        assert destination.exists()


def test_download_and_validate_with_skip_checksum(tmp_path):
    """Test download with 'skip' checksum."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Content"
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress'), \
         patch('src.downloader.cleanup_progress_file'):
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        # Should succeed and skip validation
        download_and_validate(url, str(destination), checksum='skip')
        
        assert destination.exists()


def test_download_and_validate_cleans_up_progress_on_success(tmp_path):
    """Test that progress file is cleaned up after successful validation."""
    url = "http://example.com/test.txt"
    destination = tmp_path / "test.txt"
    content = b"Content"
    checksum = hashlib.md5(content).hexdigest()
    
    with patch('src.downloader.requests.get') as mock_get, \
         patch('src.downloader.load_progress', return_value=None), \
         patch('src.downloader.save_progress'), \
         patch('src.downloader.cleanup_progress_file') as mock_cleanup:
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Length': str(len(content))}
        mock_response.iter_content = Mock(return_value=[content])
        mock_get.return_value = mock_response
        
        download_and_validate(url, str(destination), checksum=checksum)
        
        # Verify cleanup was called
        mock_cleanup.assert_called_once_with(str(destination))


# ==================== Edge Cases ====================

def test_checksum_with_binary_data(tmp_path):
    """Test checksum calculation with binary data (not text)."""
    test_file = tmp_path / "binary.bin"
    
    # Binary data with all byte values
    content = bytes(range(256))
    test_file.write_bytes(content)
    
    expected = hashlib.md5(content).hexdigest()
    actual = calculate_checksum(str(test_file))
    
    assert actual == expected


def test_checksum_with_large_file_multiple_chunks(tmp_path):
    """Test that chunked reading produces same result as whole file."""
    test_file = tmp_path / "large.bin"
    
    # 10MB file
    content = b'A' * (10 * 1024 * 1024)
    test_file.write_bytes(content)
    
    # Calculate with different chunk sizes
    checksum_8k = calculate_checksum(str(test_file), chunk_size=8192)
    checksum_64k = calculate_checksum(str(test_file), chunk_size=65536)
    checksum_1m = calculate_checksum(str(test_file), chunk_size=1024*1024)
    
    # All should be identical
    assert checksum_8k == checksum_64k == checksum_1m


def test_validation_error_message_includes_both_checksums(tmp_path):
    """Test that validation error shows both expected and actual checksums."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"Content")
    
    wrong_checksum = "0" * 32
    
    try:
        validate_checksum(str(test_file), wrong_checksum)
        pytest.fail("Should have raised ValueError")
    except ValueError as e:
        error_message = str(e)
        # Verify both checksums are in error message
        assert "expected" in error_message.lower()
        assert "got" in error_message.lower()
        assert wrong_checksum in error_message