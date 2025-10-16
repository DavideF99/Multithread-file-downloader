"""
Tests for progress tracking utilities.

Run with: pytest tests/test_progress_tracker.py -v
"""

import pytest
import os
import json
import time
import tempfile
import shutil
from datetime import datetime
from src.progress_tracker import (
    load_progress,
    save_progress,
    get_progress_file_path,
    validate_partial_file,
    cleanup_progress_file,
    get_all_progress_files,
    cleanup_stale_progress_files
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    # Cleanup
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


@pytest.fixture
def sample_progress_data():
    """Sample progress data for testing."""
    return {
        'url': 'https://example.com/data.tar.gz',
        'destination': 'downloads/data.tar.gz',
        'downloaded_bytes': 1024000,
        'total_size': 5242880,
        'checksum': 'abc123def456',
        'checksum_type': 'md5',
        'status': 'in_progress'
    }


# ==================== Basic Save/Load Tests ====================

def test_save_and_load_progress(temp_dir, sample_progress_data):
    """Test saving and loading progress data."""
    progress_file = os.path.join(temp_dir, 'test.progress')
    
    # Save progress
    save_progress(progress_file, sample_progress_data)
    
    # File should exist
    assert os.path.exists(progress_file)
    
    # Load progress
    loaded = load_progress(progress_file)
    
    # Verify data (excluding timestamp)
    assert loaded['url'] == sample_progress_data['url']
    assert loaded['destination'] == sample_progress_data['destination']
    assert loaded['downloaded_bytes'] == sample_progress_data['downloaded_bytes']
    assert loaded['total_size'] == sample_progress_data['total_size']
    assert 'last_updated' in loaded


def test_load_nonexistent_progress(temp_dir):
    """Test loading progress from non-existent file."""
    progress_file = os.path.join(temp_dir, 'nonexistent.progress')
    result = load_progress(progress_file)
    assert result is None


def test_load_corrupted_json(temp_dir):
    """Test loading corrupted JSON returns None."""
    progress_file = os.path.join(temp_dir, 'corrupted.progress')
    
    with open(progress_file, 'w') as f:
        f.write('{ invalid json syntax }')
    
    result = load_progress(progress_file)
    assert result is None


def test_save_creates_directory(temp_dir):
    """Test that save_progress creates nested directories."""
    progress_file = os.path.join(temp_dir, 'nested', 'dir', 'test.progress')
    
    save_progress(progress_file, {'test': 'data'})
    
    assert os.path.exists(progress_file)
    assert os.path.exists(os.path.dirname(progress_file))


def test_save_adds_timestamp(temp_dir, sample_progress_data):
    """Test that save_progress adds timestamp."""
    progress_file = os.path.join(temp_dir, 'test.progress')
    
    save_progress(progress_file, sample_progress_data)
    
    loaded = load_progress(progress_file)
    assert 'last_updated' in loaded
    
    # Verify it's a valid ISO timestamp
    timestamp_str = loaded['last_updated']
    # Should not raise exception
    datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))


def test_save_overwrites_existing(temp_dir):
    """Test that saving overwrites existing progress file."""
    progress_file = os.path.join(temp_dir, 'test.progress')
    
    # Save initial data
    save_progress(progress_file, {'downloaded_bytes': 100})
    
    # Save updated data
    save_progress(progress_file, {'downloaded_bytes': 200})
    
    # Load and verify
    loaded = load_progress(progress_file)
    assert loaded['downloaded_bytes'] == 200


def test_save_atomic_write(temp_dir, sample_progress_data):
    """Test that save uses atomic write (temp file + rename)."""
    progress_file = os.path.join(temp_dir, 'test.progress')
    
    save_progress(progress_file, sample_progress_data)
    
    # Verify temp file doesn't exist after save
    temp_file = progress_file + '.tmp'
    assert not os.path.exists(temp_file)
    
    # Verify final file exists
    assert os.path.exists(progress_file)


# ==================== Path Generation Tests ====================

def test_get_progress_file_path_simple():
    """Test progress file path for simple filename."""
    result = get_progress_file_path('data.txt')
    assert result == '.progress/data.txt.progress'


def test_get_progress_file_path_with_directory():
    """Test progress file path with directory structure."""
    result = get_progress_file_path('downloads/cifar10/data.tar.gz')
    assert result == '.progress/downloads/cifar10/data.tar.gz.progress'


def test_get_progress_file_path_custom_base_dir():
    """Test progress file path with custom base directory."""
    result = get_progress_file_path('data.txt', base_dir='custom_progress')
    assert result == 'custom_progress/data.txt.progress'


def test_get_progress_file_path_no_directory():
    """Test progress file path when destination has no directory."""
    result = get_progress_file_path('file.txt')
    assert result == '.progress/file.txt.progress'


def test_get_progress_file_path_absolute_path():
    """Test progress file path with absolute path."""
    result = get_progress_file_path('/absolute/path/file.txt')
    # Should mirror the structure
    assert 'absolute/path/file.txt.progress' in result


# ==================== Partial File Validation Tests ====================

def test_validate_partial_file_valid(temp_dir):
    """Test validation of valid partial file."""
    partial_file = os.path.join(temp_dir, 'partial.dat')
    
    # Create file with exact size
    data = b'x' * 1024
    with open(partial_file, 'wb') as f:
        f.write(data)
    
    assert validate_partial_file(partial_file, 1024) is True


def test_validate_partial_file_wrong_size(temp_dir):
    """Test validation fails when size doesn't match."""
    partial_file = os.path.join(temp_dir, 'partial.dat')
    
    # Create file
    with open(partial_file, 'wb') as f:
        f.write(b'x' * 500)
    
    # Expected different size
    assert validate_partial_file(partial_file, 1000) is False


def test_validate_partial_file_nonexistent():
    """Test validation fails for nonexistent file."""
    assert validate_partial_file('/nonexistent/file.dat', 100) is False


def test_validate_partial_file_empty(temp_dir):
    """Test validation of empty file."""
    partial_file = os.path.join(temp_dir, 'empty.dat')
    
    # Create empty file
    open(partial_file, 'w').close()
    
    assert validate_partial_file(partial_file, 0) is True
    assert validate_partial_file(partial_file, 100) is False


# ==================== Cleanup Tests ====================

def test_cleanup_progress_file(temp_dir):
    """Test cleaning up progress file."""
    destination = os.path.join(temp_dir, 'downloads', 'file.txt')
    progress_dir = os.path.join(temp_dir, '.progress', 'downloads')
    progress_file = os.path.join(progress_dir, 'file.txt.progress')
    
    # Create progress file
    os.makedirs(progress_dir, exist_ok=True)
    with open(progress_file, 'w') as f:
        json.dump({'test': 'data'}, f)
    
    assert os.path.exists(progress_file)
    
    # Cleanup
    cleanup_progress_file(destination, base_dir=os.path.join(temp_dir, '.progress'))
    
    # Should be deleted
    assert not os.path.exists(progress_file)


def test_cleanup_nonexistent_progress_file(temp_dir):
    """Test cleanup of nonexistent file doesn't raise error."""
    destination = os.path.join(temp_dir, 'nonexistent.txt')
    
    # Should not raise exception
    cleanup_progress_file(destination, base_dir=os.path.join(temp_dir, '.progress'))


# ==================== Get All Progress Files Tests ====================

def test_get_all_progress_files_empty(temp_dir):
    """Test getting progress files from empty directory."""
    progress_dir = os.path.join(temp_dir, '.progress')
    result = get_all_progress_files(base_dir=progress_dir)
    
    assert result == {}


def test_get_all_progress_files_single(temp_dir):
    """Test getting single progress file."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    progress_file = os.path.join(progress_dir, 'file1.txt.progress')
    progress_data = {
        'destination': 'downloads/file1.txt',
        'downloaded_bytes': 100,
        'total_size': 1000
    }
    
    with open(progress_file, 'w') as f:
        json.dump(progress_data, f)
    
    result = get_all_progress_files(base_dir=progress_dir)
    
    assert len(result) == 1
    assert 'downloads/file1.txt' in result
    assert result['downloads/file1.txt']['downloaded_bytes'] == 100


def test_get_all_progress_files_multiple(temp_dir):
    """Test getting multiple progress files."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Create multiple progress files
    files = {
        'file1.txt.progress': {'destination': 'downloads/file1.txt', 'downloaded_bytes': 100},
        'file2.txt.progress': {'destination': 'downloads/file2.txt', 'downloaded_bytes': 200},
        'file3.txt.progress': {'destination': 'downloads/file3.txt', 'downloaded_bytes': 300}
    }
    
    for filename, data in files.items():
        progress_file = os.path.join(progress_dir, filename)
        with open(progress_file, 'w') as f:
            json.dump(data, f)
    
    result = get_all_progress_files(base_dir=progress_dir)
    
    assert len(result) == 3
    assert all(dest in result for dest in [f['destination'] for f in files.values()])


def test_get_all_progress_files_nested(temp_dir):
    """Test getting progress files from nested directories."""
    progress_dir = os.path.join(temp_dir, '.progress')
    
    # Create nested structure
    nested_dir = os.path.join(progress_dir, 'downloads', 'subdir')
    os.makedirs(nested_dir, exist_ok=True)
    
    progress_file = os.path.join(nested_dir, 'file.txt.progress')
    progress_data = {'destination': 'downloads/subdir/file.txt', 'downloaded_bytes': 50}
    
    with open(progress_file, 'w') as f:
        json.dump(progress_data, f)
    
    result = get_all_progress_files(base_dir=progress_dir)
    
    assert len(result) == 1
    assert 'downloads/subdir/file.txt' in result


def test_get_all_progress_files_ignores_invalid(temp_dir):
    """Test that invalid progress files are skipped."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Valid file
    valid_file = os.path.join(progress_dir, 'valid.progress')
    with open(valid_file, 'w') as f:
        json.dump({'destination': 'downloads/valid.txt'}, f)
    
    # Invalid JSON
    invalid_file = os.path.join(progress_dir, 'invalid.progress')
    with open(invalid_file, 'w') as f:
        f.write('{ invalid }')
    
    # Missing destination
    no_dest_file = os.path.join(progress_dir, 'no_dest.progress')
    with open(no_dest_file, 'w') as f:
        json.dump({'url': 'http://example.com'}, f)
    
    result = get_all_progress_files(base_dir=progress_dir)
    
    # Only valid file should be included
    assert len(result) == 1
    assert 'downloads/valid.txt' in result


# ==================== Stale Progress Cleanup Tests ====================

def test_cleanup_stale_progress_files_fresh(temp_dir):
    """Test that fresh progress files are not deleted."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Create fresh progress file
    progress_file = os.path.join(progress_dir, 'fresh.progress')
    with open(progress_file, 'w') as f:
        json.dump({'test': 'data'}, f)
    
    # Cleanup stale files (older than 7 days)
    count = cleanup_stale_progress_files(base_dir=progress_dir, max_age_days=7)
    
    # Should not delete fresh file
    assert count == 0
    assert os.path.exists(progress_file)


def test_cleanup_stale_progress_files_old(temp_dir):
    """Test that old progress files are deleted."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Create progress file
    progress_file = os.path.join(progress_dir, 'old.progress')
    with open(progress_file, 'w') as f:
        json.dump({'test': 'data'}, f)
    
    # Modify file timestamp to make it old (8 days ago)
    old_time = time.time() - (8 * 24 * 60 * 60)
    os.utime(progress_file, (old_time, old_time))
    
    # Cleanup stale files (older than 7 days)
    count = cleanup_stale_progress_files(base_dir=progress_dir, max_age_days=7)
    
    # Should delete old file
    assert count == 1
    assert not os.path.exists(progress_file)


def test_cleanup_stale_progress_files_mixed(temp_dir):
    """Test cleanup with mix of old and fresh files."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Create fresh file
    fresh_file = os.path.join(progress_dir, 'fresh.progress')
    with open(fresh_file, 'w') as f:
        json.dump({'test': 'fresh'}, f)
    
    # Create old files
    old_files = []
    for i in range(3):
        old_file = os.path.join(progress_dir, f'old{i}.progress')
        with open(old_file, 'w') as f:
            json.dump({'test': f'old{i}'}, f)
        
        # Make it 10 days old
        old_time = time.time() - (10 * 24 * 60 * 60)
        os.utime(old_file, (old_time, old_time))
        old_files.append(old_file)
    
    # Cleanup
    count = cleanup_stale_progress_files(base_dir=progress_dir, max_age_days=7)
    
    # Should delete 3 old files
    assert count == 3
    
    # Fresh file should still exist
    assert os.path.exists(fresh_file)
    
    # Old files should be deleted
    for old_file in old_files:
        assert not os.path.exists(old_file)


def test_cleanup_stale_progress_files_empty_dir(temp_dir):
    """Test cleanup on empty directory."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    count = cleanup_stale_progress_files(base_dir=progress_dir)
    
    assert count == 0


def test_cleanup_stale_progress_files_nonexistent_dir(temp_dir):
    """Test cleanup on nonexistent directory."""
    progress_dir = os.path.join(temp_dir, 'nonexistent')
    
    count = cleanup_stale_progress_files(base_dir=progress_dir)
    
    assert count == 0


def test_cleanup_stale_progress_files_custom_age(temp_dir):
    """Test cleanup with custom max age."""
    progress_dir = os.path.join(temp_dir, '.progress')
    os.makedirs(progress_dir, exist_ok=True)
    
    # Create file that's 2 days old
    progress_file = os.path.join(progress_dir, 'test.progress')
    with open(progress_file, 'w') as f:
        json.dump({'test': 'data'}, f)
    
    two_days_ago = time.time() - (2 * 24 * 60 * 60)
    os.utime(progress_file, (two_days_ago, two_days_ago))
    
    # Cleanup with 1 day threshold - should delete
    count = cleanup_stale_progress_files(base_dir=progress_dir, max_age_days=1)
    assert count == 1
    assert not os.path.exists(progress_file)


# ==================== Edge Cases ====================

def test_save_progress_empty_dict(temp_dir):
    """Test saving empty progress dict."""
    progress_file = os.path.join(temp_dir, 'empty.progress')
    
    save_progress(progress_file, {})
    
    loaded = load_progress(progress_file)
    assert loaded == {'last_updated': loaded['last_updated']}  # Only timestamp


def test_save_progress_with_nested_data(temp_dir):
    """Test saving progress with nested structures."""
    progress_file = os.path.join(temp_dir, 'nested.progress')
    
    data = {
        'url': 'http://example.com',
        'metadata': {
            'retries': 3,
            'errors': ['timeout', 'connection']
        },
        'chunks': [{'id': 0, 'size': 1024}, {'id': 1, 'size': 2048}]
    }
    
    save_progress(progress_file, data)
    loaded = load_progress(progress_file)
    
    assert loaded['metadata']['retries'] == 3
    assert len(loaded['chunks']) == 2


def test_progress_file_with_unicode(temp_dir):
    """Test progress file with Unicode characters."""
    progress_file = os.path.join(temp_dir, 'unicode.progress')
    
    data = {
        'destination': 'downloads/文件.txt',
        'url': 'http://example.com/données.tar.gz'
    }
    
    save_progress(progress_file, data)
    loaded = load_progress(progress_file)
    
    assert loaded['destination'] == 'downloads/文件.txt'


def test_concurrent_save_progress(temp_dir):
    """Test that concurrent saves don't corrupt file."""
    import threading
    
    progress_file = os.path.join(temp_dir, 'concurrent.progress')
    
    def save_thread(thread_id):
        for i in range(10):
            save_progress(progress_file, {
                'thread': thread_id,
                'iteration': i
            })
    
    # Run 5 threads concurrently
    threads = []
    for i in range(5):
        t = threading.Thread(target=save_thread, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # File should still be valid JSON
    loaded = load_progress(progress_file)
    assert loaded is not None
    assert 'thread' in loaded
    assert 'iteration' in loaded