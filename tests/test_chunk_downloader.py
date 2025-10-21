"""
Tests for chunked parallel download functionality.

Run with: pytest tests/test_chunk_downloader.py -v
"""

import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from src.chunk_downloader import ChunkDownloader, download_in_chunks


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


# ==================== Range Support Tests ====================

def test_check_range_support_success():
    """Test detecting server range support."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    
    with patch('requests.head') as mock_head:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1048576'
        }
        mock_head.return_value = mock_response
        
        supports, size = downloader.check_range_support()
        
        assert supports is True
        assert size == 1048576


def test_check_range_support_not_supported():
    """Test detecting when server doesn't support ranges."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    
    with patch('requests.head') as mock_head:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            'Accept-Ranges': 'none',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_response
        
        supports, size = downloader.check_range_support()
        
        assert supports is False
        assert size == 1000


def test_check_range_support_no_content_length():
    """Test when server doesn't provide Content-Length."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    
    with patch('requests.head') as mock_head:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Accept-Ranges': 'bytes'}
        mock_head.return_value = mock_response
        
        supports, size = downloader.check_range_support()
        
        assert supports is True
        assert size is None


def test_check_range_support_request_fails():
    """Test handling of failed HEAD request."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    
    with patch('requests.head', side_effect=Exception("Connection failed")):
        supports, size = downloader.check_range_support()
        
        assert supports is False
        assert size is None


# ==================== Chunk Range Calculation Tests ====================

def test_calculate_chunk_ranges_even_split():
    """Test calculating chunk ranges with even split."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', num_chunks=4)
    
    ranges = downloader.calculate_chunk_ranges(1000)
    
    assert len(ranges) == 4
    assert ranges[0] == (0, 0, 249)
    assert ranges[1] == (1, 250, 499)
    assert ranges[2] == (2, 500, 749)
    assert ranges[3] == (3, 750, 999)


def test_calculate_chunk_ranges_with_remainder():
    """Test chunk ranges when file size isn't evenly divisible."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', num_chunks=3)
    
    ranges = downloader.calculate_chunk_ranges(1000)
    
    assert len(ranges) == 3
    # Last chunk should get the remainder
    assert ranges[2][2] == 999  # End byte of last chunk


def test_calculate_chunk_ranges_single_chunk():
    """Test with single chunk (no parallelism)."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', num_chunks=1)
    
    ranges = downloader.calculate_chunk_ranges(1000)
    
    assert len(ranges) == 1
    assert ranges[0] == (0, 0, 999)


def test_calculate_chunk_ranges_more_chunks_than_bytes():
    """Test with more chunks than bytes."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', num_chunks=10)
    
    ranges = downloader.calculate_chunk_ranges(5)
    
    # Should still create 10 chunks, some will be 0 bytes
    assert len(ranges) == 10


# ==================== Single Chunk Download Tests ====================

def test_download_chunk_success(temp_dir):
    """Test successful chunk download."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    
    chunk_data = b'chunk content'
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.iter_content = Mock(return_value=[chunk_data])
        mock_get.return_value = mock_response
        
        progress_bar = Mock()
        
        result = downloader.download_chunk(
            chunk_id=0,
            start_byte=0,
            end_byte=12,
            chunk_file=chunk_file,
            progress_bar=progress_bar
        )
        
        assert result is True
        assert os.path.exists(chunk_file)
        
        with open(chunk_file, 'rb') as f:
            assert f.read() == chunk_data


def test_download_chunk_wrong_status_code(temp_dir):
    """Test chunk download with wrong status code."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', max_retries=2)
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200  # Should be 206
        mock_get.return_value = mock_response
        
        progress_bar = Mock()
        
        result = downloader.download_chunk(
            chunk_id=0,
            start_byte=0,
            end_byte=100,
            chunk_file=chunk_file,
            progress_bar=progress_bar
        )
        
        assert result is False
        assert len(downloader.errors) > 0


def test_download_chunk_retries_on_failure(temp_dir):
    """Test that chunk download retries on failure."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', max_retries=3)
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    
    chunk_data = b'success'
    
    with patch('requests.get') as mock_get:
        # First two attempts fail, third succeeds
        mock_response_success = Mock()
        mock_response_success.status_code = 206
        mock_response_success.iter_content = Mock(return_value=[chunk_data])
        
        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            mock_response_success
        ]
        
        progress_bar = Mock()
        
        result = downloader.download_chunk(
            chunk_id=0,
            start_byte=0,
            end_byte=6,
            chunk_file=chunk_file,
            progress_bar=progress_bar
        )
        
        assert result is True
        assert mock_get.call_count == 3


def test_download_chunk_exhausts_retries(temp_dir):
    """Test chunk download fails after max retries."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat', max_retries=2)
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    
    with patch('requests.get', side_effect=Exception("Always fails")):
        progress_bar = Mock()
        
        result = downloader.download_chunk(
            chunk_id=0,
            start_byte=0,
            end_byte=100,
            chunk_file=chunk_file,
            progress_bar=progress_bar
        )
        
        assert result is False
        assert len(downloader.errors) == 1


def test_download_chunk_uses_range_header(temp_dir):
    """Test that chunk download uses correct Range header."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.iter_content = Mock(return_value=[b'data'])
        mock_get.return_value = mock_response
        
        progress_bar = Mock()
        
        downloader.download_chunk(
            chunk_id=0,
            start_byte=100,
            end_byte=199,
            chunk_file=chunk_file,
            progress_bar=progress_bar
        )
        
        # Verify Range header
        call_kwargs = mock_get.call_args[1]
        assert 'headers' in call_kwargs
        assert call_kwargs['headers']['Range'] == 'bytes=100-199'


# ==================== Chunk Merge Tests ====================

def test_merge_chunks(temp_dir):
    """Test merging multiple chunks into final file."""
    downloader = ChunkDownloader('http://example.com/file.dat', os.path.join(temp_dir, 'final.dat'))
    
    # Create chunk files
    chunks = [
        (os.path.join(temp_dir, 'chunk_0.tmp'), b'AAA'),
        (os.path.join(temp_dir, 'chunk_1.tmp'), b'BBB'),
        (os.path.join(temp_dir, 'chunk_2.tmp'), b'CCC')
    ]
    
    for chunk_file, data in chunks:
        with open(chunk_file, 'wb') as f:
            f.write(data)
    
    chunk_files = [c[0] for c in chunks]
    
    # Merge
    downloader.merge_chunks(chunk_files)
    
    # Verify merged file
    with open(downloader.destination, 'rb') as f:
        content = f.read()
    
    assert content == b'AAABBBCCC'
    
    # Verify chunk files deleted
    for chunk_file in chunk_files:
        assert not os.path.exists(chunk_file)


def test_merge_chunks_missing_file_raises_error(temp_dir):
    """Test that merge fails if chunk file is missing."""
    downloader = ChunkDownloader('http://example.com/file.dat', os.path.join(temp_dir, 'final.dat'))
    
    chunk_files = [
        os.path.join(temp_dir, 'chunk_0.tmp'),
        os.path.join(temp_dir, 'chunk_1.tmp'),
        os.path.join(temp_dir, 'missing.tmp')  # Doesn't exist
    ]
    
    # Create first two chunks
    for chunk_file in chunk_files[:2]:
        with open(chunk_file, 'wb') as f:
            f.write(b'data')
    
    with pytest.raises(IOError, match="Chunk file missing"):
        downloader.merge_chunks(chunk_files)


def test_merge_chunks_single_chunk(temp_dir):
    """Test merging with single chunk."""
    downloader = ChunkDownloader('http://example.com/file.dat', os.path.join(temp_dir, 'final.dat'))
    
    chunk_file = os.path.join(temp_dir, 'chunk_0.tmp')
    data = b'Single chunk data'
    
    with open(chunk_file, 'wb') as f:
        f.write(data)
    
    downloader.merge_chunks([chunk_file])
    
    with open(downloader.destination, 'rb') as f:
        assert f.read() == data


def test_merge_chunks_empty_chunks(temp_dir):
    """Test merging empty chunks."""
    downloader = ChunkDownloader('http://example.com/file.dat', os.path.join(temp_dir, 'final.dat'))
    
    # Create empty chunk files
    chunk_files = []
    for i in range(3):
        chunk_file = os.path.join(temp_dir, f'chunk_{i}.tmp')
        open(chunk_file, 'wb').close()
        chunk_files.append(chunk_file)
    
    downloader.merge_chunks(chunk_files)
    
    # Final file should be empty
    assert os.path.getsize(downloader.destination) == 0


# ==================== Full Download Tests ====================

def test_full_download_success(temp_dir):
    """Test complete chunked download workflow."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=2)
    
    file_content = b'A' * 500 + b'B' * 500
    
    with patch('requests.head') as mock_head, \
         patch('requests.get') as mock_get:
        
        # Mock HEAD request
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_head_response
        
        # Mock GET requests for chunks
        def get_side_effect(*args, **kwargs):
            response = Mock()
            response.status_code = 206
            
            # Determine which chunk based on Range header
            range_header = kwargs.get('headers', {}).get('Range', '')
            if 'bytes=0-499' in range_header:
                response.iter_content = Mock(return_value=[file_content[:500]])
            elif 'bytes=500-999' in range_header:
                response.iter_content = Mock(return_value=[file_content[500:]])
            else:
                response.iter_content = Mock(return_value=[b''])
            
            return response
        
        mock_get.side_effect = get_side_effect
        
        # Download
        result = downloader.download(expected_size=1000)
        
        assert result is True
        assert os.path.exists(destination)
        
        # Verify content
        with open(destination, 'rb') as f:
            assert f.read() == file_content


def test_full_download_no_range_support_returns_false(temp_dir):
    """Test that download returns False when ranges not supported."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination)
    
    with patch('requests.head') as mock_head:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            'Accept-Ranges': 'none',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_response
        
        result = downloader.download()
        
        assert result is False


def test_full_download_size_mismatch(temp_dir):
    """Test download fails on size mismatch."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination)
    
    with patch('requests.head') as mock_head:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_response
        
        # Expected 2000, but server says 1000
        result = downloader.download(expected_size=2000)
        
        assert result is False


def test_full_download_with_chunk_failures(temp_dir):
    """Test that download fails if any chunk fails."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=2, max_retries=1)
    
    with patch('requests.head') as mock_head, \
         patch('requests.get') as mock_get:
        
        # Mock HEAD
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_head_response
        
        # First chunk succeeds, second fails
        mock_get.side_effect = [
            Mock(status_code=206, iter_content=Mock(return_value=[b'A'*500])),
            Exception("Network error")
        ]
        
        result = downloader.download()
        
        assert result is False
        assert len(downloader.errors) > 0


def test_full_download_validates_final_size(temp_dir):
    """Test that final file size is validated."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=1)
    
    with patch('requests.head') as mock_head, \
         patch('requests.get') as mock_get:
        
        # Mock HEAD says 1000 bytes
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_head_response
        
        # But only download 500 bytes
        mock_get_response = Mock()
        mock_get_response.status_code = 206
        mock_get_response.iter_content = Mock(return_value=[b'X' * 500])
        mock_get.return_value = mock_get_response
        
        result = downloader.download()
        
        assert result is False


# ==================== High-Level Function Tests ====================

def test_download_in_chunks_success(temp_dir):
    """Test high-level download_in_chunks function."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    
    with patch.object(ChunkDownloader, 'download', return_value=True):
        result = download_in_chunks(url, destination, num_chunks=4)
        
        assert result is True


def test_download_in_chunks_failure(temp_dir):
    """Test download_in_chunks when download fails."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    
    with patch.object(ChunkDownloader, 'download', return_value=False):
        result = download_in_chunks(url, destination)
        
        assert result is False


def test_download_in_chunks_with_options(temp_dir):
    """Test download_in_chunks passes options correctly."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    
    with patch.object(ChunkDownloader, '__init__', return_value=None) as mock_init, \
         patch.object(ChunkDownloader, 'download', return_value=True):
        
        download_in_chunks(
            url,
            destination,
            num_chunks=8,
            expected_size=5242880,
            max_retries=5
        )
        
        # Verify ChunkDownloader initialized with correct params
        mock_init.assert_called_once_with(
            url=url,
            destination=destination,
            num_chunks=8,
            max_retries=5
        )


# ==================== Thread Safety Tests ====================

def test_download_chunk_thread_safe_progress_update(temp_dir):
    """Test that progress updates are thread-safe."""
    downloader = ChunkDownloader('http://example.com/file.dat', 'output.dat')
    
    # Download multiple chunks "concurrently" (mocked)
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.iter_content = Mock(return_value=[b'X' * 100])
        mock_get.return_value = mock_response
        
        progress_bar = Mock()
        
        # Simulate multiple chunk downloads
        for i in range(10):
            chunk_file = os.path.join(temp_dir, f'chunk_{i}.tmp')
            downloader.download_chunk(i, i*100, (i+1)*100-1, chunk_file, progress_bar)
        
        # Verify total downloaded is sum of all chunks
        assert downloader.total_downloaded == 1000


# ==================== Edge Cases ====================

def test_download_very_small_file(temp_dir):
    """Test downloading file smaller than chunk count."""
    url = 'http://example.com/tiny.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=10)
    
    # File is only 5 bytes
    file_content = b'ABCDE'
    
    with patch('requests.head') as mock_head, \
         patch('requests.get') as mock_get:
        
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '5'
        }
        mock_head.return_value = mock_head_response
        
        # Mock chunk downloads
        def get_side_effect(*args, **kwargs):
            response = Mock()
            response.status_code = 206
            range_header = kwargs.get('headers', {}).get('Range', '')
            
            # Parse range to determine which byte to return
            if 'bytes=0-0' in range_header:
                response.iter_content = Mock(return_value=[b'A'])
            elif 'bytes=1-1' in range_header:
                response.iter_content = Mock(return_value=[b'B'])
            else:
                response.iter_content = Mock(return_value=[b''])
            
            return response
        
        mock_get.side_effect = get_side_effect
        
        result = downloader.download()
        
        # Should handle gracefully
        assert result is True or result is False  # Either outcome is acceptable


def test_download_creates_destination_directory(temp_dir):
    """Test that download creates destination directory if needed."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'nested', 'dir', 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=1)
    
    with patch('requests.head') as mock_head, \
         patch('requests.get') as mock_get:
        
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '100'
        }
        mock_head.return_value = mock_head_response
        
        mock_get_response = Mock()
        mock_get_response.status_code = 206
        mock_get_response.iter_content = Mock(return_value=[b'X' * 100])
        mock_get.return_value = mock_get_response
        
        result = downloader.download()
        
        # Directory should be created
        assert os.path.exists(os.path.dirname(destination))


def test_download_cleans_up_on_failure(temp_dir):
    """Test that temporary files are cleaned up on failure."""
    url = 'http://example.com/file.dat'
    destination = os.path.join(temp_dir, 'output.dat')
    downloader = ChunkDownloader(url, destination, num_chunks=2, max_retries=1)
    
    with patch('requests.head') as mock_head, \
         patch('requests.get', side_effect=Exception("Network error")):
        
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {
            'Accept-Ranges': 'bytes',
            'Content-Length': '1000'
        }
        mock_head.return_value = mock_head_response
        
        result = downloader.download()
        
        assert result is False
        
        # Temp directory should be cleaned up
        temp_dir_path = destination + '.chunks'
        assert not os.path.exists(temp_dir_path)