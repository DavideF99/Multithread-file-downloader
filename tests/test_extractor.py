# tests/test_extractor.py
import pytest
import os
import tarfile
import zipfile
import gzip
from unittest.mock import Mock, patch

# Extractor functions
from src.extractor import (
    extract_archive,
    extract_tar,
    extract_zip,
    extract_gzip,
    check_disk_space
)

# Downloader integration function
from src.downloader import download_extract_validate


# ==================== Format Detection Tests ====================

def test_extract_tar_gz_auto_detect(tmp_path):
    """Test auto-detection of tar.gz format."""
    archive = tmp_path / "test.tar.gz"
    extract_to = tmp_path / "extracted"
    
    # Create tar.gz with test file
    test_content = b"Test content"
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(test_content)
    
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(test_file, arcname='test.txt')
    
    # Extract (format auto-detected from extension)
    extract_archive(str(archive), str(extract_to), remove_archive=False)
    
    # Verify
    extracted_file = extract_to / "test.txt"
    assert extracted_file.exists()
    assert extracted_file.read_bytes() == test_content


def test_extract_zip_auto_detect(tmp_path):
    """Test auto-detection of zip format."""
    archive = tmp_path / "test.zip"
    extract_to = tmp_path / "extracted"
    
    # Create zip with test file
    test_content = b"Zip content"
    
    with zipfile.ZipFile(archive, 'w') as zip_file:
        zip_file.writestr('test.txt', test_content)
    
    # Extract
    extract_archive(str(archive), str(extract_to), remove_archive=False)
    
    # Verify
    extracted_file = extract_to / "test.txt"
    assert extracted_file.exists()
    assert extracted_file.read_bytes() == test_content


def test_extract_gz_auto_detect(tmp_path):
    """Test auto-detection of gz format."""
    original_file = tmp_path / "test.txt"
    archive = tmp_path / "test.txt.gz"
    extract_to = tmp_path / "extracted"
    
    # Create gzipped file
    test_content = b"Gzip content"
    original_file.write_bytes(test_content)
    
    with open(original_file, 'rb') as f_in:
        with gzip.open(archive, 'wb') as f_out:
            f_out.write(f_in.read())
    
    # Extract
    extract_archive(str(archive), str(extract_to), remove_archive=False)
    
    # Verify
    extracted_file = extract_to / "test.txt"
    assert extracted_file.exists()
    assert extracted_file.read_bytes() == test_content


def test_extract_unknown_format_raises_error(tmp_path):
    """Test that unknown format raises error."""
    archive = tmp_path / "test.unknown"
    archive.write_bytes(b"data")
    
    with pytest.raises(ValueError, match="Cannot determine archive format"):
        extract_archive(str(archive))


# ==================== Extraction Tests ====================

def test_extract_tar_with_multiple_files(tmp_path):
    """Test extracting tar with multiple files and directories."""
    archive = tmp_path / "multi.tar"
    extract_to = tmp_path / "extracted"
    
    # Create test files
    files = {
        'file1.txt': b'Content 1',
        'dir/file2.txt': b'Content 2',
        'dir/subdir/file3.txt': b'Content 3'
    }
    
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    
    for path, content in files.items():
        file_path = temp_dir / path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
    
    # Create tar
    with tarfile.open(archive, 'w') as tar:
        for path in files.keys():
            tar.add(temp_dir / path, arcname=path)
    
    # Extract
    extract_tar(str(archive), str(extract_to), 'tar')
    
    # Verify all files
    for path, content in files.items():
        extracted = extract_to / path
        assert extracted.exists()
        assert extracted.read_bytes() == content


def test_extract_tar_gz_compressed(tmp_path):
    """Test extracting compressed tar.gz."""
    archive = tmp_path / "compressed.tar.gz"
    extract_to = tmp_path / "extracted"
    
    # Create large-ish content (will compress well)
    test_content = b"Repeated content " * 1000
    test_file = tmp_path / "large.txt"
    test_file.write_bytes(test_content)
    
    # Create tar.gz
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(test_file, arcname='large.txt')
    
    # Verify compression worked
    compressed_size = archive.stat().st_size
    original_size = len(test_content)
    assert compressed_size < original_size  # Should be compressed
    
    # Extract
    extract_tar(str(archive), str(extract_to), 'tar.gz')
    
    # Verify
    extracted = extract_to / "large.txt"
    assert extracted.exists()
    assert extracted.read_bytes() == test_content


def test_extract_zip_with_folders(tmp_path):
    """Test extracting zip with folder structure."""
    archive = tmp_path / "folders.zip"
    extract_to = tmp_path / "extracted"
    
    files = {
        'root.txt': b'Root file',
        'folder1/file1.txt': b'File 1',
        'folder1/subfolder/file2.txt': b'File 2',
        'folder2/file3.txt': b'File 3'
    }
    
    # Create zip
    with zipfile.ZipFile(archive, 'w') as zip_file:
        for path, content in files.items():
            zip_file.writestr(path, content)
    
    # Extract
    extract_zip(str(archive), str(extract_to))
    
    # Verify
    for path, content in files.items():
        extracted = extract_to / path
        assert extracted.exists()
        assert extracted.read_bytes() == content


# ==================== Security Tests ====================

def test_extract_tar_blocks_path_traversal(tmp_path):
    """Test that path traversal is blocked."""
    archive = tmp_path / "malicious.tar"
    extract_to = tmp_path / "extracted"
    
    # Create tar with path traversal attempt
    with tarfile.open(archive, 'w') as tar:
        # Create a member with dangerous path
        info = tarfile.TarInfo(name='../../../etc/passwd')
        info.size = 10
        tar.addfile(info, fileobj=None)
    
    # Should raise error
    with pytest.raises(ValueError, match="Path traversal attempt detected"):
        extract_tar(str(archive), str(extract_to), 'tar')


def test_extract_zip_blocks_path_traversal(tmp_path):
    """Test that zip path traversal is blocked."""
    archive = tmp_path / "malicious.zip"
    extract_to = tmp_path / "extracted"
    
    # Create zip with dangerous path
    with zipfile.ZipFile(archive, 'w') as zip_file:
        zip_file.writestr('../../../etc/passwd', b'malicious')
    
    # Should raise error
    with pytest.raises(ValueError, match="Path traversal attempt detected"):
        extract_zip(str(archive), str(extract_to))


def test_extract_tar_skips_absolute_paths(tmp_path):
    """Test that absolute paths are skipped."""
    archive = tmp_path / "absolute.tar"
    extract_to = tmp_path / "extracted"
    
    temp_file = tmp_path / "safe.txt"
    temp_file.write_bytes(b"safe")
    
    # Create tar with absolute path
    with tarfile.open(archive, 'w') as tar:
        # Add safe file
        tar.add(temp_file, arcname='safe.txt')
        # Add absolute path (should be skipped)
        info = tarfile.TarInfo(name='/etc/passwd')
        info.size = 0
        tar.addfile(info)
    
    # Extract (should succeed, skip absolute path)
    extract_tar(str(archive), str(extract_to), 'tar')
    
    # Verify only safe file extracted
    assert (extract_to / 'safe.txt').exists()
    assert not (extract_to / 'etc' / 'passwd').exists()


# ==================== Archive Cleanup Tests ====================

def test_extract_removes_archive_by_default(tmp_path):
    """Test that archive is removed after extraction."""
    archive = tmp_path / "test.tar.gz"
    extract_to = tmp_path / "extracted"
    
    # Create archive
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"content")
    
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(test_file, arcname='test.txt')
    
    # Extract with removal
    extract_archive(str(archive), str(extract_to), remove_archive=True)
    
    # Verify archive deleted
    assert not archive.exists()
    assert (extract_to / 'test.txt').exists()


def test_extract_keeps_archive_if_requested(tmp_path):
    """Test that archive can be kept after extraction."""
    archive = tmp_path / "test.tar.gz"
    extract_to = tmp_path / "extracted"
    
    # Create archive
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"content")
    
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(test_file, arcname='test.txt')
    
    # Extract without removal
    extract_archive(str(archive), str(extract_to), remove_archive=False)
    
    # Verify archive kept
    assert archive.exists()
    assert (extract_to / 'test.txt').exists()


# ==================== Disk Space Tests ====================

def test_check_disk_space_sufficient(tmp_path):
    """Test disk space check when sufficient space available."""
    # Request 1KB (should always be available)
    result = check_disk_space(1024, str(tmp_path))
    assert result is True


def test_check_disk_space_insufficient():
    """Test disk space check when insufficient space."""
    # Request absurd amount (1 petabyte)
    with pytest.raises(OSError, match="Insufficient disk space"):
        check_disk_space(1024 ** 5, '.')


# ==================== Integration Tests ====================

def test_download_extract_validate_workflow(tmp_path):
    """Test complete workflow: download → validate → extract."""
    url = "http://example.com/data.tar.gz"
    destination = tmp_path / "data.tar.gz"
    
    # Create test archive
    test_content = b"Dataset content"
    test_file = tmp_path / "data.txt"
    test_file.write_bytes(test_content)
    
    with tarfile.open(destination, 'w:gz') as tar:
        tar.add(test_file, arcname='data.txt')
    
    # Calculate checksum
    import hashlib
    with open(destination, 'rb') as f:
        checksum = hashlib.md5(f.read()).hexdigest()
    
    # Mock download (file already exists)
    with patch('src.downloader.download_with_resume'), \
         patch('src.downloader.validate_checksum', return_value=True), \
         patch('src.downloader.cleanup_progress_file'):
        
        result = download_extract_validate(
            url=url,
            destination=str(destination),
            checksum=checksum,
            extract_after_download=True,
            extract_format='tar.gz',
            keep_archive=False
        )
        
        # Verify extraction
        extract_dir = os.path.dirname(destination)
        extracted_file = os.path.join(extract_dir, 'data.txt')
        assert os.path.exists(extracted_file)
        
        # Verify archive removed
        assert not destination.exists()


def test_download_extract_validate_no_extraction(tmp_path):
    """Test workflow without extraction."""
    url = "http://example.com/data.txt"
    destination = tmp_path / "data.txt"
    content = b"Plain file"
    
    with patch('src.downloader.download_with_resume'), \
         patch('src.downloader.validate_checksum', return_value=True), \
         patch('src.downloader.cleanup_progress_file'):
        
        # Create file (simulating download)
        destination.write_bytes(content)
        
        result = download_extract_validate(
            url=url,
            destination=str(destination),
            extract_after_download=False
        )
        
        # Verify no extraction
        assert result == str(destination)
        assert destination.exists()


def test_download_extract_validate_keeps_archive(tmp_path):
    """Test workflow that keeps archive after extraction."""
    url = "http://example.com/data.zip"
    destination = tmp_path / "data.zip"
    
    # Create zip
    with zipfile.ZipFile(destination, 'w') as zip_file:
        zip_file.writestr('data.txt', b'content')
    
    with patch('src.downloader.download_with_resume'), \
         patch('src.downloader.validate_checksum', return_value=True), \
         patch('src.downloader.cleanup_progress_file'):
        
        download_extract_validate(
            url=url,
            destination=str(destination),
            extract_after_download=True,
            extract_format='zip',
            keep_archive=True
        )
        
        # Verify both exist
        assert destination.exists()  # Archive kept
        assert (tmp_path / 'data.txt').exists()  # File extracted


# ==================== Edge Cases ====================

def test_extract_empty_archive(tmp_path):
    """Test extracting empty archive."""
    archive = tmp_path / "empty.tar"
    extract_to = tmp_path / "extracted"
    
    # Create empty tar
    with tarfile.open(archive, 'w') as tar:
        pass
    
    # Should succeed
    extract_tar(str(archive), str(extract_to), 'tar')
    
    # Directory created but empty
    assert extract_to.exists()


def test_extract_single_file_at_root(tmp_path):
    """Test extracting archive with single file (no folders)."""
    archive = tmp_path / "single.tar.gz"
    extract_to = tmp_path / "extracted"
    
    # Create archive with file at root
    test_file = tmp_path / "single.txt"
    test_file.write_bytes(b"Single file")
    
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(test_file, arcname='single.txt')
    
    extract_tar(str(archive), str(extract_to), 'tar.gz')
    
    # Verify file at root of extract_to
    assert (extract_to / 'single.txt').exists()


def test_extract_nested_folders(tmp_path):
    """Test extracting deeply nested folder structure."""
    archive = tmp_path / "nested.zip"
    extract_to = tmp_path / "extracted"
    
    # Create deeply nested structure
    with zipfile.ZipFile(archive, 'w') as zip_file:
        zip_file.writestr('a/b/c/d/e/file.txt', b'Deep file')
    
    extract_zip(str(archive), str(extract_to))
    
    # Verify
    deep_file = extract_to / 'a' / 'b' / 'c' / 'd' / 'e' / 'file.txt'
    assert deep_file.exists()
    assert deep_file.read_bytes() == b'Deep file'