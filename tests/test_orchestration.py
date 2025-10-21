"""
Tests for orchestration module and main workflow.

Run with: pytest tests/test_orchestration.py -v
"""

import pytest
import os
import sys
import tempfile
import shutil
from unittest.mock import Mock, patch
from src.orchestration import download_dataset, download_all_datasets, main
from src.config_loader import DatasetConfig


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


@pytest.fixture
def sample_config(temp_dir):
    """Sample dataset config."""
    return DatasetConfig(
        name='test_dataset',
        url='http://example.com/data.tar.gz',
        file_size=1048576,
        checksum='abc123' + '0' * 26,  # Valid MD5
        checksum_type='md5',
        download_strategy='single_threaded',
        destination_folder=temp_dir
    )


def test_download_dataset_single_file(temp_dir, sample_config):
    """Test downloading single-file dataset."""
    with patch('src.orchestration.download_extract_validate') as mock_download, \
         patch('src.orchestration.check_disk_space'):
        
        mock_download.return_value = os.path.join(temp_dir, 'data.tar.gz')
        result = download_dataset(sample_config)
        
        assert result is not None
        mock_download.assert_called_once()


def test_download_all_datasets_success(temp_dir):
    """Test downloading all datasets from config."""
    config_file = os.path.join(temp_dir, 'test.yaml')
    
    config_content = """
datasets:
  - name: "test"
    url: "http://example.com/data.tar.gz"
    file_size: 1000
    checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    destination_folder: "downloads"
"""
    
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    with patch('src.orchestration.download_dataset') as mock_download:
        mock_download.return_value = 'path'
        results = download_all_datasets(config_file)
        
        assert len(results) == 1
        assert 'test' in results


def test_main_cli_basic(temp_dir):
    """Test CLI basic invocation."""
    config_file = os.path.join(temp_dir, 'test.yaml')
    
    with open(config_file, 'w') as f:
        f.write("datasets: []")
    
    with patch('sys.argv', ['prog', config_file]), \
         patch('src.orchestration.download_all_datasets', return_value={}), \
         patch('src.orchestration.setup_logging'):
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 0