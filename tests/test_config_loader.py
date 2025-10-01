import pytest
import yaml
import os
from src.config_loader import load_config, validate_dataset_config, DatasetConfig

# Test 1: Valid single-file dataset
def test_validate_single_file_dataset():
    """Test validation passes for valid single-file config."""
    config = {
        'name': 'test_dataset',
        'url': 'http://example.com/data.tar.gz',
        'file_size': 1000,
        'checksum': 'a' * 32,  # Valid MD5
        'checksum_type': 'md5',
        'download_strategy': 'single_threaded',
        'destination_folder': 'downloads/test'
    }
    
    # Should not raise any exception
    result = validate_dataset_config(config)
    assert result == config

# Test 2: Missing name field
def test_validate_missing_name():
    """Test validation fails when 'name' is missing."""
    config = {
        'url': 'http://example.com/data.tar.gz',
        'file_size': 1000
    }
    
    with pytest.raises(ValueError, match="missing required field: 'name'"):
        validate_dataset_config(config)

# YOUR TASK: Write 5 more tests for:
# - Test 3: Both url and urls present
# - Test 4: Neither url nor urls present  
# - Test 5: Invalid checksum format
# - Test 6: Invalid download_strategy
# - Test 7: Multi-file with mismatched lengths