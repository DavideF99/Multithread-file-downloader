import pytest
import yaml
import os
from src.config_loader import load_config, validate_dataset_config, DatasetConfig

# Use fixtures to reduce the duplication of config dictionaries
@pytest.fixture
def base_config():
    """Minimal valid config that tests can modify."""
    return {
        'name': 'test_dataset',
        'url': 'http://example.com/data.tar.gz',
        'file_size': 1000,
        'checksum': 'a' * 32,  # Valid MD5
        'checksum_type': 'md5',
        'download_strategy': 'single_threaded',
        'destination_folder': 'downloads/test'
    }

# Test 1: Valid single-file dataset
def test_validate_single_file_dataset(base_config):
    """Test validation passes for valid single-file config."""
    
    # Should not raise any exception
    result = validate_dataset_config(base_config)
    assert result == base_config

# Test 2: Missing name field
def test_validate_missing_name():
    """Test validation fails when 'name' is missing."""
    config = {
        'url': 'http://example.com/data.tar.gz',
        'file_size': 1000
    }

    with pytest.raises(ValueError, match="missing required field: 'name'"):
        validate_dataset_config(config)

# - Test 3: Both url and urls present
def test_both_url_and_urls_present():
    """Test validation fails when both url and urls are present"""
    config = {
        'name' : 'test_dataset',
        'url' : 'http://example.com/data.tar.gz',
        'urls' : ['http://example.com/data.tar.gz',
                  'http://example2.com/data.tar.gz',
                  'http://example3.com/data.tar.gz'],
        'file_size' : 1000
    }

    name = config['name']
    with pytest.raises(ValueError, match=f"Dataset '{name}' cannot have both 'url' and 'urls'"):
        validate_dataset_config(config)

# - Test 4: Neither url nor urls present 
def test_neither_url_or_urls_present(base_config):
    """Test validation fails when neither url or urls are present"""
    del base_config['url']

    name = base_config['name']
    with pytest.raises(ValueError, match=f"Dataset '{name}' must have either 'url' or 'urls'"):
        validate_dataset_config(base_config)

# - Test 5: Missing checksum
def test_missing_checksum(base_config):
    """Test that missing checksum raises ValueError."""
    del base_config['checksum']
    
    with pytest.raises(ValueError, match="missing 'checksum'"):
        validate_dataset_config(base_config)

# - Test 6: Invalid md5 checksum format
def test_invalid_md5_checksum_format(base_config):
    """Test that invalid MD5 format raises ValueError."""
    base_config['checksum'] = 'xyz123' 
    
    with pytest.raises(ValueError, match="invalid MD5 checksum format"):
        validate_dataset_config(base_config)

# - Test 7: Invalid sha256 checksum format
def test_invalid_sha256_checksum_format(base_config):
    """Test that invalid SHA256 format raises ValueError."""
    base_config['checksum'] = 'xyz123' 
    base_config['checksum_type'] = 'sha256'
    
    with pytest.raises(ValueError, match="invalid SHA256 checksum format"):
        validate_dataset_config(base_config)

# - Test 8: Invalid checksum type
def test_invalid_checksum_type(base_config):
    """Test that invalid checksum_type raises ValueError."""
    base_config['checksum_type'] = 'sha521'
    
    with pytest.raises(ValueError, match="checksum_type must be 'md5' or 'sha256'"):
        validate_dataset_config(base_config)

# - Test 9: Validate 'skip checksum
def test_skip_checksum_bypasses_validation(base_config):
    """Test that 'skip' checksum doesn't validate format."""
    base_config['checksum'] = 'skip'
    
    # Should NOT raise an error
    result = validate_dataset_config(base_config)
    assert result['checksum'] == 'skip'

# - Test 10: Invalid download_strategy
def test_valid_download_strategy(base_config):
    """Test that invalid download_strategy raises ValueError"""
    base_config['download_strategy'] = 'double_threaded'

    with pytest.raises(ValueError, match="download_strategy"):
        validate_dataset_config(base_config)

# - Test 11: Multi-file with mismatched urls and file_sizes lengths
def test_mismatch_urls_and_file_lengths():
    """Test that validation fails when the length of 'urls' does not match the length of 'file_sizes'"""
    config = config = {
        'name': 'test_dataset',
        'urls': [
            'http://example.com/data1.tar.gz',
            'http://example.com/data2.tar.gz',
            'http://example.com/data3.tar.gz'
        ],
        'file_sizes': [1000, 2000],  # ← Only 2, but 3 URLs!
        'checksums': ['a' * 32, 'b' * 32, 'c' * 32],
        'checksum_type': 'md5',
        'destination_folder': 'downloads/test'
    }

    with pytest.raises(ValueError, match=r"urls \(\d+\) and file_sizes \(\d+\).*length mismatch"):
        validate_dataset_config(config)

# - Test 12: Multi-file with mismatched urls and checksums lengths
def test_mismatch_urls_and_checksum_lengths():
    """Test that validation fails when the length of 'urls' does not match the length of 'checksums'"""
    config = config = config = {
        'name': 'test_dataset',
        'urls': [
            'http://example.com/data1.tar.gz',
            'http://example.com/data2.tar.gz',
            'http://example.com/data3.tar.gz'
        ],
        'file_sizes': [1000, 2000, 4000],  
        'checksums': ['a' * 32, 'b' * 32], # ← Only 2, but 3 URLs!
        'checksum_type': 'md5',
        'destination_folder': 'downloads/test'
    }

    with pytest.raises(ValueError, match=r"urls \(3\) and checksums \(2\).*length mismatch"):
        validate_dataset_config(config)


def test_load_config_with_valid_yaml(tmp_path):
    """Test loading a complete valid YAML config file."""
    # Create a temporary YAML file
    config_content = """
datasets:
  - name: "test_dataset_1"
    url: "http://example.com/data1.tar.gz"
    file_size: 1000
    checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    checksum_type: "md5"
    download_strategy: "single_threaded"
    destination_folder: "downloads/test1"
    
  - name: "test_dataset_2"
    url: "http://example.com/data2.tar.gz"
    file_size: 2000
    checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    checksum_type: "md5"
    download_strategy: "chunked"
    extract_after_download: true
    extract_format: "tar.gz"
    destination_folder: "downloads/test2"
"""
    
    # Write to temporary file
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    
    # Load and validate
    datasets = load_config(str(config_file))
    
    # Assertions
    assert len(datasets) == 2
    assert datasets[0].name == "test_dataset_1"
    assert datasets[0].file_size == 1000
    assert datasets[1].name == "test_dataset_2"
    assert datasets[1].extract_after_download == True