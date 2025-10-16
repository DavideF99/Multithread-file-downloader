"""
Configuration loader for dataset YAML files.

Loads and validates dataset configurations from YAML files.
"""

import yaml
import re
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class DatasetConfig:
    """
    Represents a single dataset configuration.
    
    Attributes:
        name: Dataset name
        url: Single URL (for single-file datasets)
        urls: Multiple URLs (for multi-file datasets)
        file_size: Expected file size in bytes (single file)
        file_sizes: Expected file sizes (multi-file)
        checksum: Expected checksum (single file)
        checksums: Expected checksums (multi-file)
        checksum_type: Type of checksum ('md5' or 'sha256')
        download_strategy: Download strategy ('single_threaded', 'multi_file', 'chunked')
        extract_after_download: Whether to extract after download
        extract_format: Archive format ('tar.gz', 'zip', 'gz', etc.)
        destination_folder: Base destination folder
    """
    name: str
    url: Optional[str] = None
    urls: Optional[List[str]] = None
    file_size: Optional[int] = None
    file_sizes: Optional[List[int]] = None
    checksum: Optional[str] = None
    checksums: Optional[List[str]] = None
    checksum_type: str = "md5"
    download_strategy: str = "single_threaded"
    extract_after_download: bool = False
    extract_format: Optional[str] = None
    destination_folder: str = "downloads"


def load_config(config_path: str) -> List[DatasetConfig]:
    """
    Load dataset configurations from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
    
    Returns:
        List of DatasetConfig objects
    
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If YAML is invalid or malformed
    
    Example:
        >>> configs = load_config('datasets.yaml')
        >>> for config in configs:
        ...     print(f"Dataset: {config.name}, URL: {config.url}")
    """
    # Check if file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    # Open and read YAML
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format: {e}")
    
    # Validate top-level structure
    if 'datasets' not in config:
        raise ValueError("Config must contain 'datasets' key")
    
    if not isinstance(config['datasets'], list):
        raise ValueError("'datasets' must be a list")
    
    # Validate each dataset
    results = []
    for dataset_dict in config['datasets']:
        validated = validate_dataset_config(dataset_dict)
        
        # Convert dict to DatasetConfig object
        dataset_config = DatasetConfig(**validated)
        results.append(dataset_config)
    
    return results


def validate_dataset_config(dataset_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a single dataset configuration dictionary.
    
    Args:
        dataset_dict: Dictionary containing dataset configuration
    
    Returns:
        Validated dictionary (same as input if valid)
    
    Raises:
        ValueError: If validation fails with descriptive error message
    
    Required checks:
    1. 'name' field exists and is non-empty string
    2. Either 'url' OR 'urls' exists (not both, not neither)
    3. 'file_size' exists and is positive integer
    4. 'checksum' format is valid (MD5 32 hex or SHA256 64 hex)
    5. 'download_strategy' is one of: ["single_threaded", "multi_file", "chunked"]
    6. 'destination_folder' exists and is string
    7. If 'urls' exists, 'file_sizes' and 'checksums' must match length
    """
    
    # Check 1: Validate 'name' exists
    if 'name' not in dataset_dict:
        raise ValueError("Dataset missing required field: 'name'")
    
    if not isinstance(dataset_dict['name'], str) or not dataset_dict['name'].strip():
        raise ValueError("Dataset 'name' must be a non-empty string")
    
    name = dataset_dict['name']

    # Check 2: Either url or urls exists (not both, not neither)
    has_url = 'url' in dataset_dict
    has_urls = 'urls' in dataset_dict
    
    if has_url and has_urls:
        raise ValueError(f"Dataset '{name}' cannot have both 'url' and 'urls'")
    
    if not has_url and not has_urls:
        raise ValueError(f"Dataset '{name}' must have either 'url' or 'urls'")

    # Check 3: Validate 'file_size' exists and is positive
    if has_url:  # Single file
        if 'file_size' not in dataset_dict:
            raise ValueError(f"Dataset '{name}' missing 'file_size'")
        
        if not isinstance(dataset_dict['file_size'], int) or dataset_dict['file_size'] <= 0:
            raise ValueError(f"Dataset '{name}' file_size must be positive integer")
    
    if has_urls:  # Multiple files
        if 'file_sizes' not in dataset_dict:
            raise ValueError(f"Dataset '{name}' missing 'file_sizes'")
        
        if not isinstance(dataset_dict['file_sizes'], list):
            raise ValueError(f"Dataset '{name}' file_sizes must be a list")

    # Check 4: Validate checksum format
    if has_url:  # Single file dataset
        if 'checksum' not in dataset_dict:
            raise ValueError(f"Dataset '{name}' missing 'checksum'")
        
        checksum = dataset_dict['checksum']
        checksum_type = dataset_dict.get('checksum_type', 'md5')
        
        if checksum.lower() != 'skip':
            if checksum_type == 'md5':
                if not re.match(r'^[a-fA-F0-9]{32}$', checksum):
                    raise ValueError(f"Dataset '{name}' has invalid MD5 checksum format")
            elif checksum_type == 'sha256':
                if not re.match(r'^[a-fA-F0-9]{64}$', checksum):
                    raise ValueError(f"Dataset '{name}' has invalid SHA256 checksum format")
            else:
                raise ValueError(f"Dataset '{name}' checksum_type must be 'md5' or 'sha256'")

    if has_urls:  # Multi-file dataset
        if 'checksums' not in dataset_dict:
            raise ValueError(f"Dataset '{name}' missing 'checksums'")
        
        checksums = dataset_dict['checksums']
        checksum_type = dataset_dict.get('checksum_type', 'md5')
        
        # Validate each checksum in the list
        for i, checksum in enumerate(checksums):
            if checksum.lower() != 'skip':
                if checksum_type == 'md5':
                    if not re.match(r'^[a-fA-F0-9]{32}$', checksum):
                        raise ValueError(
                            f"Dataset '{name}' has invalid MD5 checksum format at index {i}"
                        )
                elif checksum_type == 'sha256':
                    if not re.match(r'^[a-fA-F0-9]{64}$', checksum):
                        raise ValueError(
                            f"Dataset '{name}' has invalid SHA256 checksum format at index {i}"
                        )
                else:
                    raise ValueError(f"Dataset '{name}' checksum_type must be 'md5' or 'sha256'")

    # Check 5: Validate download_strategy
    valid_strategies = ["single_threaded", "multi_file", "chunked"]
    strategy = dataset_dict.get('download_strategy', 'single_threaded')
    
    if strategy not in valid_strategies:
        raise ValueError(
            f"Dataset '{name}' download_strategy must be one of {valid_strategies}"
        )

    # Check 6: Validate destination_folder exists
    if 'destination_folder' not in dataset_dict:
        raise ValueError(f"Dataset '{name}' missing 'destination_folder'")
    
    if not isinstance(dataset_dict['destination_folder'], str):
        raise ValueError(f"Dataset '{name}' destination_folder must be string")

    # Check 7: If multi-file, validate lengths match
    if has_urls:
        urls = dataset_dict['urls']
        file_sizes = dataset_dict.get('file_sizes', [])
        checksums = dataset_dict.get('checksums', [])
        
        if len(urls) != len(file_sizes):
            raise ValueError(
                f"Dataset '{name}': urls ({len(urls)}) and file_sizes ({len(file_sizes)}) "
                "length mismatch"
            )
        
        if 'checksums' in dataset_dict and len(urls) != len(checksums):
            raise ValueError(
                f"Dataset '{name}': urls ({len(urls)}) and checksums ({len(checksums)}) "
                "length mismatch"
            )
    
    return dataset_dict