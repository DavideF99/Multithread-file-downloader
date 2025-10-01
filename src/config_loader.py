import yaml
from typing import Dict, List, Optional
from dataclasses import dataclass
import os

@dataclass
class DatasetConfig:
    """
    Represents a single dataset configuration.
    
    Think: What data does YAML have that you need to pass around?
    Look at your datasets.yaml structure and map each field.
    """
    name: str
    url: Optional[str] = None  # Single URL
    urls: Optional[List[str]] = None  # Multiple URLs (for multi-file datasets)
    file_size: Optional[int] = None  # bytes
    file_sizes: Optional[List[int]] = None  # For multi-file
    checksum: Optional[str] = None
    checksums: Optional[List[str]] = None  # For multi-file
    checksum_type: str = "md5"  # "md5" or "sha256"
    download_strategy: str = "single_threaded"  # "single_threaded" | "multi_file" | "chunked"
    extract_after_download: bool = False
    extract_format: Optional[str] = None  # "tar.gz" | "zip" | "gz" | None
    destination_folder: str = "downloads"

def load_config(config_path: str) -> List[DatasetConfig]:
    """
    Steps to implement:
    
    1. Check if file exists
       - If not, raise FileNotFoundError with helpful message
    
    2. Open and read the YAML file
       - Use yaml.safe_load()
       - Catch yaml.YAMLError if malformed
    
    3. Validate top-level structure
       - Check 'datasets' key exists
       - Check it's a list
    
    4. For each dataset dict in the list:
       - Call validate_dataset_config()
       - Convert dict to DatasetConfig object
       - Add to results list
    
    5. Return list of DatasetConfig objects
    """
    # Step 1: Check if file exists
    # ❌ PROBLEM 1: You check if `file` is None, but you haven't opened it yet!
    # try: 
    #     if file is None:  # ← `file` doesn't exist yet!
    
    # ✅ FIX: Check the path first
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    # Step 2: Open and read YAML
    # ❌ PROBLEM 2: You call yaml.safe_load() without reading the file
    # yaml.safe_load(file)  # ← What is `file` here?
    
    # ✅ FIX: Open file, then load
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format: {e}")
    
    # Step 3: Validate top-level structure
    # ❌ PROBLEM 3: You check if "datasets" in file, but `file` is not a dict
    # if "datasets" in file:  # ← Should be `config`
    
    # ✅ FIX: Check the loaded config dict
    if 'datasets' not in config:
        raise ValueError("Config must contain 'datasets' key")
    
    if not isinstance(config['datasets'], list):
        raise ValueError("'datasets' must be a list")
    
    # Step 4: Validate each dataset
    # ❌ PROBLEM 4: You iterate over dataset_dict, but where did it come from?
    # for dataset in dataset_dict:  # ← Should be config['datasets']
    
    # ✅ FIX: Iterate over the actual datasets list
    results = []
    for dataset_dict in config['datasets']:
        validated = validate_dataset_config(dataset_dict)
        
        # Convert dict to DatasetConfig object
        dataset_config = DatasetConfig(**validated)  # ← Use ** unpacking!
        results.append(dataset_config)
    
    # Step 5: Return list
    return results

def validate_dataset_config(dataset_dict: dict) -> dict:
    """
    Validate a single dataset configuration dictionary.
    
    Required checks:
    1. 'name' field exists and is non-empty string
    2. Either 'url' OR 'urls' exists (not both, not neither)
    3. 'file_size' exists and is positive integer
    4. 'checksum' format is valid (MD5 32 hex or SHA256 64 hex)
    5. 'download_strategy' is one of: ["single_threaded", "multi_file", "chunked"]
    6. 'destination_folder' exists and is string
    7. If 'urls' exists, 'file_sizes' and 'checksums' must match length
    
    YOUR TASK: Think about each validation. What error message would help debug?
    """

    # Check 1: Validate 'name' exists
    if 'name' not in dataset_dict:
        raise ValueError("Dataset missing required field: 'name'")
    
    if not isinstance(dataset_dict['name'], str) or not dataset_dict['name'].strip():
        raise ValueError("Dataset 'name' must be a non-empty string")
    
    name = dataset_dict['name']  # Store for better error messages

    # Check 2: Either url or urls exists (not both, not neither)
    has_url = 'url' in dataset_dict
    has_urls = 'urls' in dataset_dict
    
    if has_url and has_urls:
        raise ValueError(f"Dataset '{name}' cannot have both 'url' and 'urls'")
    
    if not has_url and not has_urls:
        raise ValueError(f"Dataset '{name}' must have either 'url' or 'urls'")

   # Check 3: Validate 'file_size' exists and is positive
    # ❌ YOUR CODE: if "file_size" < 0:  # ← Can't compare string to int!
    
    # ✅ CORRECT:
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
    # ❌ YOUR CODE: if "checksum" != "md5" or "checksum" != "sha256"
    #               ^ This checks the string literal, not the value!
    
    # ✅ CORRECT:
    if 'checksum' not in dataset_dict:
        raise ValueError(f"Dataset '{name}' missing 'checksum'")
    
    checksum = dataset_dict['checksum']
    checksum_type = dataset_dict.get('checksum_type', 'md5')
    
    if checksum.lower() != 'skip':
        import re
        if checksum_type == 'md5':
            if not re.match(r'^[a-fA-F0-9]{32}$', checksum):
                raise ValueError(f"Dataset '{name}' has invalid MD5 checksum format")
        elif checksum_type == 'sha256':
            if not re.match(r'^[a-fA-F0-9]{64}$', checksum):
                raise ValueError(f"Dataset '{name}' has invalid SHA256 checksum format")
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
    
    return dataset_dict  # Return validated dict