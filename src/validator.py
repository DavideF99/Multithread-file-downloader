# src/validator.py
"""
Checksum validation utilities for downloaded files.

Supports MD5 and SHA256 checksums for ensuring file integrity.
"""

import os
import hashlib
from tqdm import tqdm
from src.logger import get_logger


def calculate_checksum(file_path, checksum_type='md5', chunk_size=8192):
    """
    Calculate checksum of a file.
    
    Args:
        file_path: Path to file
        checksum_type: 'md5' or 'sha256'
        chunk_size: Size of chunks to read (default 8KB)
    
    Returns:
        str: Hexadecimal checksum string
    
    Raises:
        ValueError: If checksum_type is invalid
        IOError: If file cannot be read
    """
    logger = get_logger()
    
    # Select hash algorithm
    if checksum_type.lower() == 'md5':
        hasher = hashlib.md5()
    elif checksum_type.lower() == 'sha256':
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"Unsupported checksum type: {checksum_type}")
    
    # Read file in chunks and update hash
    try:
        file_size = os.path.getsize(file_path)
        
        # Progress bar for validation
        progress = tqdm(
            total=file_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=f'Validating {checksum_type.upper()}'
        )
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                progress.update(len(chunk))
        
        progress.close()
        
        return hasher.hexdigest()
        
    except IOError as e:
        logger.error(f"Failed to read file for checksum: {e}")
        raise


def validate_checksum(file_path, expected_checksum, checksum_type='md5'):
    """
    Validate file checksum against expected value.
    
    Args:
        file_path: Path to file to validate
        expected_checksum: Expected checksum (hex string)
        checksum_type: 'md5' or 'sha256'
    
    Returns:
        bool: True if validation passes
    
    Raises:
        ValueError: If checksum validation fails
    """
    logger = get_logger()
    
    # Skip validation if requested
    if expected_checksum.lower() == 'skip':
        logger.info("Checksum validation skipped (checksum='skip')")
        return True
    
    logger.info(f"Validating {checksum_type.upper()} checksum...")
    
    # Calculate actual checksum
    actual_checksum = calculate_checksum(file_path, checksum_type)
    
    # Compare (case-insensitive)
    if actual_checksum.lower() == expected_checksum.lower():
        logger.info(f"Checksum validation passed: {actual_checksum}")
        return True
    else:
        logger.error(f"Checksum validation FAILED!")
        logger.error(f"  Expected: {expected_checksum}")
        logger.error(f"  Actual:   {actual_checksum}")
        logger.error(f"  File may be corrupted or expected checksum is incorrect")
        raise ValueError(
            f"Checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
        )