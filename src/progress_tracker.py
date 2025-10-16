"""
Progress tracking utilities for resumable downloads.

Handles saving/loading download progress to JSON files for resume capability.
"""

import os
import json
from datetime import datetime
from typing import Dict, Optional, Any
from src.logger import get_logger


def load_progress(progress_file: str) -> Optional[Dict[str, Any]]:
    """
    Load progress from JSON file.
    
    Args:
        progress_file: Path to progress JSON file
    
    Returns:
        dict or None: Progress data if file exists and valid, None otherwise
    
    Example:
        >>> progress = load_progress('.progress/dataset.tar.gz.progress')
        >>> if progress:
        ...     print(f"Resume from byte {progress['downloaded_bytes']}")
    """
    logger = get_logger()
    
    if not os.path.exists(progress_file):
        logger.debug(f"No progress file found: {progress_file}")
        return None
    
    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        
        logger.debug(f"Loaded progress: {progress.get('downloaded_bytes', 0)} bytes")
        return progress
        
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load progress file: {e}")
        return None


def save_progress(progress_file: str, progress_data: Dict[str, Any]) -> None:
    """
    Save progress to JSON file atomically.
    
    Uses atomic write (write to temp file, then rename) to prevent corruption.
    
    Args:
        progress_file: Path to progress file
        progress_data: Dict containing progress information
    
    Required keys in progress_data:
        - url: Source URL
        - destination: Local file path
        - downloaded_bytes: Bytes downloaded so far
        - total_size: Total file size (optional)
        - status: 'in_progress' or 'complete'
    
    Example:
        >>> save_progress('.progress/file.progress', {
        ...     'url': 'https://example.com/data.tar.gz',
        ...     'destination': 'downloads/data.tar.gz',
        ...     'downloaded_bytes': 1024000,
        ...     'total_size': 5242880,
        ...     'status': 'in_progress'
        ... })
    """
    logger = get_logger()
    
    # Create directory if needed
    progress_dir = os.path.dirname(progress_file)
    if progress_dir:
        os.makedirs(progress_dir, exist_ok=True)
    
    # Add timestamp
    progress_data['last_updated'] = datetime.utcnow().isoformat() + 'Z'
    
    # Write atomically (write to temp file, then rename)
    temp_file = progress_file + '.tmp'
    try:
        with open(temp_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
        
        # Atomic rename (overwrites existing file)
        os.replace(temp_file, progress_file)
        logger.debug(f"Saved progress: {progress_data.get('downloaded_bytes', 0)} bytes")
        
    except IOError as e:
        logger.error(f"Failed to save progress: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass


def get_progress_file_path(destination: str, base_dir: str = '.progress') -> str:
    """
    Generate progress file path from destination path.
    
    Mirrors the destination directory structure in the progress directory.
    
    Args:
        destination: Destination file path (e.g., "downloads/cifar10/data.tar.gz")
        base_dir: Base directory for progress files (default: '.progress')
    
    Returns:
        str: Progress file path (e.g., ".progress/downloads/cifar10/data.tar.gz.progress")
    
    Example:
        >>> get_progress_file_path('downloads/dataset/file.tar.gz')
        '.progress/downloads/dataset/file.tar.gz.progress'
    """
    # Get relative path components
    dest_dir = os.path.dirname(destination)
    dest_file = os.path.basename(destination)
    
    # Mirror structure in progress folder
    if dest_dir:
        progress_dir = os.path.join(base_dir, dest_dir)
    else:
        progress_dir = base_dir
    
    return os.path.join(progress_dir, dest_file + '.progress')


def validate_partial_file(destination: str, expected_bytes: int) -> bool:
    """
    Validate that partial file size matches expected downloaded bytes.
    
    Args:
        destination: Path to partial file
        expected_bytes: Expected file size from progress
    
    Returns:
        bool: True if valid (file exists and size matches), False otherwise
    
    Example:
        >>> if validate_partial_file('downloads/data.tar.gz', 1024000):
        ...     print("Can resume download")
        ... else:
        ...     print("Must start fresh")
    """
    logger = get_logger()
    
    if not os.path.exists(destination):
        logger.debug(f"Partial file does not exist: {destination}")
        return False
    
    actual_size = os.path.getsize(destination)
    
    if actual_size == expected_bytes:
        logger.debug(f"Partial file valid: {actual_size} bytes")
        return True
    else:
        logger.warning(
            f"Partial file size mismatch: expected {expected_bytes}, got {actual_size}"
        )
        return False


def cleanup_progress_file(destination: str, base_dir: str = '.progress') -> None:
    """
    Delete progress file after successful download.
    
    Args:
        destination: Destination file path
        base_dir: Base directory for progress files
    
    Example:
        >>> cleanup_progress_file('downloads/dataset.tar.gz')
    """
    logger = get_logger()
    progress_file = get_progress_file_path(destination, base_dir)
    
    if os.path.exists(progress_file):
        try:
            os.remove(progress_file)
            logger.debug(f"Deleted progress file: {progress_file}")
        except OSError as e:
            logger.warning(f"Failed to delete progress file: {e}")


def get_all_progress_files(base_dir: str = '.progress') -> Dict[str, Dict[str, Any]]:
    """
    Get all active progress files.
    
    Useful for resuming interrupted downloads or showing download status.
    
    Args:
        base_dir: Base directory for progress files
    
    Returns:
        dict: Mapping of destination paths to progress data
    
    Example:
        >>> active_downloads = get_all_progress_files()
        >>> for dest, progress in active_downloads.items():
        ...     print(f"{dest}: {progress['downloaded_bytes']} / {progress['total_size']}")
    """
    logger = get_logger()
    progress_files = {}
    
    if not os.path.exists(base_dir):
        return progress_files
    
    # Walk through progress directory
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.progress'):
                progress_file = os.path.join(root, file)
                progress_data = load_progress(progress_file)
                
                if progress_data and 'destination' in progress_data:
                    destination = progress_data['destination']
                    progress_files[destination] = progress_data
    
    logger.info(f"Found {len(progress_files)} active progress files")
    return progress_files


def cleanup_stale_progress_files(base_dir: str = '.progress', max_age_days: int = 7) -> int:
    """
    Clean up old progress files.
    
    Removes progress files older than max_age_days that likely correspond
    to abandoned downloads.
    
    Args:
        base_dir: Base directory for progress files
        max_age_days: Maximum age in days before cleanup
    
    Returns:
        int: Number of files cleaned up
    
    Example:
        >>> cleaned = cleanup_stale_progress_files(max_age_days=7)
        >>> print(f"Cleaned up {cleaned} stale progress files")
    """
    logger = get_logger()
    cleaned_count = 0
    
    if not os.path.exists(base_dir):
        return 0
    
    now = datetime.utcnow()
    max_age_seconds = max_age_days * 24 * 60 * 60
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.progress'):
                progress_file = os.path.join(root, file)
                
                try:
                    # Check file modification time
                    mtime = os.path.getmtime(progress_file)
                    age_seconds = (now.timestamp() - mtime)
                    
                    if age_seconds > max_age_seconds:
                        os.remove(progress_file)
                        cleaned_count += 1
                        logger.debug(f"Removed stale progress file: {progress_file}")
                        
                except OSError as e:
                    logger.warning(f"Failed to process progress file {progress_file}: {e}")
    
    if cleaned_count > 0:
        logger.info(f"Cleaned up {cleaned_count} stale progress files")
    
    return cleaned_count