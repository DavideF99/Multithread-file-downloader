# src/extractor.py
"""
Archive extraction utilities for ML dataset downloader.

Supports: tar, tar.gz, zip, gz formats
Includes security protections against path traversal and malicious archives.
"""

import os
import tarfile
import zipfile
import gzip
import shutil
from typing import Optional
from tqdm import tqdm
from src.logger import get_logger

def extract_archive(archive_path, extract_to=None, archive_format=None, remove_archive=True):
    """
    Extract archive file to destination.
    
    Args:
        archive_path: Path to archive file
        extract_to: Destination directory (default: same directory as archive)
        archive_format: Format hint ('tar.gz', 'tar', 'zip', 'gz')
        remove_archive: Delete archive after successful extraction
    
    Returns:
        str: Path to extracted content
    
    Raises:
        ValueError: If archive format is unsupported or extraction fails
    """
    logger = get_logger()
    
    # Default extract location: same directory as archive
    if extract_to is None:
        extract_to = os.path.dirname(archive_path)
    
    # Create extraction directory
    os.makedirs(extract_to, exist_ok=True)
    
    # Auto-detect format from extension if not provided
    if archive_format is None:
        if archive_path.endswith('.tar.gz') or archive_path.endswith('.tgz'):
            archive_format = 'tar.gz'
        elif archive_path.endswith('.tar'):
            archive_format = 'tar'
        elif archive_path.endswith('.zip'):
            archive_format = 'zip'
        elif archive_path.endswith('.gz'):
            archive_format = 'gz'
        else:
            raise ValueError(f"Cannot determine archive format from filename: {archive_path}")
    
    logger.info(f"Extracting {archive_format} archive: {archive_path}")
    logger.info(f"Destination: {extract_to}")
    
    try:
        if archive_format in ['tar.gz', 'tgz', 'tar']:
            extract_tar(archive_path, extract_to, archive_format)
        elif archive_format == 'zip':
            extract_zip(archive_path, extract_to)
        elif archive_format == 'gz':
            extract_gzip(archive_path, extract_to)
        else:
            raise ValueError(f"Unsupported archive format: {archive_format}")
        
        logger.info(f"Extraction complete: {extract_to}")
        
        # Remove archive if requested
        if remove_archive:
            logger.info(f"Removing archive: {archive_path}")
            os.remove(archive_path)
        
        return extract_to
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


def extract_tar(archive_path: str, extract_to: str, archive_format: str) -> None:
    """
    Extract tar or tar.gz archive.
    
    Args:
        archive_path: Path to tar archive
        extract_to: Destination directory
        archive_format: 'tar' or 'tar.gz'
    
    Raises:
        ValueError: If path traversal detected
    """
    logger = get_logger()
    
    # Select mode based on format
    if archive_format in ['tar.gz', 'tgz']:
        mode = 'r:gz'
    else:
        mode = 'r'
    
    # Create extraction directory if it doesn't exist
    os.makedirs(extract_to, exist_ok=True)
    
    # Open and extract with path traversal protection
    with tarfile.open(archive_path, mode) as tar:
        # Get all members and filter safe ones
        members = tar.getmembers()
        safe_members = []
        
        # Security: Check each member for safety
        for member in members:
            # Skip absolute paths
            if member.name.startswith('/'):
                logger.warning(f"Skipping absolute path: {member.name}")
                continue
            
            # Skip device files
            if member.isdev():
                logger.warning(f"Skipping device file: {member.name}")
                continue
            
            # Check for path traversal
            member_path = os.path.join(extract_to, member.name)
            abs_extract = os.path.abspath(extract_to)
            abs_member = os.path.abspath(member_path)
            
            if not abs_member.startswith(abs_extract):
                raise ValueError(f"Path traversal attempt detected: {member.name}")
            
            # Member is safe
            safe_members.append(member)
        
        # Extract safe members with progress
        if safe_members:
            logger.info(f"Extracting {len(safe_members)} files...")
            
            progress = tqdm(
                total=len(safe_members),
                unit='file',
                desc='Extracting'
            )
            
            for member in safe_members:
                tar.extract(member, path=extract_to)
                progress.update(1)
            
            progress.close()
        else:
            logger.info("No files to extract (archive is empty or all files filtered)")


def extract_zip(archive_path, extract_to):
    """
    Extract zip archive.
    
    Args:
        archive_path: Path to zip archive
        extract_to: Destination directory
    """
    logger = get_logger()
    
    with zipfile.ZipFile(archive_path, 'r') as zip_file:
        # Security: Check for path traversal
        for member in zip_file.namelist():
            member_path = os.path.join(extract_to, member)
            
            if not os.path.abspath(member_path).startswith(os.path.abspath(extract_to)):
                raise ValueError(f"Path traversal attempt detected: {member}")
            
            if member.startswith('/'):
                logger.warning(f"Skipping absolute path: {member}")
                continue
        
        # Extract with progress
        members = zip_file.namelist()
        logger.info(f"Extracting {len(members)} files...")
        
        progress = tqdm(
            total=len(members),
            unit='file',
            desc='Extracting'
        )
        
        for member in members:
            if member.startswith('/'):
                continue
            
            zip_file.extract(member, path=extract_to)
            progress.update(1)
        
        progress.close()


def extract_gzip(archive_path, extract_to):
    """
    Extract single gzip-compressed file.
    
    Args:
        archive_path: Path to .gz file
        extract_to: Destination directory
    """
    logger = get_logger()
    
    # Determine output filename (remove .gz extension)
    if archive_path.endswith('.gz'):
        output_name = os.path.basename(archive_path)[:-3]
    else:
        output_name = os.path.basename(archive_path) + '.extracted'
    
    output_path = os.path.join(extract_to, output_name)
    
    logger.info(f"Decompressing to: {output_path}")
    
    # Decompress with progress
    file_size = os.path.getsize(archive_path)
    progress = tqdm(
        total=file_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        desc='Decompressing'
    )
    
    with gzip.open(archive_path, 'rb') as gz_file:
        with open(output_path, 'wb') as out_file:
            while True:
                chunk = gz_file.read(8192)
                if not chunk:
                    break
                out_file.write(chunk)
                progress.update(len(chunk))
    
    progress.close()
    
def check_disk_space(required_bytes, path='.'):
    """
    Check if sufficient disk space is available.
    
    Args:
        required_bytes: Required space in bytes
        path: Path to check (default: current directory)
    
    Returns:
        bool: True if sufficient space available
    
    Raises:
        OSError: If insufficient disk space
    """
    # ✅ Cross-platform solution
    stat = shutil.disk_usage(path)
    available_bytes = stat.free
    
    if available_bytes < required_bytes:
        required_mb = required_bytes / (1024 * 1024)
        available_mb = available_bytes / (1024 * 1024)
        raise OSError(
            f"Insufficient disk space: need {required_mb:.1f}MB, "
            f"have {available_mb:.1f}MB available"
        )
    
    return True