import os
import time
import requests
from tqdm import tqdm
from src.logger import get_logger
import json
from datetime import datetime
from typing import Optional
import hashlib
from src.progress_tracker import (
    load_progress,
    save_progress,
    get_progress_file_path,
    validate_partial_file,
    cleanup_progress_file
)
from src.extractor import extract_archive, check_disk_space 
from src.validator import validate_checksum  

def download_file(url: str, destination: str, expected_size: Optional[int] = None, 
                  max_retries: int = 3, base_delay: int = 1, max_delay: int = 60) -> None:
    """Download file with retry logic and progress tracking.
    Steps:
    1. Create destination directory if needed
    2. Initialize retry counter
    3. Start retry loop (max_retries times):
        a. Try to make HTTP GET request with stream=True
        b. Check status code (200, 404, 500, etc.)
        c. Get Content-Length from headers
        d. If expected_size provided, validate against Content-Length
        e. Initialize progress bar with total size
        f. Open destination file in binary write mode
        g. Stream chunks and write incrementally
        h. Update progress bar for each chunk
        i. If successful, break retry loop
        j. If retryable error (timeout, 5xx), calculate backoff delay
        k. Log retry attempt and wait
    4. If all retries exhausted, raise final exception
    5. Close progress bar and file
    6. Validate final file size matches expected
    """
    
    logger = get_logger()
    
    # Step 1: Create destination directory
    dest_dir = os.path.dirname(destination)
    if dest_dir:  # Only create if there's a directory path
        os.makedirs(dest_dir, exist_ok=True)
    
    # Step 2: Initialize retry counter
    attempt = 0
    
    # Step 3: Retry loop
    while attempt < max_retries:
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
            
            # Step 3a: Make HTTP request with streaming
            response = requests.get(url, stream=True, timeout=30)
            
            # Step 3b: Check status code
            if response.status_code == 404:
                raise ValueError(f"URL not found (404): {url}")
            elif response.status_code == 403:
                raise ValueError(f"Access forbidden (403): {url}")
            
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx
            
            # Step 3c: Get Content-Length
            content_length = response.headers.get('Content-Length')
            
            # Step 3d: Validate expected size
            if content_length:
                total_size = int(content_length)
                
                if expected_size and expected_size != total_size:
                    raise ValueError(
                        f"Size mismatch: expected {expected_size}, got {total_size}"
                    )
            else:
                total_size = None  # Unknown size
                logger.warning(f"No Content-Length header for {url}")
            
            # Step 3e: Initialize progress bar
            progress = tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=os.path.basename(destination)
            )
            
            # Step 3f & 3g: Open file and stream chunks
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
                        progress.update(len(chunk))  # Step 3h
            
            # Step 3i: Success - break retry loop
            progress.close()
            logger.info(f"Download complete: {destination}")
            
            # Step 6: Validate final file size
            if expected_size:
                actual_size = os.path.getsize(destination)
                if actual_size != expected_size:
                    raise ValueError(
                        f"Downloaded file size mismatch: expected {expected_size}, got {actual_size}"
                    )
            
            return  # Success!
            
        except requests.exceptions.Timeout as e:
            # Step 3j: Timeout - retry with backoff
            logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
            
        except requests.exceptions.HTTPError as e:
            # HTTP error - check if retryable (5xx)
            if e.response and 500 <= e.response.status_code < 600:
                logger.warning(f"Server error {e.response.status_code} on attempt {attempt + 1}, will retry")
                # Continue to retry logic below
            else:
                # Client error (4xx) or other - don't retry
                logger.error(f"HTTP error (non-retryable): {e}")
                raise
                
        except requests.exceptions.RequestException as e:
            # Other request exceptions - don't retry
            logger.error(f"Request failed: {e}")
            raise
        
        except Exception as e:
            # Unexpected error - log and re-raise
            logger.error(f"Unexpected error during download: {e}")
            raise
        
        finally:
            # Clean up progress bar if it exists
            if 'progress' in locals():
                progress.close()
        
        # Step 3k: Calculate backoff and retry
        attempt += 1
        if attempt < max_retries:
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    # Step 4: All retries exhausted
    raise Exception(f"Failed to download {url} after {max_retries} attempts")

def download_with_resume(url, destination, expected_size=None, checksum=None, 
                         checksum_type='md5', max_retries=3, base_delay=1, max_delay=60):
    """
    Download file with resume capability.
    
    Args:
        url: URL to download from
        destination: Local file path
        expected_size: Expected total file size (optional)
        checksum: Expected checksum for validation (optional)
        checksum_type: Type of checksum ('md5' or 'sha256')
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff
        max_delay: Maximum backoff delay
    
    Returns:
        None
    
    Raises:
        Various exceptions on failure
    """
    logger = get_logger()
    progress_file = get_progress_file_path(destination)
    
    # Step 1: Check for existing progress
    progress_data = load_progress(progress_file)
    resume_from = 0
    
    if progress_data:
        # Validate progress data
        if progress_data.get('url') != url:
            logger.warning(f"URL mismatch in progress file, starting fresh")
            progress_data = None
        elif progress_data.get('total_size') and expected_size and \
             progress_data['total_size'] != expected_size:
            logger.warning(f"File size changed on server, starting fresh")
            progress_data = None
        else:
            # Validate partial file
            resume_from = progress_data.get('downloaded_bytes', 0)
            if not validate_partial_file(destination, resume_from):
                logger.warning(
                    f"Partial file size mismatch (expected {resume_from} bytes), starting fresh"
                )
                progress_data = None
                resume_from = 0
                if os.path.exists(destination):
                    os.remove(destination)
    
    # Initialize progress data if starting fresh
    if not progress_data:
        progress_data = {
            'url': url,
            'destination': destination,
            'total_size': expected_size,
            'downloaded_bytes': 0,
            'checksum': checksum,
            'checksum_type': checksum_type,
            'status': 'in_progress'
        }
        save_progress(progress_file, progress_data)
    
    # Log resume or fresh start
    if resume_from > 0:
        logger.info(f"Resuming download from byte {resume_from}")
    else:
        logger.info(f"Starting fresh download")
    
    # Create destination directory
    dest_dir = os.path.dirname(destination)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)
    
    # Step 2: Download with resume logic
    attempt = 0
    
    while attempt < max_retries:
        try:
            # Prepare headers for resume
            headers = {}
            if resume_from > 0:
                headers['Range'] = f'bytes={resume_from}-'
            
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
            
            # Make request
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            
            # Check status codes
            if response.status_code == 404:
                raise ValueError(f"URL not found (404): {url}")
            elif response.status_code == 403:
                raise ValueError(f"Access forbidden (403): {url}")
            elif response.status_code == 416:
                # Range not satisfiable - file already complete?
                logger.info("Server says range not satisfiable, checking file")
                if validate_partial_file(destination, expected_size):
                    logger.info("File already complete")
                    progress_data['status'] = 'complete'
                    save_progress(progress_file, progress_data)
                    return
                else:
                    raise ValueError("Invalid range request and file incomplete")
            
            # Handle response status
            if response.status_code == 206:
                # Partial content - resume supported
                logger.info("Server supports resume, continuing from existing data")
            elif response.status_code == 200:
                if resume_from > 0:
                    # Server doesn't support resume, starting fresh
                    logger.warning("Server doesn't support resume, starting fresh")
                    resume_from = 0
                    progress_data['downloaded_bytes'] = 0
                    if os.path.exists(destination):
                        os.remove(destination)
            else:
                response.raise_for_status()
            
            # Get total size
            content_length = response.headers.get('Content-Length')
            if content_length:
                content_size = int(content_length)
                
                # For 206 responses, Content-Length is remaining bytes
                if response.status_code == 206:
                    total_size = resume_from + content_size
                else:
                    total_size = content_size
                
                # Update progress data
                if not progress_data.get('total_size'):
                    progress_data['total_size'] = total_size
                
                # Validate size if expected
                if expected_size and total_size != expected_size:
                    raise ValueError(
                        f"Size mismatch: expected {expected_size}, got {total_size}"
                    )
            else:
                total_size = progress_data.get('total_size')
                logger.warning(f"No Content-Length header")
            
            # Initialize progress bar
            progress_bar = tqdm(
                total=total_size,
                initial=resume_from,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=os.path.basename(destination)
            )
            
            # Open file in append mode if resuming, write mode if fresh
            file_mode = 'ab' if resume_from > 0 else 'wb'
            
            # Download and write chunks
            bytes_since_last_save = 0
            save_interval = 1024 * 1024  # Save progress every 1 MB
            
            with open(destination, file_mode) as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        chunk_size = len(chunk)
                        progress_bar.update(chunk_size)
                        
                        # Update progress tracking
                        progress_data['downloaded_bytes'] += chunk_size
                        bytes_since_last_save += chunk_size
                        
                        # Save progress periodically
                        if bytes_since_last_save >= save_interval:
                            save_progress(progress_file, progress_data)
                            bytes_since_last_save = 0
            
            # Close progress bar
            progress_bar.close()
            
            # Final progress save
            progress_data['status'] = 'complete'
            save_progress(progress_file, progress_data)
            
            logger.info(f"Download complete: {destination}")
            
            # Validate final size
            if expected_size:
                actual_size = os.path.getsize(destination)
                if actual_size != expected_size:
                    raise ValueError(
                        f"Downloaded file size mismatch: expected {expected_size}, got {actual_size}"
                    )
            
            return  # Success!
            
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
            resume_from = progress_data.get('downloaded_bytes', 0)
            
        except requests.exceptions.HTTPError as e:
            if e.response and 500 <= e.response.status_code < 600:
                logger.warning(f"Server error {e.response.status_code}, will retry")
                resume_from = progress_data.get('downloaded_bytes', 0)
            else:
                logger.error(f"HTTP error (non-retryable): {e}")
                raise
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        
        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
            raise
        
        finally:
            if 'progress_bar' in locals():
                progress_bar.close()
        
        # Backoff and retry
        attempt += 1
        if attempt < max_retries:
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    # All retries exhausted
    raise Exception(f"Failed to download {url} after {max_retries} attempts")

def download_and_validate(url, destination, expected_size=None, checksum=None,
                          checksum_type='md5', max_retries=3, base_delay=1, max_delay=60):
    """
    Download file with resume capability and checksum validation.
    
    This is a wrapper around download_with_resume() that adds validation.
    
    Args:
        url: URL to download from
        destination: Local file path
        expected_size: Expected file size (optional)
        checksum: Expected checksum (optional)
        checksum_type: 'md5' or 'sha256'
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff
        max_delay: Maximum backoff delay
    
    Returns:
        None
    
    Raises:
        Various exceptions on failure
    """
    logger = get_logger()
    
    try:
        # Download file
        download_with_resume(
            url=url,
            destination=destination,
            expected_size=expected_size,
            checksum=checksum,
            checksum_type=checksum_type,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay
        )
        
        # Validate checksum if provided (uses imported function)
        if checksum:
            try:
                validate_checksum(destination, checksum, checksum_type)
                logger.info(f"Download and validation complete: {destination}")
                
                # Clean up progress file after successful validation
                cleanup_progress_file(destination)
                
            except ValueError as e:
                # Validation failed - delete corrupted file
                logger.error(f"Validation failed, deleting file: {destination}")
                if os.path.exists(destination):
                    os.remove(destination)
                raise
        else:
            logger.info(f"Download complete (no checksum validation): {destination}")
            cleanup_progress_file(destination)
            
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

def download_extract_validate(url, destination, expected_size=None, checksum=None,
                               checksum_type='md5', extract_after_download=False,
                               extract_format=None, keep_archive=False,
                               max_retries=3, base_delay=1, max_delay=60):
    """
    Complete workflow: Download → Validate → Extract.
    
    This orchestrator function coordinates the overall download workflow.
    
    Args:
        url: URL to download from
        destination: Local file path
        expected_size: Expected file size (optional)
        checksum: Expected checksum (optional)
        checksum_type: 'md5' or 'sha256'
        extract_after_download: Extract archive after download
        extract_format: Archive format ('tar.gz', 'zip', etc.)
        keep_archive: Keep archive file after extraction
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff
        max_delay: Maximum backoff delay
    
    Returns:
        str: Path to final data (extracted folder or downloaded file)
    """
    logger = get_logger()
    
    try:
        # Step 1: Download and validate
        download_and_validate(
            url=url,
            destination=destination,
            expected_size=expected_size,
            checksum=checksum,
            checksum_type=checksum_type,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay
        )
        
        # Step 2: Extract if requested
        if extract_after_download:
            if not extract_format:
                logger.warning("extract_after_download=True but no extract_format provided")
                return destination
            
            # Check disk space
            if expected_size:
                check_disk_space(expected_size * 3, os.path.dirname(destination))
            
            # Extract using extractor module
            extract_to = os.path.dirname(destination)
            extract_archive(
                archive_path=destination,
                extract_to=extract_to,
                archive_format=extract_format,
                remove_archive=not keep_archive
            )
            
            logger.info(f"Download, validation, and extraction complete: {extract_to}")
            return extract_to
        else:
            logger.info(f"Download and validation complete: {destination}")
            return destination
            
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise