import os
import time
import requests
from tqdm import tqdm
from src.logger import get_logger
import json
from datetime import datetime

def download_file(url, destination, expected_size=None, max_retries=3, base_delay=1, max_delay=60):
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

def load_progress(progress_file):
    """
    Load progress from JSON file.
    
    Returns:
        dict or None: Progress data if file exists and valid, None otherwise
    """
    if not os.path.exists(progress_file):
        return None
    
    try:
        with open(progress_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        get_logger().warning(f"Failed to load progress file: {e}")
        return None


def save_progress(progress_file, progress_data):
    """
    Save progress to JSON file.
    
    Args:
        progress_file: Path to progress file
        progress_data: Dict containing progress information
    """
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
        os.replace(temp_file, progress_file)  # Atomic rename
    except IOError as e:
        get_logger().error(f"Failed to save progress: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)


def get_progress_file_path(destination):
    """
    Generate progress file path from destination path.
    
    Args:
        destination: Destination file path (e.g., "downloads/cifar10/data.tar.gz")
    
    Returns:
        str: Progress file path (e.g., ".progress/cifar10/data.tar.gz.progress")
    """
    # Get relative path components
    dest_dir = os.path.dirname(destination)
    dest_file = os.path.basename(destination)
    
    # Mirror structure in .progress/ folder
    if dest_dir:
        progress_dir = os.path.join('.progress', dest_dir)
    else:
        progress_dir = '.progress'
    
    return os.path.join(progress_dir, dest_file + '.progress')


def validate_partial_file(destination, expected_bytes):
    """
    Validate that partial file size matches expected downloaded bytes.
    
    Args:
        destination: Path to partial file
        expected_bytes: Expected file size from progress
    
    Returns:
        bool: True if valid, False otherwise
    """
    if not os.path.exists(destination):
        return False
    
    actual_size = os.path.getsize(destination)
    return actual_size == expected_bytes


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


def cleanup_progress_file(destination):
    """
    Delete progress file after successful download.
    
    Args:
        destination: Destination file path
    """
    progress_file = get_progress_file_path(destination)
    if os.path.exists(progress_file):
        try:
            os.remove(progress_file)
            get_logger().debug(f"Deleted progress file: {progress_file}")
        except OSError as e:
            get_logger().warning(f"Failed to delete progress file: {e}")