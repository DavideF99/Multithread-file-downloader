import os
import time
import requests
from tqdm import tqdm
from src.logger import get_logger

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