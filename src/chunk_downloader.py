"""
Chunked download implementation for large files.

Downloads large files in parallel chunks using HTTP Range requests,
then merges them into a single file. Significantly faster for large files
when the server supports byte-range requests.
"""

import os
import requests
import threading
from typing import List, Optional, Tuple
from tqdm import tqdm
from src.logger import get_logger


class ChunkDownloader:
    """
    Download manager for chunked/parallel downloads.
    
    Splits large files into chunks and downloads them in parallel threads.
    """
    
    def __init__(self, url: str, destination: str, num_chunks: int = 4,
                 max_retries: int = 3):
        """
        Initialize chunk downloader.
        
        Args:
            url: URL to download from
            destination: Final destination file path
            num_chunks: Number of parallel chunks (threads)
            max_retries: Retry attempts per chunk
        """
        self.url = url
        self.destination = destination
        self.num_chunks = num_chunks
        self.max_retries = max_retries
        self.logger = get_logger()
        
        # Thread-safe progress tracking
        self.lock = threading.Lock()
        self.total_downloaded = 0
        self.chunk_status = {}  # chunk_id -> bytes_downloaded
        self.errors = []
    
    def check_range_support(self) -> Tuple[bool, Optional[int]]:
        """
        Check if server supports Range requests and get file size.
        
        Returns:
            tuple: (supports_range, file_size)
        
        Example:
            >>> downloader = ChunkDownloader(url, dest)
            >>> supports, size = downloader.check_range_support()
            >>> if not supports:
            ...     print("Server doesn't support chunked download")
        """
        try:
            # HEAD request to check headers
            response = requests.head(self.url, timeout=10)
            response.raise_for_status()
            
            # Check for Accept-Ranges header
            accepts_ranges = response.headers.get('Accept-Ranges', '').lower()
            supports_range = accepts_ranges == 'bytes'
            
            # Get file size
            content_length = response.headers.get('Content-Length')
            file_size = int(content_length) if content_length else None
            
            if not supports_range:
                self.logger.warning("Server does not support Range requests")
            
            if not file_size:
                self.logger.warning("Could not determine file size")
            
            return supports_range, file_size
            
        except Exception as e:
            self.logger.error(f"Failed to check range support: {e}")
            return False, None
    
    def calculate_chunk_ranges(self, file_size: int) -> List[Tuple[int, int, int]]:
        """
        Calculate byte ranges for each chunk.
        
        Args:
            file_size: Total file size in bytes
        
        Returns:
            list: List of (chunk_id, start_byte, end_byte) tuples
        
        Example:
            >>> ranges = downloader.calculate_chunk_ranges(1000000)
            >>> # [(0, 0, 249999), (1, 250000, 499999), ...]
        """
        chunk_size = file_size // self.num_chunks
        ranges = []
        
        for i in range(self.num_chunks):
            start = i * chunk_size
            
            # Last chunk gets any remainder
            if i == self.num_chunks - 1:
                end = file_size - 1
            else:
                end = start + chunk_size - 1
            
            ranges.append((i, start, end))
        
        self.logger.debug(f"Split {file_size} bytes into {len(ranges)} chunks")
        return ranges
    
    def download_chunk(self, chunk_id: int, start_byte: int, end_byte: int,
                       chunk_file: str, progress_bar: tqdm) -> bool:
        """
        Download a single chunk with retry logic.
        
        Args:
            chunk_id: Chunk identifier
            start_byte: Starting byte position
            end_byte: Ending byte position (inclusive)
            chunk_file: Temporary file for this chunk
            progress_bar: Shared progress bar for updates
        
        Returns:
            bool: True if successful, False otherwise
        """
        attempt = 0
        
        while attempt < self.max_retries:
            try:
                headers = {'Range': f'bytes={start_byte}-{end_byte}'}
                
                self.logger.debug(
                    f"Chunk {chunk_id}: Downloading bytes {start_byte}-{end_byte} "
                    f"(attempt {attempt + 1})"
                )
                
                response = requests.get(
                    self.url,
                    headers=headers,
                    stream=True,
                    timeout=30
                )
                
                # Check for partial content response
                if response.status_code != 206:
                    raise ValueError(
                        f"Expected 206 Partial Content, got {response.status_code}"
                    )
                
                # Download chunk
                with open(chunk_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            
                            # Update progress (thread-safe)
                            with self.lock:
                                self.total_downloaded += len(chunk)
                                progress_bar.update(len(chunk))
                
                self.logger.debug(f"Chunk {chunk_id}: Download complete")
                return True
                
            except Exception as e:
                attempt += 1
                self.logger.warning(
                    f"Chunk {chunk_id} failed (attempt {attempt}): {e}"
                )
                
                if attempt >= self.max_retries:
                    with self.lock:
                        self.errors.append(f"Chunk {chunk_id}: {e}")
                    return False
        
        return False
    
    def merge_chunks(self, chunk_files: List[str]) -> None:
        """
        Merge downloaded chunks into final file.
        
        Args:
            chunk_files: List of temporary chunk files in order
        
        Raises:
            IOError: If merge fails
        """
        self.logger.info(f"Merging {len(chunk_files)} chunks into {self.destination}")
        
        try:
            with open(self.destination, 'wb') as outfile:
                for chunk_file in chunk_files:
                    if not os.path.exists(chunk_file):
                        raise IOError(f"Chunk file missing: {chunk_file}")
                    
                    with open(chunk_file, 'rb') as infile:
                        while True:
                            chunk = infile.read(8192)
                            if not chunk:
                                break
                            outfile.write(chunk)
                    
                    # Clean up chunk file
                    os.remove(chunk_file)
            
            self.logger.info("Chunk merge complete")
            
        except Exception as e:
            self.logger.error(f"Failed to merge chunks: {e}")
            raise
    
    def download(self, expected_size: Optional[int] = None) -> bool:
        """
        Execute chunked download.
        
        Args:
            expected_size: Expected file size for validation
        
        Returns:
            bool: True if successful, False otherwise
        
        Example:
            >>> downloader = ChunkDownloader(url, dest, num_chunks=8)
            >>> if downloader.download(expected_size=5242880):
            ...     print("Download successful!")
        """
        # Check server support
        supports_range, file_size = self.check_range_support()
        
        if not supports_range or not file_size:
            self.logger.warning("Falling back to single-threaded download")
            return False
        
        # Validate expected size
        if expected_size and file_size != expected_size:
            self.logger.error(
                f"File size mismatch: expected {expected_size}, got {file_size}"
            )
            return False
        
        # Create destination directory
        dest_dir = os.path.dirname(self.destination)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        
        # Calculate chunk ranges
        chunk_ranges = self.calculate_chunk_ranges(file_size)
        
        # Create temporary directory for chunks
        temp_dir = self.destination + '.chunks'
        os.makedirs(temp_dir, exist_ok=True)
        
        # Prepare chunk files
        chunk_files = []
        for chunk_id, _, _ in chunk_ranges:
            chunk_file = os.path.join(temp_dir, f'chunk_{chunk_id:04d}.tmp')
            chunk_files.append(chunk_file)
        
        # Initialize progress bar
        progress_bar = tqdm(
            total=file_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=os.path.basename(self.destination)
        )
        
        # Download chunks in parallel
        self.logger.info(f"Starting chunked download with {self.num_chunks} threads")
        
        threads = []
        for chunk_id, start_byte, end_byte in chunk_ranges:
            chunk_file = chunk_files[chunk_id]
            
            thread = threading.Thread(
                target=self.download_chunk,
                args=(chunk_id, start_byte, end_byte, chunk_file, progress_bar)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        progress_bar.close()
        
        # Check for errors
        if self.errors:
            self.logger.error(f"Chunked download failed with {len(self.errors)} errors:")
            for error in self.errors:
                self.logger.error(f"  {error}")
            
            # Clean up chunk files
            for chunk_file in chunk_files:
                if os.path.exists(chunk_file):
                    os.remove(chunk_file)
            
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
            
            return False
        
        # Merge chunks
        try:
            self.merge_chunks(chunk_files)
            
            # Clean up temp directory
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
            
            # Validate final size
            actual_size = os.path.getsize(self.destination)
            if actual_size != file_size:
                self.logger.error(
                    f"Final file size mismatch: expected {file_size}, got {actual_size}"
                )
                return False
            
            self.logger.info(f"Chunked download successful: {self.destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to merge chunks: {e}")
            return False


def download_in_chunks(url: str, destination: str, num_chunks: int = 4,
                       expected_size: Optional[int] = None,
                       max_retries: int = 3) -> bool:
    """
    High-level function for chunked downloads.
    
    Args:
        url: URL to download from
        destination: Destination file path
        num_chunks: Number of parallel chunks
        expected_size: Expected file size
        max_retries: Retry attempts per chunk
    
    Returns:
        bool: True if successful, False if should fallback to regular download
    
    Example:
        >>> success = download_in_chunks(
        ...     'https://example.com/large_file.tar.gz',
        ...     'downloads/large_file.tar.gz',
        ...     num_chunks=8
        ... )
        >>> if not success:
        ...     # Fallback to regular download
        ...     download_file(url, destination)
    """
    downloader = ChunkDownloader(
        url=url,
        destination=destination,
        num_chunks=num_chunks,
        max_retries=max_retries
    )
    
    return downloader.download(expected_size=expected_size)