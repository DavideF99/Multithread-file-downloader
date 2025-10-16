"""
Thread manager for concurrent multi-file downloads.

Manages downloading multiple files in parallel using thread pools,
with progress tracking and error handling.
"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass
from tqdm import tqdm
from src.logger import get_logger
from src.downloader import download_and_validate


@dataclass
class DownloadTask:
    """
    Represents a single download task.
    """
    url: str
    destination: str
    expected_size: Optional[int] = None
    checksum: Optional[str] = None
    checksum_type: str = 'md5'
    task_id: Optional[str] = None  # Identifier for this task
    
    def __post_init__(self):
        """Generate task_id if not provided."""
        if self.task_id is None:
            self.task_id = os.path.basename(self.destination)


@dataclass
class DownloadResult:
    """
    Result of a download task.
    """
    task: DownloadTask
    success: bool
    error: Optional[Exception] = None
    destination: Optional[str] = None


class ThreadManager:
    """
    Manages concurrent downloads using thread pools.
    
    Features:
    - Thread pool for parallel downloads
    - Progress tracking across all files
    - Error collection and reporting
    - Configurable worker count
    """
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize thread manager.
        
        Args:
            max_workers: Maximum number of concurrent download threads
        """
        self.max_workers = max_workers
        self.logger = get_logger()
        
        # Thread-safe tracking
        self.lock = threading.Lock()
        self.results = []
        self.active_tasks = {}
    
    def download_task(self, task: DownloadTask, 
                     max_retries: int = 3) -> DownloadResult:
        """
        Execute a single download task.
        
        Args:
            task: DownloadTask to execute
            max_retries: Retry attempts
        
        Returns:
            DownloadResult with success status and any errors
        """
        self.logger.info(f"Starting download: {task.task_id}")
        
        try:
            # Track active task
            with self.lock:
                self.active_tasks[task.task_id] = task
            
            # Execute download
            download_and_validate(
                url=task.url,
                destination=task.destination,
                expected_size=task.expected_size,
                checksum=task.checksum,
                checksum_type=task.checksum_type,
                max_retries=max_retries
            )
            
            self.logger.info(f"Download complete: {task.task_id}")
            
            return DownloadResult(
                task=task,
                success=True,
                destination=task.destination
            )
            
        except Exception as e:
            self.logger.error(f"Download failed: {task.task_id} - {e}")
            
            return DownloadResult(
                task=task,
                success=False,
                error=e
            )
        
        finally:
            # Remove from active tasks
            with self.lock:
                self.active_tasks.pop(task.task_id, None)
    
    def download_multiple(self, tasks: List[DownloadTask],
                         max_retries: int = 3,
                         progress_callback: Optional[Callable] = None) -> List[DownloadResult]:
        """
        Download multiple files concurrently.
        
        Args:
            tasks: List of DownloadTask objects
            max_retries: Retry attempts per task
            progress_callback: Optional callback(completed, total, result)
        
        Returns:
            List of DownloadResult objects
        
        Example:
            >>> tasks = [
            ...     DownloadTask('http://example.com/file1.tar.gz', 'downloads/file1.tar.gz'),
            ...     DownloadTask('http://example.com/file2.tar.gz', 'downloads/file2.tar.gz'),
            ... ]
            >>> manager = ThreadManager(max_workers=4)
            >>> results = manager.download_multiple(tasks)
            >>> successful = sum(1 for r in results if r.success)
            >>> print(f"{successful}/{len(tasks)} downloads successful")
        """
        if not tasks:
            self.logger.warning("No tasks to download")
            return []
        
        self.logger.info(
            f"Starting {len(tasks)} downloads with {self.max_workers} workers"
        )
        
        results = []
        
        # Create progress bar for overall progress
        progress_bar = tqdm(
            total=len(tasks),
            unit='file',
            desc='Overall Progress'
        )
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.download_task, task, max_retries): task
                for task in tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Update progress
                    progress_bar.update(1)
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(len(results), len(tasks), result)
                    
                except Exception as e:
                    self.logger.error(f"Task execution failed: {task.task_id} - {e}")
                    results.append(DownloadResult(
                        task=task,
                        success=False,
                        error=e
                    ))
                    progress_bar.update(1)
        
        progress_bar.close()
        
        # Summary
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        
        self.logger.info(
            f"Download complete: {successful} successful, {failed} failed"
        )
        
        return results
    
    def get_active_downloads(self) -> Dict[str, DownloadTask]:
        """
        Get currently active download tasks.
        
        Returns:
            dict: Mapping of task_id to DownloadTask
        """
        with self.lock:
            return self.active_tasks.copy()


def download_multiple_files(tasks: List[DownloadTask], max_workers: int = 4,
                            max_retries: int = 3) -> List[DownloadResult]:
    """
    High-level function to download multiple files concurrently.
    
    Args:
        tasks: List of DownloadTask objects
        max_workers: Number of concurrent downloads
        max_retries: Retry attempts per file
    
    Returns:
        List of DownloadResult objects
    
    Example:
        >>> from src.thread_manager import download_multiple_files, DownloadTask
        >>> 
        >>> tasks = [
        ...     DownloadTask(
        ...         url='https://example.com/dataset1.tar.gz',
        ...         destination='downloads/dataset1.tar.gz',
        ...         expected_size=1048576,
        ...         checksum='abc123...'
        ...     ),
        ...     DownloadTask(
        ...         url='https://example.com/dataset2.tar.gz',
        ...         destination='downloads/dataset2.tar.gz',
        ...         expected_size=2097152,
        ...         checksum='def456...'
        ...     ),
        ... ]
        >>> 
        >>> results = download_multiple_files(tasks, max_workers=4)
        >>> 
        >>> for result in results:
        ...     if result.success:
        ...         print(f"✓ {result.task.task_id}")
        ...     else:
        ...         print(f"✗ {result.task.task_id}: {result.error}")
    """
    manager = ThreadManager(max_workers=max_workers)
    return manager.download_multiple(tasks, max_retries=max_retries)


def create_download_tasks_from_config(config_list: List[Any]) -> List[DownloadTask]:
    """
    Create DownloadTask objects from DatasetConfig list.
    
    Handles both single-file and multi-file datasets.
    
    Args:
        config_list: List of DatasetConfig objects
    
    Returns:
        List of DownloadTask objects
    
    Example:
        >>> from src.config_loader import load_config
        >>> configs = load_config('datasets.yaml')
        >>> tasks = create_download_tasks_from_config(configs)
        >>> results = download_multiple_files(tasks)
    """
    tasks = []
    
    for config in config_list:
        # Single file dataset
        if config.url:
            destination = os.path.join(
                config.destination_folder,
                config.name,
                os.path.basename(config.url)
            )
            
            task = DownloadTask(
                url=config.url,
                destination=destination,
                expected_size=config.file_size,
                checksum=config.checksum,
                checksum_type=config.checksum_type,
                task_id=f"{config.name}/{os.path.basename(config.url)}"
            )
            tasks.append(task)
        
        # Multi-file dataset
        elif config.urls:
            for i, url in enumerate(config.urls):
                destination = os.path.join(
                    config.destination_folder,
                    config.name,
                    os.path.basename(url)
                )
                
                file_size = config.file_sizes[i] if config.file_sizes else None
                checksum = config.checksums[i] if config.checksums else None
                
                task = DownloadTask(
                    url=url,
                    destination=destination,
                    expected_size=file_size,
                    checksum=checksum,
                    checksum_type=config.checksum_type,
                    task_id=f"{config.name}/{os.path.basename(url)}"
                )
                tasks.append(task)
    
    return tasks