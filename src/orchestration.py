"""
Main orchestration module for ML dataset downloader.

Coordinates the entire download workflow:
- Load configuration
- Determine download strategy
- Execute downloads (single/chunked/multi-threaded)
- Validate checksums
- Extract archives
- Command-line interface
"""

import sys
import argparse
import os
from typing import List, Optional
from src.logger import setup_logging, get_logger
from src.config_loader import load_config, DatasetConfig
from src.downloader import download_extract_validate
from src.chunk_downloader import download_in_chunks
from src.thread_manager import (
    DownloadTask,
    download_multiple_files,
    create_download_tasks_from_config
)
from src.extractor import check_disk_space
from src.validator import validate_checksum


def download_dataset(config: DatasetConfig, use_chunked: bool = False,
                     num_chunks: int = 4, max_retries: int = 3) -> str:
    """
    Download a single dataset based on its configuration.
    
    Determines the appropriate download strategy and executes it.
    
    Args:
        config: DatasetConfig object
        use_chunked: Force chunked download strategy
        num_chunks: Number of chunks for parallel download
        max_retries: Retry attempts
    
    Returns:
        str: Path to downloaded/extracted data
    
    Raises:
        Exception: On download or extraction failure
    
    Example:
        >>> from src.config_loader import load_config
        >>> configs = load_config('datasets.yaml')
        >>> result = download_dataset(configs[0])
        >>> print(f"Dataset ready at: {result}")
    """
    logger = get_logger()
    logger.info(f"Processing dataset: {config.name}")
    logger.info(f"Strategy: {config.download_strategy}")
    
    # Single file dataset
    if config.url:
        filename = os.path.basename(config.url)
        destination = os.path.join(config.destination_folder, config.name, filename)
        
        # Check disk space (3x file size for extraction)
        if config.file_size:
            required_space = config.file_size * 3 if config.extract_after_download else config.file_size
            check_disk_space(required_space, config.destination_folder)
        
        # Determine download strategy
        if use_chunked or config.download_strategy == 'chunked':
            logger.info(f"Using chunked download with {num_chunks} chunks")
            
            # Try chunked download
            success = download_in_chunks(
                url=config.url,
                destination=destination,
                num_chunks=num_chunks,
                expected_size=config.file_size,
                max_retries=max_retries
            )
            
            # Fallback to regular download if chunked fails
            if not success:
                logger.warning("Chunked download not supported, falling back to regular download")
                result = download_extract_validate(
                    url=config.url,
                    destination=destination,
                    expected_size=config.file_size,
                    checksum=config.checksum,
                    checksum_type=config.checksum_type,
                    extract_after_download=config.extract_after_download,
                    extract_format=config.extract_format,
                    max_retries=max_retries
                )
                return result
            
            # Validate checksum after chunked download
            if config.checksum and config.checksum.lower() != 'skip':
                logger.info("Validating checksum...")
                validate_checksum(destination, config.checksum, config.checksum_type)
            
            # Extract if needed
            if config.extract_after_download:
                from src.extractor import extract_archive
                extract_to = os.path.dirname(destination)
                extract_archive(
                    archive_path=destination,
                    extract_to=extract_to,
                    archive_format=config.extract_format,
                    remove_archive=True
                )
                return extract_to
            
            return destination
        
        else:
            # Regular single-threaded download
            logger.info("Using single-threaded download")
            result = download_extract_validate(
                url=config.url,
                destination=destination,
                expected_size=config.file_size,
                checksum=config.checksum,
                checksum_type=config.checksum_type,
                extract_after_download=config.extract_after_download,
                extract_format=config.extract_format,
                max_retries=max_retries
            )
            return result
    
    # Multi-file dataset
    elif config.urls:
        logger.info(f"Multi-file dataset with {len(config.urls)} files")
        
        # Create download tasks
        tasks = []
        for i, url in enumerate(config.urls):
            filename = os.path.basename(url)
            destination = os.path.join(config.destination_folder, config.name, filename)
            
            file_size = config.file_sizes[i] if config.file_sizes else None
            checksum = config.checksums[i] if config.checksums else None
            
            task = DownloadTask(
                url=url,
                destination=destination,
                expected_size=file_size,
                checksum=checksum,
                checksum_type=config.checksum_type,
                task_id=f"{config.name}/{filename}"
            )
            tasks.append(task)
        
        # Check total disk space
        if config.file_sizes:
            total_size = sum(config.file_sizes)
            required_space = total_size * 3 if config.extract_after_download else total_size
            check_disk_space(required_space, config.destination_folder)
        
        # Download files concurrently
        logger.info(f"Downloading {len(tasks)} files concurrently")
        results = download_multiple_files(tasks, max_workers=4, max_retries=max_retries)
        
        # Check for failures
        failed = [r for r in results if not r.success]
        if failed:
            logger.error(f"{len(failed)} files failed to download:")
            for result in failed:
                logger.error(f"  {result.task.task_id}: {result.error}")
            raise Exception(f"{len(failed)} files failed to download")
        
        # Extract files if needed
        if config.extract_after_download:
            logger.info("Extracting downloaded files...")
            from src.extractor import extract_archive
            
            extract_to = os.path.join(config.destination_folder, config.name)
            
            for result in results:
                if result.destination and config.extract_format:
                    try:
                        extract_archive(
                            archive_path=result.destination,
                            extract_to=extract_to,
                            archive_format=config.extract_format,
                            remove_archive=True
                        )
                    except Exception as e:
                        logger.error(f"Failed to extract {result.destination}: {e}")
            
            return extract_to
        
        return os.path.join(config.destination_folder, config.name)
    
    else:
        raise ValueError(f"Dataset '{config.name}' has neither 'url' nor 'urls'")


def download_all_datasets(config_path: str, use_chunked: bool = False,
                          num_chunks: int = 4, max_retries: int = 3,
                          dataset_filter: Optional[List[str]] = None) -> dict:
    """
    Download all datasets from configuration file.
    
    Args:
        config_path: Path to YAML configuration file
        use_chunked: Use chunked downloads where possible
        num_chunks: Number of chunks for parallel download
        max_retries: Retry attempts per download
        dataset_filter: List of dataset names to download (None = all)
    
    Returns:
        dict: Mapping of dataset names to their final paths
    
    Example:
        >>> results = download_all_datasets('datasets.yaml', use_chunked=True)
        >>> for name, path in results.items():
        ...     print(f"{name}: {path}")
    """
    logger = get_logger()
    logger.info(f"Loading configuration from: {config_path}")
    
    # Load configuration
    try:
        configs = load_config(config_path)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise
    
    # Filter datasets if requested
    if dataset_filter:
        configs = [c for c in configs if c.name in dataset_filter]
        logger.info(f"Filtered to {len(configs)} datasets: {dataset_filter}")
    
    if not configs:
        logger.warning("No datasets to download")
        return {}
    
    logger.info(f"Found {len(configs)} datasets to download")
    
    # Download each dataset
    results = {}
    failed = []
    
    for i, config in enumerate(configs, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Dataset {i}/{len(configs)}: {config.name}")
        logger.info(f"{'='*60}")
        
        try:
            result_path = download_dataset(
                config,
                use_chunked=use_chunked,
                num_chunks=num_chunks,
                max_retries=max_retries
            )
            results[config.name] = result_path
            logger.info(f"✓ {config.name} complete: {result_path}")
            
        except Exception as e:
            logger.error(f"✗ {config.name} failed: {e}")
            failed.append((config.name, str(e)))
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("DOWNLOAD SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total datasets: {len(configs)}")
    logger.info(f"Successful: {len(results)}")
    logger.info(f"Failed: {len(failed)}")
    
    if failed:
        logger.error("\nFailed datasets:")
        for name, error in failed:
            logger.error(f"  {name}: {error}")
    
    return results


def main():
    """
    Command-line interface for ML dataset downloader.
    
    Usage:
        python -m src.orchestration datasets.yaml
        python -m src.orchestration datasets.yaml --chunked --chunks 8
        python -m src.orchestration datasets.yaml --datasets cifar10 imagenet
    """
    parser = argparse.ArgumentParser(
        description='ML Dataset Downloader - Production-ready multi-threaded downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all datasets from config
  python -m src.orchestration datasets.yaml
  
  # Use chunked downloads with 8 parallel chunks
  python -m src.orchestration datasets.yaml --chunked --chunks 8
  
  # Download specific datasets only
  python -m src.orchestration datasets.yaml --datasets cifar10 mnist
  
  # Increase retry attempts
  python -m src.orchestration datasets.yaml --retries 5
  
  # Custom log level
  python -m src.orchestration datasets.yaml --log-level DEBUG
        """
    )
    
    parser.add_argument(
        'config',
        help='Path to YAML configuration file'
    )
    
    parser.add_argument(
        '--chunked',
        action='store_true',
        help='Use chunked/parallel downloads for large files'
    )
    
    parser.add_argument(
        '--chunks',
        type=int,
        default=4,
        help='Number of parallel chunks for chunked downloads (default: 4)'
    )
    
    parser.add_argument(
        '--retries',
        type=int,
        default=3,
        help='Maximum retry attempts per download (default: 3)'
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        help='Specific dataset names to download (default: all)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        default='logs/downloader.log',
        help='Log file path (default: logs/downloader.log)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    import logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_file=args.log_file, log_level=log_level)
    
    logger = get_logger()
    logger.info("="*60)
    logger.info("ML DATASET DOWNLOADER")
    logger.info("="*60)
    logger.info(f"Configuration: {args.config}")
    logger.info(f"Chunked downloads: {args.chunked}")
    logger.info(f"Chunks per file: {args.chunks}")
    logger.info(f"Max retries: {args.retries}")
    logger.info(f"Log level: {args.log_level}")
    
    try:
        # Download datasets
        results = download_all_datasets(
            config_path=args.config,
            use_chunked=args.chunked,
            num_chunks=args.chunks,
            max_retries=args.retries,
            dataset_filter=args.datasets
        )
        
        # Exit with success
        logger.info("\nAll downloads complete!")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()