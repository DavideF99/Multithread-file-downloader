"""
ML Dataset Downloader - Production-ready multi-threaded file downloader.

A robust, resumable, and configurable downloader for large ML datasets with:
- Multi-threaded downloads
- Chunked download support
- Resume capability
- Checksum validation
- Archive extraction
- Progress tracking
"""

__version__ = "1.0.0"
__author__ = "Your Name"

# Public API exports
from src.config_loader import DatasetConfig, load_config, validate_dataset_config
from src.orchestration import (
    download_dataset,
    download_all_datasets,
    main as run_downloader
)
from src.downloader import (
    download_file,
    download_with_resume,
    download_and_validate,
    download_extract_validate
)
from src.validator import calculate_checksum, validate_checksum
from src.extractor import extract_archive, check_disk_space
from src.logger import setup_logging, get_logger

__all__ = [
    # Version
    "__version__",
    
    # Configuration
    "DatasetConfig",
    "load_config",
    "validate_dataset_config",
    
    # High-level orchestration (recommended)
    "download_dataset",
    "download_all_datasets",
    "run_downloader",
    
    # Low-level download functions
    "download_file",
    "download_with_resume",
    "download_and_validate",
    "download_extract_validate",
    
    # Validation
    "calculate_checksum",
    "validate_checksum",
    
    # Extraction
    "extract_archive",
    "check_disk_space",
    
    # Logging
    "setup_logging",
    "get_logger",
]