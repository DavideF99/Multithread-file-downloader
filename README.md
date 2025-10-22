# 🚀 ML Dataset Downloader

A production-ready, multi-threaded file downloader specifically designed for large machine learning datasets. Features intelligent resume capability, parallel chunked downloads, checksum validation, and automatic archive extraction.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Features

### Core Capabilities

- **🔄 Resumable Downloads** - Automatic resume from interruption points
- **⚡ Parallel Chunked Downloads** - Split large files into concurrent chunks
- **🧵 Multi-threaded Multi-file** - Download multiple files simultaneously
- **✅ Integrity Validation** - MD5 and SHA256 checksum verification
- **📦 Automatic Extraction** - Support for tar.gz, zip, gz formats
- **🔒 Security** - Path traversal protection and safe extraction
- **📊 Progress Tracking** - Real-time progress bars with speed metrics
- **🔁 Smart Retry Logic** - Exponential backoff for transient failures
- **📝 Comprehensive Logging** - Rotating logs with debug information

### Download Strategies

1. **Single-threaded** - Simple, reliable downloads for smaller files
2. **Chunked** - Parallel chunk downloads for large files (requires Range support)
3. **Multi-file** - Concurrent downloads of multiple files

## 📋 Table of Contents

- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Configuration](#️-configuration)
- [Usage Examples](#-usage-examples)
- [Architecture](#️-architecture)
- [Advanced Features](#-advanced-features)
- [Troubleshooting](#-troubleshooting)
- [Testing](#-testing)
- [Contributing](#-contributing)

## 🔧 Installation

### Requirements

- Python 3.8 or higher
- pip package manager

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/yourusername/ml-dataset-downloader.git
cd ml-dataset-downloader

# Install required packages
pip install -r requires.txt

# Or install individually
pip install pyyaml requests tqdm pytest
```

### Install as Package

```bash
# Install in development mode
pip install -e .

# Or install normally
python setup.py install
```

## 🚀 Quick Start

### 1. Create a Configuration File

Create `datasets.yaml`:

```yaml
# Global settings
settings:
  max_retries: 3
  timeout_seconds: 30
  chunk_size_mb: 10
  concurrent_downloads: 4
  log_level: "INFO"

# Dataset definitions
datasets:
  - name: "cifar10"
    url: "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz"
    file_size: 170498071
    checksum: "c58f30108f718f92721af3b95e74349a"
    checksum_type: "md5"
    download_strategy: "chunked"
    extract_after_download: true
    extract_format: "tar.gz"
    destination_folder: "downloads/cifar10"
```

### 2. Run the Downloader

```bash
# Download all datasets
python -m src.orchestration datasets.yaml

# Use chunked downloads with 8 parallel chunks
python -m src.orchestration datasets.yaml --chunked --chunks 8

# Download specific datasets only
python -m src.orchestration datasets.yaml --datasets cifar10 mnist

# Enable debug logging
python -m src.orchestration datasets.yaml --log-level DEBUG
```

### 3. Use as Python Library

```python
from src.orchestration import download_dataset
from src.config_loader import load_config

# Load configuration
configs = load_config('datasets.yaml')

# Download first dataset
result_path = download_dataset(configs[0], use_chunked=True, num_chunks=8)
print(f"Dataset downloaded to: {result_path}")
```

## ⚙️ Configuration

### Dataset Configuration Schema

```yaml
datasets:
  - name: "dataset_name" # Required: Unique identifier
    url: "https://example.com/file" # Required: Download URL (single file)
    # OR
    urls: # Alternative: Multiple files
      - "https://example.com/file1"
      - "https://example.com/file2"

    file_size: 1048576 # Required: Size in bytes (single file)
    # OR
    file_sizes: [1048576, 2097152] # Alternative: Sizes for multiple files

    checksum: "abc123..." # Required: MD5 or SHA256 hash
    # OR
    checksums: ["abc123...", "def456..."] # Alternative: Multiple checksums

    checksum_type: "md5" # Optional: "md5" or "sha256" (default: md5)
    download_strategy: "chunked" # Optional: "single_threaded", "multi_file", "chunked"
    extract_after_download: true # Optional: Extract archive (default: false)
    extract_format: "tar.gz" # Optional: "tar.gz", "zip", "tar", "gz"
    destination_folder: "downloads" # Required: Download destination
```

### Global Settings

```yaml
settings:
  max_retries: 3 # Retry attempts per download
  timeout_seconds: 30 # HTTP request timeout
  chunk_size_mb: 10 # Size of each chunk in MB
  concurrent_downloads: 4 # Max parallel file downloads
  log_level: "INFO" # DEBUG, INFO, WARNING, ERROR
```

## 📚 Usage Examples

### Example 1: Single Large File with Chunked Download

```python
from src.chunk_downloader import download_in_chunks

success = download_in_chunks(
    url='https://example.com/large_dataset.tar.gz',
    destination='downloads/dataset.tar.gz',
    num_chunks=8,
    expected_size=5242880000,  # 5GB
    max_retries=3
)
```

### Example 2: Download with Resume Capability

```python
from src.downloader import download_with_resume

download_with_resume(
    url='https://example.com/dataset.tar.gz',
    destination='downloads/dataset.tar.gz',
    expected_size=1048576000,
    checksum='abc123def456...',
    checksum_type='sha256',
    max_retries=5
)
```

### Example 3: Complete Workflow (Download → Validate → Extract)

```python
from src.downloader import download_extract_validate

result_path = download_extract_validate(
    url='https://example.com/dataset.tar.gz',
    destination='downloads/dataset.tar.gz',
    expected_size=1048576000,
    checksum='abc123def456...',
    checksum_type='md5',
    extract_after_download=True,
    extract_format='tar.gz',
    keep_archive=False  # Delete archive after extraction
)
```

### Example 4: Concurrent Multi-file Download

```python
from src.thread_manager import download_multiple_files, DownloadTask

tasks = [
    DownloadTask(
        url='https://example.com/file1.tar.gz',
        destination='downloads/file1.tar.gz',
        expected_size=1048576,
        checksum='abc123...',
        checksum_type='md5'
    ),
    DownloadTask(
        url='https://example.com/file2.tar.gz',
        destination='downloads/file2.tar.gz',
        expected_size=2097152,
        checksum='def456...',
        checksum_type='md5'
    ),
]

results = download_multiple_files(tasks, max_workers=4)

for result in results:
    if result.success:
        print(f"✓ {result.task.task_id}")
    else:
        print(f"✗ {result.task.task_id}: {result.error}")
```

### Example 5: Download All Datasets from Config

```python
from src.orchestration import download_all_datasets

results = download_all_datasets(
    config_path='datasets.yaml',
    use_chunked=True,
    num_chunks=8,
    max_retries=3,
    dataset_filter=['cifar10', 'mnist']  # Optional filter
)

for name, path in results.items():
    print(f"{name}: {path}")
```

## 🏗️ Architecture

### Project Structure

```
multithread_file_downloader/
├── src/
│   ├── __init__.py              # Public API exports
│   ├── orchestration.py         # High-level workflow coordination
│   ├── config_loader.py         # YAML configuration parsing
│   ├── downloader.py            # Core download functions
│   ├── chunk_downloader.py      # Parallel chunked downloads
│   ├── thread_manager.py        # Multi-file concurrency
│   ├── validator.py             # Checksum validation
│   ├── extractor.py             # Archive extraction
│   ├── progress_tracker.py      # Resume capability
│   └── logger.py                # Logging configuration
├── tests/
│   ├── test_config_loader.py
│   ├── test_downloader.py
│   ├── test_extractor.py
│   ├── test_validator.py
│   └── test_manually.py
├── datasets.yaml                # Configuration file
├── setup.py                     # Package installation
├── requires.txt                 # Dependencies
└── README.md                    # This file
```

### Module Overview

| Module                | Purpose                | Key Functions                                                              |
| --------------------- | ---------------------- | -------------------------------------------------------------------------- |
| `orchestration.py`    | High-level API and CLI | `download_dataset()`, `download_all_datasets()`, `main()`                  |
| `config_loader.py`    | Configuration parsing  | `load_config()`, `validate_dataset_config()`                               |
| `downloader.py`       | Core downloads         | `download_file()`, `download_with_resume()`, `download_extract_validate()` |
| `chunk_downloader.py` | Parallel chunks        | `ChunkDownloader`, `download_in_chunks()`                                  |
| `thread_manager.py`   | Multi-file threading   | `ThreadManager`, `download_multiple_files()`                               |
| `validator.py`        | Integrity checks       | `calculate_checksum()`, `validate_checksum()`                              |
| `extractor.py`        | Archive handling       | `extract_archive()`, `check_disk_space()`                                  |
| `progress_tracker.py` | Resume support         | `save_progress()`, `load_progress()`                                       |
| `logger.py`           | Logging setup          | `setup_logging()`, `get_logger()`                                          |

### Data Flow

```
Configuration File (YAML)
         ↓
    Config Loader ──→ Validation
         ↓
    Orchestrator ──→ Strategy Selection
         ↓
    ┌────────────┬──────────────┬─────────────┐
    ↓            ↓              ↓             ↓
Single Thread  Chunked    Multi-file   Resume Download
    ↓            ↓              ↓             ↓
    └────────────┴──────────────┴─────────────┘
                 ↓
           Checksum Validation
                 ↓
           Archive Extraction
                 ↓
           Final Dataset
```

## 🔍 Advanced Features

### Resume Capability

The downloader automatically saves progress and can resume interrupted downloads:

```python
# Progress is saved to .progress/ directory
# Automatically resumes if download is interrupted

download_with_resume(
    url='https://example.com/large_file.tar.gz',
    destination='downloads/large_file.tar.gz',
    # ... other params
)

# If interrupted, simply run again - it will resume!
```

### Chunked Downloads

For large files, split download into parallel chunks:

```python
# Server must support HTTP Range requests
# Automatically falls back to single-threaded if not supported

downloader = ChunkDownloader(
    url='https://example.com/5gb_file.tar.gz',
    destination='downloads/5gb_file.tar.gz',
    num_chunks=8,  # 8 parallel downloads
    max_retries=3
)

success = downloader.download(expected_size=5368709120)
```

### Checksum Validation

Ensure file integrity with MD5 or SHA256:

```python
from src.validator import calculate_checksum, validate_checksum

# Calculate checksum
checksum = calculate_checksum('file.tar.gz', checksum_type='sha256')
print(f"SHA256: {checksum}")

# Validate against expected
validate_checksum('file.tar.gz', 'abc123...', checksum_type='sha256')
```

### Archive Extraction with Security

Safe extraction with path traversal protection:

```python
from src.extractor import extract_archive

extract_archive(
    archive_path='downloads/dataset.tar.gz',
    extract_to='downloads/dataset/',
    archive_format='tar.gz',
    remove_archive=True  # Delete after extraction
)
```

### Disk Space Checking

Prevent extraction failures:

```python
from src.extractor import check_disk_space

# Check if 5GB is available
check_disk_space(5 * 1024 * 1024 * 1024, path='downloads/')
```

### Custom Logging

Configure logging to your needs:

```python
from src.logger import setup_logging
import logging

# Setup with custom settings
setup_logging(
    log_file='custom_logs/downloader.log',
    log_level=logging.DEBUG
)
```

## 🐛 Troubleshooting

### Common Issues

#### 1. "Server does not support Range requests"

**Problem:** Chunked download fails because server doesn't support partial content.

**Solution:** Use single-threaded download or the automatic fallback:

```python
success = download_in_chunks(...)
if not success:
    # Automatically falls back to regular download
    download_file(url, destination)
```

#### 2. "Checksum validation failed"

**Problem:** Downloaded file is corrupted or checksum is incorrect.

**Solutions:**

- Verify the expected checksum is correct
- Try downloading again (may be transient network issue)
- Check if the file on server has changed

```bash
# Manually verify checksum
md5sum downloads/file.tar.gz
sha256sum downloads/file.tar.gz
```

#### 3. "Insufficient disk space"

**Problem:** Not enough space for download or extraction.

**Solution:** Free up space or change destination:

```python
# Check available space
from src.extractor import check_disk_space
import shutil

stat = shutil.disk_usage('downloads/')
print(f"Available: {stat.free / (1024**3):.2f} GB")
```

#### 4. Downloads hang or timeout

**Problem:** Network issues or slow server.

**Solutions:**

- Increase timeout in configuration
- Increase max_retries
- Use chunked downloads for better resilience

```yaml
settings:
  timeout_seconds: 120 # Increase from default 30
  max_retries: 10 # Increase retries
```

#### 5. "Path traversal attempt detected"

**Problem:** Archive contains malicious paths.

**Solution:** This is a security feature. Do not disable. Contact dataset provider.

### Debug Mode

Enable detailed logging:

```bash
python -m src.orchestration datasets.yaml --log-level DEBUG
```

Or in code:

```python
import logging
from src.logger import setup_logging

setup_logging(log_level=logging.DEBUG)
```

### Progress Files

Resume progress is stored in `.progress/` directory:

```bash
# View active downloads
ls -la .progress/

# Clean up stale progress files (>7 days old)
python -c "from src.progress_tracker import cleanup_stale_progress_files; cleanup_stale_progress_files()"
```

## 📊 Performance Tips

### 1. Optimize Chunk Count

```python
# For files < 100MB: single-threaded (fastest)
# For files 100MB - 1GB: 4-8 chunks
# For files > 1GB: 8-16 chunks

# Example for 5GB file
download_in_chunks(url, dest, num_chunks=16)
```

### 2. Adjust Concurrent Downloads

```yaml
settings:
  concurrent_downloads:
    8 # Increase for faster multi-file downloads
    # But respect server limits!
```

### 3. Disable Progress Bars for Scripts

```python
# Progress bars add overhead in non-interactive environments
import os
os.environ['TQDM_DISABLE'] = '1'
```

### 4. Use Appropriate Checksum Type

```yaml
# MD5 is faster but less secure
checksum_type: "md5"

# SHA256 is slower but more secure
checksum_type: "sha256"
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_downloader.py

# Run with coverage
pytest --cov=src tests/

# Run with verbose output
pytest -v tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

```bash
# Clone repository
git clone https://github.com/yourusername/ml-dataset-downloader.git
cd ml-dataset-downloader

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with test dependencies
pip install -e .
pip install pytest pytest-cov

# Run tests
pytest tests/
```

## 📚 Additional Resources

- [YAML Configuration Guide](docs/configuration.md)
- [API Reference](docs/api_reference.md)
- [Architecture Deep Dive](docs/architecture.md)
- [Contributing Guidelines](CONTRIBUTING.md)

## ⭐ Acknowledgments

- Built for efficient ML dataset management
- Inspired by production data pipeline requirements
- Optimized for reliability in research environments

## 📧 Support

- **Issues:** [GitHub Issues](https://github.com/yourusername/ml-dataset-downloader/issues)
- **Discussions:** [GitHub Discussions](https://github.com/yourusername/ml-dataset-downloader/discussions)
- **Email:** your.email@example.com

---

**Happy Downloading! 🚀**

Made with ❤️ for the ML community
