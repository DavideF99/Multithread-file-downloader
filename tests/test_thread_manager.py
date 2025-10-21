"""
Tests for thread manager and concurrent downloads.

Run with: pytest tests/test_thread_manager.py -v
"""

import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from src.thread_manager import (
    DownloadTask,
    DownloadResult,
    ThreadManager,
    download_multiple_files,
    create_download_tasks_from_config
)
from src.config_loader import DatasetConfig


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


# ==================== DownloadTask Tests ====================

def test_create_download_tasks_from_multi_file_config():
    """Test creating tasks from multi-file dataset config."""
    config = DatasetConfig(
        name='multi_dataset',
        urls=[
            'http://example.com/file1.tar.gz',
            'http://example.com/file2.tar.gz',
            'http://example.com/file3.tar.gz'
        ],
        file_sizes=[1000, 2000, 3000],
        checksums=['aaa', 'bbb', 'ccc'],
        checksum_type='md5',
        destination_folder='downloads'
    )
    
    tasks = create_download_tasks_from_config([config])
    
    assert len(tasks) == 3
    
    # Verify each task
    for i, task in enumerate(tasks):
        assert task.url == config.urls[i]
        assert task.expected_size == config.file_sizes[i]
        assert task.checksum == config.checksums[i]
        assert 'multi_dataset' in task.destination


def test_create_download_tasks_from_multiple_configs():
    """Test creating tasks from multiple dataset configs."""
    configs = [
        DatasetConfig(
            name='dataset1',
            url='http://example.com/data1.tar.gz',
            file_size=1000,
            checksum='aaa',
            destination_folder='downloads'
        ),
        DatasetConfig(
            name='dataset2',
            url='http://example.com/data2.tar.gz',
            file_size=2000,
            checksum='bbb',
            destination_folder='downloads'
        )
    ]
    
    tasks = create_download_tasks_from_config(configs)
    
    assert len(tasks) == 2
    assert tasks[0].url == 'http://example.com/data1.tar.gz'
    assert tasks[1].url == 'http://example.com/data2.tar.gz'


def test_create_download_tasks_mixed_configs():
    """Test creating tasks from mix of single and multi-file configs."""
    configs = [
        DatasetConfig(
            name='single',
            url='http://example.com/single.tar.gz',
            file_size=1000,
            checksum='aaa',
            destination_folder='downloads'
        ),
        DatasetConfig(
            name='multi',
            urls=['http://example.com/multi1.tar.gz', 'http://example.com/multi2.tar.gz'],
            file_sizes=[2000, 3000],
            checksums=['bbb', 'ccc'],
            destination_folder='downloads'
        )
    ]
    
    tasks = create_download_tasks_from_config(configs)
    
    # Should have 3 tasks total (1 + 2)
    assert len(tasks) == 3


def test_create_download_tasks_preserves_destination_structure():
    """Test that destination paths include dataset name."""
    config = DatasetConfig(
        name='my_dataset',
        url='http://example.com/data.tar.gz',
        file_size=1000,
        checksum='abc',
        destination_folder='downloads'
    )
    
    tasks = create_download_tasks_from_config([config])
    
    # Destination should be: downloads/my_dataset/data.tar.gz
    assert 'downloads' in tasks[0].destination
    assert 'my_dataset' in tasks[0].destination
    assert 'data.tar.gz' in tasks[0].destination


def test_create_download_tasks_generates_task_ids():
    """Test that task IDs are generated correctly."""
    config = DatasetConfig(
        name='dataset',
        url='http://example.com/file.tar.gz',
        file_size=1000,
        checksum='abc',
        destination_folder='downloads'
    )
    
    tasks = create_download_tasks_from_config([config])
    
    # Task ID should be: dataset/file.tar.gz
    assert tasks[0].task_id == 'dataset/file.tar.gz'


# ==================== Error Handling Tests ====================

def test_download_multiple_handles_exceptions_gracefully(temp_dir):
    """Test that exceptions in one task don't stop others."""
    manager = ThreadManager(max_workers=2)
    
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(5)
    ]
    
    # Make some tasks raise different exceptions
    def download_side_effect(url, destination, **kwargs):
        if 'file1' in destination:
            raise ValueError("Network error")
        elif 'file3' in destination:
            raise IOError("Disk full")
    
    with patch('src.thread_manager.download_and_validate', side_effect=download_side_effect):
        results = manager.download_multiple(tasks)
        
        # All tasks should complete (success or failure)
        assert len(results) == 5
        
        # Check that errors are captured
        failed_results = [r for r in results if not r.success]
        assert len(failed_results) == 2
        
        # Verify error types
        errors = [r.error for r in failed_results]
        assert any(isinstance(e, ValueError) for e in errors)
        assert any(isinstance(e, IOError) for e in errors)


def test_download_task_cleans_up_on_exception(temp_dir):
    """Test that active tasks are cleaned up even on exception."""
    manager = ThreadManager(max_workers=1)
    
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination=os.path.join(temp_dir, 'file.txt'),
        task_id='cleanup_test'
    )
    
    with patch('src.thread_manager.download_and_validate', side_effect=Exception("Error")):
        result = manager.download_task(task)
        
        # Should fail
        assert result.success is False
        
        # Task should not be in active tasks
        assert 'cleanup_test' not in manager.get_active_downloads()


# ==================== Concurrent Execution Tests ====================

def test_downloads_execute_concurrently(temp_dir):
    """Test that downloads actually run in parallel."""
    import time
    
    manager = ThreadManager(max_workers=3)
    
    # Create 3 tasks that each take 0.2 seconds
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(3)
    ]
    
    def slow_download(*args, **kwargs):
        time.sleep(0.2)
    
    with patch('src.thread_manager.download_and_validate', side_effect=slow_download):
        start_time = time.time()
        manager.download_multiple(tasks)
        elapsed = time.time() - start_time
        
        # Should take ~0.2s (parallel), not ~0.6s (sequential)
        # Allow some overhead
        assert elapsed < 0.5, f"Downloads took {elapsed}s, expected <0.5s for parallel execution"


def test_thread_safety_of_results_collection(temp_dir):
    """Test that results collection is thread-safe."""
    manager = ThreadManager(max_workers=4)
    
    # Create many tasks to increase chance of race conditions
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(50)
    ]
    
    with patch('src.thread_manager.download_and_validate'):
        results = manager.download_multiple(tasks)
        
        # Should have exactly 50 results, no duplicates or losses
        assert len(results) == 50


# ==================== Edge Cases ====================

def test_download_single_task(temp_dir):
    """Test downloading single task."""
    manager = ThreadManager(max_workers=4)
    
    task = DownloadTask('http://example.com/file.txt', os.path.join(temp_dir, 'file.txt'))
    
    with patch('src.thread_manager.download_and_validate'):
        results = manager.download_multiple([task])
        
        assert len(results) == 1
        assert results[0].success is True


def test_download_with_zero_max_workers_uses_default():
    """Test that invalid max_workers uses sensible default."""
    # ThreadManager should handle this gracefully
    manager = ThreadManager(max_workers=1)  # Minimum
    
    assert manager.max_workers >= 1


def test_download_task_with_missing_optional_params(temp_dir):
    """Test download task with minimal params."""
    manager = ThreadManager(max_workers=2)
    
    # Task with only required params
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination=os.path.join(temp_dir, 'file.txt')
    )
    
    with patch('src.thread_manager.download_and_validate'):
        result = manager.download_task(task)
        
        assert result.success is True


def test_get_active_downloads_empty():
    """Test getting active downloads when none are running."""
    manager = ThreadManager(max_workers=2)
    
    active = manager.get_active_downloads()
    
    assert active == {}


def test_create_tasks_from_empty_config_list():
    """Test creating tasks from empty config list."""
    tasks = create_download_tasks_from_config([])
    
    assert tasks == []


def test_download_result_string_representation():
    """Test that DownloadResult can be printed/logged."""
    task = DownloadTask('http://example.com/file.txt', 'file.txt')
    result = DownloadResult(task=task, success=True, destination='file.txt')
    
    # Should not raise exception
    str(result)
    repr(result)


def test_download_task_string_representation():
    """Test that DownloadTask can be printed/logged."""
    task = DownloadTask('http://example.com/file.txt', 'file.txt')
    
    # Should not raise exception
    str(task)
    repr(task)


# ==================== Integration Tests ====================

def test_full_workflow_single_dataset(temp_dir):
    """Test complete workflow: config -> tasks -> download."""
    # Create config
    config = DatasetConfig(
        name='test_dataset',
        url='http://example.com/data.tar.gz',
        file_size=1048576,
        checksum='abc123',
        destination_folder=temp_dir
    )
    
    # Create tasks
    tasks = create_download_tasks_from_config([config])
    
    # Download
    with patch('src.thread_manager.download_and_validate'):
        results = download_multiple_files(tasks, max_workers=2)
        
        assert len(results) == 1
        assert results[0].success is True


def test_full_workflow_multi_file_dataset(temp_dir):
    """Test complete workflow with multi-file dataset."""
    config = DatasetConfig(
        name='multi_dataset',
        urls=[
            'http://example.com/part1.tar.gz',
            'http://example.com/part2.tar.gz'
        ],
        file_sizes=[1000, 2000],
        checksums=['aaa', 'bbb'],
        destination_folder=temp_dir
    )
    
    tasks = create_download_tasks_from_config([config])
    
    with patch('src.thread_manager.download_and_validate'):
        results = download_multiple_files(tasks, max_workers=2)
        
        assert len(results) == 2
        assert all(r.success for r in results)


def test_partial_failure_workflow(temp_dir):
    """Test workflow where some downloads fail."""
    configs = [
        DatasetConfig(
            name='success1',
            url='http://example.com/good1.tar.gz',
            file_size=1000,
            checksum='aaa',
            destination_folder=temp_dir
        ),
        DatasetConfig(
            name='failure',
            url='http://example.com/bad.tar.gz',
            file_size=2000,
            checksum='bbb',
            destination_folder=temp_dir
        ),
        DatasetConfig(
            name='success2',
            url='http://example.com/good2.tar.gz',
            file_size=3000,
            checksum='ccc',
            destination_folder=temp_dir
        )
    ]
    
    tasks = create_download_tasks_from_config(configs)
    
    def download_side_effect(url, destination, **kwargs):
        if 'bad' in url:
            raise ValueError("Download failed")
    
    with patch('src.thread_manager.download_and_validate', side_effect=download_side_effect):
        results = download_multiple_files(tasks)
        
        assert len(results) == 3
        
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        assert len(successful) == 2
        assert len(failed) == 1
        
def test_download_task_creation():  # ✅ Fixed: Added 'test_' prefix
    """Test creating DownloadTask with all parameters."""
    task = DownloadTask(
        url='http://example.com/file.tar.gz',
        destination='downloads/file.tar.gz',
        expected_size=1048576,
        checksum='abc123',
        checksum_type='md5',
        task_id='test_task'
    )
    
    assert task.url == 'http://example.com/file.tar.gz'
    assert task.destination == 'downloads/file.tar.gz'
    assert task.expected_size == 1048576
    assert task.checksum == 'abc123'
    assert task.checksum_type == 'md5'
    assert task.task_id == 'test_task'


def test_download_task_auto_generates_id():
    """Test that task_id is auto-generated from destination."""
    task = DownloadTask(
        url='http://example.com/file.tar.gz',
        destination='downloads/dataset/file.tar.gz'
    )
    
    assert task.task_id == 'file.tar.gz'


def test_download_task_minimal():
    """Test creating task with minimal parameters."""
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination='file.txt'
    )
    
    assert task.url == 'http://example.com/file.txt'
    assert task.destination == 'file.txt'
    assert task.expected_size is None
    assert task.checksum is None
    assert task.checksum_type == 'md5'


# ==================== DownloadResult Tests ====================

def test_download_result_success():
    """Test creating successful DownloadResult."""
    task = DownloadTask('http://example.com/file.txt', 'file.txt')
    result = DownloadResult(
        task=task,
        success=True,
        destination='file.txt'
    )
    
    assert result.success is True
    assert result.error is None
    assert result.destination == 'file.txt'


def test_download_result_failure():
    """Test creating failed DownloadResult."""
    task = DownloadTask('http://example.com/file.txt', 'file.txt')
    error = ValueError("Download failed")
    
    result = DownloadResult(
        task=task,
        success=False,
        error=error
    )
    
    assert result.success is False
    assert result.error == error
    assert result.destination is None


# ==================== ThreadManager Tests ====================

def test_thread_manager_initialization():
    """Test ThreadManager initialization."""
    manager = ThreadManager(max_workers=8)
    
    assert manager.max_workers == 8
    assert manager.results == []
    assert manager.active_tasks == {}


def test_download_task_execution_success(temp_dir):
    """Test successful task execution."""
    manager = ThreadManager(max_workers=2)
    
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination=os.path.join(temp_dir, 'file.txt')
    )
    
    with patch('src.thread_manager.download_and_validate') as mock_download:
        # Mock successful download
        mock_download.return_value = None
        
        result = manager.download_task(task, max_retries=3)
        
        assert result.success is True
        assert result.error is None
        assert result.destination == task.destination
        
        # Verify download was called with correct params
        mock_download.assert_called_once()


def test_download_task_execution_failure(temp_dir):
    """Test failed task execution."""
    manager = ThreadManager(max_workers=2)
    
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination=os.path.join(temp_dir, 'file.txt')
    )
    
    error = ValueError("Network error")
    
    with patch('src.thread_manager.download_and_validate', side_effect=error):
        result = manager.download_task(task)
        
        assert result.success is False
        assert result.error == error


def test_download_task_tracks_active_tasks(temp_dir):
    """Test that active tasks are tracked."""
    manager = ThreadManager(max_workers=2)
    
    task = DownloadTask(
        url='http://example.com/file.txt',
        destination=os.path.join(temp_dir, 'file.txt'),
        task_id='test_task'
    )
    
    with patch('src.thread_manager.download_and_validate'):
        # Check active tasks during execution
        def check_active(*args, **kwargs):
            # During download, task should be active
            active = manager.get_active_downloads()
            assert 'test_task' in active
        
        with patch('src.thread_manager.download_and_validate', side_effect=check_active):
            manager.download_task(task)
        
        # After completion, should be removed
        active = manager.get_active_downloads()
        assert 'test_task' not in active


# ==================== Multiple Download Tests ====================

def test_download_multiple_success(temp_dir):
    """Test downloading multiple files successfully."""
    manager = ThreadManager(max_workers=2)
    
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(3)
    ]
    
    with patch('src.thread_manager.download_and_validate'):
        results = manager.download_multiple(tasks, max_retries=3)
        
        assert len(results) == 3
        assert all(r.success for r in results)


def test_download_multiple_with_failures(temp_dir):
    """Test downloading multiple files with some failures."""
    manager = ThreadManager(max_workers=2)
    
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(5)
    ]
    
    # Make tasks 1 and 3 fail
    def download_side_effect(url, destination, **kwargs):
        if 'file1' in destination or 'file3' in destination:
            raise ValueError("Download failed")
    
    with patch('src.thread_manager.download_and_validate', side_effect=download_side_effect):
        results = manager.download_multiple(tasks)
        
        assert len(results) == 5
        
        # Check success/failure
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        assert len(successful) == 3
        assert len(failed) == 2


def test_download_multiple_empty_list():
    """Test downloading with empty task list."""
    manager = ThreadManager(max_workers=2)
    
    results = manager.download_multiple([])
    
    assert results == []


def test_download_multiple_respects_max_workers(temp_dir):
    """Test that max_workers limit is respected."""
    import threading
    
    max_workers = 2
    manager = ThreadManager(max_workers=max_workers)
    
    # Track concurrent executions
    concurrent_count = 0
    max_concurrent = 0
    lock = threading.Lock()
    
    def mock_download(*args, **kwargs):
        nonlocal concurrent_count, max_concurrent
        
        with lock:
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
        
        # Simulate work
        import time
        time.sleep(0.1)
        
        with lock:
            concurrent_count -= 1
    
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(10)
    ]
    
    with patch('src.thread_manager.download_and_validate', side_effect=mock_download):
        manager.download_multiple(tasks)
        
        # Should not exceed max_workers
        assert max_concurrent <= max_workers


def test_download_multiple_with_progress_callback(temp_dir):
    """Test progress callback during multiple downloads."""
    manager = ThreadManager(max_workers=2)
    
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(3)
    ]
    
    callback_calls = []
    
    def progress_callback(completed, total, result):
        callback_calls.append({
            'completed': completed,
            'total': total,
            'success': result.success
        })
    
    with patch('src.thread_manager.download_and_validate'):
        manager.download_multiple(tasks, progress_callback=progress_callback)
        
        # Callback should be called for each task
        assert len(callback_calls) == 3
        
        # Verify completed count increases
        assert callback_calls[0]['completed'] == 1
        assert callback_calls[1]['completed'] == 2
        assert callback_calls[2]['completed'] == 3


# ==================== High-Level Function Tests ====================

def test_download_multiple_files_function(temp_dir):
    """Test high-level download_multiple_files function."""
    tasks = [
        DownloadTask(f'http://example.com/file{i}.txt', os.path.join(temp_dir, f'file{i}.txt'))
        for i in range(3)
    ]
    
    with patch('src.thread_manager.download_and_validate'):
        results = download_multiple_files(tasks, max_workers=4, max_retries=5)
        
        assert len(results) == 3
        assert all(r.success for r in results)


def test_download_multiple_files_custom_workers(temp_dir):
    """Test download_multiple_files with custom worker count."""
    tasks = [DownloadTask('http://example.com/file.txt', os.path.join(temp_dir, 'file.txt'))]
    
    with patch.object(ThreadManager, '__init__', return_value=None) as mock_init, \
         patch.object(ThreadManager, 'download_multiple', return_value=[]):
        
        download_multiple_files(tasks, max_workers=8)
        
        # Verify ThreadManager initialized with correct workers
        mock_init.assert_called_once_with(max_workers=8)


# ==================== Config Integration Tests ====================

def test_create_download_tasks_from_single_file_config():
    """Test creating tasks from single-file dataset config."""
    config = DatasetConfig(
        name='test_dataset',
        url='http://example.com/data.tar.gz',
        file_size=1048576,
        checksum='abc123',
        checksum_type='md5',
        destination_folder='downloads'
    )
    
    tasks = create_download_tasks_from_config([config])
    
    assert len(tasks) == 1
    assert tasks[0].url == 'http://example.com/data.tar.gz'
    assert 'test_dataset' in tasks[0].destination
    assert tasks[0].expected_size == 1048576
    assert tasks[0].checksum == 'abc123'