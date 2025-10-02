from setuptools import setup, find_packages

setup(
    name="multithread_file_downloader",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'requests',
        'tqdm',
        'pytest',
    ],
    python_requires='>=3.8',
)