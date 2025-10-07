# Test script - create test_manual.py
from src.config_loader import load_config

if __name__ == "__main__":
    try:
        datasets = load_config('config/datasets.yaml')
        print(f"Successfully loaded {len(datasets)} datasets:")
        for ds in datasets:
            print(f"  - {ds.name}: {ds.download_strategy}")
    except Exception as e:
        print(f"Error: {e}")