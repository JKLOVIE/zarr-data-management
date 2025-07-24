"""
功能二‑1：数据集注册表
"""
from pathlib import Path
from typing import List, Dict
from .utils import load_catalog, save_catalog

def list_datasets() -> List[str]:
    return list(load_catalog().keys())

def show_dataset_info(name: str) -> Dict:
    cat = load_catalog()
    return cat.get(name, {})

def remove_dataset(name: str, delete_files: bool = False):
    cat = load_catalog()
    meta = cat.pop(name, None)
    if meta:
        save_catalog(cat)
        if delete_files:
            path = Path(meta["path"])
            import shutil; shutil.rmtree(path, ignore_errors=True)
