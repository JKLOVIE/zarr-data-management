"""
metazarr
========
一个基于 Zarr ‑ Dask ‑ Xarray 的气象/海洋数据管理工具包。
"""
from .creator import create_dataset, append_dataset          # 功能一
from .catalog import list_datasets, show_dataset_info        # 功能二‑1
from .accessor import open_dataset                           # 功能二‑2/3

__all__ = [
    "create_dataset",
    "append_dataset",
    "list_datasets",
    "show_dataset_info",
    "open_dataset",
]
