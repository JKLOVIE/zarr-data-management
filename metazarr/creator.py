"""
功能一：原始（GRIB/NetCDF/HDF） ➜ 标准 Zarr
"""
from __future__ import annotations
import re, shutil, json
from pathlib import Path
from typing import List, Dict

import dask
import xarray as xr
import zarr

from .config import *
from .utils import log, ensure_empty_dir, scan_files, load_catalog, save_catalog, timestamp
from .exceptions import ValidationError, ConversionError

# ─── 主入口 ────────────────────────────────────────────────────────────────────
def create_dataset(
    *,
    data_kind: DataKind,
    raw_format: RawFormat,
    description: str,
    src_paths: List[str | Path],
    dst_path: str | Path,
    org_mode: OrgMode,
    name: str,
    spatial_res: tuple[int, int, int] | None = None,
    temporal_res: str | None = None,
    allow_update: bool = False,
) -> Path:
    """
    把指定路径下的原始文件扫描、解析、合并，写成 Zarr。
    返回 Zarr 根目录 Path。
    """
    dst_path = Path(dst_path)
    ensure_empty_dir(dst_path)

    log.info("扫描原始文件…")
    files = list(scan_files([Path(p) for p in src_paths]))
    if not files:
        raise ValidationError("未找到任何原始文件。")

    # ── 解析文件名 ────────────────────────────────────────────────────────────
    parsed: Dict[str, dict] = {}
    for f in files:
        m = FILENAME_RE.match(f.name)
        if not m:
            log.warning("跳过不符合命名规则的文件: %s", f)
            continue
        info = m.groupdict()
        day_key = info["datetime"][:8]  # YYYYMMDD
        parsed.setdefault(day_key, {}).setdefault(info["var"], []).append(
            dict(path=f, step=info.get("step"))
        )

    if not parsed:
        raise ValidationError("没有文件符合命名规范。")

    # ── 针对 forecast: 同一天同要素不同预报时次合并 step 维度 ──────────────
    zs = None
    for day, var_map in parsed.items():
        ds_day = []
        for var, lst in var_map.items():
            sub_datasets = []
            for item in sorted(lst, key=lambda x: x["step"] or "000"):
                sub = _open_raw(item["path"], raw_format)
                if data_kind is DataKind.FORECAST:
                    step = int(item["step"] or 0)
                    sub = sub.expand_dims({"step": [step]})
                sub_datasets.append(sub)
            ds_var = xr.concat(sub_datasets, dim="step" if data_kind is DataKind.FORECAST else "time")
            ds_day.append(ds_var)
        ds_day = xr.merge(ds_day)
        # 写入（按天或其他方式分组）：
        target = dst_path / day if allow_update else dst_path
        if allow_update:
            target = dst_path / f"{day}.zarr"
            target.mkdir(parents=True, exist_ok=True)
        else:
            target = dst_path
        _write_zarr(ds_day, target, consolidate=(not allow_update))

    # ── 更新 Catalog ──────────────────────────────────────────────────────────
    cat = load_catalog()
    cat[name] = {
        "name": name,
        "kind": data_kind.value,
        "format": "zarr",
        "description": description,
        "path": str(dst_path),
        "spatial_res": spatial_res,
        "temporal_res": temporal_res,
        "org_mode": org_mode.value,
        "created": timestamp(),
        "allow_update": allow_update,
    }
    save_catalog(cat)
    log.info("✅ 数据集 %s 创建完成 @ %s", name, dst_path)
    return dst_path


def append_dataset(name: str, new_files: List[str | Path]):
    """
    在 allow_update=True 的前提下，追加同构数据（按天文件夹）到已有 Zarr。
    """
    cat = load_catalog()
    meta = cat.get(name)
    if not meta or not meta["allow_update"]:
        raise ValidationError(f"数据集 {name} 不存在或未开启可追加模式。")

    dst = Path(meta["path"])
    raw_format = RawFormat.GRIB if any(str(p).lower().endswith(".grb") for p in new_files) else RawFormat.NETCDF
    create_dataset(
        data_kind=DataKind(meta["kind"]),
        raw_format=raw_format,
        description=meta["description"],
        src_paths=new_files,
        dst_path=dst,
        org_mode=OrgMode(meta["org_mode"]),
        name=name,
        allow_update=True,
    )

# ─── 内部工具 ─────────────────────────────────────────────────────────────────
def _open_raw(path: Path, raw_format: RawFormat) -> xr.Dataset:
    """根据不同原始格式返回 Xarray Dataset"""
    if raw_format is RawFormat.GRIB:
        # cfgrib / eccodes
        return xr.open_dataset(path, engine="cfgrib", chunks={}, backend_kwargs={"indexpath": ""})
    elif raw_format is RawFormat.NETCDF:
        return xr.open_dataset(path, engine="netcdf4", chunks={})
    elif raw_format is RawFormat.HDF:
        return xr.open_dataset(path, engine="h5netcdf", chunks={})
    else:
        raise ConversionError(f"不支持的输入格式 {raw_format}")

def _write_zarr(ds: xr.Dataset, store_path: Path, consolidate=True):
    log.info("写入 Zarr: %s …", store_path)
    # ↘ 关键写入：分块策略、压缩器可根据分辨率自动拟合
    ds.to_zarr(
        str(store_path),
        mode="w",
        consolidated=consolidate,
        encoding={v: {"compressor": DEFAULT_COMPRESSOR} for v in ds.data_vars},
    )
