"""
creator.py
功能一：原始 (GRIB / NetCDF / HDF) ➜ Zarr
        或直接登记现成 Zarr 目录集合
"""
from __future__ import annotations
import time, shutil
from pathlib import Path
from typing import List, Dict
import shutil
import os
import dask
from dask.diagnostics import ProgressBar
import xarray as xr

from .config import *
from .utils import (
    log,
    ensure_empty_dir,
    scan_files,
    load_catalog,
    save_catalog,
    timestamp,
)
from .exceptions import ValidationError, ConversionError

# ──────────────── 内部小工具 ──────────────────────────────────
def _open_raw(path: Path, fmt: RawFormat) -> xr.Dataset:
    if fmt is RawFormat.GRIB:
        return xr.open_dataset(path, engine="cfgrib", chunks={}, backend_kwargs={"indexpath": ""})
    if fmt is RawFormat.NETCDF:
        return xr.open_dataset(path, engine="netcdf4", chunks={})
    if fmt is RawFormat.HDF:
        return xr.open_dataset(path, engine="h5netcdf", chunks={})
    raise ConversionError(f"不支持的输入格式 {fmt}")

def _auto_chunks(ds: xr.Dataset) -> dict:
    """简单 heuristic：time/step=1，lat≤180，lon≤360"""
    chunks = {}
    for dim, n in ds.sizes.items():
        if dim in {"time", "step"}:
            chunks[dim] = 1
        elif dim.lower().startswith("lat"):
            chunks[dim] = min(n, 180)
        elif dim.lower().startswith("lon"):
            chunks[dim] = min(n, 360)
    return chunks

def _write_zarr(ds: xr.Dataset, target: Path, consolidate: bool = True):
    t0 = time.time()
    ds.chunk(_auto_chunks(ds)).to_zarr(
        str(target),
        mode="w",
        compute=True,
        consolidated=consolidate,
        encoding={v: {"compressor": DEFAULT_COMPRESSOR} for v in ds.data_vars},
    )
    log.info("✅ 写入 %s (%.1fs)", target.name, time.time() - t0)


# ──────────────── catalog 写入统一函数 ────────────────────────
def _update_catalog(
    *,
    name: str,
    description: str,
    data_kind: DataKind,
    path: Path,
    spatial_res,
    temporal_res,
    coords: List[str],
    variables: List[str],
    org_mode: OrgMode,
    allow_update: bool,
    start_time: str | None = None, 
    end_time: str | None = None, 
):
    cat = load_catalog()
    cat[name] = {
        "name":         name,
        "kind":         data_kind.value,
        "format":       "zarr",
        "description":  description,
        "path":         str(path),
        "spatial_res":  spatial_res,
        "temporal_res": temporal_res,
        "start_time":   start_time,     
        "end_time":     end_time,      
        "coords":       coords,
        "variables":    variables,
        "org_mode":     org_mode.value,
        "created":      timestamp(),        # 北京时间 (+08:00)
        "allow_update": allow_update,
    }
    save_catalog(cat)
    log.info("📚 Catalog 已更新 → %s", name)

# ────────────────────────── 主入口 ───────────────────────────
def create_dataset(
    *,
    data_kind: DataKind,
    raw_format: RawFormat,
    description: str,
    src_paths: List[str | Path],
    dst_path: str | Path | None = None,
    org_mode: OrgMode,
    name: str,
    spatial_res: tuple[int, int, int] | None = None,
    temporal_res: str | None = None,
    allow_update: bool = False,
) -> Path:
    """
    • raw_format ∈ {GRIB, NETCDF, HDF}  → 转 Zarr
    • raw_format == ZARR                → 仅登记目录下所有 *.zarr（不复制）
    返回根目录 Path。
    """
    cat = load_catalog()
    # 1) 同名数据集已存在
    if name in cat:
        raise ValidationError(
            f"数据集名 {name!r} 已存在。\n"
            "如果想向该数据集追加，请使用 append_dataset()；"
            "如果想创建新集，请换一个 name。"
        )
    # 2) 目录被其他数据集占用
    for meta in cat.values():
        if Path(meta["path"]).resolve() == Path(dst_path or src_paths[0]).resolve():
            raise ValidationError(
                f"目录 {meta['path']} 已被数据集 {meta['name']} 使用。\n"
                "请指定一个新的 dst_path，或使用 append_dataset() 追加。"
            )
    # ────────────── ZARR: 只登记现成数据 ──────────────
    if raw_format is RawFormat.ZARR:
        root = Path(dst_path).resolve() if dst_path else Path(src_paths[0]).resolve()
        if not root.is_dir():
            raise ValidationError(f"{root} 不是有效目录")
        zarr_stores = sorted(root.glob("*.zarr"))
        if not zarr_stores:
            raise ValidationError(f"{root} 下未发现 *.zarr 目录")

        ds_all = xr.open_mfdataset(
            [str(p) for p in zarr_stores],
            engine="zarr",
            concat_dim="time",
            combine="nested",
            coords="minimal",
            parallel=True,
            backend_kwargs={"consolidated": False},
        )
        time_coord = "valid_time" if "valid_time" in ds_all.coords else (
                     "time"       if "time" in ds_all.coords else None)
        start_t = end_t = None
        if time_coord:
            times = ds_all[time_coord].values
            start_t = str(times.min())[:19]      # yyyy-mm-ddThh:mm:ss
            end_t   = str(times.max())[:19]
        _update_catalog(
            name=name,
            description=description,
            data_kind=data_kind,
            path=root,
            spatial_res=spatial_res,
            temporal_res=temporal_res,
            coords=list(ds_all.coords),
            variables=list(ds_all.data_vars),
            org_mode=org_mode,
            allow_update=allow_update,
            start_time=start_t,
            end_time=end_t,
        )
        log.info("✅ 已登记 Zarr 数据集 %s (共 %d 个 .zarr)", name, len(zarr_stores))
        return root

    # ────────────── GRIB/NetCDF/HDF ➜ Zarr 转换 ──────────────
    dst_path = Path(dst_path) if dst_path else Path(f"./{name}_zarr")
    ensure_empty_dir(dst_path)

    log.info("扫描原始文件…")
    files = list(scan_files([Path(p) for p in src_paths]))
    if not files:
        raise ValidationError("未找到任何原始文件")

    # 解析文件名 → 以 10 位起报时 (YYYYMMDDHH) 分组
    parsed: Dict[str, dict] = {}
    for f in files:
        m = FILENAME_RE.match(f.name)
        if not m:
            log.warning("跳过不符合命名规则: %s", f)
            continue
        gd = m.groupdict()
        cycle_key = gd["datetime"]             # YYYYMMDDHH
        parsed.setdefault(cycle_key, {}).setdefault(gd["var"] or "var", []).append(
            dict(path=f, step=gd.get("step"))
        )
    all_cycles = sorted(parsed.keys())     # ['2024070100', '2024070200', ...] 或 ['20240701', '20240702', ...]
    start_cycle = all_cycles[0]
    end_cycle   = all_cycles[-1]
    if not parsed:
        raise ValidationError("没有文件符合命名规范")

    tasks = []
    for cycle, var_map in parsed.items():
        pieces_each_var = []
        for var, lst in var_map.items():
            subpieces = []
            for item in sorted(lst, key=lambda x: x["step"] or "000"):
                ds = _open_raw(item["path"], raw_format)
                if data_kind is DataKind.FORECAST:
                    ds = ds.expand_dims(step=[int(item["step"] or 0)])
                subpieces.append(ds)
            concat_dim = "step" if data_kind is DataKind.FORECAST else "time"
            pieces_each_var.append(xr.concat(subpieces, dim=concat_dim))
        ds_cycle = xr.merge(pieces_each_var)

        tgt = dst_path / (f"{cycle}.zarr" if allow_update else ".")
        if tgt.suffix != ".zarr":
            tgt = dst_path
        else:
            tgt.mkdir(parents=True, exist_ok=True)

        tasks.append(dask.delayed(_write_zarr)(ds_cycle, tgt, consolidate=not allow_update))

    with ProgressBar():
        dask.compute(*tasks)

    _update_catalog(
        name=name,
        description=description,
        data_kind=data_kind,
        path=dst_path,
        spatial_res=spatial_res,
        temporal_res=temporal_res,
        coords=list(ds_cycle.coords),
        variables=list(ds_cycle.data_vars),
        org_mode=org_mode,
        allow_update=allow_update,
        start_time=start_cycle,      # ← 新增
        end_time=end_cycle,          # ← 新增
    )
    log.info("✅ 数据集 %s 创建完成 @ %s", name, dst_path)
    return dst_path

# ──────────────────────── 追加函数 ────────────────────────
def append_dataset(name: str, new_files: List[str | Path]):
    meta = load_catalog().get(name)
    if not meta or not meta["allow_update"]:
        raise ValidationError(f"数据集 {name} 不存在或未开启可追加模式")

    dst = Path(meta["path"])
    fmt = (
        RawFormat.ZARR
        if all(Path(p).suffix == ".zarr" for p in new_files)
        else RawFormat.GRIB
        if any(str(p).lower().endswith(".grb") for p in new_files)
        else RawFormat.NETCDF
    )

    create_dataset(
        data_kind=DataKind(meta["kind"]),
        raw_format=fmt,
        description=meta["description"],
        src_paths=new_files,
        dst_path=dst,
        org_mode=OrgMode(meta["org_mode"]),
        name=name,
        allow_update=True,
    )
def delete_dataset(name: str, *, remove_files: bool = False):
    """
    删除 catalog 中的某个数据集。
    参数
    -----
    name          数据集注册名
    remove_files  True ⇒ 同时删除 catalog[path] 所指 Zarr 目录
    """
    cat = load_catalog()
    meta = cat.pop(name, None)
    if meta is None:
        raise ValidationError(f"数据集 {name} 不存在")

    # 1) 删除磁盘数据（可选）
    if remove_files:
        p = Path(meta["path"])
        if p.exists():
            log.info("🗑️  正在删除磁盘数据 %s …", p)
            # 既可能是单个 .zarr，也可能是一个目录包含多 .zarr
            if p.is_dir():
                shutil.rmtree(p)
            else:
                os.remove(p)

    # 2) 写回 catalog
    save_catalog(cat)
    log.info("✅ 已删除数据集 %s（remove_files=%s）", name, remove_files)