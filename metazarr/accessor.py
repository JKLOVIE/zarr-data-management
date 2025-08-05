"""
功能二-2/3：打开、检索、裁剪、导出
"""
from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Sequence, Tuple, Any

import xarray as xr
import numpy as np
import pandas as pd
import json

from .config   import OutputFormat
from .catalog  import show_dataset_info
from .exceptions import RangeError

# ────────────────────────────────────────────────────────────────────────
def _collect_zarr_stores(root: Path) -> List[str]:
    if root.suffix == ".zarr":
        return [str(root)]
    stores = sorted(str(p) for p in root.glob("*.zarr"))
    if not stores:
        raise FileNotFoundError(f"{root} 下未发现 *.zarr")
    return stores


def open_dataset(name: str) -> "MZDataset":
    info = show_dataset_info(name)
    if not info:
        raise FileNotFoundError(f"数据集 {name!r} 未注册")

    root   = Path(info["path"])
    stores = _collect_zarr_stores(root)

    ds = xr.open_mfdataset(
        stores,
        engine="zarr",
        concat_dim="valid_time",
        combine="nested",
        coords="minimal",
        parallel=True,
        consolidated=True,
        chunks={},
        backend_kwargs={"consolidated": False},
    )

    return MZDataset(ds, info)

# ────────────────────────────────────────────────────────────────────────
class MZDataset:
    def __init__(self, ds: xr.Dataset, meta: Dict):
        self._ds  = ds
        self.meta = meta

    # ---------- 核心：统一导出 JSON ----------
    def to_json(
        self, *,
        vars: Sequence[str] | None = None,
        time=None, lat=None, lon=None, level=None, step=None,
        orient: str = "records",
        max_points: int = 1_000_000,
        squeeze: bool = True,
    ) -> Dict[str, Any]:

        orient = orient.lower()

        # ―― 先做裁剪 ―――――――――――――――――――――――――――――――――――
        ds_sub = self.subset(vars=vars, time=time, lat=lat,
                             lon=lon, level=level, step=step)

        # ----- 1) ndarray --------------------------------------------------
        if orient == "ndarray":
            if vars and len(vars) == 1:
                return _da_to_ndarray_json(ds_sub[vars[0]], squeeze)
            else:
                out = {}
                for v in (vars or ds_sub.data_vars):
                    out[v] = _da_to_ndarray_json(ds_sub[v], squeeze)
                return out

        # ----- 2) records / split -----------------------------------------
        df = ds_sub.to_dataframe().reset_index()
        if len(df) > max_points:
            raise ValueError(f"返回 {len(df)} 行，超过上限 {max_points}")

        return json.loads(df.to_json(orient=orient, date_unit="s"))

    # ---------- ndarray helper ----------
    def subset_ndarray(self, *, var, **kw):
        """保留兼容的旧 API（单变量）"""
        return self.to_json(vars=[var], orient="ndarray", **kw)

    # ---------- 数据选择 ----------
    def subset(
        self,
        *, vars: Sequence[str] | None = None,
        time=None, lat=None, lon=None, level=None, step=None,
    ) -> xr.Dataset:

        ds = self._ds if not vars else self._ds[vars]

        def _sel(coord, rng):
            if rng is None:
                return
            if isinstance(rng, (list, tuple)) and len(rng) == 2 and rng[0] == rng[1]:
                return {coord: rng[0]}
            return {coord: slice(rng[0], rng[-1])}

        for coord, rng in zip(
            ["valid_time", "latitude", "longitude", "pressure_level", "step"],
            [time, lat, lon, level, step],
        ):
            sel = _sel(coord, rng)
            if not sel:
                continue
            sel_val = sel[coord]
            if not isinstance(sel_val, slice) and sel_val not in ds[coord]:
                raise RangeError(f"{coord}={sel_val} 超出范围")
            ds = ds.sel(**sel)

        return ds

    # ---------- 导出磁盘 ----------
    def to(self, ds: xr.Dataset, fmt: OutputFormat, out_path: str | Path):
        out_path = Path(out_path)
        if fmt is OutputFormat.ZARR:
            ds.to_zarr(out_path, mode="w", consolidated=True)
        elif fmt is OutputFormat.NETCDF:
            ds.to_netcdf(out_path, mode="w")
        elif fmt is OutputFormat.HDF:
            ds.to_netcdf(out_path, engine="h5netcdf", mode="w")
        else:
            raise ValueError(fmt)

# ======================================================================
#                         —— 内部工具函数 ——
# ======================================================================
def _da_to_ndarray_json(da: xr.DataArray, squeeze: bool = True) -> Dict:
    if squeeze:
        da = da.squeeze()

    coords_json = {}
    for c in da.coords:
        arr = da[c].values
        if np.issubdtype(arr.dtype, np.datetime64):
            coords_json[c] = arr.astype("datetime64[ms]").astype(str).tolist()
        elif c in {"time", "valid_time"} and np.issubdtype(arr.dtype, np.integer):
            coords_json[c] = arr.view("datetime64[ns]").astype("datetime64[ms]").astype(str).tolist()
        else:
            coords_json[c] = arr.tolist()

    return {
        "dims":   list(da.dims),
        "coords": coords_json,
        "data":   da.values.tolist(),
        "attrs":  dict(da.attrs),
    }
