"""
功能二‑2/3：打开、检索、裁剪、导出
"""
from __future__ import annotations
from pathlib import Path
import xarray as xr
from typing import List, Dict, Sequence, Tuple
from .config import OutputFormat
from .catalog import show_dataset_info
from .exceptions import RangeError
import numpy as np
import json
import pandas as pd
def _collect_zarr_stores(root: Path) -> List[str]:
    """
    如果 root 本身就是 *.zarr → 直接返回 [root]；
    否则收集 root/ 下所有 *.zarr 子目录并排序成字符串列表。
    """
    if root.suffix == ".zarr":
        return [str(root)]
    stores = sorted(str(p) for p in root.glob("*.zarr"))
    if not stores:
        raise FileNotFoundError(f"{root} 下未发现任何 *.zarr 目录")
    return stores

def open_dataset(name: str) -> "MZDataset":
    info = show_dataset_info(name)
    if not info:
        raise FileNotFoundError(f"数据集 {name} 未注册")

    root = Path(info["path"])
    stores = _collect_zarr_stores(root)

    # ——— 关键：open_mfdataset 支持 engine="zarr" ————————————————
    ds = xr.open_mfdataset(
        stores,
        engine="zarr",
        concat_dim="valid_time",            # 按时间维拼接
        combine="nested",             # 保留各子集原坐标顺序
        coords="minimal",
        parallel=True,                # 多线程读取
        consolidated=True,
        chunks={},                    # 继续 lazy‑load
        backend_kwargs={
            "consolidated": False   # ← 关键！告诉 xarray 这些 store 无 .zmetadata
        },
    )

    return MZDataset(ds, info)

class MZDataset:
    def __init__(self, ds: xr.Dataset, meta: Dict):
        self._ds = ds
        self.meta = meta
    def subset_json(
        self, *,
        vars=None, time=None, lat=None, lon=None, level=None, step=None,
        orient="records", max_points: int = 1_000_000,
    ) -> dict:
        """
        返回 DataFrame-to-JSON 的结果（records / split …）。
        - orient     同 pandas DataFrame.to_json
        - max_points 限制返回行数，防止一次拉取过大
        """
        ds_sub = self.subset(vars=vars, time=time, lat=lat,
                             lon=lon, level=level, step=step)

        df = ds_sub.to_dataframe().reset_index()
        if len(df) > max_points:
            raise ValueError(f"查询结果 {len(df)} 行，超过上限 {max_points}")

        return json.loads(df.to_json(orient=orient, date_unit="s"))
    def subset_ndarray(
        self, *, var: str,
        time=None, lat=None, lon=None, level=None, step=None,
        squeeze: bool = True
    ) -> dict:
        ds_sub = self.subset(vars=[var], time=time, lat=lat,
                             lon=lon, level=level, step=step)
        da = ds_sub[var].load()
        if squeeze:
            da = da.squeeze()

        # ---- 关键：coord 转 list 时检查 dtype ----
        coords_json = {}
        for c in da.coords:
            arr = da[c].values
            if np.issubdtype(arr.dtype, np.datetime64):
                # 转成 ISO-8601 字符串列表
                coords_json[c] = [str(t) for t in arr.astype("datetime64[ns]")]
            else:
                coords_json[c] = arr.tolist()

        return {
            "dims":   list(da.dims),
            "coords": coords_json,
            "data":   da.values.tolist(),
            "attrs":  dict(da.attrs),
        }
    # ── 检索接口 ─────────────────────────────────────────────────────────────
    def subset(
        self,
        *,
        vars: Sequence[str] | None = None,
        time: Tuple[str, str] | Sequence[str] | None = None,
        lat: Tuple[float, float] | Sequence[float] | None = None,
        lon: Tuple[float, float] | Sequence[float] | None = None,
        level: Tuple[float, float] | Sequence[float] | None = None,
        step: Tuple[int, int] | Sequence[int] | None = None,
    ) -> xr.Dataset:
        ds = self._ds
        if vars:
            ds = ds[vars]

        # helper：把用户输入转换成 .sel() 可用的 dict
        def _sel(coord, rng):
            if rng is None:
                return
            if len(rng) == 2 and rng[0] == rng[1]:
                return {coord: rng[0]}          # 单点 / 线
            return {coord: slice(rng[0], rng[-1])}  # 区域

        for coord, rng in zip(
            ["valid_time", "latitude", "longitude", "pressure_level", "step"],
            [time,   lat,        lon,          level, step],
        ):
            sel = _sel(coord, rng)
            if not sel:
                continue

            sel_val = sel[coord]
            # 仅对“标量 / 列表”做范围校验；slice 交给 xarray 处理
            if not isinstance(sel_val, slice) and sel_val not in ds[coord]:
                raise RangeError(f"{coord} 值 {sel_val} 超出范围")

            ds = ds.sel(**sel)

        return ds


    # ── 导出 ────────────────────────────────────────────────────────────────
    def to(
        self,
        ds: xr.Dataset,
        fmt: OutputFormat,
        out_path: str | Path,
    ):
        out_path = Path(out_path)
        if fmt is OutputFormat.ZARR:
            ds.to_zarr(out_path, mode="w", consolidated=True)
        elif fmt is OutputFormat.NETCDF:
            ds.to_netcdf(out_path, mode="w")
        elif fmt is OutputFormat.HDF:
            ds.to_netcdf(out_path, engine="h5netcdf", mode="w")
        elif fmt is OutputFormat.GRIB:
            # 写 GRIB 需 eccodes; 这里只留接口
            from eccodes import (
                codes_grib_new_from_message, codes_set_values, codes_write, codes_release
            )
            raise NotImplementedError("GRIB 编码写入待实现")
        else:
            raise ValueError(fmt)

    # ── 可视化（简单示例）────────────────────────────────────────────────────
    def quick_plot(self, var: str, time_idx: int = 0, level_idx: int = 0):
        import matplotlib.pyplot as plt
        da = self._ds[var].isel(time=time_idx)
        if "level" in da.dims:
            da = da.isel(level=level_idx)
        da.plot()
        plt.title(f"{var} @ {str(da.time.values)}")
        plt.show()
