# quick_test.py  --放在项目根随便跑
import xarray as xr, numpy as np, pandas as pd, pathlib, os
from metazarr import create_dataset, open_dataset
from metazarr.config import DataKind, RawFormat, OrgMode


raw_dir = pathlib.Path("raw_nc"); raw_dir.mkdir(exist_ok=True)

time = pd.date_range("2025-07-24T00", periods=1, freq="h")   # 修正
ds = xr.Dataset(
    {"T": (("time", "lat", "lon"), np.random.rand(1, 3, 3))},
    coords={
        "time": time,
        "lat": [0, 1.5, 3.0],
        "lon": [10, 20, 30],
    },
)
ds.to_netcdf(raw_dir / "T2025072400.nc")

create_dataset(
    data_kind    = DataKind.NON_FORECAST,
    raw_format   = RawFormat.NETCDF,
    description  = "dummy test",
    src_paths    = [raw_dir],
    dst_path     = "zarr_out",
    org_mode     = OrgMode.ALLIN1,
    name         = "dummy",
    spatial_res  = (3,3,1),
    temporal_res = "1H",
)

mz = open_dataset("dummy")
print(mz._ds)              # 看一下数据结构
mz.quick_plot("T")         # 弹一张图
