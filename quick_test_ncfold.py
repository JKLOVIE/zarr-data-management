# quick_test_ncfold.py
from metazarr import create_dataset
from metazarr.config import DataKind, RawFormat, OrgMode

create_dataset(
    data_kind     = DataKind.NON_FORECAST,      # 普通历史 NetCDF
    raw_format    = RawFormat.NETCDF,
    description   = "3‑day demo from /testnc to /testzarr",
    src_paths     = ["/var/lib/clickhouse/testnc"],   # 会递归找 *.nc
    dst_path      = "/var/lib/clickhouse/testzarr",   # 目标根
    org_mode      = OrgMode.ALLIN1,                   # 这里无实际影响
    name          = "clickhouse_demo",
    allow_update  = True,                             # ⚠️ 关键：启用“按天分目录”
)
