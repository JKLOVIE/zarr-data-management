from metazarr import create_dataset
from metazarr.config import DataKind, RawFormat, OrgMode

create_dataset(
    data_kind   = DataKind.NON_FORECAST,
    raw_format  = RawFormat.ZARR,                # ← 关键
    description = "NCEP 已整理好的日拼 Zarr",
    src_paths   = ["/var/lib/clickhouse/outputzarr"],            # 只给目录
    dst_path    = "",
    org_mode    = OrgMode.ALLIN1,
    spatial_res   = "0.25°×0.25°×37lev",
    temporal_res  = "3h",
    name        = "zarr_ready_test",
    allow_update=True,                           # 后续可追加同目录更多 .zarr
)
