from enum import Enum
import re
from pathlib import Path

# ─── 数据类型枚举 ────────────────────────────────────────────────────────────────
class DataKind(str, Enum):
    FORECAST = "forecast"
    NON_FORECAST = "non_forecast"

class RawFormat(str, Enum):
    GRIB = "grib"
    NETCDF = "netcdf"
    HDF = "hdf"
    ZARR = 'zarr'

class OrgMode(str, Enum):
    HOURLY  = "xhour"
    DAILY   = "day"
    WEEKLY  = "week"
    MONTHLY = "month"
    YEARLY  = "year"
    ALLIN1  = "all"

class OutputFormat(str, Enum):
    ZARR  = "zarr"
    GRIB  = "grib"
    NETCDF = "netcdf"
    HDF   = "hdf"

# ─── 文件名与文件夹名正则 ───────────────────────────────────────────────────────
FILENAME_RE = re.compile(
    r"^(?:(?P<var>[A-Za-z0-9]+))?"
    r"(?P<datetime>\d{8,10})"      # YYYYMMDDHH
    r"(?:\.(?P<step>\d{3}))?"    # .FFF 可选
    r"\.(?P<suffix>grb|nc|hdf)$",
    re.IGNORECASE,
)

FOLDERNAME_RE = re.compile(r"^\d{10}$")  # YYYYMMDDHH

# ─── 默认 Zarr 压缩器示例 ───────────────────────────────────────────────────────
import numcodecs
DEFAULT_COMPRESSOR = numcodecs.Blosc(cname="zstd", clevel=3, shuffle=2)

CATALOG_PATH = Path.home() / ".metazarr_catalog.json"