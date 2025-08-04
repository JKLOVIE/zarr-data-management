# tests/test_query.py
from metazarr import open_dataset
from pathlib import Path
from metazarr.config import OutputFormat
mz = open_dataset("zarr_ready_test")
print("✅ 打开成功，整体尺寸:", mz._ds.dims)

# —— 裁剪：时间 + 区域 + 气压层 ——
ds_sub = mz.subset(
    time=("2020-11-02", "2020-11-03"),   # 一整天
    lat=(30, 20),                       # 注意：支持反向 slice
    lon=(130, 150),
    level=(1000, 1000),                 # 单层 = 线裁剪
)

print("✂️  裁剪后尺寸:", ds_sub['t'].values)

# —— 导出 NetCDF ——
#out_nc = Path("slice_20201102_30N20N_130E150E.nc")
#mz.to(ds_sub, fmt=OutputFormat.NETCDF, out_path=out_nc)
#print("📤 已导出:", out_nc.resolve())

# —— 快速可视化 —— 画第一个变量第一个时间片 ——
#mz.quick_plot(var=list(ds_sub.data_vars)[0])