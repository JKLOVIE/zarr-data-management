from metazarr import list_datasets, show_dataset_info
print("📜 当前 catalog：", list_datasets())      # ① 先打印所有名字

name = "zarr_ready_test"                              # ② 确认与上行输出一致
info = show_dataset_info(name)
if info is None:
    print(f"❌ catalog 中找不到 {name}")
else:
    for k, v in info.items():
        print(f"{k:<12}: {v}")
