from metazarr.creator import delete_dataset

# 仅从 catalog 中移除
delete_dataset("zarr_ready_test", remove_files=False)

