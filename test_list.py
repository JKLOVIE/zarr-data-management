# test_list.py
from metazarr import list_datasets, show_dataset_info
print("👉 当前已注册的数据集：", list_datasets(), "\n")

info = show_dataset_info("clickhouse_demo")
print("clickhouse_demo 详情：")
for k, v in info.items():
    print(f"  {k:<12} : {v}")
