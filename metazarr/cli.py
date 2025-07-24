import argparse, sys, json, pathlib
from .creator import create_dataset
from .catalog import list_datasets, show_dataset_info
from .config import DataKind, RawFormat, OrgMode

def main():
    parser = argparse.ArgumentParser(description="metazarr command‑line interface")
    sub = parser.add_subparsers(dest="cmd")

    # create
    p_create = sub.add_parser("create", help="创建 Zarr 数据集")
    p_create.add_argument("--kind", required=True, choices=list(DataKind))
    p_create.add_argument("--format", required=True, choices=list(RawFormat))
    p_create.add_argument("--desc", required=True)
    p_create.add_argument("--src", required=True, nargs="+")
    p_create.add_argument("--dst", required=True)
    p_create.add_argument("--org", required=True, choices=list(OrgMode))
    p_create.add_argument("--name", required=True)

    # list
    sub.add_parser("ls", help="列出数据集")

    # info
    p_info = sub.add_parser("info", help="数据集详情")
    p_info.add_argument("name")

    args = parser.parse_args()

    if args.cmd == "create":
        create_dataset(
            data_kind=DataKind(args.kind),
            raw_format=RawFormat(args.format),
            description=args.desc,
            src_paths=args.src,
            dst_path=args.dst,
            org_mode=OrgMode(args.org),
            name=args.name,
        )
    elif args.cmd == "ls":
        for n in list_datasets():
            print(n)
    elif args.cmd == "info":
        print(json.dumps(show_dataset_info(args.name), indent=2, ensure_ascii=False))
    else:
        parser.print_help()

if __name__ == "__main__":
    sys.exit(main())
