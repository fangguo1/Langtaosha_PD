#!/usr/bin/env python3
"""按 local_data 三个目录顺序回填入库。

会分别调用 3 次 `scripts/backfill_biorxiv_history.py`：
1) local_data/biorxiv_daily        -> source_name=biorxiv_daily
2) local_data/biorxiv_history/records -> source_name=biorxiv_history
3) local_data/langtaosha           -> source_name=langtaosha

用法：
    python scripts/backfill_local_data_all.py

    python scripts/backfill_local_data_all.py \
        --config-path src/config/config_tecent_backend_server_mimic.yaml \
        --stage all \
        --limit 100 \
        --max-retries 5
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List


PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKFILL_SCRIPT = PROJECT_ROOT / "scripts" / "backfill_biorxiv_history.py"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_use.yaml"


@dataclass(frozen=True)
class BackfillTarget:
    name: str
    records_root: Path
    source_name: str


TARGETS: List[BackfillTarget] = [
    BackfillTarget(
        name="biorxiv_daily",
        records_root=PROJECT_ROOT / "local_data" / "biorxiv_daily"/"2026",
        source_name="biorxiv_daily",
    ),
    BackfillTarget(
        name="langtaosha",
        records_root=PROJECT_ROOT / "local_data" / "langtaosha"/"raw",
        source_name="langtaosha",
    ),
    BackfillTarget(
        name="biorxiv_history",
        records_root=PROJECT_ROOT / "local_data" / "biorxiv_history" / "records",
        source_name="biorxiv_history",
    ),
   
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按三个 local_data 目录依次调用 backfill_biorxiv_history.py 完成入库"
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="配置文件路径",
    )
    parser.add_argument(
        "--stage",
        choices=["all", "pg", "vector"],
        default="all",
        help="执行阶段：all/pg/vector",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="向量回填每批数量",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="单条最大重试次数",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="每个目录最大处理记录数（联调用）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只统计不写入",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="显示详细日志和进度条",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="某个目录失败时继续执行后续目录（默认失败即停止）",
    )
    parser.add_argument(
        "--history-start-year",
        type=int,
        default=2026,
        help="biorxiv_history 从该年份开始向前处理（默认 2026）",
    )
    return parser.parse_args()


def build_cmd(args: argparse.Namespace, target: BackfillTarget) -> List[str]:
    cmd = [
        sys.executable,
        str(BACKFILL_SCRIPT),
        "--config-path",
        str(args.config_path),
        "--records-root",
        str(target.records_root),
        "--source-name",
        target.source_name,
        "--stage",
        args.stage,
        "--limit",
        str(args.limit),
        "--max-retries",
        str(args.max_retries),
        "--log-level",
        args.log_level,
    ]
    if args.max_records is not None:
        cmd.extend(["--max-records", str(args.max_records)])
    if args.dry_run:
        cmd.append("--dry-run")
    if args.verbose:
        cmd.append("--verbose")
    return cmd


def main() -> int:
    args = parse_args()

    if not BACKFILL_SCRIPT.exists():
        print(f"❌ 脚本不存在: {BACKFILL_SCRIPT}")
        return 1
    if not args.config_path.exists():
        print(f"❌ 配置文件不存在: {args.config_path}")
        return 1

    print("=" * 72)
    print("Local Data Backfill Runner")
    print("=" * 72)
    print(f"config_path: {args.config_path}")
    print(f"stage      : {args.stage}")
    print(f"targets    : {[t.name for t in TARGETS]}")
    print("-" * 72)

    failed: List[str] = []
    for idx, target in enumerate(TARGETS, start=1):
        # biorxiv_history 特殊处理：按年份倒序执行，从 history_start_year 开始往前走
        if target.name == "biorxiv_history":
            print(f"\n[{idx}/{len(TARGETS)}] 开始处理: {target.name} (按年份倒序)")
            print(f"records_root: {target.records_root}")
            print(f"source_name : {target.source_name}")
            print(f"start_year  : {args.history_start_year}")

            if args.stage in ("all", "pg") and not target.records_root.exists():
                print(f"❌ 目录不存在: {target.records_root}")
                failed.append(target.name)
                if not args.continue_on_error:
                    break
                continue

            year_dirs = []
            for child in target.records_root.iterdir():
                if child.is_dir() and child.name.isdigit():
                    year = int(child.name)
                    if year <= args.history_start_year:
                        year_dirs.append((year, child))
            year_dirs.sort(key=lambda x: x[0], reverse=True)

            if not year_dirs:
                print("❌ 未找到可处理的年份目录")
                failed.append(target.name)
                if not args.continue_on_error:
                    break
                continue

            for year, year_dir in year_dirs:
                print(f"\n  -> 年份 {year}: {year_dir}")
                per_year_target = BackfillTarget(
                    name=f"{target.name}_{year}",
                    records_root=year_dir,
                    source_name=target.source_name,
                )
                cmd = build_cmd(args, per_year_target)
                print("执行命令：")
                print(" ".join(cmd))
                print("-" * 72)

                completed = subprocess.run(cmd, cwd=PROJECT_ROOT)
                if completed.returncode != 0:
                    print(f"❌ 处理失败: {target.name} 年份 {year} (exit={completed.returncode})")
                    failed.append(f"{target.name}:{year}")
                    if not args.continue_on_error:
                        break
                else:
                    print(f"✅ 处理完成: {target.name} 年份 {year}")

            if failed and not args.continue_on_error:
                break
            continue

        # 其他目录按单次执行
        print(f"\n[{idx}/{len(TARGETS)}] 开始处理: {target.name}")
        print(f"records_root: {target.records_root}")
        print(f"source_name : {target.source_name}")

        if args.stage in ("all", "pg") and not target.records_root.exists():
            print(f"❌ 目录不存在: {target.records_root}")
            failed.append(target.name)
            if not args.continue_on_error:
                break
            continue

        cmd = build_cmd(args, target)
        print("执行命令：")
        print(" ".join(cmd))
        print("-" * 72)

        completed = subprocess.run(cmd, cwd=PROJECT_ROOT)
        if completed.returncode != 0:
            print(f"❌ 处理失败: {target.name} (exit={completed.returncode})")
            failed.append(target.name)
            if not args.continue_on_error:
                break
        else:
            print(f"✅ 处理完成: {target.name}")

    print("\n" + "=" * 72)
    if failed:
        print(f"完成，但有失败目录: {failed}")
        print("=" * 72)
        return 1
    print("全部目录处理完成")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
