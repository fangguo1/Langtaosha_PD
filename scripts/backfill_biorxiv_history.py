#!/usr/bin/env python3
"""bioRxiv history 回填脚本（阶段 5/6）

两阶段流程：
1) PG 入库（insert-only）: 遍历 local_data/biorxiv_history/records 下 JSONL，调用 index_dict。
2) 向量回填: 从 embedding_status 中拉取 pending/failed，执行向量写入并更新状态。

示例：
    # 阶段 6：先跑 10 条做联调
    python scripts/backfill_biorxiv_history.py \
        --config-path src/config/config_tecent_backend_server_mimic.yaml \
        --max-records 10 \
        --limit 10

    # 只做向量回填（失败 + pending），每批 100
    python scripts/backfill_biorxiv_history.py \
        --stage vector \
        --limit 100 \
        --max-retries 5
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sqlalchemy import text

from src.docset_hub.indexing import PaperIndexer

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm 不可用时降级
    tqdm = None


def _resolve_default_records_root() -> Path:
    candidates = [
        #PROJECT_ROOT / "local_data" / "biorxiv_history" / "records",
        PROJECT_ROOT / "mimic_data" / "biorxiv_daily" / "2026",
    ]
    for root in candidates:
        if not root.exists():
            continue
        jsonl_files = list(root.glob("**/*.jsonl"))
        if any(file_path.stat().st_size > 0 for file_path in jsonl_files):
            return root
    return candidates[0]


def _iter_jsonl_files(records_root: Path) -> list[Path]:
    if not records_root.exists():
        raise FileNotFoundError(f"records 目录不存在: {records_root}")

    files = sorted(records_root.glob("**/*.jsonl"))
    if not files:
        raise FileNotFoundError(f"未找到 JSONL 文件: {records_root}")
    return files


def _iter_json_lines(file_path: Path) -> Iterator[tuple[int, Dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)


def _count_nonempty_lines(file_path: Path) -> int:
    count = 0
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _build_text_from_paper_info(paper_info: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    """按 indexer 规则构造向量文本。"""
    canonical_title = paper_info.get("canonical_title")
    canonical_abstract = paper_info.get("canonical_abstract")

    title = canonical_title
    abstract = canonical_abstract

    if not title and not abstract:
        # 回退到当前 source 的 title/abstract
        for source in paper_info.get("sources", []):
            if source.get("source_name") == source_name:
                title = source.get("title")
                abstract = source.get("abstract")
                break

    if title and abstract:
        return {"should_vectorize": True, "text": f"{title}\n{abstract}", "text_type": "abstract"}
    if title:
        return {"should_vectorize": True, "text": title, "text_type": "title"}

    return {"should_vectorize": False, "text": "", "text_type": ""}


def _phase_pg_backfill(
    indexer: PaperIndexer,
    records_root: Path,
    source_name: str,
    max_records: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    files = _iter_jsonl_files(records_root)
    files_progress = None

    stats: Dict[str, Any] = {
        "files": len(files),
        "empty_files": 0,
        "total": 0,
        "success": 0,
        "failed": 0,
        "status_code_counts": Counter(),
        "vector_skipped": 0,
        "queued_pending": 0,
        "queue_skipped_no_text": 0,
        "errors": [],
        "elapsed_sec": 0.0,
    }

    start = time.time()
    if tqdm is not None:
        files_progress = tqdm(files, total=len(files), desc="Phase A files", unit="file")
    else:
        files_progress = files

    for file_path in files_progress:
        if file_path.stat().st_size == 0:
            stats["empty_files"] += 1
            logging.warning("跳过空文件: %s", file_path)
            continue

        logging.info("处理文件: %s", file_path)

        try:
            line_total = _count_nonempty_lines(file_path)
            line_progress = None
            line_iter = _iter_json_lines(file_path)
            if tqdm is not None:
                line_progress = tqdm(
                    line_iter,
                    total=line_total,
                    desc=f"  {file_path.name}",
                    unit="rec",
                    leave=False,
                )
                iter_obj = line_progress
            else:
                iter_obj = line_iter

            for line_no, record in iter_obj:
                if max_records is not None and stats["total"] >= max_records:
                    if line_progress is not None:
                        line_progress.close()
                    stats["elapsed_sec"] = time.time() - start
                    return stats

                stats["total"] += 1

                if dry_run:
                    # dry-run 只做读取和 JSON 解析统计
                    continue

                result = indexer.index_dict(
                    raw_payload=record,
                    source_name=source_name,
                    mode="insert",
                )

                if result.get("success"):
                    stats["success"] += 1
                    metadata = result.get("metadata") or {}
                    status_code = metadata.get("status_code") or "UNKNOWN"
                    stats["status_code_counts"][status_code] += 1

                    vectorization = result.get("vectorization") or {}
                    if vectorization.get("skipped"):
                        stats["vector_skipped"] += 1

                    # 阶段 A 需要显式写入 pending 队列，供阶段 B 回填
                    should_queue = _should_queue_for_backfill(
                        status_code=status_code,
                        canonical_changed=bool(metadata.get("canonical_changed", False)),
                        canonical_source_name=metadata.get("canonical_source_name"),
                        resolved_source_name=source_name,
                    )
                    if should_queue and not dry_run:
                        paper_id = metadata.get("paper_id")
                        work_id = metadata.get("work_id")
                        canonical_source_id = metadata.get("canonical_source_id")
                        queue_source_name = metadata.get("canonical_source_name") or source_name

                        if paper_id and work_id:
                            paper_info = indexer.metadata_db.read_paper(paper_id)
                            text_info = _build_text_from_paper_info(paper_info or {}, queue_source_name)
                            if text_info.get("should_vectorize"):
                                indexer.metadata_db.upsert_embedding_status_pending(
                                    paper_id=paper_id,
                                    work_id=work_id,
                                    canonical_source_id=canonical_source_id,
                                    source_name=queue_source_name,
                                    text_type=text_info.get("text_type", "abstract") or "abstract",
                                )
                                stats["queued_pending"] += 1
                            else:
                                stats["queue_skipped_no_text"] += 1
                else:
                    stats["failed"] += 1
                    stats["errors"].append(
                        {
                            "file": str(file_path),
                            "line": line_no,
                            "error": result.get("error", "index_dict failed"),
                        }
                    )
            if line_progress is not None:
                line_progress.close()
        except Exception as exc:
            stats["failed"] += 1
            stats["errors"].append(
                {
                    "file": str(file_path),
                    "line": 0,
                    "error": f"文件处理异常: {exc}",
                }
            )

    if tqdm is not None and files_progress is not None:
        files_progress.close()

    stats["elapsed_sec"] = time.time() - start
    return stats


def _should_queue_for_backfill(
    status_code: Optional[str],
    canonical_changed: bool,
    canonical_source_name: Optional[str],
    resolved_source_name: str,
) -> bool:
    """与 indexer 的 insert-only 触发规则保持一致。"""
    is_canonical_source = canonical_source_name == resolved_source_name
    if status_code == "INSERT_NEW_PAPER":
        return True
    if status_code == "INSERT_APPEND_SOURCE":
        return canonical_changed
    if status_code == "INSERT_UPDATE_SAME_SOURCE":
        return is_canonical_source
    return False


def _snapshot_candidates(
    indexer: PaperIndexer,
    source_name: Optional[str],
    limit: int,
) -> list[Dict[str, Any]]:
    """获取候选快照，避免处理过程中状态变更导致分页偏移。"""
    all_rows: list[Dict[str, Any]] = []
    offset = 0
    while True:
        rows = indexer.metadata_db.list_embedding_candidates(
            source_name=source_name,
            statuses=["pending", "failed"],
            limit=limit,
            offset=offset,
        )
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
    return all_rows


def _phase_vector_backfill(
    indexer: PaperIndexer,
    source_name: Optional[str],
    limit: int,
    max_retries: int,
    max_records: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "total_candidates": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped_max_retries": 0,
        "skipped_no_text": 0,
        "skipped_no_source": 0,
        "errors": [],
        "failure_buckets": Counter(),
        "batch_elapsed_sec": [],
        "elapsed_sec": 0.0,
    }

    started = time.time()
    batch_started = time.time()
    processed_in_batch = 0

    candidates = _snapshot_candidates(indexer=indexer, source_name=source_name, limit=limit)
    candidates_iter = candidates
    if tqdm is not None:
        candidates_iter = tqdm(candidates, total=len(candidates), desc="Phase B candidates", unit="doc")

    for candidate in candidates_iter:
        stats["total_candidates"] += 1

        if max_records is not None and stats["processed"] >= max_records:
            break

        paper_id = candidate.get("paper_id")
        work_id = candidate.get("work_id")
        candidate_source = candidate.get("source_name")
        attempts = int(candidate.get("attempt_count") or 0)

        if attempts >= max_retries:
            stats["skipped_max_retries"] += 1
            continue

        if not candidate_source and candidate.get("canonical_source_id") is not None:
            candidate_source = indexer.metadata_db.get_source_name_by_paper_source_id(
                candidate["canonical_source_id"]
            )

        if not candidate_source:
            stats["skipped_no_source"] += 1
            if not dry_run:
                indexer.metadata_db.mark_embedding_failed(
                    paper_id=paper_id,
                    error_message="skip vectorize: source_name is empty",
                )
            continue

        stats["processed"] += 1
        processed_in_batch += 1

        if dry_run:
            # dry-run 下不更新数据库状态。
            continue

        try:
            paper_info = indexer.metadata_db.read_paper(paper_id)
            if not paper_info:
                raise ValueError(f"paper_id={paper_id} 不存在")

            text_info = _build_text_from_paper_info(paper_info, candidate_source)
            if not text_info["should_vectorize"]:
                stats["skipped_no_text"] += 1
                indexer.metadata_db.mark_embedding_failed(
                    paper_id=paper_id,
                    error_message="skip vectorize: title and abstract are empty",
                )
                continue

            indexer.vector_db.add_document(
                source_name=candidate_source,
                work_id=work_id,
                text=text_info["text"],
                text_type=text_info["text_type"] or candidate.get("text_type") or "abstract",
                paper_id=str(paper_id) if paper_id is not None else None,
            )
            indexer.metadata_db.mark_embedding_succeeded(paper_id=paper_id)
            stats["succeeded"] += 1

        except Exception as exc:
            msg = str(exc)
            stats["failed"] += 1
            stats["errors"].append(
                {
                    "paper_id": paper_id,
                    "work_id": work_id,
                    "source_name": candidate_source,
                    "error": msg,
                }
            )
            stats["failure_buckets"][msg.split(":", 1)[0][:120]] += 1
            try:
                indexer.metadata_db.mark_embedding_failed(paper_id=paper_id, error_message=msg)
            except Exception as status_exc:
                logging.error("更新 failed 状态失败: paper_id=%s, error=%s", paper_id, status_exc)

        if processed_in_batch >= limit:
            elapsed = time.time() - batch_started
            stats["batch_elapsed_sec"].append(elapsed)
            logging.info("向量批次完成: batch_size=%s, elapsed=%.2fs", processed_in_batch, elapsed)
            batch_started = time.time()
            processed_in_batch = 0

    if processed_in_batch > 0:
        elapsed = time.time() - batch_started
        stats["batch_elapsed_sec"].append(elapsed)
        logging.info("向量批次完成: batch_size=%s, elapsed=%.2fs", processed_in_batch, elapsed)

    stats["elapsed_sec"] = time.time() - started
    return stats


def _assert_latest_schema(indexer: PaperIndexer) -> None:
    """校验最新 schema 关键表/字段，不做兼容兜底。"""
    required_columns = {
        "paper_sources": {
            "paper_id",
            "paper_source_id",
            "source_name",
            "source_record_id",
            "doi",
            "online_at",
            "version",
        },
        "embedding_status": {
            "paper_id",
            "work_id",
            "canonical_source_id",
            "source_name",
            "text_type",
            "status",
            "attempt_count",
            "last_error_message",
            "last_attempt_at",
            "last_success_at",
            "updated_at",
        },
    }

    with indexer.metadata_db.engine.connect() as conn:
        for table_name, required in required_columns.items():
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            ).fetchall()
            existing = {row[0] for row in rows}
            if not existing:
                raise RuntimeError(
                    f"数据库缺少表 `{table_name}`。请先执行最新 schema/migration。"
                )
            missing = sorted(required - existing)
            if missing:
                raise RuntimeError(
                    f"表 `{table_name}` 缺少字段: {missing}。请先执行最新 schema/migration。"
                )


def _print_summary(stage: str, pg_stats: Optional[Dict[str, Any]], vector_stats: Optional[Dict[str, Any]], dry_run: bool) -> None:
    print("\n" + "=" * 72)
    print(f"Backfill Summary (stage={stage}, dry_run={dry_run})")
    print("=" * 72)

    if pg_stats is not None:
        print("\n[Phase A] PG Insert-only")
        print(f"  files              : {pg_stats['files']}")
        print(f"  empty_files        : {pg_stats['empty_files']}")
        print(f"  total              : {pg_stats['total']}")
        print(f"  success            : {pg_stats['success']}")
        print(f"  failed             : {pg_stats['failed']}")
        print(f"  vector_skipped     : {pg_stats['vector_skipped']}")
        print(f"  queued_pending     : {pg_stats['queued_pending']}")
        print(f"  queue_skip_no_text : {pg_stats['queue_skipped_no_text']}")
        print(f"  elapsed_sec        : {pg_stats['elapsed_sec']:.2f}")

        status_counts = pg_stats.get("status_code_counts") or {}
        if status_counts:
            print("  status_code_counts :")
            for key, value in sorted(status_counts.items(), key=lambda x: x[0]):
                print(f"    - {key}: {value}")

        if pg_stats.get("errors"):
            print("  errors(sample)     :")
            for row in pg_stats["errors"][:5]:
                print(f"    - {row}")

    if vector_stats is not None:
        print("\n[Phase B] Vector Backfill")
        print(f"  total_candidates   : {vector_stats['total_candidates']}")
        print(f"  processed          : {vector_stats['processed']}")
        print(f"  succeeded          : {vector_stats['succeeded']}")
        print(f"  failed             : {vector_stats['failed']}")
        print(f"  skipped_max_retries: {vector_stats['skipped_max_retries']}")
        print(f"  skipped_no_text    : {vector_stats['skipped_no_text']}")
        print(f"  skipped_no_source  : {vector_stats['skipped_no_source']}")
        print(f"  elapsed_sec        : {vector_stats['elapsed_sec']:.2f}")

        if vector_stats.get("batch_elapsed_sec"):
            batches = vector_stats["batch_elapsed_sec"]
            avg_batch = sum(batches) / len(batches)
            print(f"  batch_count        : {len(batches)}")
            print(f"  avg_batch_sec      : {avg_batch:.2f}")

        failure_buckets = vector_stats.get("failure_buckets") or {}
        if failure_buckets:
            print("  failure_buckets    :")
            for key, value in failure_buckets.most_common(8):
                print(f"    - {key}: {value}")

        if vector_stats.get("errors"):
            print("  errors(sample)     :")
            for row in vector_stats["errors"][:5]:
                print(f"    - {row}")

    print("=" * 72)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="bioRxiv history 两阶段 backfill（阶段 5/6）",
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="src/config/config_tecent_backend_server_mimic.yaml",
        help="配置文件路径",
    )
    parser.add_argument(
        "--records-root",
        type=str,
        default=str(_resolve_default_records_root()),
        help="bioRxiv history records 根目录（默认自动选择有数据目录）",
    )
    parser.add_argument(
        "--source-name",
        type=str,
        default="biorxiv_history",
        help="source_name 过滤（默认 biorxiv_history）",
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
        help="每批大小（向量回填分页批次）",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="单条最大重试次数（attempt_count >= max_retries 将跳过）",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="最大处理记录数（用于阶段6小样本，例如 10 或 100）",
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
        help="显示中间处理过程（详细日志 + tqdm 进度条）",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    effective_log_level = args.log_level
    if args.verbose and args.log_level == "WARNING":
        effective_log_level = "INFO"

    logging.basicConfig(
        level=getattr(logging, effective_log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config_path = Path(args.config_path)
    records_root = Path(args.records_root)

    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return 1

    if args.stage in ("all", "pg") and not records_root.exists():
        print(f"❌ records 目录不存在: {records_root}")
        return 1

    if args.limit <= 0:
        print("❌ --limit 必须 > 0")
        return 1

    if args.max_retries <= 0:
        print("❌ --max-retries 必须 > 0")
        return 1

    try:
        # 阶段 A 只需要 metadata 写入，禁用在线向量化；阶段 B 使用 vector_db。
        pg_indexer = PaperIndexer(config_path=config_path, enable_vectorization=False)
        vector_indexer = PaperIndexer(config_path=config_path, enable_vectorization=True)
        _assert_latest_schema(pg_indexer)

        pg_stats: Optional[Dict[str, Any]] = None
        vector_stats: Optional[Dict[str, Any]] = None

        if args.stage in ("all", "pg"):
            logging.info("开始阶段 A（PG insert-only）")
            pg_stats = _phase_pg_backfill(
                indexer=pg_indexer,
                records_root=records_root,
                source_name=args.source_name,
                max_records=args.max_records,
                dry_run=args.dry_run,
            )

        if args.stage in ("all", "vector"):
            logging.info("开始阶段 B（vector backfill）")
            vector_stats = _phase_vector_backfill(
                indexer=vector_indexer,
                source_name=args.source_name,
                limit=args.limit,
                max_retries=args.max_retries,
                max_records=args.max_records,
                dry_run=args.dry_run,
            )

        _print_summary(args.stage, pg_stats, vector_stats, args.dry_run)

        has_failures = False
        if pg_stats and pg_stats.get("failed", 0) > 0:
            has_failures = True
        if vector_stats and vector_stats.get("failed", 0) > 0:
            has_failures = True

        return 1 if has_failures else 0

    except Exception as exc:
        logging.exception("backfill 执行失败: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
