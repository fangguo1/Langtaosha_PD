#!/usr/bin/env python3
"""Backfill BM25 sparse collections from canonical metadata."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.docset_hub.storage.metadata_db import MetadataDB
from src.docset_hub.storage.vector_db import VectorDB
from src.docset_hub.storage.vector_db_client import VectorDBError

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional CLI progress only.
    tqdm = None


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_test.yaml"
DEFAULT_STATE_FILE = PROJECT_ROOT / "local_data" / "sparse_bm25_backfill_state.json"


'''
/home/wnlab/miniconda3/envs/langtaosha/bin/python scripts/backfill_sparse_collections.py \
  --config src/config/config_tecent_backend_server_mimic.yaml \
  --batch-size 300 \
  --resume \
  --state-file local_data/sparse_bm25_backfill_state_mimic.json

'''

@dataclass
class BackfillSummary:
    sources: List[str]
    fetched: int = 0
    indexed: int = 0
    skipped: int = 0
    failed: int = 0
    dry_run: bool = False
    by_source: Dict[str, Dict[str, int]] = field(default_factory=dict)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Tencent VectorDB BM25 sparse collections.")
    parser.add_argument("--config", "--config-path", dest="config_path", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--sources", type=str, default=None, help="Comma-separated sources; defaults to vector_db.allowed_sources")
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument("--limit", type=int, default=None, help="Maximum papers per source")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-progress", action="store_true", help="Disable tqdm progress bars")
    parser.add_argument("--summary-json", type=Path, default=None, help="Optional machine-readable summary JSON path")
    return parser.parse_args(argv)


def parse_sources(raw_sources: Optional[str]) -> Optional[List[str]]:
    if not raw_sources:
        return None
    return [source.strip() for source in raw_sources.split(",") if source.strip()]


def build_index_text(title: Optional[str], abstract: Optional[str]) -> Dict[str, Any]:
    title = (title or "").strip()
    abstract = (abstract or "").strip()
    if title and abstract:
        return {"should_index": True, "text": f"{title}\n{abstract}", "text_type": "abstract"}
    if title:
        return {"should_index": True, "text": title, "text_type": "title"}
    return {"should_index": False, "text": "", "text_type": ""}


def fetch_candidate_papers(
    metadata_db: MetadataDB,
    source_name: str,
    limit: int,
    after_paper_id: int = 0,
) -> List[Dict[str, Any]]:
    """Fetch canonical papers for a source, ordered by paper_id."""
    with metadata_db.engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    p.paper_id,
                    p.work_id,
                    p.canonical_title,
                    p.canonical_abstract,
                    ps.source_name
                FROM papers p
                JOIN paper_sources ps
                  ON ps.paper_source_id = p.canonical_source_id
                WHERE ps.source_name = :source_name
                  AND p.paper_id > :after_paper_id
                  AND p.work_id IS NOT NULL
                  AND (
                    NULLIF(BTRIM(COALESCE(p.canonical_title, '')), '') IS NOT NULL
                    OR NULLIF(BTRIM(COALESCE(p.canonical_abstract, '')), '') IS NOT NULL
                  )
                ORDER BY p.paper_id ASC
                LIMIT :limit
                """
            ),
            {
                "source_name": source_name,
                "after_paper_id": after_paper_id,
                "limit": limit,
            },
        ).fetchall()

    return [
        {
            "paper_id": row[0],
            "work_id": row[1],
            "canonical_title": row[2],
            "canonical_abstract": row[3],
            "source_name": row[4],
        }
        for row in rows
    ]


def build_sparse_documents(candidates: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    documents: List[Dict[str, Any]] = []
    skipped = 0
    for candidate in candidates:
        text_info = build_index_text(
            candidate.get("canonical_title"),
            candidate.get("canonical_abstract"),
        )
        if not text_info["should_index"]:
            skipped += 1
            continue
        documents.append(
            {
                "work_id": candidate["work_id"],
                "paper_id": candidate.get("paper_id"),
                "text": text_info["text"],
                "text_type": text_info["text_type"],
            }
        )
    return {"documents": documents, "skipped": skipped}


def load_state(state_file: Path) -> Dict[str, Any]:
    if not state_file.exists():
        return {}
    with open(state_file, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_file: Path, state: Dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=state_file.parent,
            prefix=f".{state_file.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, state_file)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def _progress_enabled(args: argparse.Namespace) -> bool:
    return tqdm is not None and not bool(getattr(args, "no_progress", False))


def _set_progress_postfix(progress: Any, **values: Any) -> None:
    if progress is not None and hasattr(progress, "set_postfix"):
        progress.set_postfix(**values)


def run_backfill(
    args: argparse.Namespace,
    metadata_db: Optional[MetadataDB] = None,
    vector_db: Optional[VectorDB] = None,
    fetcher: Callable[[MetadataDB, str, int, int], List[Dict[str, Any]]] = fetch_candidate_papers,
) -> BackfillSummary:
    metadata_db = metadata_db or MetadataDB(config_path=args.config_path)
    vector_db = vector_db or VectorDB(config_path=args.config_path)
    sources = parse_sources(args.sources) or list(vector_db.allowed_sources)
    state = load_state(args.state_file) if args.resume else {}
    summary = BackfillSummary(
        sources=sources,
        dry_run=args.dry_run,
    )
    show_progress = _progress_enabled(args)

    source_iter = sources
    if show_progress:
        source_iter = tqdm(
            sources,
            total=len(sources),
            desc="Sparse sources",
            unit="source",
            dynamic_ncols=True,
        )

    for source_name in source_iter:
        vector_db._validate_source(source_name)
        if not args.dry_run:
            vector_db.ensure_sparse_collection(source_name)

        remaining = args.limit
        after_paper_id = int((state.get(source_name) or {}).get("last_paper_id", 0)) if args.resume else 0
        source_progress = None
        source_fetched = 0
        source_indexed = 0
        source_skipped = 0

        if show_progress:
            source_progress = tqdm(
                total=args.limit,
                desc=f"sparse:{source_name}",
                unit="paper",
                dynamic_ncols=True,
                leave=False,
            )

        while True:
            batch_limit = args.batch_size if remaining is None else min(args.batch_size, remaining)
            if batch_limit <= 0:
                break

            candidates = fetcher(metadata_db, source_name, batch_limit, after_paper_id)
            if not candidates:
                break

            summary.fetched += len(candidates)
            batch = build_sparse_documents(candidates)
            documents = batch["documents"]
            summary.skipped += batch["skipped"]
            source_skipped += batch["skipped"]

            if args.dry_run:
                indexed_count = len(documents)
            else:
                try:
                    result = vector_db.add_sparse_documents(source_name=source_name, documents=documents)
                except VectorDBError as exc:
                    if "code=19100" in str(exc) and "memory used more than" in str(exc).lower():
                        raise VectorDBError(
                            f"{exc}\n"
                            f"Sparse collection 内存索引已达到实例上限。当前 source={source_name}, "
                            f"安全恢复游标 last_paper_id={after_paper_id}。先执行：\n"
                            f"  {sys.executable} scripts/configure_sparse_disk_swap.py "
                            f"--config-path {args.config_path} --sources {source_name} --apply\n"
                            "等待索引状态恢复 ready 后，再使用原命令和 state file 继续 --resume。"
                        ) from exc
                    raise
                indexed_count = result.get("document_count", len(documents))

            summary.indexed += indexed_count
            source_fetched += len(candidates)
            source_indexed += indexed_count
            after_paper_id = int(candidates[-1]["paper_id"])

            if args.resume and not args.dry_run:
                state[source_name] = {"last_paper_id": after_paper_id}
                save_state(args.state_file, state)

            if remaining is not None:
                remaining -= len(candidates)

            if source_progress is not None:
                source_progress.update(len(candidates))
                _set_progress_postfix(
                    source_progress,
                    indexed=source_indexed,
                    skipped=source_skipped,
                    last_paper_id=after_paper_id,
                )
            if show_progress:
                _set_progress_postfix(
                    source_iter,
                    source=source_name,
                    fetched=summary.fetched,
                    indexed=summary.indexed,
                    skipped=summary.skipped,
                )

        if source_progress is not None:
            source_progress.close()
        summary.by_source[source_name] = {
            "fetched": source_fetched,
            "indexed": source_indexed,
            "skipped": source_skipped,
            "failed": 0,
            "last_paper_id": after_paper_id,
        }

    return summary


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        summary = run_backfill(args)
        metrics = asdict(summary)
        payload = {
            "schema_version": 1,
            "operation": "sparse_backfill",
            "status": "failed" if summary.failed else "ok",
            "metrics": metrics,
        }
        print(json.dumps(metrics, ensure_ascii=False, indent=2))
        if args.summary_json:
            args.summary_json.parent.mkdir(parents=True, exist_ok=True)
            args.summary_json.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        return 0 if summary.failed == 0 else 1
    except Exception as exc:
        logging.exception("Sparse backfill failed: %s", exc)
        if args.summary_json:
            args.summary_json.parent.mkdir(parents=True, exist_ok=True)
            args.summary_json.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "operation": "sparse_backfill",
                        "status": "failed",
                        "error_summary": str(exc),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
