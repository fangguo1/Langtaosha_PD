#!/usr/bin/env python3
"""Backfill scispaCy-generated keywords for existing papers."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sqlalchemy import text

from src.docset_hub.indexing.keyword_enrichment import (
    DEFAULT_KEYWORD_SOURCE,
    DEFAULT_MODEL_NAMES,
    KeywordEnrichmentService,
)
from src.docset_hub.storage.metadata_db import MetadataDB

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional for CLI progress only.
    tqdm = None


DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_test.yaml"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-path", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--source-name", default=None)
    parser.add_argument("--limit", type=int, default=1000000)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--paper-id", dest="paper_ids", type=int, action="append", default=None)
    parser.add_argument("--only-missing-source", action="append", default=None)
    parser.add_argument("--keyword-source", default=None)
    parser.add_argument("--models", nargs="+", default=list(DEFAULT_MODEL_NAMES))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args, _ = parser.parse_known_args()
    return args


def fetch_candidate_papers(
    metadata_db: MetadataDB,
    limit: int,
    source_name: Optional[str],
    only_missing_source: str | List[str],
    paper_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    missing_sources = (
        [only_missing_source]
        if isinstance(only_missing_source, str)
        else list(only_missing_source or [DEFAULT_KEYWORD_SOURCE])
    )
    params: Dict[str, Any] = {"limit": limit}
    source_filter = ""
    if source_name:
        source_filter = "AND ps.source_name = :source_name"
        params["source_name"] = source_name
    paper_filter = ""
    if paper_ids:
        placeholders = []
        for idx, paper_id in enumerate(paper_ids):
            param_name = f"paper_id_{idx}"
            placeholders.append(f":{param_name}")
            params[param_name] = paper_id
        paper_filter = f"AND p.paper_id IN ({', '.join(placeholders)})"

    missing_conditions = []
    for idx, source in enumerate(missing_sources):
        param_name = f"missing_source_{idx}"
        params[param_name] = source
        missing_conditions.append(
            f"""
            NOT EXISTS (
                SELECT 1
                FROM paper_keywords pk
                WHERE pk.paper_id = p.paper_id
                  AND pk.source = :{param_name}
            )
            """
        )
    missing_filter = " OR ".join(missing_conditions)

    with metadata_db.engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT DISTINCT ON (p.paper_id)
                    p.paper_id,
                    p.canonical_title,
                    p.canonical_abstract,
                    ps.title AS source_title,
                    ps.abstract AS source_abstract,
                    ps.source_name
                FROM papers p
                JOIN paper_sources ps ON ps.paper_id = p.paper_id
                WHERE (p.canonical_title IS NOT NULL OR ps.title IS NOT NULL)
                  {source_filter}
                  {paper_filter}
                  AND ({missing_filter})
                ORDER BY p.paper_id, COALESCE(p.online_at, ps.online_at) DESC NULLS LAST
                LIMIT :limit
            """),
            params,
        ).fetchall()

    return [
        {
            "paper_id": row[0],
            "title": row[1] or row[3],
            "abstract": row[2] or row[4],
            "source_name": row[5],
        }
        for row in rows
    ]


def run_backfill(args: argparse.Namespace) -> Dict[str, Any]:
    metadata_db = MetadataDB(config_path=args.config_path)
    enrichment = KeywordEnrichmentService(
        config_path=args.config_path,
        model_names=getattr(args, "models", None),
        source=args.keyword_source,
    )
    missing_sources = args.only_missing_source or enrichment.sources

    candidates = fetch_candidate_papers(
        metadata_db=metadata_db,
        limit=args.limit,
        source_name=args.source_name,
        only_missing_source=missing_sources,
        paper_ids=getattr(args, "paper_ids", None),
    )

    summary: Dict[str, Any] = {
        "config_path": str(args.config_path),
        "source_name": args.source_name,
        "keyword_source": args.keyword_source,
        "keyword_sources": enrichment.sources,
        "models": enrichment.model_names,
        "dry_run": args.dry_run,
        "batch_size": args.batch_size,
        "candidates": len(candidates),
        "processed": 0,
        "inserted": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0,
        "errors": [],
    }

    progress = candidates
    if tqdm is not None:
        progress = tqdm(
            candidates,
            total=len(candidates),
            desc=f"keywords:{args.source_name or 'all'}",
            unit="paper",
            dynamic_ncols=True,
        )

    for paper in progress:
        summary["processed"] += 1
        paper_id = paper["paper_id"]
        if args.dry_run:
            logging.info("dry-run candidate paper_id=%s title=%s", paper_id, paper["title"])
            if tqdm is not None:
                progress.set_postfix(
                    processed=summary["processed"],
                    inserted=summary["inserted"],
                    failed=summary["failed"],
                )
            continue

        extraction = enrichment.extract_keywords(
            title=paper.get("title"),
            abstract=paper.get("abstract"),
        )
        if not extraction.success:
            if extraction.skipped:
                summary["skipped"] += 1
            else:
                summary["failed"] += 1
            summary["errors"].append(
                {
                    "paper_id": paper_id,
                    "error": extraction.error,
                    "skip_reason": extraction.skip_reason,
                }
            )
            if tqdm is not None:
                progress.set_postfix(
                    processed=summary["processed"],
                    inserted=summary["inserted"],
                    failed=summary["failed"],
                    skipped=summary["skipped"],
                )
            continue

        try:
            grouped_keywords: Dict[str, List[Dict[str, Any]]] = {}
            for keyword in extraction.keywords:
                keyword_source = keyword.get("source") or extraction.source
                grouped_keywords.setdefault(keyword_source, []).append(keyword)

            for keyword_source, keywords in grouped_keywords.items():
                write_result = metadata_db.upsert_generated_keywords(
                    paper_id=paper_id,
                    keywords=keywords,
                    source=keyword_source,
                )
                summary["inserted"] += write_result.get("inserted", 0)
                summary["updated"] += write_result.get("updated", 0)
                summary["skipped"] += write_result.get("skipped", 0)
        except Exception as exc:
            logging.error("failed to write generated keywords for paper_id=%s", paper_id, exc_info=True)
            summary["failed"] += 1
            summary["errors"].append({"paper_id": paper_id, "error": str(exc)})

        if tqdm is not None:
            progress.set_postfix(
                processed=summary["processed"],
                inserted=summary["inserted"],
                updated=summary["updated"],
                failed=summary["failed"],
                skipped=summary["skipped"],
            )

    return summary


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    summary = run_backfill(args)
    for key, value in summary.items():
        logging.info("%s=%s", key, value)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
