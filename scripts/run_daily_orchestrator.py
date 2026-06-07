#!/usr/bin/env python3
"""Run the daily bioRxiv/LangTaoSha ingestion orchestrator."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.docset_hub.orchestrator import DailyPipeline, DailyPipelineConfig  # noqa: E402


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily fetch, ingest, and author-enrichment pipeline.")
    parser.add_argument("--date", default=None, help="Target date YYYY-MM-DD. Defaults to yesterday.")
    parser.add_argument("--config-path", default="src/config/config_tecent_backend_server_test.yaml")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-vector", action="store_true")
    parser.add_argument("--skip-author-enrichment", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_date = parse_date(args.date) if args.date else date.today() - timedelta(days=1)
    pipeline = DailyPipeline(
        DailyPipelineConfig(
            project_root=Path(args.project_root),
            config_path=Path(args.config_path),
            target_date=target_date,
            dry_run=args.dry_run,
            run_vector_stage=not args.skip_vector,
            run_author_enrichment=not args.skip_author_enrichment,
        )
    )
    manifest = pipeline.run()
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0 if manifest.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
