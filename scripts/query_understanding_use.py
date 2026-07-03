#!/usr/bin/env python3
"""Run query understanding against the `_use` backend config."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from config.config_loader import init_config  # noqa: E402
from src.docset_hub.indexing import PaperIndexer  # noqa: E402


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_use.yaml"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect query understanding using the `_use` backend config.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("query", help="Query text to analyze.")
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Backend config path.",
    )
    return parser.parse_args(argv)


def _create_paper_indexer(
    config_path: Path,
    paper_indexer_factory: Optional[Callable[[Path], Any]] = None,
) -> Any:
    if paper_indexer_factory is not None:
        return paper_indexer_factory(config_path)
    init_config(config_path)
    return PaperIndexer(config_path=config_path, enable_vectorization=True)


def run_query_understanding(
    *,
    query: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    paper_indexer_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    indexer = _create_paper_indexer(
        config_path=config_path,
        paper_indexer_factory=paper_indexer_factory,
    )
    return indexer.query_understanding.analyze(query).to_dict()


def main(
    argv: Optional[list[str]] = None,
    *,
    stdout: Any = None,
    stderr: Any = None,
    paper_indexer_factory: Optional[Callable[[Path], Any]] = None,
) -> int:
    args = parse_args(argv)
    out = stdout or sys.stdout
    err = stderr or sys.stderr

    try:
        result = run_query_understanding(
            query=args.query,
            config_path=args.config_path,
            paper_indexer_factory=paper_indexer_factory,
        )
    except Exception as exc:  # noqa: BLE001
        err.write(f"{exc}\n")
        return 1

    out.write(json.dumps(result, ensure_ascii=False, indent=2))
    out.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
