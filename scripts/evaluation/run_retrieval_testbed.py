#!/usr/bin/env python3
"""Run retrieval strategies against a JSON retrieval feedback testbed."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.docset_hub.evaluation.config_identity import (  # noqa: E402
    build_config_fingerprint,
    create_metadata_engine_from_config,
    load_config_mapping,
)
from src.docset_hub.evaluation.contracts import TestbedQuery  # noqa: E402
from src.docset_hub.evaluation.json_testbed import (  # noqa: E402
    load_testbed_document,
    load_testbed_queries,
)
from src.docset_hub.evaluation.runner import RetrievalEvaluationRunner  # noqa: E402
from src.docset_hub.evaluation.search_strategies import (  # noqa: E402
    HybridRetrievalSearchStrategy,
    PaperIndexerSearchStrategy,
)
from src.docset_hub.indexing import PaperIndexer  # noqa: E402


SUPPORTED_STRATEGIES = ("dense", "sparse", "hybrid", "hybrid_retrieval")


class InMemoryEvaluationRepository:
    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []

    def create_run(self, payload: dict[str, Any]) -> int:
        self.runs.append(payload)
        return len(self.runs)

    def record_results(self, run_id: int, query_id: int, rows: list[dict[str, Any]]) -> None:
        return None

    def record_query_metrics(
        self,
        run_id: int,
        query_id: int,
        metrics: dict[str, Any],
        error_summary: str | None = None,
    ) -> None:
        return None

    def complete_run(
        self,
        run_id: int,
        aggregate_metrics: dict[str, Any],
        status: str,
        error_summary: str | None = None,
    ) -> None:
        return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evaluation-config-path",
        default="src/config/config_tecent_backend_server_use.yaml",
    )
    parser.add_argument(
        "--confirm-evaluation-database",
        required=True,
        help="Exact metadata_db.name of the evaluation database.",
    )
    parser.add_argument("--testbed-json", required=True)
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=list(SUPPORTED_STRATEGIES),
        choices=SUPPORTED_STRATEGIES,
    )
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--ks", nargs="+", type=int, default=[5, 10])
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for comparison.json. Defaults to local_data/retrieval_testbed/runs/<timestamp>.",
    )
    return parser.parse_args(argv)


def _assert_confirmed_database(config_fingerprint: dict[str, object], expected_name: str) -> None:
    actual_name = str(config_fingerprint.get("metadata_db_name") or "")
    if actual_name != expected_name:
        raise SystemExit(
            f"Refusing to run evaluation: expected database '{expected_name}', got '{actual_name}'."
        )


def _build_strategy(strategy_name: str, indexer: PaperIndexer):
    if strategy_name == "hybrid_retrieval":
        return HybridRetrievalSearchStrategy(indexer=indexer)
    return PaperIndexerSearchStrategy(indexer=indexer, search_type=strategy_name)


def _default_output_dir() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "local_data" / "retrieval_testbed" / "runs" / timestamp


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    evaluation_config = load_config_mapping(args.evaluation_config_path)
    evaluation_fingerprint = build_config_fingerprint(evaluation_config)
    _assert_confirmed_database(evaluation_fingerprint, args.confirm_evaluation_database)

    create_metadata_engine_from_config(evaluation_config)
    testbed_json_path = Path(args.testbed_json)
    testbed_document = load_testbed_document(testbed_json_path)
    queries = load_testbed_queries(testbed_json_path)

    indexer = PaperIndexer(
        config_path=Path(args.evaluation_config_path),
        enable_vectorization=True,
    )
    runner = RetrievalEvaluationRunner(repository=InMemoryEvaluationRepository())

    comparison: dict[str, object] = {
        "testbed_name": testbed_document.get("testbed_name"),
        "testbed_json": str(testbed_json_path),
        "evaluation_config_path": args.evaluation_config_path,
        "evaluation_config_fingerprint": evaluation_fingerprint,
        "query_count": len(queries),
        "top_k": args.top_k,
        "ks": list(args.ks),
        "strategies": {},
    }

    for strategy_name in args.strategies:
        strategy = _build_strategy(strategy_name, indexer)
        outcome = runner.run_queries(
            strategy=strategy,
            queries=queries,
            top_k=args.top_k,
            ks=args.ks,
            run_metadata={
                "testbed_name": testbed_document.get("testbed_name"),
                "strategy_config": {"strategy_name": strategy_name},
                "config_path": args.evaluation_config_path,
                "evaluation_config_fingerprint": evaluation_fingerprint,
                "corpus_snapshot": {},
                "index_version": {},
            },
        )
        comparison["strategies"][strategy_name] = outcome

    output_dir = Path(args.output_dir) if args.output_dir else _default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "comparison.json"
    output_path.write_text(
        json.dumps(comparison, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output_path": str(output_path), "query_count": len(queries)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
