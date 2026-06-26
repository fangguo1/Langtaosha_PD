"""Real PaperIndexer public L1 retrieval integration tests against mimic data."""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, init_config
from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.indexing.retrieval_helper import RankedResult


_GLOBAL_CONFIG_PATH: Path | None = None
MIMIC_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "config"
    / "config_tecent_backend_server_mimic.yaml"
)
RUN_ENV_VAR = "RUN_REAL_PAPER_INDEXER_RETRIEVAL_INTEGRATION"
CONFIG_ENV_VAR = "PAPER_INDEXER_REAL_RETRIEVAL_TEST_CONFIG"

QUERY_CASES = {
    "acute_kidney_injury": {
        "query": "acute kidney injury",
        "source_list": ["biorxiv_history"],
        "keyword_sources": ["scispacy-en_core_sci_lg-generated"],
        "expected_work_ids": {
            "W019db3ae-a05a-7c81-b772-5ebd3344c9c5",
            "W019db3ae-b742-72ea-9909-761d2272ee7b",
            "W019db3af-3d39-7d9e-b03a-0043515f51f5",
        },
    },
    "renal_fibrosis": {
        "query": "renal fibrosis",
        "source_list": ["biorxiv_history"],
        "keyword_sources": ["scispacy-en_core_sci_lg-generated"],
        "expected_work_ids": {
            "W019db3ad-c1fb-7178-acdf-eac92fe46751",
            "W019db3af-1988-7718-9f70-abc8e74f952a",
            "W019db3af-8420-7c5b-9dc3-0cae07a43372",
        },
    },
    "lung_cancer": {
        "query": "lung cancer",
        "source_list": ["biorxiv_history"],
        "keyword_sources": ["scispacy-en_core_sci_lg-generated"],
        "expected_work_ids": {
            "W019db3ae-49bd-7b63-9171-d4e5f79a5b42",
            "W019db3ae-7347-7e6b-bb3a-9e5357d991cf",
            "W019db3ad-be5f-7076-bc81-3d0a863157ea",
            "W019db3af-9629-756a-9de1-e42af5808ece",
        },
    },
}


def get_config_path_from_args() -> Path:
    global _GLOBAL_CONFIG_PATH
    if _GLOBAL_CONFIG_PATH is not None:
        return _GLOBAL_CONFIG_PATH

    config_path: Path | None = None
    is_pytest = any("pytest" in arg for arg in sys.argv)
    if not is_pytest:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config-path", type=str, default=None)
        args, _ = parser.parse_known_args()
        if args.config_path:
            config_path = Path(args.config_path)

    if config_path is None and os.environ.get(CONFIG_ENV_VAR):
        config_path = Path(os.environ[CONFIG_ENV_VAR])

    if config_path is None:
        config_path = MIMIC_CONFIG_PATH

    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")

    _GLOBAL_CONFIG_PATH = config_path
    return config_path


def _skip_unless_enabled() -> None:
    if os.environ.get(RUN_ENV_VAR) != "1":
        pytest.skip(f"set {RUN_ENV_VAR}=1 to run live PaperIndexer retrieval checks")


def _assert_ranked_results_common(results: list[RankedResult], *, retriever: str) -> None:
    assert results
    assert all(isinstance(item, RankedResult) for item in results)
    assert [item.rank for item in results] == list(range(1, len(results) + 1))
    assert all(item.retriever == retriever for item in results)
    assert all(item.work_id for item in results)
    assert all(math.isfinite(float(item.score)) for item in results)


@pytest.fixture(scope="session")
def real_retrieval_indexer() -> PaperIndexer:
    _skip_unless_enabled()
    config_path = get_config_path_from_args()
    _reset_config()
    init_config(config_path, force_reload=True)
    return PaperIndexer(
        config_path=config_path,
        enable_vectorization=True,
        enable_keyword_enrichment=False,
    )


@pytest.fixture(scope="module")
def retrieval_probe(real_retrieval_indexer: PaperIndexer) -> dict[str, object]:
    source_list = ["biorxiv_history"]
    dense_hits = real_retrieval_indexer.dense_search("acute kidney injury", source_list, top_k=3)
    sparse_hits = real_retrieval_indexer.sparse_search("acute kidney injury", source_list, top_k=3)
    expanded_hits = real_retrieval_indexer.expanded_sparse_search(
        "acute kidney injury",
        source_list,
        top_k=3,
        keyword_sources=["scispacy-en_core_sci_lg-generated"],
    )
    if not dense_hits:
        pytest.skip("mimic dense_search returned no results for probe query")
    if not sparse_hits:
        pytest.skip("mimic sparse_search returned no results for probe query")
    if not expanded_hits:
        pytest.skip("mimic expanded_sparse_search returned no results for probe query")
    return {"source_list": source_list}


@pytest.mark.integration
def test_dense_search_returns_ranked_results_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["acute_kidney_injury"]
    results = real_retrieval_indexer.dense_search(
        case["query"],
        case["source_list"],
        top_k=5,
    )

    _assert_ranked_results_common(results, retriever="dense")
    assert all(item.source_name in case["source_list"] for item in results)
    assert {item.work_id for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_sparse_search_returns_ranked_results_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["renal_fibrosis"]
    results = real_retrieval_indexer.sparse_search(
        case["query"],
        case["source_list"],
        top_k=5,
    )

    _assert_ranked_results_common(results, retriever="sparse")
    assert all(item.source_name in case["source_list"] for item in results)
    assert any(item.score > 0 for item in results)
    assert {item.work_id for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_expanded_sparse_search_returns_span_aware_ranked_results_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["lung_cancer"]
    results = real_retrieval_indexer.expanded_sparse_search(
        case["query"],
        case["source_list"],
        top_k=5,
        keyword_sources=case["keyword_sources"],
    )

    _assert_ranked_results_common(results, retriever="expanded_sparse")
    assert any(item.matched_span_count and item.matched_span_count > 0 for item in results)
    assert all((item.total_span_count or 0) >= (item.matched_span_count or 0) for item in results)
    assert any(item.score > 0 for item in results)
    assert {item.work_id for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_expanded_sparse_search_accepts_real_keyword_sources(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["acute_kidney_injury"]
    default_results = real_retrieval_indexer.expanded_sparse_search(
        case["query"],
        case["source_list"],
        top_k=5,
        keyword_sources=None,
    )
    explicit_results = real_retrieval_indexer.expanded_sparse_search(
        case["query"],
        case["source_list"],
        top_k=5,
        keyword_sources=case["keyword_sources"],
    )

    _assert_ranked_results_common(default_results, retriever="expanded_sparse")
    _assert_ranked_results_common(explicit_results, retriever="expanded_sparse")
    assert {item.work_id for item in explicit_results} & case["expected_work_ids"]
