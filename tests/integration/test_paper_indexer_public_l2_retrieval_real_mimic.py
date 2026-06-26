"""Real PaperIndexer public L2 search integration tests against mimic data."""

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


def _assert_lightweight_rows_common(results: list[dict], *, expect_metadata: bool) -> None:
    assert results
    for item in results:
        assert item["work_id"]
        assert math.isfinite(float(item["similarity"]))
        assert "retrieval_debug" in item
        if expect_metadata:
            assert "metadata" in item
            metadata = item["metadata"]
            assert metadata.get("canonical_title") or metadata.get("title")
            assert isinstance(metadata.get("sources"), list)
        else:
            assert "metadata" not in item


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
    probe = real_retrieval_indexer.search(
        "acute kidney injury",
        source_list=["biorxiv_history"],
        top_k=3,
        hydrate=False,
        search_type="dense",
    )
    if not probe:
        pytest.skip("mimic L2 search returned no results for probe query")
    return {"source_list": ["biorxiv_history"]}


@pytest.mark.integration
def test_search_dense_returns_lightweight_rows_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["acute_kidney_injury"]
    results = real_retrieval_indexer.search(
        case["query"],
        source_list=case["source_list"],
        top_k=5,
        hydrate=False,
        search_type="dense",
    )

    _assert_lightweight_rows_common(results, expect_metadata=False)
    assert all(item["source_name"] in case["source_list"] for item in results)
    assert {item["work_id"] for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_search_dense_with_hydrate_returns_metadata_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["acute_kidney_injury"]
    results = real_retrieval_indexer.search(
        case["query"],
        source_list=case["source_list"],
        top_k=5,
        hydrate=True,
        search_type="dense",
    )

    _assert_lightweight_rows_common(results, expect_metadata=True)
    assert {item["work_id"] for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_search_sparse_returns_lightweight_rows_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["renal_fibrosis"]
    results = real_retrieval_indexer.search(
        case["query"],
        source_list=case["source_list"],
        top_k=5,
        hydrate=False,
        search_type="sparse",
    )

    _assert_lightweight_rows_common(results, expect_metadata=False)
    assert all(item["source_name"] in case["source_list"] for item in results)
    assert {item["work_id"] for item in results} & case["expected_work_ids"]


@pytest.mark.integration
def test_search_expanded_sparse_returns_coverage_fields_from_real_mimic_db(
    real_retrieval_indexer: PaperIndexer,
    retrieval_probe,
) -> None:
    case = QUERY_CASES["lung_cancer"]
    results = real_retrieval_indexer.search(
        case["query"],
        source_list=case["source_list"],
        top_k=5,
        hydrate=False,
        search_type="expanded_sparse",
        keyword_sources=case["keyword_sources"],
    )

    _assert_lightweight_rows_common(results, expect_metadata=False)
    for item in results:
        assert "coverage_ratio" in item
        assert "coverage" in item
        assert item["coverage"]["matched_span_count"] >= 1
        assert item["coverage"]["total_span_count"] >= item["coverage"]["matched_span_count"]
        assert item["matched_span_count"] == item["coverage"]["matched_span_count"]
        assert item["total_span_count"] == item["coverage"]["total_span_count"]
        assert item["matched_spans"]
    assert {item["work_id"] for item in results} & case["expected_work_ids"]
