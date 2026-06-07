from __future__ import annotations

import pytest

from src.docset_hub.indexing.paper_indexer import (
    DEFAULT_HYBRID_RETRIEVAL_WEIGHTS,
    PaperIndexer,
)
from src.docset_hub.indexing.paper_keyword_lookup import PaperKeywordLookupResult
from src.docset_hub.storage.vector_db import SearchResult


class FakeVectorDB:
    def __init__(self):
        self.hybrid_config = {
            "candidate_multiplier": 2,
            "min_candidate_k": 5,
            "rrf_k": 60,
        }


def _indexer() -> PaperIndexer:
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.vector_db = FakeVectorDB()
    indexer.default_sources = ["biorxiv_history", "langtaosha"]
    return indexer


def _branch_result(
    retriever: str,
    work_id: str,
    rank: int,
    raw_score: float,
    paper_id: str | int | None = None,
) -> dict:
    return {
        "work_id": work_id,
        "paper_id": paper_id,
        "source_name": "biorxiv_history",
        "text_type": "abstract",
        "raw_score": raw_score,
        "retriever": retriever,
        "rank": rank,
        "payload": {},
        "retrieval_debug": {"branch": retriever},
    }


def test_three_way_hybrid_retrieval_promotes_multi_branch_hits(monkeypatch):
    indexer = _indexer()
    calls = []

    def dense_branch(**kwargs):
        calls.append(("dense", kwargs["top_k"]))
        return [
            _branch_result("dense", "dense-only", 1, 0.92, "1"),
            _branch_result("dense", "shared", 2, 0.85, "2"),
        ]

    def sparse_branch(**kwargs):
        calls.append(("sparse", kwargs["top_k"]))
        return [
            _branch_result("sparse", "shared", 1, 13.0, "2"),
            _branch_result("sparse", "sparse-only", 2, 10.0, "3"),
        ]

    def keyword_branch(**kwargs):
        calls.append(("keyword_lookup", kwargs["top_k"]))
        return [
            _branch_result("keyword_lookup", "shared", 1, 1.6, "2"),
        ]

    monkeypatch.setattr(indexer, "_run_dense_retrieval_branch", dense_branch)
    monkeypatch.setattr(indexer, "_run_sparse_retrieval_branch", sparse_branch)
    monkeypatch.setattr(indexer, "_run_keyword_lookup_retrieval_branch", keyword_branch)

    results = indexer.hybrid_retrieval_search(
        "exhausted t cell",
        source_list=["biorxiv_history"],
        top_k=3,
        hydrate=False,
    )

    assert {call[0] for call in calls} == {"dense", "sparse", "keyword_lookup"}
    assert {call[1] for call in calls} == {5}
    assert results[0]["work_id"] == "shared"
    assert results[0]["retrieval_debug"]["matched_retrievers"] == [
        "dense",
        "sparse",
        "keyword_lookup",
    ]
    assert results[0]["retrieval_debug"]["retrieval_weights"] == DEFAULT_HYBRID_RETRIEVAL_WEIGHTS


def test_sparse_and_keyword_adapters_drop_non_positive_evidence():
    indexer = _indexer()

    sparse = indexer._adapt_search_results_to_branch_results(
        [
            SearchResult("biorxiv_history", "zero", 0.0, "abstract", "1"),
            SearchResult("biorxiv_history", "negative", -1.0, "abstract", "2"),
            SearchResult("biorxiv_history", "positive", 3.2, "abstract", "3"),
        ],
        retriever="sparse",
        drop_non_positive=True,
    )
    keyword = indexer._adapt_keyword_lookup_results_to_branch_results(
        [
            PaperKeywordLookupResult(1, "kw-zero", 0, 1, 0.0, [], ["keyword_lookup"], {}),
            PaperKeywordLookupResult(
                2,
                "kw-positive",
                1,
                1,
                0.7,
                [{"group_id": 1}],
                ["keyword_lookup"],
                {"retriever": "keyword_lookup"},
            ),
        ]
    )

    assert [item["work_id"] for item in sparse] == ["positive"]
    assert sparse[0]["rank"] == 1
    assert [item["work_id"] for item in keyword] == ["kw-positive"]
    assert keyword[0]["rank"] == 1
    assert keyword[0]["retrieval_debug"]["matched_concepts"] == [{"group_id": 1}]


def test_hybrid_retrieval_continues_when_one_branch_fails(monkeypatch):
    indexer = _indexer()

    def dense_branch(**kwargs):
        raise RuntimeError("dense unavailable")

    def sparse_branch(**kwargs):
        return [_branch_result("sparse", "sparse-only", 1, 10.0, "3")]

    def keyword_branch(**kwargs):
        return []

    monkeypatch.setattr(indexer, "_run_dense_retrieval_branch", dense_branch)
    monkeypatch.setattr(indexer, "_run_sparse_retrieval_branch", sparse_branch)
    monkeypatch.setattr(indexer, "_run_keyword_lookup_retrieval_branch", keyword_branch)

    results = indexer.hybrid_retrieval_search(
        "p53 mutation",
        source_list=["biorxiv_history"],
        top_k=1,
        hydrate=False,
    )

    assert results[0]["work_id"] == "sparse-only"
    assert results[0]["retrieval_debug"]["branch_failures"] == {
        "dense": "dense unavailable",
    }


def test_hybrid_retrieval_raises_when_all_requested_branches_fail(monkeypatch):
    indexer = _indexer()

    def failing_branch(**kwargs):
        raise RuntimeError("branch down")

    monkeypatch.setattr(indexer, "_run_dense_retrieval_branch", failing_branch)
    monkeypatch.setattr(indexer, "_run_sparse_retrieval_branch", failing_branch)
    monkeypatch.setattr(indexer, "_run_keyword_lookup_retrieval_branch", failing_branch)

    with pytest.raises(RuntimeError, match="全部 branch 失败"):
        indexer.hybrid_retrieval_search(
            "p53 mutation",
            source_list=["biorxiv_history"],
            top_k=1,
            hydrate=False,
        )


def test_search_type_hybrid_retrieval_routes_to_three_way_method(monkeypatch):
    indexer = _indexer()
    calls = []

    def fake_hybrid_retrieval_search(**kwargs):
        calls.append(kwargs)
        return [{"work_id": "W1"}]

    monkeypatch.setattr(indexer, "hybrid_retrieval_search", fake_hybrid_retrieval_search)

    results = indexer.search(
        "CRISPR-Cas9",
        source_list=["biorxiv_history"],
        top_k=4,
        hydrate=False,
        search_type="hybrid_retrieval",
    )

    assert results == [{"work_id": "W1"}]
    assert calls == [
        {
            "query": "CRISPR-Cas9",
            "source_list": ["biorxiv_history"],
            "top_k": 4,
            "hydrate": False,
        }
    ]
