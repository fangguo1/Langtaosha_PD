from __future__ import annotations

import pytest

from src.docset_hub.indexing.paper_indexer import (
    PaperIndexer,
)
from src.docset_hub.indexing.paper_keyword_lookup import PaperKeywordLookupResult
from src.docset_hub.indexing.retrieval_helper import (
    DEFAULT_HYBRID_RETRIEVAL_WEIGHTS,
    RankedResult,
    filter_keyword_lookup_results,
    filter_positive_score_results,
    from_keyword_lookup_result,
    from_search_result,
)
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
    indexer.metadata_db = object()
    return indexer


def _ranked(
    retriever: str,
    work_id: str,
    rank: int,
    raw_score: float,
    paper_id: str | int | None = None,
) -> RankedResult:
    return RankedResult(
        work_id=work_id,
        paper_id=paper_id,
        source_name="biorxiv_history",
        score=raw_score,
        text_type="abstract",
        retriever=retriever,
        rank=rank,
        retrieval_debug={"branch": retriever},
    )


@pytest.fixture(autouse=True)
def passthrough_dense_filter(monkeypatch):
    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.filter_dense_results",
        lambda hits, **kwargs: (list(hits), {}),
    )


def test_three_way_hybrid_retrieval_promotes_multi_branch_hits(monkeypatch):
    indexer = _indexer()
    calls = []

    def dense_branch(query, source_list, top_k):
        calls.append(("dense", top_k))
        return [
            _ranked("dense", "dense-only", 1, 0.92, "1"),
            _ranked("dense", "shared", 2, 0.85, "2"),
        ]

    def sparse_branch(query, source_list, top_k):
        calls.append(("sparse", top_k))
        return [
            _ranked("sparse", "shared", 1, 13.0, "2"),
            _ranked("sparse", "sparse-only", 2, 10.0, "3"),
        ]

    def keyword_branch(query, source_list, top_k, keyword_sources=None):
        calls.append(("keyword_lookup", top_k))
        return [
            _ranked("keyword_lookup", "shared", 1, 1.6, "2"),
        ]

    monkeypatch.setattr(indexer, "dense_search", dense_branch)
    monkeypatch.setattr(indexer, "sparse_search", sparse_branch)
    monkeypatch.setattr(indexer, "_keyword_lookup_search", keyword_branch)

    results = indexer.hybrid_retrieval_search(
        "exhausted t cell",
        source_list=["biorxiv_history"],
        top_k=3,
        hydrate=False,
    )

    assert {call[0] for call in calls} == {"dense", "sparse", "keyword_lookup"}
    assert {call[1] for call in calls} == {6}
    assert results[0]["work_id"] == "shared"
    assert set(results[0]["retrieval_debug"]["matched_retrievers"]) == {
        "dense",
        "sparse",
        "keyword_lookup",
    }
    assert results[0]["retrieval_debug"]["retrieval_weights"] == DEFAULT_HYBRID_RETRIEVAL_WEIGHTS


def test_sparse_and_keyword_filters_drop_non_positive_evidence():
    sparse_hits = [
        from_search_result(
            SearchResult("biorxiv_history", "zero", 0.0, "abstract", "1"),
            retriever="sparse",
            rank=1,
        ),
        from_search_result(
            SearchResult("biorxiv_history", "negative", -1.0, "abstract", "2"),
            retriever="sparse",
            rank=2,
        ),
        from_search_result(
            SearchResult("biorxiv_history", "positive", 3.2, "abstract", "3"),
            retriever="sparse",
            rank=3,
        ),
    ]
    sparse = filter_positive_score_results(sparse_hits, drop_non_positive=True)

    keyword_hits = [
        from_keyword_lookup_result(
            PaperKeywordLookupResult(1, "kw-zero", 0, 1, 0.0, [], ["keyword_lookup"], {}),
            rank=1,
        ),
        from_keyword_lookup_result(
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
            rank=2,
        ),
    ]
    keyword = filter_keyword_lookup_results(keyword_hits)

    assert [item.work_id for item in sparse] == ["positive"]
    assert sparse[0].rank == 1
    assert [item.work_id for item in keyword] == ["kw-positive"]
    assert keyword[0].rank == 1
    assert keyword[0].retrieval_debug["matched_concepts"] == [{"group_id": 1}]


def test_hybrid_retrieval_continues_when_one_branch_fails(monkeypatch):
    indexer = _indexer()

    def dense_branch(query, source_list, top_k):
        raise RuntimeError("dense unavailable")

    def sparse_branch(query, source_list, top_k):
        return [_ranked("sparse", "sparse-only", 1, 10.0, "3")]

    def keyword_branch(query, source_list, top_k, keyword_sources=None):
        return []

    monkeypatch.setattr(indexer, "dense_search", dense_branch)
    monkeypatch.setattr(indexer, "sparse_search", sparse_branch)
    monkeypatch.setattr(indexer, "_keyword_lookup_search", keyword_branch)

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

    def failing_branch(*args, **kwargs):
        raise RuntimeError("branch down")

    monkeypatch.setattr(indexer, "dense_search", failing_branch)
    monkeypatch.setattr(indexer, "sparse_search", failing_branch)
    monkeypatch.setattr(indexer, "_keyword_lookup_search", failing_branch)

    with pytest.raises(RuntimeError, match="全部 branch 失败"):
        indexer.hybrid_retrieval_search(
            "p53 mutation",
            source_list=["biorxiv_history"],
            top_k=1,
            hydrate=False,
        )


def test_search_rejects_hybrid_retrieval_dispatch():
    indexer = _indexer()

    with pytest.raises(ValueError, match="hybrid_retrieval_search"):
        indexer.search(
            "CRISPR-Cas9",
            source_list=["biorxiv_history"],
            top_k=4,
            hydrate=False,
            search_type="hybrid_retrieval",
        )
