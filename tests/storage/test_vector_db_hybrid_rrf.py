from __future__ import annotations

from src.docset_hub.storage.vector_db import SearchResult, VectorDB


def test_rrf_merge_promotes_results_seen_by_both_retrievers():
    dense_results = [
        SearchResult("biorxiv_history", "W1", 0.8, "abstract", "1"),
        SearchResult("biorxiv_history", "W2", 0.7, "abstract", "2"),
    ]
    sparse_results = [
        SearchResult("biorxiv_history", "W2", 12.0, "abstract", "2"),
        SearchResult("biorxiv_history", "W3", 9.0, "abstract", "3"),
    ]

    merged = VectorDB._rrf_merge_results(
        dense_results=dense_results,
        sparse_results=sparse_results,
        top_k=3,
        rrf_k=60,
    )

    assert [result.work_id for result in merged] == ["W2", "W1", "W3"]
    assert merged[0].retrieval_debug == {
        "matched_retrievers": ["dense", "sparse"],
        "dense_rank": 2,
        "dense_score": 0.7,
        "sparse_rank": 1,
        "sparse_score": 12.0,
    }


def test_rrf_merge_respects_weights_and_top_k():
    dense_results = [
        SearchResult("biorxiv_history", "dense-only", 0.9, "abstract", "1"),
    ]
    sparse_results = [
        SearchResult("biorxiv_history", "sparse-only", 11.0, "abstract", "2"),
    ]

    merged = VectorDB._rrf_merge_results(
        dense_results=dense_results,
        sparse_results=sparse_results,
        top_k=1,
        rrf_k=60,
        dense_weight=0.5,
        sparse_weight=2.0,
    )

    assert [result.work_id for result in merged] == ["sparse-only"]
    assert merged[0].retrieval_debug["matched_retrievers"] == ["sparse"]


def test_hybrid_search_uses_configured_candidate_k_and_rrf(monkeypatch):
    vector_db = VectorDB.__new__(VectorDB)
    vector_db.hybrid_config = {
        "candidate_multiplier": 2,
        "min_candidate_k": 7,
        "rrf_k": 60,
        "dense_weight": 1.0,
        "sparse_weight": 1.0,
    }
    calls = []

    def fake_dense_search(query, source_list=None, top_k=10):
        calls.append(("dense", query, source_list, top_k))
        return [SearchResult("biorxiv_history", "W1", 0.8, "abstract", "1")]

    def fake_sparse_search(query, source_list=None, top_k=10):
        calls.append(("sparse", query, source_list, top_k))
        return [SearchResult("biorxiv_history", "W2", 12.0, "abstract", "2")]

    monkeypatch.setattr(vector_db, "dense_search", fake_dense_search)
    monkeypatch.setattr(vector_db, "sparse_search", fake_sparse_search)

    results = vector_db.hybrid_search(
        query="CRISPR-Cas9",
        source_list=["biorxiv_history"],
        top_k=3,
    )

    assert calls == [
        ("dense", "CRISPR-Cas9", ["biorxiv_history"], 7),
        ("sparse", "CRISPR-Cas9", ["biorxiv_history"], 7),
    ]
    assert {result.work_id for result in results} == {"W1", "W2"}
