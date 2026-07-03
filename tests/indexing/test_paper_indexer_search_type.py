from __future__ import annotations

import pytest

from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.indexing.retrieval_helper import RankedResult
from src.docset_hub.storage.vector_db import SearchResult


class FakeVectorDB:
    def __init__(self):
        self.dense_calls = []
        self.sparse_calls = []

    def dense_search(self, query, source_list, top_k):
        self.dense_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
            }
        )
        return [
            SearchResult(
                source_name="biorxiv_history",
                work_id="W1",
                score=0.25,
                text_type="abstract",
                paper_id="1",
                retrieval_debug={"retriever": "dense"},
            )
        ]

    def sparse_search(self, query, source_list, top_k):
        self.sparse_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
            }
        )
        return [
            SearchResult(
                source_name="biorxiv_history",
                work_id="W1",
                score=3.1,
                text_type="abstract",
                paper_id="1",
                retrieval_debug={"retriever": "sparse"},
            )
        ]


def _indexer_with_fake_vector_db():
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.vector_db = FakeVectorDB()
    indexer.default_sources = ["biorxiv_history", "langtaosha"]
    indexer.metadata_db = object()
    return indexer


def test_search_defaults_to_dense():
    indexer = _indexer_with_fake_vector_db()

    results = indexer.search("CRISPR-Cas9", source_list=["biorxiv_history"], hydrate=False)

    assert indexer.vector_db.dense_calls == [
        {
            "query": "CRISPR-Cas9",
            "source_list": ["biorxiv_history"],
            "top_k": 10,
        }
    ]
    assert results[0]["retrieval_debug"] == {"retriever": "dense"}


def test_search_can_route_to_sparse():
    indexer = _indexer_with_fake_vector_db()

    sparse_results = indexer.search(
        "p53 mutation",
        source_list=["biorxiv_history"],
        top_k=5,
        hydrate=False,
        search_type="sparse",
    )

    assert indexer.vector_db.sparse_calls == [
        {
            "query": "p53 mutation",
            "source_list": ["biorxiv_history"],
            "top_k": 5,
        }
    ]
    assert sparse_results[0]["retrieval_debug"] == {"retriever": "sparse"}


def test_search_rejects_hybrid_retrieval_type():
    indexer = _indexer_with_fake_vector_db()

    with pytest.raises(ValueError, match="hybrid_retrieval_search"):
        indexer.search(
            "p53 mutation",
            source_list=["biorxiv_history"],
            search_type="hybrid_retrieval",
        )
