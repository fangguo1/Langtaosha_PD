from __future__ import annotations

from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.storage.vector_db import SearchResult


class FakeVectorDB:
    def __init__(self):
        self.calls = []

    def search(self, query, source_list=None, top_k=10, search_type="dense"):
        self.calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "search_type": search_type,
            }
        )
        return [
            SearchResult(
                source_name="biorxiv_history",
                work_id="W1",
                score=0.25,
                text_type="abstract",
                paper_id="1",
                retrieval_debug={"matched_retrievers": [search_type]},
            )
        ]


def _indexer_with_fake_vector_db():
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.vector_db = FakeVectorDB()
    indexer.default_sources = ["biorxiv_history", "langtaosha"]
    return indexer


def test_search_defaults_to_dense():
    indexer = _indexer_with_fake_vector_db()

    results = indexer.search("CRISPR-Cas9", source_list=["biorxiv_history"], hydrate=False)

    assert indexer.vector_db.calls == [
        {
            "query": "CRISPR-Cas9",
            "source_list": ["biorxiv_history"],
            "top_k": 10,
            "search_type": "dense",
        }
    ]
    assert results[0]["retrieval_debug"] == {"matched_retrievers": ["dense"]}


def test_search_can_route_to_sparse_or_hybrid():
    indexer = _indexer_with_fake_vector_db()

    sparse_results = indexer.search(
        "p53 mutation",
        source_list=["biorxiv_history"],
        top_k=5,
        hydrate=False,
        search_type="sparse",
    )
    hybrid_results = indexer.search(
        "p53 mutation",
        source_list=["biorxiv_history"],
        top_k=5,
        hydrate=False,
        search_type="hybrid",
    )

    assert [call["search_type"] for call in indexer.vector_db.calls] == ["sparse", "hybrid"]
    assert sparse_results[0]["retrieval_debug"] == {"matched_retrievers": ["sparse"]}
    assert hybrid_results[0]["retrieval_debug"] == {"matched_retrievers": ["hybrid"]}
