from __future__ import annotations

import pytest

from src.docset_hub.evaluation.contracts import RankedDocument
from src.docset_hub.evaluation.search_strategies import (
    HybridRetrievalSearchStrategy,
    PaperIndexerSearchStrategy,
    normalize_results,
)


class FakeIndexer:
    def __init__(self):
        self.search_calls: list[dict] = []
        self.hybrid_calls: list[dict] = []

    def search(self, **kwargs):
        self.search_calls.append(kwargs)
        return [
            {
                "work_id": "W1",
                "similarity": 0.9,
                "retrieval_debug": {},
            }
        ]

    def hybrid_retrieval_search(self, **kwargs):
        self.hybrid_calls.append(kwargs)
        return [
            {
                "work_id": "W2",
                "similarity": 0.7,
                "retrieval_debug": {"matched_retrievers": ["dense", "sparse"]},
            }
        ]


def test_paper_indexer_strategy_calls_dense_search_without_hydration():
    fake_indexer = FakeIndexer()
    strategy = PaperIndexerSearchStrategy(indexer=fake_indexer, search_type="dense")

    results = strategy.search("synapse", top_k=10)

    assert fake_indexer.search_calls == [
        {
            "query": "synapse",
            "source_list": None,
            "top_k": 10,
            "hydrate": False,
            "search_type": "dense",
        }
    ]
    assert results == [RankedDocument(work_id="W1", rank=1, score=0.9, retrieval_debug={})]


def test_hybrid_retrieval_strategy_calls_three_way_method():
    fake_indexer = FakeIndexer()
    strategy = HybridRetrievalSearchStrategy(indexer=fake_indexer, source_list=["biorxiv_history"])

    results = strategy.search("exhausted t cell", top_k=5)

    assert fake_indexer.hybrid_calls == [
        {
            "query": "exhausted t cell",
            "source_list": ["biorxiv_history"],
            "top_k": 5,
            "hydrate": False,
        }
    ]
    assert results[0].work_id == "W2"
    assert results[0].retrieval_debug == {"matched_retrievers": ["dense", "sparse"]}


def test_normalize_results_rejects_result_without_work_id():
    with pytest.raises(ValueError, match="work_id"):
        normalize_results([{"similarity": 0.8}])
