from __future__ import annotations

import time

from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.indexing.paper_keyword_lookup import PaperKeywordLookupResult
from src.docset_hub.indexing.retrieval_helper import (
    DEFAULT_HYBRID_RETRIEVAL_WEIGHTS,
    RankedResult,
    RetrievalTimings,
    filter_dense_results,
    filter_positive_score_results,
    from_expanded_sparse_candidate,
    from_keyword_lookup_result,
    from_search_result,
    hits_to_branch_results,
    present_search_results,
    resolve_hybrid_retrieval_weights,
    retrieval_dedupe_key,
    run_retrievers_parallel,
    timed_section,
    to_lightweight_dicts,
    weighted_rrf_merge,
)
from src.docset_hub.storage.vector_db import SearchResult


class FakeMetadataDB:
    def __init__(self, papers=None):
        self.papers = papers or {}
        self.read_calls = []

    def read_paper_by_work_id(self, work_id):
        self.read_calls.append(work_id)
        return self.papers.get(work_id)

    def lookup_papers_with_keyword_terms(self, **kwargs):
        return []


class FakeBatchMetadataDB(FakeMetadataDB):
    def __init__(self, papers=None):
        super().__init__(papers=papers)
        self.batch_calls = []

    def get_search_result_summaries_by_work_ids(self, work_ids):
        self.batch_calls.append(list(work_ids))
        return {
            work_id: self.papers[work_id]
            for work_id in work_ids
            if work_id in self.papers
        }


def test_present_search_results_maps_ranked_result_to_api_dict():
    hit = RankedResult(
        work_id="W1",
        paper_id=1,
        source_name="langtaosha",
        score=0.88,
        text_type="abstract",
        retriever="dense",
        rank=1,
    )
    rows = present_search_results([hit], metadata_db=FakeMetadataDB(), hydrate=False)
    assert rows[0]["work_id"] == "W1"
    assert rows[0]["similarity"] == 0.88


def test_present_search_results_hydrates_metadata():
    hit = RankedResult(
        work_id="W1",
        paper_id=1,
        source_name="langtaosha",
        score=0.88,
        text_type="abstract",
        retriever="dense",
        rank=1,
    )
    metadata_db = FakeMetadataDB(
        papers={
            "W1": {
                "paper_id": 1,
                "work_id": "W1",
                "source_name": "langtaosha",
                "canonical_title": "Example",
            }
        }
    )
    rows = present_search_results([hit], metadata_db=metadata_db, hydrate=True)
    assert rows[0]["metadata"]["canonical_title"] == "Example"


def test_present_search_results_reuses_metadata_for_duplicate_work_ids():
    hits = [
        RankedResult("W1", 1, "langtaosha", 0.88, "title", "dense", 1),
        RankedResult("W1", 1, "langtaosha", 0.77, "abstract", "sparse", 2),
    ]
    metadata_db = FakeMetadataDB(
        papers={
            "W1": {
                "paper_id": 1,
                "work_id": "W1",
                "source_name": "langtaosha",
                "canonical_title": "Example",
            }
        }
    )

    rows = present_search_results(hits, metadata_db=metadata_db, hydrate=True)

    assert len(rows) == 2
    assert [row["metadata"]["canonical_title"] for row in rows] == ["Example", "Example"]
    assert metadata_db.read_calls == ["W1"]


def test_present_search_results_uses_batch_summary_hydration():
    hits = [
        RankedResult("W1", 1, "langtaosha", 0.88, "title", "dense", 1),
        RankedResult("W2", 2, "langtaosha", 0.77, "abstract", "sparse", 2),
    ]
    metadata_db = FakeBatchMetadataDB(
        papers={
            "W1": {"paper_id": 1, "work_id": "W1", "canonical_title": "One"},
            "W2": {"paper_id": 2, "work_id": "W2", "canonical_title": "Two"},
        }
    )

    rows = present_search_results(hits, metadata_db=metadata_db, hydrate=True)

    assert [row["metadata"]["canonical_title"] for row in rows] == ["One", "Two"]
    assert metadata_db.batch_calls == [["W1", "W2"]]
    assert metadata_db.read_calls == []


def test_from_search_result_and_to_lightweight_dicts():
    result = SearchResult("langtaosha", "W2", 0.75, "abstract", "2")
    hit = from_search_result(result, retriever="dense", rank=1)
    rows = to_lightweight_dicts([hit])
    assert rows[0]["work_id"] == "W2"
    assert rows[0]["similarity"] == 0.75


def test_filter_positive_score_results_drops_non_positive():
    hits = [
        RankedResult("W0", 0, "langtaosha", 0.0, "abstract", "sparse", 1),
        RankedResult("W1", 1, "langtaosha", 3.2, "abstract", "sparse", 2),
    ]
    filtered = filter_positive_score_results(hits, drop_non_positive=True)
    assert [item.work_id for item in filtered] == ["W1"]
    assert filtered[0].rank == 1


def test_filter_dense_results_applies_hard_rules(monkeypatch):
    hits = [
        RankedResult("W1", 1, "langtaosha", 0.9, "abstract", "dense", 1),
        RankedResult("W2", 2, "langtaosha", 0.2, "abstract", "dense", 2),
    ]

    def fake_filter(**kwargs):
        kept = [item for item in kwargs["results"] if item["work_id"] == "W1"]
        report = type("Report", (), {"to_dict": lambda self: {"kept_count": 1}})()
        return kept, report

    monkeypatch.setattr(
        "src.docset_hub.indexing.retrieval_helper.filter_dense_results_by_hard_rules",
        fake_filter,
    )
    filtered, report = filter_dense_results(
        hits,
        query="renal adhesion",
        metadata_db=FakeMetadataDB(),
        min_similarity=0.46,
    )
    assert [item.work_id for item in filtered] == ["W1"]
    assert report["kept_count"] == 1


def test_from_keyword_lookup_result_and_branch_adapter():
    lookup = PaperKeywordLookupResult(
        2,
        "kw-positive",
        1,
        1,
        0.7,
        [{"group_id": 1}],
        ["keyword_lookup"],
        {"retriever": "keyword_lookup"},
    )
    hit = from_keyword_lookup_result(lookup, rank=1)
    branch = hits_to_branch_results([hit])[0]
    assert branch["work_id"] == "kw-positive"
    assert branch["retrieval_debug"]["matched_concepts"] == [{"group_id": 1}]


def test_from_expanded_sparse_candidate():
    candidate = type(
        "Candidate",
        (),
        {
            "paper_id": 101,
            "work_id": "W101",
            "matched_span_count": 2,
            "total_span_count": 2,
            "coverage_ratio": 1.0,
            "matched_spans": [{"span_id": "s1"}],
            "retrieval_debug": {"retriever": "expanded_sparse"},
        },
    )()
    hit = from_expanded_sparse_candidate(candidate, rank=1)
    assert hit.work_id == "W101"
    assert hit.score == 1.0
    assert hit.retriever == "expanded_sparse"


def test_weighted_rrf_merge_promotes_multi_branch_hits():
    branch_results = {
        "dense": hits_to_branch_results(
            [RankedResult("shared", 2, "langtaosha", 0.85, "abstract", "dense", 2)]
        ),
        "sparse": hits_to_branch_results(
            [RankedResult("shared", 2, "langtaosha", 13.0, "abstract", "sparse", 1)]
        ),
        "keyword_lookup": hits_to_branch_results(
            [RankedResult("shared", 2, "langtaosha", 1.6, "", "keyword_lookup", 1)]
        ),
    }
    fused = weighted_rrf_merge(
        branch_results,
        top_k=1,
        weights=DEFAULT_HYBRID_RETRIEVAL_WEIGHTS,
        rrf_k=60,
    )
    assert fused[0].work_id == "shared"
    assert fused[0].retrieval_debug["matched_retrievers"] == [
        "dense",
        "sparse",
        "keyword_lookup",
    ]


def test_run_retrievers_parallel_collects_failures():
    def ok_branch(query, source_list, top_k):
        return [RankedResult("W1", 1, "langtaosha", 1.0, "abstract", "sparse", 1)]

    def fail_branch(query, source_list, top_k):
        raise RuntimeError("branch down")

    results, failures = run_retrievers_parallel(
        {"sparse": ok_branch, "dense": fail_branch},
        query="p53",
        source_list=["langtaosha"],
        top_k=3,
    )
    assert results["sparse"][0].work_id == "W1"
    assert failures["dense"] == "branch down"


def test_retrieval_timings_and_timed_section():
    sink: dict[str, float] = {}
    timings = RetrievalTimings(sink)
    with timed_section(timings, "recall"):
        time.sleep(0.001)
    assert "recall" in sink
    assert sink["recall"] >= 0


def test_retrieval_dedupe_key_and_resolve_weights():
    assert retrieval_dedupe_key("W1", 1, "dense", 1) == "work:W1"
    assert resolve_hybrid_retrieval_weights({"dense": 0.0, "sparse": 0.0, "keyword_lookup": 0.0}) == DEFAULT_HYBRID_RETRIEVAL_WEIGHTS


def test_dense_search_returns_ranked_result_without_metadata():
    indexer = PaperIndexer.__new__(PaperIndexer)

    class FakeVectorDB:
        def dense_search(self, query, source_list, top_k):
            return [SearchResult("langtaosha", "W1", 0.91, "abstract", "1")]

    indexer.vector_db = FakeVectorDB()
    hits = indexer.dense_search("renal adhesion", ["langtaosha"], top_k=5)
    assert all(isinstance(hit, RankedResult) for hit in hits)
    assert hits[0].work_id == "W1"
    assert hits[0].retriever == "dense"


def test_expanded_sparse_search_forwards_keyword_sources_to_plan_and_lookup(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.metadata_db = object()
    captured_plan = {}
    captured_lookup = {}

    def fake_build_plan(query, source_list, keyword_sources=None, profile_name="ontology_plus_keyword"):
        captured_plan["keyword_sources"] = keyword_sources
        captured_plan["source_list"] = source_list
        return object()

    def fake_match(*, metadata_db, plan, source_list, keyword_sources=None, top_k=50):
        captured_lookup["keyword_sources"] = keyword_sources
        captured_lookup["top_k"] = top_k
        return [
            type(
                "Candidate",
                (),
                {
                    "paper_id": 101,
                    "work_id": "W101",
                    "matched_span_count": 1,
                    "total_span_count": 2,
                    "coverage_ratio": 0.5,
                    "matched_spans": [],
                    "retrieval_debug": {},
                },
            )()
        ]

    monkeypatch.setattr(indexer, "build_query_semantic_plan", fake_build_plan)
    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.match_papers_by_expanded_sparse_plan",
        fake_match,
    )

    hits = indexer.expanded_sparse_search(
        "adhesion protein in kidney",
        ["biorxiv_daily"],
        top_k=7,
        keyword_sources=["paper_metadata"],
    )

    assert captured_plan["keyword_sources"] == ["paper_metadata"]
    assert captured_lookup["keyword_sources"] == ["paper_metadata"]
    assert captured_lookup["top_k"] == 7
    assert len(hits) == 1
    assert isinstance(hits[0], RankedResult)
