from src.docset_hub.indexing.expanded_sparse_retrieval import (
    build_expanded_sparse_groups,
    build_expanded_sparse_query_rows,
    match_papers_by_expanded_sparse_plan,
)
from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.indexing.query_semantic_plan import (
    QuerySemanticPlan,
    SemanticChildSpan,
    SemanticSpanGroup,
    SemanticTerm,
    SemanticTermBucket,
)
from src.docset_hub.indexing.span_matcher import ConceptMatchEvidence


def _group(
    *,
    span_id: str,
    surface_text: str,
    canonical_text: str,
    tier1_terms,
    tier2_terms,
    children=None,
    start: int = 0,
    end: int = 0,
):
    return SemanticSpanGroup(
        span_id=span_id,
        surface_text=surface_text,
        normalized_text=surface_text.lower(),
        start=start,
        end=end,
        canonical_text=canonical_text,
        own_terms=SemanticTermBucket(
            tier1=[SemanticTerm(text=term, match_mode="exact") for term in list(tier1_terms)],
            tier2=[
                term if isinstance(term, SemanticTerm) else SemanticTerm(text=term, match_mode="exact")
                for term in list(tier2_terms)
            ],
        ),
        children=list(children or []),
        evidence=[
            ConceptMatchEvidence(
                candidate_text=surface_text,
                normalized_text=surface_text.lower(),
                start=start,
                end=end,
                candidate_kind="connector_split",
                source="umls",
                canonical=canonical_text,
                concept_id=f"{span_id}:primary",
                confidence=0.95,
            )
        ],
    )


def _child(
    *,
    span_id: str,
    surface_text: str,
    canonical_text: str,
    tier1_terms,
    tier2_terms,
    start: int = 0,
    end: int = 0,
):
    return SemanticChildSpan(
        span_id=span_id,
        surface_text=surface_text,
        normalized_text=surface_text.lower(),
        start=start,
        end=end,
        canonical_text=canonical_text,
        own_terms=SemanticTermBucket(
            tier1=[SemanticTerm(text=term, match_mode="exact") for term in list(tier1_terms)],
            tier2=[
                term if isinstance(term, SemanticTerm) else SemanticTerm(text=term, match_mode="exact")
                for term in list(tier2_terms)
            ],
        ),
        evidence=[
            ConceptMatchEvidence(
                candidate_text=surface_text,
                normalized_text=surface_text.lower(),
                start=start,
                end=end,
                candidate_kind="subphrase_ngram",
                source="umls",
                canonical=canonical_text,
                concept_id=f"{span_id}:primary",
                confidence=0.95,
            )
        ],
    )


def _plan():
    return QuerySemanticPlan(
        original_query="adhesion protein in kidney",
        normalized_query="adhesion protein in kidney",
        spans=[
            _group(
                span_id="s1",
                surface_text="adhesion protein",
                canonical_text="Adhesion protein",
                tier1_terms=["adhesion protein"],
                tier2_terms=[
                    "cell adhesion protein",
                    "adhesion molecule",
                    "cell adhesion molecule",
                ],
                children=[
                    _child(
                        span_id="s1.1",
                        surface_text="adhesion",
                        canonical_text="Adhesion",
                        tier1_terms=["adhesion"],
                        tier2_terms=["process of adhesion"],
                        start=0,
                        end=8,
                    ),
                    _child(
                        span_id="s1.2",
                        surface_text="protein",
                        canonical_text="Protein",
                        tier1_terms=["protein"],
                        tier2_terms=["proteins"],
                        start=9,
                        end=16,
                    ),
                ],
                start=0,
                end=16,
            ),
            _group(
                span_id="s2",
                surface_text="kidney",
                canonical_text="Kidney",
                tier1_terms=["kidney"],
                tier2_terms=[
                    "renal",
                    SemanticTerm(text="renal", match_mode="prefix"),
                    "kidney tissue",
                    "renal tissue",
                ],
                start=20,
                end=26,
            ),
        ],
    )


def test_build_expanded_sparse_groups_keeps_group_boundaries():
    groups = build_expanded_sparse_groups(_plan())

    assert [group.group_id for group in groups] == [1, 2]
    assert groups[0].span_id == "s1"
    assert [(term["text"], term["match_mode"]) for term in groups[0].own_tier1_terms] == [
        ("adhesion protein", "exact")
    ]
    assert groups[0].children[0]["span_id"] == "s1.1"
    assert [(term["text"], term["match_mode"]) for term in groups[0].children[0]["own_tier1_terms"]] == [
        ("adhesion", "exact")
    ]
    assert groups[1].span_id == "s2"
    assert ("renal", "prefix") in [
        (term["text"], term["match_mode"]) for term in groups[1].own_tier2_terms
    ]


class FakeMetadataDB:
    def __init__(self):
        self.calls = []

    def lookup_papers_by_expanded_sparse_groups(
        self,
        span_groups,
        source_list=None,
        keyword_sources=None,
        top_k=50,
    ):
        self.calls.append(
            {
                "span_groups": span_groups,
                "source_list": source_list,
                "keyword_sources": keyword_sources,
                "top_k": top_k,
            }
        )
        return [
            {
                "paper_id": 101,
                "work_id": "W101",
                "matched_span_count": 2,
                "total_span_count": 2,
                "coverage_ratio": 0.75,
                "matched_spans": [
                    {
                        "group_id": 1,
                        "span_id": "s1",
                        "canonical_text": "Adhesion protein",
                        "matched_terms": ["adhesion"],
                        "matched_fields": ["title"],
                        "matched_scopes": ["child"],
                        "matched_child_span_ids": ["s1.1"],
                        "own_term_matched": False,
                        "matched_child_count": 1,
                        "total_child_count": 2,
                        "span_score": 0.5,
                    },
                    {
                        "group_id": 2,
                        "span_id": "s2",
                        "canonical_text": "Kidney",
                        "matched_terms": ["renal"],
                        "matched_fields": ["title"],
                        "matched_scopes": ["parent"],
                        "matched_child_span_ids": [],
                        "own_term_matched": True,
                        "matched_child_count": 0,
                        "total_child_count": 0,
                        "span_score": 1.0,
                    },
                ],
                "retrieval_debug": {"retriever": "expanded_sparse"},
            }
        ]


def test_build_expanded_sparse_query_rows_preserves_term_scope():
    rows = build_expanded_sparse_query_rows(_plan())

    assert {
        "group_id": 1,
        "span_id": "s1",
        "canonical_text": "Adhesion protein",
        "span_scope": "parent",
        "child_span_id": None,
        "term_tier": "tier1",
        "match_mode": "exact",
        "term": "adhesion protein",
    } in rows
    assert {
        "group_id": 1,
        "span_id": "s1",
        "canonical_text": "Adhesion protein",
        "span_scope": "child",
        "child_span_id": "s1.1",
        "term_tier": "tier1",
        "match_mode": "exact",
        "term": "adhesion",
    } in rows
    assert {
        "group_id": 2,
        "span_id": "s2",
        "canonical_text": "Kidney",
        "span_scope": "parent",
        "child_span_id": None,
        "term_tier": "tier2",
        "match_mode": "prefix",
        "term": "renal",
    } in rows


def test_match_papers_by_expanded_sparse_plan_uses_metadata_db_lookup():
    metadata_db = FakeMetadataDB()

    results = match_papers_by_expanded_sparse_plan(
        metadata_db=metadata_db,
        plan=_plan(),
        source_list=["biorxiv_daily"],
        keyword_sources=["paper_metadata"],
        top_k=7,
    )

    assert len(metadata_db.calls) == 1
    call = metadata_db.calls[0]
    assert call["source_list"] == ["biorxiv_daily"]
    assert call["keyword_sources"] == ["paper_metadata"]
    assert call["top_k"] == 7
    assert call["span_groups"][0]["span_id"] == "s1"
    assert call["span_groups"][0]["span_scope"] == "parent"
    assert any(row["child_span_id"] == "s1.1" for row in call["span_groups"])
    assert any(row["match_mode"] == "prefix" and row["term"] == "renal" for row in call["span_groups"])

    assert len(results) == 1
    assert results[0].work_id == "W101"
    assert results[0].matched_span_count == 2
    assert results[0].coverage_ratio == 0.75
    assert results[0].matched_spans[0]["matched_terms"] == ["adhesion"]
    assert results[0].matched_spans[0]["matched_scopes"] == ["child"]
    assert results[0].matched_spans[0]["span_score"] == 0.5


def test_paper_indexer_builds_semantic_plan_with_ontology_plus_keyword_profile(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.metadata_db = object()
    indexer.default_sources = ["langtaosha"]
    captured = {}

    class FakePipeline:
        def run(self, query):
            captured["query"] = query
            return type(
                "Result",
                (),
                {
                    "selected_concepts": [object()],
                    "semantic_plan": {
                        "original_query": query,
                        "spans": [{"span_id": "s1"}],
                    },
                },
            )()

    def fake_from_profile(*, profile, metadata_db):
        captured["profile_name"] = profile.name
        captured["enable_ontology"] = profile.enable_ontology
        captured["enable_keyword"] = profile.enable_keyword
        captured["paper_sources"] = profile.paper_sources
        captured["keyword_sources"] = profile.keyword_sources
        captured["metadata_db"] = metadata_db
        return FakePipeline()

    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.SpanMatcherPipeline.from_profile",
        fake_from_profile,
    )

    plan = indexer.build_query_semantic_plan(
        query="adhesion protein in kidney",
        source_list=["biorxiv_daily"],
        keyword_sources=["paper_metadata"],
    )

    assert plan["original_query"] == "adhesion protein in kidney"
    assert captured["query"] == "adhesion protein in kidney"
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["enable_ontology"] is True
    assert captured["enable_keyword"] is True
    assert captured["paper_sources"] == ("biorxiv_daily",)
    assert captured["keyword_sources"] == ("paper_metadata",)
    assert captured["metadata_db"] is indexer.metadata_db


def test_paper_indexer_runs_expanded_sparse_retrieval_branch(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.metadata_db = object()
    plan = _plan()

    monkeypatch.setattr(
        indexer,
        "build_query_semantic_plan",
        lambda query, source_list, keyword_sources=None: plan,
    )
    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.match_papers_by_expanded_sparse_plan",
        lambda metadata_db, plan, source_list, keyword_sources=None, top_k=50: [
            type(
                "Candidate",
                (),
                {
                    "paper_id": 101,
                    "work_id": "W101",
                    "matched_span_count": 2,
                    "total_span_count": 2,
                    "coverage_ratio": 1.0,
                    "matched_spans": [{"span_id": "s1", "matched_scopes": ["parent", "child"]}],
                    "retrieval_debug": {"retriever": "expanded_sparse"},
                },
            )()
        ],
    )

    branch_results = indexer._run_expanded_sparse_retrieval_branch(
        query="adhesion protein in kidney",
        source_list=["biorxiv_daily"],
        top_k=5,
        keyword_sources=["paper_metadata"],
    )

    assert len(branch_results) == 1
    assert branch_results[0]["paper_id"] == 101
    assert branch_results[0]["work_id"] == "W101"
    assert branch_results[0]["retrieval_debug"]["retriever"] == "expanded_sparse"
    assert branch_results[0]["retrieval_debug"]["matched_span_count"] == 2
    assert branch_results[0]["retrieval_debug"]["matched_spans"][0]["matched_scopes"] == ["parent", "child"]
