from src.docset_hub.indexing.coverage_engine import (
    analyze_document_coverage,
    summarize_expanded_sparse_matches,
)
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
):
    return SemanticSpanGroup(
        span_id=span_id,
        surface_text=surface_text,
        normalized_text=surface_text.lower(),
        start=0,
        end=len(surface_text),
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
                start=0,
                end=len(surface_text),
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
):
    return SemanticChildSpan(
        span_id=span_id,
        surface_text=surface_text,
        normalized_text=surface_text.lower(),
        start=0,
        end=len(surface_text),
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
                start=0,
                end=len(surface_text),
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
                    ),
                    _child(
                        span_id="s1.2",
                        surface_text="protein",
                        canonical_text="Protein",
                        tier1_terms=["protein"],
                        tier2_terms=["proteins"],
                    ),
                ],
            ),
            _group(
                span_id="s2",
                surface_text="kidney",
                canonical_text="Kidney",
                tier1_terms=["kidney"],
                tier2_terms=["renal", SemanticTerm(text="renal", match_mode="prefix"), "kidney tissue", "renal tissue"],
            ),
        ],
    )


def test_coverage_engine_returns_full_coverage_for_alias_hits():
    result = analyze_document_coverage(
        plan=_plan(),
        document_fields={
            "title": "Cell adhesion molecule in renal epithelial injury",
            "abstract": "",
            "paper_keywords": [],
        },
    )

    assert result.matched_span_count == 2
    assert result.total_span_count == 2
    assert result.coverage_ratio == 1.0
    assert result.matched_spans[0]["span_score"] == 1.0
    assert result.matched_spans[0]["own_term_matched"] is True
    assert result.matched_spans[0]["matched_terms"] == ["cell adhesion molecule"]
    assert result.matched_spans[1]["matched_terms"] == ["renal"]
    assert result.matched_spans[1]["matched_scopes"] == ["parent"]
    assert result.matched_spans[1]["span_score"] == 1.0


def test_coverage_engine_returns_half_coverage_when_one_span_is_missing():
    result = analyze_document_coverage(
        plan=_plan(),
        document_fields={
            "title": "Expression in kidney disease",
            "abstract": "",
            "paper_keywords": [],
        },
    )

    assert result.matched_span_count == 1
    assert result.total_span_count == 2
    assert result.coverage_ratio == 0.5
    assert [item["span_id"] for item in result.missing_spans] == ["s1"]
    assert result.matched_spans[0]["span_score"] == 1.0


def test_coverage_engine_grants_partial_child_credit_when_parent_terms_do_not_hit():
    result = analyze_document_coverage(
        plan=_plan(),
        document_fields={
            "title": "Adhesion control in epithelial tissue",
            "abstract": "",
            "paper_keywords": [],
        },
    )

    assert result.matched_span_count == 1
    assert result.matched_spans[0]["span_id"] == "s1"
    assert result.matched_spans[0]["matched_terms"] == ["adhesion"]
    assert result.matched_spans[0]["matched_scopes"] == ["child"]
    assert result.matched_spans[0]["matched_child_span_ids"] == ["s1.1"]
    assert result.matched_spans[0]["own_term_matched"] is False
    assert result.matched_spans[0]["matched_child_count"] == 1
    assert result.matched_spans[0]["total_child_count"] == 2
    assert result.matched_spans[0]["span_score"] == 0.5
    assert result.coverage_ratio == 0.25
    assert [item["span_id"] for item in result.missing_spans] == ["s2"]


def test_coverage_engine_supports_prefix_term_matching_without_substring_false_positive():
    positive = analyze_document_coverage(
        plan=QuerySemanticPlan(
            original_query="kidney",
            normalized_query="kidney",
            spans=[
                _group(
                    span_id="s1",
                    surface_text="kidney",
                    canonical_text="Kidney",
                    tier1_terms=["kidney"],
                    tier2_terms=[SemanticTerm(text="renal", match_mode="prefix")],
                )
            ],
        ),
        document_fields={"title": "renalac transport", "abstract": "", "paper_keywords": []},
    )
    negative = analyze_document_coverage(
        plan=QuerySemanticPlan(
            original_query="kidney",
            normalized_query="kidney",
            spans=[
                _group(
                    span_id="s1",
                    surface_text="kidney",
                    canonical_text="Kidney",
                    tier1_terms=["kidney"],
                    tier2_terms=[SemanticTerm(text="renal", match_mode="prefix")],
                )
            ],
        ),
        document_fields={"title": "adrenal signaling", "abstract": "", "paper_keywords": []},
    )

    assert positive.matched_span_count == 1
    assert positive.matched_spans[0]["matched_terms"] == ["renal"]
    assert negative.matched_span_count == 0


def test_coverage_engine_exact_term_requires_token_boundary():
    positive = analyze_document_coverage(
        plan=QuerySemanticPlan(
            original_query="ren",
            normalized_query="ren",
            spans=[
                _group(
                    span_id="s1",
                    surface_text="ren",
                    canonical_text="Ren",
                    tier1_terms=["ren"],
                    tier2_terms=[],
                )
            ],
        ),
        document_fields={"title": "ren signaling", "abstract": "", "paper_keywords": []},
    )
    negative = analyze_document_coverage(
        plan=QuerySemanticPlan(
            original_query="ren",
            normalized_query="ren",
            spans=[
                _group(
                    span_id="s1",
                    surface_text="ren",
                    canonical_text="Ren",
                    tier1_terms=["ren"],
                    tier2_terms=[],
                )
            ],
        ),
        document_fields={"title": "currents analysis", "abstract": "", "paper_keywords": []},
    )

    assert positive.matched_span_count == 1
    assert negative.matched_span_count == 0


def test_summarize_expanded_sparse_matches_matches_in_memory_coverage():
    result = summarize_expanded_sparse_matches(
        plan=_plan(),
        matched_spans=[
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
    )

    assert result.matched_span_count == 2
    assert result.total_span_count == 2
    assert result.coverage_ratio == 0.75
