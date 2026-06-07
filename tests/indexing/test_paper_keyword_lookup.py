from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.paper_keyword_lookup import (
    ROUTE_WEIGHTS,
    build_keyword_lookup_plan,
    build_keyword_lookup_terms_from_selected_concepts,
    match_paper_keywords_with_lookup_plan,
    match_paper_keywords_using_span_matcher,
)
from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult


def _candidate(
    text: str,
    normalized_text: str | None = None,
    kind: str = "connector_split",
    start: int = 0,
    end: int | None = None,
) -> PhraseCandidate:
    return PhraseCandidate(
        text=text,
        normalized_text=normalized_text or text.lower(),
        kind=kind,
        start=start,
        end=len(text) if end is None else end,
    )


def _evidence(
    canonical: str,
    source: str = "keyword",
    concept_id: str | None = None,
    aliases: list[str] | None = None,
    confidence: float = 1.0,
    match_type: str = "unknown",
    payload: dict | None = None,
) -> ConceptMatchEvidence:
    return ConceptMatchEvidence(
        candidate_text=canonical,
        normalized_text=canonical.lower(),
        start=0,
        end=len(canonical),
        candidate_kind="connector_split",
        source=source,
        canonical=canonical,
        concept_id=concept_id,
        confidence=confidence,
        match_type=match_type,
        aliases=aliases or [],
        payload=payload or {},
    )


def _selected_concept(
    text: str,
    primary: ConceptMatchEvidence,
    evidence: list[ConceptMatchEvidence] | None = None,
    normalized_text: str | None = None,
) -> SelectedConcept:
    return SelectedConcept(
        candidate=_candidate(text, normalized_text=normalized_text),
        evidence=[primary] + list(evidence or []),
    )


def test_route_weights_follow_implementation_log_values():
    assert ROUTE_WEIGHTS["selected_surface_variant"] == 0.90
    assert ROUTE_WEIGHTS["sub_concept_surface_variant"] == 0.90
    assert ROUTE_WEIGHTS["db_substring_candidate"] == 0.50
    assert ROUTE_WEIGHTS["db_prefix_candidate"] == 0.50
    assert ROUTE_WEIGHTS["db_trigram_candidate"] == 0.50
    assert ROUTE_WEIGHTS["db_compositional_candidate"] == 0.70


def test_build_keyword_lookup_terms_expands_and_dedupes_selected_concepts():
    concept = _selected_concept(
        "T cell",
        primary=_evidence(
            "T-cell",
            source="umls",
            concept_id="C0039194",
            aliases=["T lymphocyte", "T cells"],
        ),
        evidence=[
            _evidence("T cell", source="keyword", aliases=["T lymphocyte"]),
        ],
        normalized_text="t cell",
    )

    terms = build_keyword_lookup_terms_from_selected_concepts([concept])
    by_term = {term.term: term for term in terms}

    assert by_term["t-cell"].term_role == "primary_canonical"
    assert by_term["t-cell"].term_weight == 1.0
    assert by_term["t cell"].term_role == "canonical"
    assert by_term["t cell"].term_weight == 0.95
    assert by_term["t lymphocyte"].term_role == "alias"
    assert by_term["t lymphocyte"].term_weight == 0.8
    assert {term.concept_idx for term in terms} == {1}
    assert {term.concept_id for term in terms} == {"C0039194"}


def test_build_keyword_lookup_terms_respects_max_terms_per_concept():
    concept = _selected_concept(
        "melanoma",
        primary=_evidence(
            "Melanoma",
            concept_id="C0025202",
            aliases=["Malignant Melanoma", "Melanomas", "Skin Melanoma"],
        ),
    )

    terms = build_keyword_lookup_terms_from_selected_concepts(
        [concept],
        max_terms_per_concept=2,
    )

    assert [term.term for term in terms] == ["melanoma", "malignant melanoma"]


def test_build_keyword_lookup_plan_groups_sub_concepts_with_lower_weights():
    selected = SelectedConcept(
        candidate=_candidate("adhesion protein", start=0, end=16),
        evidence=[_evidence("adhesion protein", concept_id="keyword:adhesion protein")],
    )
    adhesion = SpanMatchResult(
        candidate=_candidate("adhesion", kind="subphrase_ngram", start=0, end=8),
        evidence=[_evidence("adhesion", concept_id="keyword:adhesion")],
    )
    protein = SpanMatchResult(
        candidate=_candidate("protein", kind="subphrase_ngram", start=9, end=16),
        evidence=[_evidence("protein", concept_id="keyword:protein")],
    )

    terms = build_keyword_lookup_plan([selected], span_results=[adhesion, protein])
    by_term = {
        (term.item_role, term.term): term
        for term in terms
        if term.term_role == "primary_canonical"
    }

    assert by_term[("selected_concept", "adhesion protein")].group_id == 1
    assert by_term[("selected_concept", "adhesion protein")].route == "selected_exact"
    assert by_term[("sub_concept", "adhesion")].group_id == 1
    assert by_term[("sub_concept", "adhesion")].item_weight == 0.55
    assert by_term[("sub_concept", "adhesion")].route == "sub_concept_exact"
    assert by_term[("broad_sub_concept", "protein")].group_id == 1
    assert by_term[("broad_sub_concept", "protein")].item_weight == 0.30


def test_build_keyword_lookup_plan_classifies_surface_variant_routes():
    concept = _selected_concept(
        "enhancer-promoter interaction",
        primary=_evidence(
            "enhancer-promoter interactions",
            concept_id="keyword:enhancer-promoter interactions",
            match_type="keyword_normalized",
            payload={"surface_match_type": "plural_variant"},
        ),
    )

    terms = build_keyword_lookup_plan([concept], include_sub_concepts=False)
    by_term = {term.term: term for term in terms}

    assert by_term["enhancer-promoter interactions"].route == "selected_surface_variant"
    assert by_term["enhancer-promoter interactions"].route_weight == 0.90


class FakeMetadataDB:
    def __init__(self):
        self.calls = []

    def lookup_papers_by_keyword_terms(self, **kwargs):
        self.calls.append(kwargs)
        return [
            {
                "paper_id": 101,
                "work_id": "W101",
                "matched_concept_count": 2,
                "total_concept_count": 2,
                "keyword_lookup_score": 1.8,
                "matched_concepts": [{"concept_idx": 1}, {"concept_idx": 2}],
                "recall_sources": ["keyword_lookup"],
                "retrieval_debug": {"retriever": "keyword_lookup"},
            }
        ]

    def lookup_papers_by_keyword_lookup_terms(self, **kwargs):
        self.calls.append(kwargs)
        return [
            {
                "paper_id": 202,
                "work_id": "W202",
                "matched_group_count": 1,
                "matched_primary_group_count": 0,
                "total_group_count": 1,
                "keyword_lookup_score": 0.7,
                "matched_concepts": [{"group_id": 1}],
                "recall_sources": ["keyword_lookup"],
                "retrieval_debug": {"retriever": "keyword_lookup"},
            }
        ]

    def suggest_query_terms(self, query, **kwargs):
        if "exhaust" not in query:
            return []
        return [
            {
                "keyword": "T-cell exhaustion",
                "keyword_type": "concept",
                "source": "generated",
                "doc_count": 1,
                "avg_weight": 1.0,
            },
            {
                "keyword": "exhaustion",
                "keyword_type": "concept",
                "source": "generated",
                "doc_count": 6,
                "avg_weight": 0.4,
            },
        ]


def test_match_paper_keywords_using_span_matcher_delegates_to_metadata_db():
    selected = [
        _selected_concept("melanoma", _evidence("Melanoma", concept_id="C0025202")),
        _selected_concept("deep learning", _evidence("Deep Learning", concept_id="C4704761")),
    ]
    metadata_db = FakeMetadataDB()

    results = match_paper_keywords_using_span_matcher(
        metadata_db=metadata_db,
        selected_concepts=selected,
        source_list=["biorxiv_daily"],
        keyword_sources=["scispacy-en_core_sci_lg-generated"],
        top_k=5,
    )

    assert len(results) == 1
    assert results[0].paper_id == 101
    assert results[0].recall_sources == ["keyword_lookup"]
    call = metadata_db.calls[0]
    assert call["source_list"] == ["biorxiv_daily"]
    assert call["keyword_sources"] == ["scispacy-en_core_sci_lg-generated"]
    assert call["top_k"] == 5
    assert {term["concept_idx"] for term in call["query_terms"]} == {1, 2}


def test_match_paper_keywords_using_span_matcher_returns_empty_without_concepts():
    metadata_db = FakeMetadataDB()

    assert match_paper_keywords_using_span_matcher(metadata_db, []) == []
    assert metadata_db.calls == []


def test_match_paper_keywords_using_span_matcher_rejects_raw_query_terms():
    metadata_db = FakeMetadataDB()

    with pytest.raises(TypeError, match="SelectedConcept"):
        match_paper_keywords_using_span_matcher(metadata_db, ["genome"])

    assert metadata_db.calls == []


def test_match_paper_keywords_with_lookup_plan_adds_db_candidate_routes():
    selected = [
        _selected_concept(
            "exhausted t cell",
            _evidence(
                "Exhausted T-Cell",
                source="umls",
                concept_id="C3899239",
                aliases=["Exhausted T Cell"],
            ),
            normalized_text="exhausted t cell",
        )
    ]
    metadata_db = FakeMetadataDB()

    results = match_paper_keywords_with_lookup_plan(
        metadata_db=metadata_db,
        selected_concepts=selected,
        span_results=[],
        keyword_sources=["generated"],
    )

    assert results[0].paper_id == 202
    call = metadata_db.calls[0]
    candidate_terms = {
        term["term"]: term
        for term in call["query_terms"]
        if term["item_role"] == "substring_candidate"
    }
    assert candidate_terms["t-cell exhaustion"]["route"] == "db_compositional_candidate"
    assert candidate_terms["t-cell exhaustion"]["route_weight"] == 0.70
    assert candidate_terms["exhaustion"]["route"] == "db_substring_candidate"
    assert candidate_terms["exhaustion"]["route_weight"] == 0.50
