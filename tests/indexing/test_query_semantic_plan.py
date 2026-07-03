from types import SimpleNamespace

from src.docset_hub.indexing.query_semantic_plan import build_query_semantic_plan
from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult


def _candidate(text: str, *, kind: str = "connector_split", start: int = 0) -> PhraseCandidate:
    return PhraseCandidate(
        text=text,
        normalized_text=text.lower(),
        kind=kind,
        start=start,
        end=start + len(text),
    )


def _evidence(
    *,
    candidate_text: str,
    normalized_text: str,
    start: int,
    end: int,
    source: str,
    canonical: str,
    concept_id: str,
    confidence: float,
    aliases=None,
    match_type: str = "ontology_exact",
):
    return ConceptMatchEvidence(
        candidate_text=candidate_text,
        normalized_text=normalized_text,
        start=start,
        end=end,
        candidate_kind="connector_split",
        source=source,
        canonical=canonical,
        concept_id=concept_id,
        confidence=confidence,
        aliases=list(aliases or []),
        match_type=match_type,
    )


def test_build_query_semantic_plan_builds_parent_and_child_spans():
    adhesion_candidate = _candidate("adhesion protein", start=0)
    kidney_candidate = _candidate("kidney", start=20)
    adhesion_child = _candidate("adhesion", kind="subphrase_ngram", start=0)
    protein_child = _candidate("protein", kind="subphrase_ngram", start=9)
    selected_concepts = [
        SelectedConcept(
            candidate=adhesion_candidate,
            evidence=[
                _evidence(
                    candidate_text="adhesion protein",
                    normalized_text="adhesion protein",
                    start=0,
                    end=16,
                    source="umls",
                    canonical="Adhesion protein",
                    concept_id="C100",
                    confidence=0.96,
                    aliases=["Cell adhesion protein", "Adhesion molecule"],
                ),
                _evidence(
                    candidate_text="adhesion protein",
                    normalized_text="adhesion protein",
                    start=0,
                    end=16,
                    source="mesh",
                    canonical="Cell adhesion molecule",
                    concept_id="D100",
                    confidence=0.94,
                    aliases=["Cell adhesion molecule"],
                ),
                _evidence(
                    candidate_text="adhesion protein",
                    normalized_text="adhesion protein",
                    start=0,
                    end=16,
                    source="keyword",
                    canonical="CAM",
                    concept_id="keyword:cam",
                    confidence=1.0,
                    aliases=["keyword-only-alias"],
                    match_type="keyword_alias",
                ),
            ],
        ),
        SelectedConcept(
            candidate=kidney_candidate,
            evidence=[
                _evidence(
                    candidate_text="kidney",
                    normalized_text="kidney",
                    start=20,
                    end=26,
                    source="umls",
                    canonical="Kidney",
                    concept_id="C200",
                    confidence=0.95,
                    aliases=["Renal", "Kidney tissue"],
                ),
                _evidence(
                    candidate_text="kidney",
                    normalized_text="kidney",
                    start=20,
                    end=26,
                    source="mesh",
                    canonical="Renal tissue",
                    concept_id="D200",
                    confidence=0.93,
                ),
            ],
        ),
    ]
    span_results = [
        SpanMatchResult(candidate=adhesion_candidate, evidence=selected_concepts[0].evidence),
        SpanMatchResult(
            candidate=adhesion_child,
            evidence=[
                _evidence(
                    candidate_text="adhesion",
                    normalized_text="adhesion",
                    start=0,
                    end=8,
                    source="umls",
                    canonical="Adhesion",
                    concept_id="C110",
                    confidence=0.92,
                    aliases=["Process of adhesion"],
                )
            ],
        ),
        SpanMatchResult(
            candidate=protein_child,
            evidence=[
                _evidence(
                    candidate_text="protein",
                    normalized_text="protein",
                    start=9,
                    end=16,
                    source="mesh",
                    canonical="Protein",
                    concept_id="D110",
                    confidence=0.93,
                    aliases=["Proteins"],
                )
            ],
        ),
        SpanMatchResult(candidate=kidney_candidate, evidence=selected_concepts[1].evidence),
    ]

    plan = build_query_semantic_plan(
        original_query="adhesion protein in kidney",
        normalized_query="adhesion protein in kidney",
        selected_concepts=selected_concepts,
        span_results=span_results,
    )

    assert [group.surface_text for group in plan.spans] == ["adhesion protein", "kidney"]
    assert plan.spans[0].canonical_text == "Adhesion protein"
    assert [(term.text, term.match_mode) for term in plan.spans[0].own_terms.tier1] == [
        ("adhesion protein", "exact")
    ]
    assert [(term.text, term.match_mode) for term in plan.spans[0].own_terms.tier2] == [
        ("adhesion molecule", "exact"),
        ("cell adhesion molecule", "exact"),
        ("cell adhesion protein", "exact"),
    ]
    assert [child.surface_text for child in plan.spans[0].children] == ["adhesion", "protein"]
    assert [(term.text, term.match_mode) for term in plan.spans[0].children[0].own_terms.tier1] == [
        ("adhesion", "exact")
    ]
    assert [(term.text, term.match_mode) for term in plan.spans[0].children[1].own_terms.tier2] == [
        ("proteins", "exact")
    ]
    assert "keyword-only-alias" not in [term.text for term in plan.spans[0].own_terms.tier2]
    assert "cam" not in [term.text for term in plan.spans[0].own_terms.tier2]
    assert [(term.text, term.match_mode) for term in plan.spans[1].own_terms.tier1] == [
        ("kidney", "exact")
    ]
    assert [(term.text, term.match_mode) for term in plan.spans[1].own_terms.tier2] == [
        ("kidney tissue", "exact"),
        ("renal", "exact"),
        ("renal tissue", "exact"),
    ]
    assert plan.spans[1].children == []


def test_build_query_semantic_plan_parses_trailing_dash_alias_as_prefix_term():
    selected = SelectedConcept(
        candidate=_candidate("kidney", start=0),
        evidence=[
            _evidence(
                candidate_text="kidney",
                normalized_text="kidney",
                start=0,
                end=6,
                source="umls",
                canonical="Kidney",
                concept_id="C200",
                confidence=0.95,
                aliases=["Renal", "Renal-"],
            )
        ],
    )

    plan = build_query_semantic_plan(
        original_query="kidney",
        normalized_query="kidney",
        selected_concepts=[selected],
        span_results=[SpanMatchResult(candidate=selected.candidate, evidence=selected.evidence)],
    )

    assert [(term.text, term.match_mode) for term in plan.spans[0].own_terms.tier2] == [
        ("renal", "exact"),
        ("renal", "prefix"),
    ]


def test_build_query_semantic_plan_keeps_keyword_only_span_but_without_tier2_terms():
    selected = SelectedConcept(
        candidate=_candidate("deep learning", start=0),
        evidence=[
            _evidence(
                candidate_text="deep learning",
                normalized_text="deep learning",
                start=0,
                end=13,
                source="keyword",
                canonical="Deep Learning",
                concept_id="keyword:deep learning",
                confidence=1.0,
                aliases=["DL"],
                match_type="keyword_exact",
            )
        ],
    )

    plan = build_query_semantic_plan(
        original_query="deep learning",
        normalized_query="deep learning",
        selected_concepts=[selected],
        span_results=[SpanMatchResult(candidate=selected.candidate, evidence=selected.evidence)],
    )

    assert len(plan.spans) == 1
    assert plan.spans[0].surface_text == "deep learning"
    assert [(term.text, term.match_mode) for term in plan.spans[0].own_terms.tier1] == [
        ("deep learning", "exact")
    ]
    assert plan.spans[0].own_terms.tier2 == []


def test_serialize_semantic_plan_serializes_spans_terms_and_children():
    from src.docset_hub.indexing.query_semantic_plan import serialize_semantic_plan

    plan = SimpleNamespace(
        original_query="renal adhesion",
        normalized_query="renal adhesion",
        spans=[
            SimpleNamespace(
                span_id="s1",
                surface_text="renal",
                normalized_text="renal",
                start=0,
                end=5,
                canonical_text="Renal",
                own_terms=SimpleNamespace(
                    tier1=[SimpleNamespace(text="renal", match_mode="exact")],
                    tier2=[SimpleNamespace(text="kidney", match_mode="exact")],
                ),
                children=[
                    SimpleNamespace(
                        span_id="s1.1",
                        surface_text="ren",
                        normalized_text="ren",
                        start=0,
                        end=3,
                        canonical_text="Ren",
                        own_terms=SimpleNamespace(
                            tier1=[SimpleNamespace(text="ren", match_mode="prefix")],
                            tier2=[],
                        ),
                    )
                ],
            )
        ],
    )

    payload = serialize_semantic_plan(plan)

    assert payload["original_query"] == "renal adhesion"
    assert payload["spans"][0]["span_id"] == "s1"
    assert payload["spans"][0]["own_terms"]["tier1"] == [{"text": "renal", "match_mode": "exact"}]
    assert payload["spans"][0]["own_terms"]["tier2"] == [{"text": "kidney", "match_mode": "exact"}]
    assert payload["spans"][0]["children"][0]["span_id"] == "s1.1"
    assert payload["spans"][0]["children"][0]["own_terms"]["tier1"] == [
        {"text": "ren", "match_mode": "prefix"}
    ]
