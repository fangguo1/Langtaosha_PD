import pytest

from src.docset_hub.indexing.query_phrase_analyzer import (
    InMemoryPhraseLexicon,
    PhraseCandidate,
)
from src.docset_hub.indexing.span_matcher import (
    CompositeSpanMatcher,
    ConceptMatchEvidence,
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    OntologyLinkerConfigurationError,
    OntologyLinkerServiceUnavailable,
    RemoteOntologySpanMatcher,
    SpanMatcherExecutor,
    SpanMatchResult,
    SubphraseCandidateGenerator,
)


def candidate(text, kind="scispacy_entity", start=0):
    return PhraseCandidate(
        text=text,
        normalized_text=text.lower(),
        kind=kind,
        start=start,
        end=start + len(text),
    )


def test_keyword_span_matcher_returns_surface_evidence():
    lexicon = InMemoryPhraseLexicon(
        entries={
            "T-cell": {
                "canonical": "T-cell",
                "doc_count": 62,
                "variant_count": 97,
                "matched_phrase_count": 241,
            }
        }
    )
    matcher = KeywordSurfaceSpanMatcher(lexicon)

    evidence = matcher.match(candidate("T cell", kind="connector_split"))[0]

    assert evidence.source == "keyword"
    assert evidence.concept_id == "keyword:t-cell"
    assert evidence.canonical == "T-cell"
    assert evidence.match_type == "keyword_normalized"
    assert evidence.payload["doc_count"] == 62
    assert evidence.candidate_kind == "connector_split"


def test_keyword_span_matcher_uses_alias_records():
    lexicon = InMemoryPhraseLexicon(
        entries={"programmed cell death protein 1": {"canonical": "PD-1", "doc_count": 4}},
        aliases={"PD1": "programmed cell death protein 1"},
    )
    matcher = KeywordSurfaceSpanMatcher(lexicon)

    evidence = matcher.match(candidate("PD1"))[0]

    assert evidence.source == "keyword"
    assert evidence.canonical == "PD-1"
    assert evidence.match_type == "keyword_alias"


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def post(self, url, json, timeout):
        self.requests.append({"url": url, "json": json, "timeout": timeout})
        if self.error:
            raise self.error
        return self.response


def test_remote_ontology_span_matcher_batches_candidates_and_maps_evidence():
    session = FakeSession(
        FakeResponse(
            payload={
                "results": [
                    {
                        "candidate_id": "c0",
                        "evidence": [
                            {
                                "source": "umls",
                                "concept_id": "C0025202",
                                "canonical": "Melanoma",
                                "confidence": 0.98,
                                "aliases": ["Malignant Melanoma", "Melanomas"],
                                "semantic_types": ["T191"],
                            }
                        ],
                    },
                    {"candidate_id": "c1", "evidence": []},
                ]
            }
        )
    )
    matcher = RemoteOntologySpanMatcher(
        "http://127.0.0.1:8765/",
        sources=("umls", "mesh"),
        top_k=2,
        threshold=0.75,
        session=session,
    )

    buckets = matcher.match_many([candidate("melanoma", start=21), candidate("automation")])

    assert session.requests[0]["url"] == "http://127.0.0.1:8765/v1/link"
    assert session.requests[0]["json"]["sources"] == ["umls", "mesh"]
    assert session.requests[0]["json"]["candidates"][0]["id"] == "c0"
    assert buckets[0][0].source == "umls"
    assert buckets[0][0].concept_id == "C0025202"
    assert buckets[0][0].canonical == "Melanoma"
    assert buckets[0][0].aliases == ["Malignant Melanoma", "Melanomas"]
    assert buckets[0][0].start == 21
    serialized = SpanMatchResult(candidate("melanoma", start=21), buckets[0]).to_dict()
    assert serialized["evidence"][0]["aliases"] == ["Malignant Melanoma", "Melanomas"]
    assert buckets[1] == []


def test_remote_ontology_span_matcher_promotes_nested_payload_aliases_to_evidence_field():
    session = FakeSession(
        FakeResponse(
            payload={
                "results": [
                    {
                        "candidate_id": "c0",
                        "evidence": [
                            {
                                "source": "umls",
                                "concept_id": "C0025202",
                                "canonical": "Melanoma",
                                "confidence": 0.98,
                                "payload": {
                                    "aliases": ["Malignant Melanoma", "Melanomas"],
                                    "definition": "A malignant neoplasm of melanocytes.",
                                },
                            }
                        ],
                    }
                ]
            }
        )
    )
    matcher = RemoteOntologySpanMatcher("http://127.0.0.1:8765/", session=session)

    evidence = matcher.match(candidate("melanoma"))[0]
    serialized = SpanMatchResult(candidate("melanoma"), [evidence]).to_dict()

    assert evidence.aliases == ["Malignant Melanoma", "Melanomas"]
    assert serialized["evidence"][0]["aliases"] == ["Malignant Melanoma", "Melanomas"]
    assert "aliases" not in evidence.payload["payload"]
    assert evidence.payload["payload"]["definition"] == "A malignant neoplasm of melanocytes."


def test_remote_ontology_span_matcher_filters_disallowed_ontology_evidence_for_retrieval():
    session = FakeSession(
        FakeResponse(
            payload={
                "results": [
                    {
                        "candidate_id": "c0",
                        "evidence": [
                            {
                                "source": "umls",
                                "concept_id": "C1700001",
                                "canonical": "Method",
                                "confidence": 0.97,
                                "semantic_types": ["T170"],
                            },
                            {
                                "source": "umls",
                                "concept_id": "C0025202",
                                "canonical": "Melanoma",
                                "confidence": 0.96,
                                "semantic_types": ["T191"],
                            },
                            {
                                "source": "mesh",
                                "concept_id": "D000001",
                                "canonical": "Databases as Topic",
                                "confidence": 0.95,
                                "semantic_types": ["T170"],
                            },
                            {
                                "source": "mesh",
                                "concept_id": "D008545",
                                "canonical": "Melanoma",
                                "confidence": 0.94,
                                "semantic_types": ["T191"],
                            },
                        ],
                    }
                ]
            }
        )
    )
    matcher = RemoteOntologySpanMatcher("http://127.0.0.1:8765/", session=session)

    evidence = matcher.match(candidate("melanoma"))

    assert [(item.source, item.concept_id) for item in evidence] == [
        ("umls", "C0025202"),
        ("mesh", "D008545"),
    ]
    mesh_evidence = [item for item in evidence if item.source == "mesh"][0]
    assert mesh_evidence.payload["filter_status"] == "allow"
    assert mesh_evidence.payload["filter_reason"] == "mesh_tui_group:DISO"


def test_remote_ontology_span_matcher_keeps_unknown_category_ontology_evidence():
    session = FakeSession(
        FakeResponse(
            payload={
                "results": [
                    {
                        "candidate_id": "c0",
                        "evidence": [
                            {
                                "source": "umls",
                                "concept_id": "C0017428",
                                "canonical": "Genome",
                                "confidence": 0.91,
                            },
                            {
                                "source": "mesh",
                                "concept_id": "D005823",
                                "canonical": "Genome",
                                "confidence": 0.9,
                            },
                        ],
                    }
                ]
            }
        )
    )
    matcher = RemoteOntologySpanMatcher("http://127.0.0.1:8765/", session=session)

    evidence = matcher.match(candidate("genome"))

    assert [(item.source, item.concept_id) for item in evidence] == [
        ("umls", "C0017428"),
        ("mesh", "D005823"),
    ]


def test_remote_ontology_span_matcher_disables_environment_proxies_by_default():
    matcher = RemoteOntologySpanMatcher("http://127.0.0.1:8765")

    assert matcher.session.trust_env is False


def test_remote_ontology_span_matcher_reports_http_errors():
    matcher = RemoteOntologySpanMatcher(
        "http://service",
        session=FakeSession(FakeResponse(status_code=400, text="bad payload")),
    )

    with pytest.raises(OntologyLinkerConfigurationError):
        matcher.match(candidate("melanoma"))


def test_remote_ontology_span_matcher_reports_connection_errors():
    matcher = RemoteOntologySpanMatcher(
        "http://service",
        session=FakeSession(error=RuntimeError("connection refused")),
    )

    with pytest.raises(OntologyLinkerServiceUnavailable):
        matcher.match(candidate("melanoma"))


class StaticMatcher:
    def __init__(self, buckets):
        self.buckets = buckets

    def match(self, candidate):
        return self.match_many([candidate])[0]

    def match_many(self, candidates):
        return self.buckets


def test_composite_span_matcher_orders_umls_before_mesh_before_keyword():
    candidate_item = candidate("melanoma")
    matcher = CompositeSpanMatcher(
        [
            StaticMatcher(
                [
                    [
                        ConceptMatchEvidence(
                            candidate_text="melanoma",
                            normalized_text="melanoma",
                            start=0,
                            end=8,
                            candidate_kind="scispacy_entity",
                            source="keyword",
                            canonical="melanoma",
                            concept_id="keyword:melanoma",
                            confidence=1.0,
                        ),
                        ConceptMatchEvidence(
                            candidate_text="melanoma",
                            normalized_text="melanoma",
                            start=0,
                            end=8,
                            candidate_kind="scispacy_entity",
                            source="mesh",
                            canonical="Melanoma",
                            concept_id="D008545",
                            confidence=0.91,
                        ),
                    ]
                ]
            ),
            StaticMatcher(
                [
                    [
                        ConceptMatchEvidence(
                            candidate_text="melanoma",
                            normalized_text="melanoma",
                            start=0,
                            end=8,
                            candidate_kind="scispacy_entity",
                            source="umls",
                            canonical="Melanoma",
                            concept_id="C0025202",
                            confidence=0.8,
                        )
                    ]
                ]
            ),
        ]
    )

    evidence = matcher.match(candidate_item)

    assert [item.source for item in evidence] == ["umls", "mesh", "keyword"]


def test_subphrase_generator_expands_only_inside_parent_spans():
    generator = SubphraseCandidateGenerator(max_tokens=3)
    candidates = [
        candidate(
            "t cell automation in melanoma using deep learning solutions",
            kind="full_query",
            start=0,
        ),
        candidate("t cell automation", kind="connector_split", start=0),
        candidate("melanoma", kind="connector_split", start=21),
        candidate("deep learning solutions", kind="connector_split", start=36),
    ]

    expanded = generator.expand(candidates)
    subphrases = [item for item in expanded if item.kind == "subphrase_ngram"]
    surfaces = {(item.normalized_text, item.start, item.end) for item in subphrases}

    assert ("t cell", 0, 6) in surfaces
    assert ("deep learning", 36, 49) in surfaces
    assert ("automation in", 7, 20) not in surfaces
    assert ("melanoma using", 21, 35) not in surfaces
    assert all(item.kind != "subphrase_ngram" or item.normalized_text != "t" for item in expanded)


def test_span_matcher_executor_matches_generated_subphrases():
    lexicon = InMemoryPhraseLexicon(entries={"deep learning": {"canonical": "deep learning", "doc_count": 8}})
    executor = SpanMatcherExecutor(KeywordSurfaceSpanMatcher(lexicon))

    results = executor.match_candidates([candidate("deep learning solutions", kind="connector_split", start=36)])

    matched_surfaces = [result.candidate.normalized_text for result in results if result.is_matched]
    assert matched_surfaces == ["deep learning"]
    matched_result = next(result for result in results if result.is_matched)
    assert matched_result.evidence[0].source == "keyword"
    assert matched_result.candidate.start == 36
    assert matched_result.candidate.end == 49


def test_maximal_selector_keeps_matched_children_when_parent_is_unmatched():
    results = [
        SpanMatchResult(candidate("t cell automation", kind="connector_split"), []),
        SpanMatchResult(
            candidate("t cell", kind="subphrase_ngram"),
            [
                ConceptMatchEvidence(
                    candidate_text="t cell",
                    normalized_text="t cell",
                    start=0,
                    end=6,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="T-Lymphocytes",
                    concept_id="C0039194",
                    confidence=0.91,
                )
            ],
        ),
        SpanMatchResult(
            candidate("cell", kind="subphrase_ngram", start=2),
            [
                ConceptMatchEvidence(
                    candidate_text="cell",
                    normalized_text="cell",
                    start=2,
                    end=6,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="Cell",
                    concept_id="C0007634",
                    confidence=0.99,
                )
            ],
        ),
    ]

    selected = MaximalConceptSelector().select(results)

    assert [item.candidate.normalized_text for item in selected] == ["t cell"]


def test_maximal_selector_prefers_longer_keyword_parent_over_overlapping_umls_child():
    results = [
        SpanMatchResult(
            candidate("T-cell exhaustion", kind="connector_split"),
            [
                ConceptMatchEvidence(
                    candidate_text="T-cell exhaustion",
                    normalized_text="t-cell exhaustion",
                    start=0,
                    end=17,
                    candidate_kind="connector_split",
                    source="keyword",
                    canonical="T-cell exhaustion",
                    concept_id="keyword:t-cell exhaustion",
                    confidence=0.92,
                )
            ],
        ),
        SpanMatchResult(
            candidate("T-cell", kind="subphrase_ngram"),
            [
                ConceptMatchEvidence(
                    candidate_text="T-cell",
                    normalized_text="t-cell",
                    start=0,
                    end=6,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="T-Lymphocytes",
                    concept_id="C0039194",
                    confidence=0.99,
                )
            ],
        ),
    ]

    selected = MaximalConceptSelector().select(results)

    assert [item.candidate.normalized_text for item in selected] == ["t-cell exhaustion"]


def test_maximal_selector_skips_partial_ontology_parent_without_keyword_support():
    results = [
        SpanMatchResult(
            candidate("deep learning solutions", kind="connector_split"),
            [
                ConceptMatchEvidence(
                    candidate_text="deep learning solutions",
                    normalized_text="deep learning solutions",
                    start=36,
                    end=59,
                    candidate_kind="connector_split",
                    source="umls",
                    canonical="Deep Learning",
                    concept_id="C4704761",
                    confidence=0.73,
                )
            ],
        ),
        SpanMatchResult(
            candidate("deep learning", kind="subphrase_ngram", start=36),
            [
                ConceptMatchEvidence(
                    candidate_text="deep learning",
                    normalized_text="deep learning",
                    start=36,
                    end=49,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="Deep Learning",
                    concept_id="C4704761",
                    confidence=0.96,
                ),
                ConceptMatchEvidence(
                    candidate_text="deep learning",
                    normalized_text="deep learning",
                    start=36,
                    end=49,
                    candidate_kind="subphrase_ngram",
                    source="keyword",
                    canonical="deep learning",
                    concept_id="keyword:deep learning",
                    confidence=1.0,
                ),
            ],
        ),
        SpanMatchResult(
            candidate("solutions", kind="subphrase_ngram", start=50),
            [
                ConceptMatchEvidence(
                    candidate_text="solutions",
                    normalized_text="solutions",
                    start=50,
                    end=59,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="Solutions",
                    concept_id="C0037633",
                    confidence=0.98,
                )
            ],
        ),
    ]

    selected = MaximalConceptSelector().select(results)

    assert [item.candidate.normalized_text for item in selected] == ["deep learning"]


def test_selector_effective_results_hide_unselected_subphrases():
    results = [
        SpanMatchResult(candidate("t cell automation", kind="connector_split"), []),
        SpanMatchResult(
            candidate("t cell", kind="subphrase_ngram"),
            [
                ConceptMatchEvidence(
                    candidate_text="t cell",
                    normalized_text="t cell",
                    start=0,
                    end=6,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="T-Lymphocyte",
                    concept_id="C0039194",
                    confidence=0.97,
                )
            ],
        ),
        SpanMatchResult(
            candidate("cell", kind="subphrase_ngram", start=2),
            [
                ConceptMatchEvidence(
                    candidate_text="cell",
                    normalized_text="cell",
                    start=2,
                    end=6,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="Cells",
                    concept_id="C0007634",
                    confidence=0.99,
                )
            ],
        ),
        SpanMatchResult(
            candidate("deep learning", kind="subphrase_ngram", start=36),
            [
                ConceptMatchEvidence(
                    candidate_text="deep learning",
                    normalized_text="deep learning",
                    start=36,
                    end=49,
                    candidate_kind="subphrase_ngram",
                    source="umls",
                    canonical="Deep Learning",
                    concept_id="C4704761",
                    confidence=0.96,
                )
            ],
        ),
        SpanMatchResult(
            candidate("learning", kind="subphrase_ngram", start=41),
            [
                ConceptMatchEvidence(
                    candidate_text="learning",
                    normalized_text="learning",
                    start=41,
                    end=49,
                    candidate_kind="subphrase_ngram",
                    source="mesh",
                    canonical="Learning",
                    concept_id="C0023185",
                    confidence=0.98,
                )
            ],
        ),
    ]
    selector = MaximalConceptSelector()

    selected = selector.select(results)
    effective = selector.filter_effective_results(results, selected)

    assert [concept.candidate.normalized_text for concept in selected] == ["t cell", "deep learning"]
    assert [result.candidate.normalized_text for result in effective] == [
        "t cell automation",
        "t cell",
        "deep learning",
    ]
