"""Tests for deterministic query phrase analysis."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.query_phrase_analyzer import (
    AtomicPhraseExtractor,
    InMemoryPhraseLexicon,
    MetadataDBPhraseLexicon,
    QueryPhraseAnalyzer,
    QueryPhraseNormalizer,
    classify_query_type,
    extract_atomic_phrase_candidates,
    lookup_phrase_candidates,
)


class FakeSpan:
    def __init__(self, text, start_char, end_char):
        self.text = text
        self.start_char = start_char
        self.end_char = end_char


class FakeDoc:
    def __init__(self, noun_chunks=None, ents=None):
        self._noun_chunks = noun_chunks or []
        self.ents = ents or []

    @property
    def noun_chunks(self):
        return iter(self._noun_chunks)


class MappingNLP:
    def __init__(self, spans_by_query):
        self.spans_by_query = spans_by_query

    def __call__(self, text):
        spans = []
        for span_text in self.spans_by_query.get(text, []):
            start = text.find(span_text)
            if start >= 0:
                spans.append(FakeSpan(span_text, start, start + len(span_text)))
        return FakeDoc(noun_chunks=spans)


class FakeMappingResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.calls.append({"sql": str(sql), "params": params})
        return FakeMappingResult(self.row)


class FakeEngine:
    def __init__(self, row):
        self.connection = FakeConnection(row)

    def connect(self):
        return self.connection


class FakeMetadataDB:
    def __init__(self, row):
        self.engine = FakeEngine(row)
        self.default_sources = ["langtaosha", "biorxiv_history", "biorxiv_daily"]


def _record(canonical, doc_count=10, variant_count=1, matched_phrase_count=None):
    return {
        "canonical": canonical,
        "doc_count": doc_count,
        "variant_count": variant_count,
        "matched_phrase_count": matched_phrase_count if matched_phrase_count is not None else doc_count,
    }


@pytest.fixture()
def analyzer():
    lexicon = InMemoryPhraseLexicon(
        entries={
            "developmental disorders": _record("developmental disorders", doc_count=23),
            "brain-computer interface": _record("brain-computer interface", doc_count=9),
            "enhancer-promoter interaction": _record("enhancer-promoter interaction", doc_count=3),
            "T-cell exhaustion": _record("T-cell exhaustion", doc_count=11),
            "adhesion protein": _record("adhesion protein", doc_count=12),
            "kidney": _record("kidney", doc_count=240, variant_count=7),
            "obesity": _record("obesity", doc_count=150, variant_count=9),
            "macrophage": _record("macrophage", doc_count=130, variant_count=6),
            "genome 3D structure": _record("genome 3D structure", doc_count=12, variant_count=2),
            "regulation": _record("regulation", doc_count=900, variant_count=65),
            "macrophage activation": _record("macrophage activation", doc_count=80, variant_count=6),
            "chromatin architecture": _record("chromatin architecture", doc_count=35, variant_count=3),
            "neurons": _record("neurons", doc_count=150, variant_count=8),
            "synaptic plasticity": _record("synaptic plasticity", doc_count=85, variant_count=5),
            "memory": _record("memory", doc_count=220, variant_count=12),
            "synapse": _record("synapse", doc_count=500, variant_count=80, matched_phrase_count=220),
            "T cell": _record("T cell", doc_count=1000, variant_count=60, matched_phrase_count=400),
            "cancer": _record("cancer", doc_count=2000, variant_count=100, matched_phrase_count=800),
            "genome": _record("genome", doc_count=1200, variant_count=90, matched_phrase_count=500),
            "neuron": _record("neuron", doc_count=700, variant_count=70, matched_phrase_count=260),
            "interaction": _record("interaction", doc_count=900, variant_count=50, matched_phrase_count=300),
            "structure": _record("structure", doc_count=850, variant_count=45, matched_phrase_count=240),
            "protein": _record("protein", doc_count=1100, variant_count=80, matched_phrase_count=420),
            "function": _record("function", doc_count=760, variant_count=40, matched_phrase_count=200),
            "cell": _record("cell", doc_count=1800, variant_count=95, matched_phrase_count=600),
            "gene": _record("gene", doc_count=1300, variant_count=75, matched_phrase_count=500),
        },
        aliases={
            "BCI": "brain-computer interface",
            "exhausted t cell": "T-cell exhaustion",
        },
    )
    scispacy_pipeline = MappingNLP(
        {
            "genome 3d structure regulation": ["genome 3d structure", "regulation"],
            "genome interaction": ["genome", "interaction"],
            "genome structure": ["genome", "structure"],
            "protein function": ["protein", "function"],
            "cell interaction": ["cell", "interaction"],
            "gene regulation": ["gene", "regulation"],
        }
    )
    return QueryPhraseAnalyzer(lexicon=lexicon, scispacy_pipeline=scispacy_pipeline)


def test_normalizer_is_conservative_and_does_not_rewrite_terms():
    normalizer = QueryPhraseNormalizer()

    result = normalizer.normalize_query("  Genome–Interaction, ")

    assert result.display_query == "Genome-Interaction"
    assert result.normalized_query == "genome-interaction"


def test_extractor_keeps_full_query_and_removes_connector_spans():
    normalizer = QueryPhraseNormalizer()
    extractor = AtomicPhraseExtractor(normalizer=normalizer)

    candidates = extractor.extract("adhesion protein in kidney")
    candidate_texts = {candidate.normalized_text for candidate in candidates}
    candidate_kinds = {candidate.normalized_text: candidate.kind for candidate in candidates}

    assert "adhesion protein in kidney" in candidate_texts
    assert "adhesion protein" in candidate_texts
    assert "kidney" in candidate_texts
    assert "in" not in candidate_texts
    assert candidate_kinds["adhesion protein in kidney"] == "full_query"
    assert candidate_kinds["adhesion protein"] == "connector_split"
    assert candidate_kinds["kidney"] == "connector_split"


def test_extractor_marks_scispacy_candidate_sources():
    normalizer = QueryPhraseNormalizer()
    extractor = AtomicPhraseExtractor(normalizer=normalizer)
    doc = FakeDoc(
        ents=[FakeSpan("BRCA1 mutation", 0, 14)],
        noun_chunks=[FakeSpan("breast cancer", 18, 31)],
    )

    candidates = extractor.extract("brca1 mutation in breast cancer", scispacy_doc=doc)
    candidate_kinds = {candidate.normalized_text: candidate.kind for candidate in candidates}

    assert candidate_kinds["brca1 mutation in breast cancer"] == "full_query"
    assert candidate_kinds["brca1 mutation"] == "scispacy_entity"
    assert candidate_kinds["breast cancer"] == "scispacy_noun_chunk"


def test_public_helper_functions_compose_with_dict_payloads():
    lexicon = InMemoryPhraseLexicon(
        entries={
            "developmental disorders": _record("developmental disorders", doc_count=23),
        }
    )

    candidates = extract_atomic_phrase_candidates("developmental disorder")
    matches = lookup_phrase_candidates(candidates, lexicon)
    full_query_match = next(match for match in matches if match["kind"] == "full_query")
    result = classify_query_type(full_query_match, [], original_query="developmental disorder")

    assert result["query_type"] == "atomic_phrase"
    assert result["atomic_phrases"][0]["canonical"] == "developmental disorders"


def test_metadata_db_phrase_lexicon_reads_keyword_counts_from_engine():
    db = FakeMetadataDB(
        {
            "canonical": "Synapse",
            "doc_count": 44,
            "variant_count": 12,
            "matched_phrase_count": 216,
            "related_doc_count": 127,
        }
    )
    lexicon = MetadataDBPhraseLexicon(metadata_db=db)

    record = lexicon.lookup("synapse")

    assert record["canonical"] == "Synapse"
    assert record["doc_count"] == 44
    assert record["variant_count"] == 12
    assert record["matched_phrase_count"] == 216
    assert db.engine.connection.calls[0]["params"]["normalized_phrase"] == "synapse"
    assert "paper_sources" in db.engine.connection.calls[0]["sql"]


def test_plural_variant_atomic_phrase(analyzer):
    result = analyzer.analyze("developmental disorder")

    assert result.query_type == "atomic_phrase"
    assert result.retrieval_policy == "phrase_strict"
    assert result.atomic_phrases[0].canonical == "developmental disorders"
    assert result.full_query_match.match_type == "plural_variant"


def test_compositional_relation_from_connector_split(analyzer):
    result = analyzer.analyze("adhesion protein in kidney")

    assert result.query_type == "compositional_relation"
    assert result.retrieval_policy == "coverage_rerank"
    assert result.coverage_required_count == 2
    assert {phrase.canonical for phrase in result.atomic_phrases} == {"adhesion protein", "kidney"}


def test_ambiguous_generic_head_without_full_phrase_match(analyzer):
    result = analyzer.analyze("genome interaction")

    assert result.query_type == "ambiguous"
    assert result.retrieval_policy == "soft_rerank"
    assert "avoid_auto_rewrite" in result.warnings


def test_full_phrase_protects_generic_head(analyzer):
    result = analyzer.analyze("enhancer-promoter interaction")

    assert result.query_type == "atomic_phrase"
    assert result.retrieval_policy == "phrase_strict"
    assert result.atomic_phrases[0].canonical == "enhancer-promoter interaction"


def test_broad_concept_from_high_match_counts(analyzer):
    result = analyzer.analyze("synapse")

    assert result.query_type == "broad_concept"
    assert result.retrieval_policy == "diversified"
    assert result.atomic_phrases[0].canonical == "synapse"


def test_short_phrase_is_not_automatically_broad(analyzer):
    result = analyzer.analyze("developmental disorder")

    assert result.query_type == "atomic_phrase"
    assert result.retrieval_policy == "phrase_strict"


def test_broad_multi_token_concept(analyzer):
    result = analyzer.analyze("T cell")

    assert result.query_type == "broad_concept"
    assert result.retrieval_policy == "diversified"


def test_same_surface_form_can_be_broad_or_ambiguous_by_lexicon_evidence(analyzer):
    broad_lexicon = InMemoryPhraseLexicon(
        entries={
            "gene regulation": _record("gene regulation", doc_count=500, variant_count=40),
            "gene": _record("gene", doc_count=1300, variant_count=75),
            "regulation": _record("regulation", doc_count=900, variant_count=65),
        }
    )
    weak_lexicon = InMemoryPhraseLexicon(
        entries={
            "gene": _record("gene", doc_count=1300, variant_count=75),
            "regulation": _record("regulation", doc_count=900, variant_count=65),
        }
    )
    scispacy_pipeline = analyzer.scispacy_pipeline

    broad_result = QueryPhraseAnalyzer(
        lexicon=broad_lexicon,
        scispacy_pipeline=scispacy_pipeline,
    ).analyze("gene regulation")
    ambiguous_result = QueryPhraseAnalyzer(
        lexicon=weak_lexicon,
        scispacy_pipeline=scispacy_pipeline,
    ).analyze("gene regulation")

    assert broad_result.query_type == "broad_concept"
    assert broad_result.retrieval_policy == "diversified"
    assert ambiguous_result.query_type == "ambiguous"
    assert ambiguous_result.retrieval_policy == "soft_rerank"


@pytest.mark.parametrize(
    ("query", "expected_type", "expected_policy"),
    [
        ("developmental disorder", "atomic_phrase", "phrase_strict"),
        ("brain computer interface", "atomic_phrase", "phrase_strict"),
        ("enhancer-promoter interaction", "atomic_phrase", "phrase_strict"),
        ("enhancer promoter interaction", "atomic_phrase", "phrase_strict"),
        ("exhausted t cell", "atomic_phrase", "phrase_strict"),
        ("T cell exhaustion", "atomic_phrase", "phrase_strict"),
        ("adhesion protein in kidney", "compositional_relation", "coverage_rerank"),
        ("obesity and macrophage", "compositional_relation", "coverage_rerank"),
        ("genome 3D structure regulation", "compositional_relation", "coverage_rerank"),
        ("macrophage activation in obesity", "compositional_relation", "coverage_rerank"),
        ("chromatin architecture in neurons", "compositional_relation", "coverage_rerank"),
        ("synaptic plasticity and memory", "compositional_relation", "coverage_rerank"),
        ("synapse", "broad_concept", "diversified"),
        ("T cell", "broad_concept", "diversified"),
        ("cancer", "broad_concept", "diversified"),
        ("genome", "broad_concept", "diversified"),
        ("neuron", "broad_concept", "diversified"),
        ("genome interaction", "ambiguous", "soft_rerank"),
        ("genome structure", "ambiguous", "soft_rerank"),
        ("protein function", "ambiguous", "soft_rerank"),
        ("cell interaction", "ambiguous", "soft_rerank"),
    ],
)
def test_query_type_matrix(query, expected_type, expected_policy, analyzer):
    result = analyzer.analyze(query)

    assert result.query_type == expected_type
    assert result.retrieval_policy == expected_policy
