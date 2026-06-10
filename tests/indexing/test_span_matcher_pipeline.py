from __future__ import annotations

from types import SimpleNamespace

from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import (
    CompositeSpanMatcher,
    ConceptMatchEvidence,
    KeywordSurfaceSpanMatcher,
    RemoteOntologySpanMatcher,
    SpanMatchResult,
)
from src.docset_hub.indexing.span_matcher_pipeline import (
    SpanMatcherPipeline,
    SpanMatcherProfile,
)


def test_ontology_plus_keyword_profile_is_default_online_profile():
    profile = SpanMatcherProfile.ontology_plus_keyword()

    assert profile.name == "ontology_plus_keyword"
    assert profile.enable_ontology is True
    assert profile.enable_keyword is True
    assert profile.include_subphrases is True
    assert profile.build_semantic_plan is True
    assert profile.paper_sources == ("langtaosha", "biorxiv_history", "biorxiv_daily")


def test_keyword_only_profile_is_explicit_db_only_mode():
    profile = SpanMatcherProfile.keyword_only(paper_sources=("langtaosha",))

    assert profile.name == "keyword_only"
    assert profile.enable_ontology is False
    assert profile.enable_keyword is True
    assert profile.paper_sources == ("langtaosha",)


def test_ontology_only_profile_disables_keyword_matcher():
    profile = SpanMatcherProfile.ontology_only(ontology_sources=("mesh",))

    assert profile.name == "ontology_only"
    assert profile.enable_ontology is True
    assert profile.enable_keyword is False
    assert profile.ontology_sources == ("mesh",)


def _candidate(text: str, start: int, end: int) -> PhraseCandidate:
    return PhraseCandidate(
        text=text,
        normalized_text=text,
        kind="connector_split",
        start=start,
        end=end,
    )


def _evidence(candidate: PhraseCandidate, confidence: float = 1.0) -> ConceptMatchEvidence:
    return ConceptMatchEvidence(
        candidate_text=candidate.text,
        normalized_text=candidate.normalized_text,
        start=candidate.start,
        end=candidate.end,
        candidate_kind=candidate.kind,
        source="keyword",
        concept_id=f"keyword:{candidate.normalized_text}",
        canonical=candidate.text,
        confidence=confidence,
        match_type="keyword_exact",
        aliases=[],
        payload={},
    )


def test_pipeline_run_returns_selected_concepts_and_semantic_plan():
    normalizer = SimpleNamespace(
        normalize_query=lambda query: SimpleNamespace(
            original_query=query,
            normalized_query=query,
        )
    )
    candidates = [_candidate("kidney", 0, 6)]
    analyzer = SimpleNamespace(
        normalizer=normalizer,
        scispacy_pipeline=None,
        extractor=SimpleNamespace(
            extract=lambda normalized_query, scispacy_doc=None: candidates
        ),
    )
    executor = SimpleNamespace(
        expand_candidates=lambda items: list(items),
        match_candidates=lambda items: [
            SpanMatchResult(candidate=candidates[0], evidence=[_evidence(candidates[0])])
        ],
    )

    pipeline = SpanMatcherPipeline(
        profile=SpanMatcherProfile.keyword_only(enable_scispacy=False),
        analyzer=analyzer,
        executor=executor,
    )

    result = pipeline.run("kidney")

    assert result.profile_name == "keyword_only"
    assert result.normalized_query == "kidney"
    assert [concept.candidate.text for concept in result.selected_concepts] == ["kidney"]
    assert result.semantic_plan is not None
    assert [span.surface_text for span in result.semantic_plan.spans] == ["kidney"]
    assert set(result.timings_ms) >= {"normalize", "extract", "match", "select", "build_plan"}


def test_pipeline_applies_evidence_threshold_before_selection():
    normalizer = SimpleNamespace(
        normalize_query=lambda query: SimpleNamespace(
            original_query=query,
            normalized_query=query,
        )
    )
    candidates = [_candidate("kidney", 0, 6)]
    analyzer = SimpleNamespace(
        normalizer=normalizer,
        scispacy_pipeline=None,
        extractor=SimpleNamespace(
            extract=lambda normalized_query, scispacy_doc=None: candidates
        ),
    )
    executor = SimpleNamespace(
        expand_candidates=lambda items: list(items),
        match_candidates=lambda items: [
            SpanMatchResult(candidate=candidates[0], evidence=[_evidence(candidates[0], confidence=0.5)])
        ],
    )
    pipeline = SpanMatcherPipeline(
        profile=SpanMatcherProfile.keyword_only(enable_scispacy=False, evidence_threshold=0.9),
        analyzer=analyzer,
        executor=executor,
    )

    result = pipeline.run("kidney")

    assert result.selected_concepts == []
    assert result.semantic_plan is not None
    assert result.semantic_plan.spans == []


class FakeMetadataDB:
    default_sources = ["langtaosha"]


def test_from_profile_builds_ontology_plus_keyword_pipeline():
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.ontology_plus_keyword(
            enable_scispacy=False,
            ontology_base_url="http://127.0.0.1:8765",
            paper_sources=("langtaosha",),
        ),
        metadata_db=FakeMetadataDB(),
    )

    assert pipeline.profile.name == "ontology_plus_keyword"
    assert isinstance(pipeline.executor.matcher, CompositeSpanMatcher)
    matcher_types = {type(matcher) for matcher in pipeline.executor.matcher.matchers}
    assert RemoteOntologySpanMatcher in matcher_types
    assert KeywordSurfaceSpanMatcher in matcher_types


def test_from_profile_builds_keyword_only_pipeline_without_ontology():
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.keyword_only(
            enable_scispacy=False,
            paper_sources=("langtaosha",),
        ),
        metadata_db=FakeMetadataDB(),
    )

    matcher_types = {type(matcher) for matcher in pipeline.executor.matcher.matchers}
    assert RemoteOntologySpanMatcher not in matcher_types
    assert KeywordSurfaceSpanMatcher in matcher_types
