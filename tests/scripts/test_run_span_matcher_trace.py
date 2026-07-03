from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

from src.docset_hub.indexing.query_semantic_plan import (
    QuerySemanticPlan,
    SemanticChildSpan,
    SemanticSpanGroup,
    SemanticTerm,
    SemanticTermBucket,
)
from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_trace_script():
    module_path = PROJECT_ROOT / "scripts" / "run_span_matcher_trace.py"
    spec = importlib.util.spec_from_file_location("run_span_matcher_trace", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_render_trace_report_shows_all_stages():
    script = load_trace_script()
    candidate = PhraseCandidate("melanoma", "melanoma", "connector_split", 21, 29)
    evidence = ConceptMatchEvidence(
        candidate_text="melanoma",
        normalized_text="melanoma",
        start=21,
        end=29,
        candidate_kind="connector_split",
        source="umls",
        canonical="Melanoma",
        concept_id="C0025202",
        confidence=0.98,
        payload={"filter_status": "allow", "filter_reason": "umls_group:DISO"},
    )
    report = script.render_trace_report(
        query="melanoma",
        normalized_query="melanoma",
        extractor_candidates=[candidate],
        expanded_candidates=[candidate],
        raw_ontology_items={"melanoma": [{"source": "umls", "concept_id": "C0025202", "canonical": "Melanoma"}]},
        filtered_ontology_evidence={"melanoma": [evidence]},
        keyword_evidence={},
        span_results=[SpanMatchResult(candidate=candidate, evidence=[evidence])],
        selected_concepts=[SelectedConcept(candidate=candidate, evidence=[evidence])],
        semantic_plan=QuerySemanticPlan(
            original_query="melanoma",
            normalized_query="melanoma",
            spans=[
                SemanticSpanGroup(
                    span_id="s1",
                    surface_text="melanoma",
                    normalized_text="melanoma",
                    start=21,
                    end=29,
                    canonical_text="Melanoma",
                    own_terms=SemanticTermBucket(
                        tier1=[SemanticTerm(text="melanoma", match_mode="exact")],
                        tier2=[
                            SemanticTerm(text="malignant melanoma", match_mode="exact"),
                            SemanticTerm(text="melan", match_mode="prefix"),
                        ],
                    ),
                    children=[
                        SemanticChildSpan(
                            span_id="s1.1",
                            surface_text="melan",
                            normalized_text="melan",
                            start=21,
                            end=26,
                            canonical_text="Melan",
                            own_terms=SemanticTermBucket(
                                tier1=[SemanticTerm(text="melan", match_mode="exact")],
                                tier2=[],
                            ),
                            evidence=[evidence],
                        )
                    ],
                    evidence=[evidence],
                )
            ],
        ),
    )

    assert "=== Normalized Query ===" in report
    assert "=== Extractor Candidates ===" in report
    assert "=== Expanded Candidates ===" in report
    assert "=== Raw Ontology Evidence ===" in report
    assert "=== Filtered Ontology Evidence ===" in report
    assert "=== Final Span Results ===" in report
    assert "=== Selected Concepts ===" in report
    assert "=== Query Semantic Plan ===" in report
    assert "own.tier1=melanoma [exact]" in report
    assert "own.tier2=malignant melanoma [exact], melan [prefix]" in report
    assert "children:" in report
    assert "s1.1: melan" in report
    assert "filter_reason=umls_group:DISO" in report


def test_run_trace_uses_span_matcher_pipeline(monkeypatch):
    script = load_trace_script()
    captured = {}

    class FakePipeline:
        def run(self, query, *, trace=False):
            captured["query"] = query
            captured["trace"] = trace
            return SimpleNamespace(
                query=query,
                normalized_query=query,
                extractor_candidates=[],
                expanded_candidates=[],
                trace=SimpleNamespace(
                    raw_ontology_items={},
                    filtered_ontology_evidence={},
                    keyword_evidence={},
                ),
                span_results=[],
                selected_concepts=[],
                semantic_plan=QuerySemanticPlan(
                    original_query=query,
                    normalized_query=query,
                    spans=[],
                ),
            )

    def fake_from_profile(*, profile, metadata_db):
        captured["profile_name"] = profile.name
        captured["metadata_db"] = metadata_db
        return FakePipeline()

    monkeypatch.setattr(script.SpanMatcherPipeline, "from_profile", fake_from_profile)

    report = script.run_trace(
        SimpleNamespace(
            ontology_linker_url="http://127.0.0.1:8765",
            ontology_source_list="umls,mesh",
            ontology_top_k=2,
            ontology_threshold=0.9,
            skip_scispacy=True,
            scispacy_model="en_core_sci_lg",
            no_subphrase_ngram=False,
            use_db_keywords=False,
            config_path="src/config/config_tecent_backend_server_mimic.yaml",
            paper_source_list="langtaosha",
        ),
        "kidney",
    )

    assert captured["query"] == "kidney"
    assert captured["trace"] is True
    assert captured["profile_name"] == "ontology_only"
    assert captured["metadata_db"] is None
    assert "=== Query Semantic Plan ===" in report
