from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

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
    )

    assert "=== Normalized Query ===" in report
    assert "=== Extractor Candidates ===" in report
    assert "=== Expanded Candidates ===" in report
    assert "=== Raw Ontology Evidence ===" in report
    assert "=== Filtered Ontology Evidence ===" in report
    assert "=== Final Span Results ===" in report
    assert "=== Selected Concepts ===" in report
    assert "filter_reason=umls_group:DISO" in report
