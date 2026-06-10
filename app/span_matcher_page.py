from __future__ import annotations

import os
import time
from typing import Any, Callable, Dict, List, Optional

from flask import render_template, request

from src.docset_hub.indexing import SpanMatcherError, SpanMatcherPipeline, SpanMatcherProfile


DEFAULT_SPAN_SCISPACY_MODEL = "en_core_sci_lg"
DEFAULT_ONTOLOGY_LINKER_URL = "http://127.0.0.1:8765"
SPAN_MATCH_DISPLAY_THRESHOLD = 0.9


def _parse_csv_items(value: Optional[str], default: Optional[List[str]] = None) -> List[str]:
    text_value = (value or "").strip()
    if not text_value:
        return list(default or [])
    return [item.strip() for item in text_value.split(",") if item.strip()]


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _serialize_span_aliases(evidence: Any) -> List[str]:
    aliases = [str(alias) for alias in (evidence.aliases or []) if alias]
    if not aliases and evidence.match_type.endswith("_alias"):
        aliases.append(evidence.candidate_text)
    return aliases

def _serialize_selected_candidate(concept: Any) -> Dict[str, Any]:
    candidate = concept.candidate
    return {
        "text": candidate.text,
        "normalized_text": candidate.normalized_text,
        "kind": candidate.kind,
        "start": candidate.start,
        "end": candidate.end,
        "matches": [
            {
                "source": evidence.source,
                "canonical": evidence.canonical,
                "concept_id": evidence.concept_id,
                "match_type": evidence.match_type,
                "confidence": evidence.confidence,
                "aliases": _serialize_span_aliases(evidence),
            }
            for evidence in concept.evidence
        ],
    }


def _serialize_semantic_plan(plan: Any) -> Dict[str, Any]:
    def serialize_terms(terms: Any) -> List[Dict[str, Any]]:
        return [
            {
                "text": getattr(term, "text", ""),
                "match_mode": getattr(term, "match_mode", "exact"),
            }
            for term in list(terms or [])
        ]

    def serialize_child(child: Any) -> Dict[str, Any]:
        return {
            "span_id": child.span_id,
            "surface_text": child.surface_text,
            "normalized_text": child.normalized_text,
            "start": child.start,
            "end": child.end,
            "canonical_text": child.canonical_text,
            "own_terms": {
                "tier1": serialize_terms(getattr(child.own_terms, "tier1", [])),
                "tier2": serialize_terms(getattr(child.own_terms, "tier2", [])),
            },
        }

    return {
        "original_query": plan.original_query,
        "normalized_query": plan.normalized_query,
        "spans": [
            {
                "span_id": span.span_id,
                "surface_text": span.surface_text,
                "normalized_text": span.normalized_text,
                "start": span.start,
                "end": span.end,
                "canonical_text": span.canonical_text,
                "own_terms": {
                    "tier1": serialize_terms(getattr(span.own_terms, "tier1", [])),
                    "tier2": serialize_terms(getattr(span.own_terms, "tier2", [])),
                },
                "children": [
                    serialize_child(child)
                    for child in list(getattr(span, "children", []) or [])
                ],
            }
            for span in plan.spans
        ],
    }


def _build_span_matcher_profile(paper_indexer: Any) -> SpanMatcherProfile:
    return SpanMatcherProfile.ontology_plus_keyword(
        enable_scispacy=os.environ.get("SKIP_SCISPACY", "0") != "1",
        scispacy_model=os.environ.get("SCISPACY_MODEL", DEFAULT_SPAN_SCISPACY_MODEL),
        ontology_base_url=(
            os.environ.get("ONTOLOGY_LINKER_URL", DEFAULT_ONTOLOGY_LINKER_URL)
            or ""
        ).strip(),
        ontology_sources=tuple(
            _parse_csv_items(
                os.environ.get("ONTOLOGY_SOURCE_LIST"),
                default=["umls", "mesh"],
            )
        ),
        ontology_top_k=_env_int("ONTOLOGY_TOP_K", 2),
        ontology_threshold=_env_float("ONTOLOGY_THRESHOLD", 0.9),
        ontology_timeout=_env_float("ONTOLOGY_TIMEOUT", 20.0),
        paper_sources=tuple(
            _parse_csv_items(
                os.environ.get("PAPER_SOURCES"),
                default=list(paper_indexer.default_sources),
            )
        ),
        keyword_sources=tuple(_parse_csv_items(os.environ.get("KEYWORD_SOURCE"))),
        include_subphrases=os.environ.get("NO_SUBPHRASE_NGRAM", "0") != "1",
        evidence_threshold=SPAN_MATCH_DISPLAY_THRESHOLD,
    )


def run_span_matcher_test(query: str, *, paper_indexer: Any) -> Dict[str, Any]:
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    started_at = time.perf_counter()
    profile = _build_span_matcher_profile(paper_indexer)
    result = SpanMatcherPipeline.from_profile(
        profile=profile,
        metadata_db=paper_indexer.metadata_db,
    ).run(normalized_query)
    selected_candidates = [
        _serialize_selected_candidate(concept)
        for concept in result.selected_concepts
    ]
    elapsed_ms = round((time.perf_counter() - started_at) * 1000.0, 3)

    return {
        "success": True,
        "query": result.query,
        "normalized_query": result.normalized_query,
        "count": len(selected_candidates),
        "selected_candidates": selected_candidates,
        "semantic_plan": _serialize_semantic_plan(result.semantic_plan),
        "elapsed_ms": elapsed_ms,
        "timings_ms": result.timings_ms,
    }


def register_span_matcher_page_routes(app) -> None:
    @app.route("/span-matcher")
    def span_matcher_page() -> str:
        query = (request.args.get("q") or "").strip()
        return render_template("span_matcher.html", initial_query=query)


def register_span_matcher_api_routes(
    app,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
    *,
    paper_indexer: Any,
) -> None:
    @app.route("/api/span-matcher", methods=["GET"])
    def api_span_matcher():
        try:
            data = run_span_matcher_test(
                query=(request.args.get("query") or "").strip(),
                paper_indexer=paper_indexer,
            )
            return api_success(data)
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except SpanMatcherError as exc:
            return api_error(str(exc), status_code=502, code="SPAN_MATCHER_FAILED")
        except Exception as exc:
            return api_error(str(exc), status_code=500, code="SPAN_MATCHER_FAILED")


def register_span_matcher_routes(
    app,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
    *,
    paper_indexer: Any,
) -> None:
    register_span_matcher_page_routes(app)
    register_span_matcher_api_routes(
        app,
        api_success,
        api_error,
        paper_indexer=paper_indexer,
    )
