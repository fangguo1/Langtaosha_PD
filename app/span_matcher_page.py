from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional

from flask import render_template, request

from src.docset_hub.indexing import (
    CompositeSpanMatcher,
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    MetadataDBPhraseLexicon,
    QueryPhraseAnalyzer,
    RemoteOntologySpanMatcher,
    SpanMatcherError,
    SpanMatcherExecutor,
    SpanMatchResult,
)


DEFAULT_SPAN_SCISPACY_MODEL = "en_core_sci_lg"
DEFAULT_ONTOLOGY_LINKER_URL = "http://127.0.0.1:8765"
SPAN_MATCH_DISPLAY_THRESHOLD = 0.9
_SPAN_MATCHER_CONTEXTS: dict[int, dict[str, Any]] = {}


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


def _load_span_scispacy_pipeline() -> Optional[Any]:
    if os.environ.get("SKIP_SCISPACY", "0") == "1":
        return None

    model_name = os.environ.get("SCISPACY_MODEL", DEFAULT_SPAN_SCISPACY_MODEL)
    try:
        import spacy
    except ImportError:
        return None

    try:
        return spacy.load(model_name)
    except OSError:
        return None


def _get_span_matcher_context(paper_indexer: Any) -> Dict[str, Any]:
    context_key = id(paper_indexer)
    cached_context = _SPAN_MATCHER_CONTEXTS.get(context_key)
    if cached_context is not None:
        return cached_context

    paper_sources = _parse_csv_items(
        os.environ.get("PAPER_SOURCES"),
        default=list(paper_indexer.default_sources),
    )
    keyword_sources = _parse_csv_items(os.environ.get("KEYWORD_SOURCE"))
    lexicon = MetadataDBPhraseLexicon(
        metadata_db=paper_indexer.metadata_db,
        paper_source_names=paper_sources,
        keyword_sources=keyword_sources,
    )

    matchers = []
    ontology_linker_url = (
        os.environ.get("ONTOLOGY_LINKER_URL", DEFAULT_ONTOLOGY_LINKER_URL)
        or ""
    ).strip()
    if ontology_linker_url:
        matchers.append(
            RemoteOntologySpanMatcher(
                base_url=ontology_linker_url,
                sources=_parse_csv_items(
                    os.environ.get("ONTOLOGY_SOURCE_LIST"),
                    default=["umls", "mesh"],
                ),
                top_k=_env_int("ONTOLOGY_TOP_K", 2),
                threshold=_env_float("ONTOLOGY_THRESHOLD", 0.9),
                timeout=_env_float("ONTOLOGY_TIMEOUT", 20.0),
            )
        )
    matchers.append(KeywordSurfaceSpanMatcher(lexicon))

    context = {
        "analyzer": QueryPhraseAnalyzer(
            lexicon=lexicon,
            scispacy_pipeline=_load_span_scispacy_pipeline(),
        ),
        "executor": SpanMatcherExecutor(
            matcher=CompositeSpanMatcher(matchers),
            include_subphrases=os.environ.get("NO_SUBPHRASE_NGRAM", "0") != "1",
        ),
        "selector": MaximalConceptSelector(),
        "paper_sources": paper_sources,
        "keyword_sources": keyword_sources,
        "ontology_linker_url": ontology_linker_url,
    }
    _SPAN_MATCHER_CONTEXTS[context_key] = context
    return context


def _serialize_span_aliases(evidence: Any) -> List[str]:
    aliases = [str(alias) for alias in (evidence.aliases or []) if alias]
    if not aliases and evidence.match_type.endswith("_alias"):
        aliases.append(evidence.candidate_text)
    return aliases


def _filter_span_results_for_display(results: List[SpanMatchResult]) -> List[SpanMatchResult]:
    return [
        SpanMatchResult(
            candidate=result.candidate,
            evidence=[
                evidence
                for evidence in result.evidence
                if float(evidence.confidence) > SPAN_MATCH_DISPLAY_THRESHOLD
            ],
        )
        for result in results
    ]


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


def run_span_matcher_test(query: str, *, paper_indexer: Any) -> Dict[str, Any]:
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    context = _get_span_matcher_context(paper_indexer)
    analyzer = context["analyzer"]
    executor = context["executor"]
    selector = context["selector"]

    normalized = analyzer.normalizer.normalize_query(normalized_query)
    scispacy_doc = None
    if analyzer.scispacy_pipeline is not None and normalized.normalized_query:
        scispacy_doc = analyzer.scispacy_pipeline(normalized.normalized_query)

    candidates = analyzer.extractor.extract(
        normalized.normalized_query,
        scispacy_doc=scispacy_doc,
    )
    span_results = executor.match_candidates(candidates)
    display_results = _filter_span_results_for_display(span_results)
    selected_concepts = selector.select(display_results)
    selected_candidates = [
        _serialize_selected_candidate(concept)
        for concept in selected_concepts
    ]

    return {
        "success": True,
        "query": normalized.original_query,
        "normalized_query": normalized.normalized_query,
        "count": len(selected_candidates),
        "selected_candidates": selected_candidates,
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
