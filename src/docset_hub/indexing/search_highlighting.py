"""Build structured highlight metadata for scholar search results."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .query_understanding import PHRASE_STOPWORDS, QueryCorrector, QueryNormalizer


MAX_HIGHLIGHT_TERMS = 12
MIN_TOKEN_CHARS = 3


def build_search_highlight(
    *,
    query: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    search_mode: str,
) -> Dict[str, Any]:
    """Return a serializable plan describing what the frontend should mark."""

    understanding = understanding or {}
    route = understanding.get("route")
    intent = understanding.get("intent")

    if (
        search_mode == "smart"
        and intent == "author_name"
        and route == "metadata_author"
        and understanding.get("matched_author")
    ):
        terms = _dedupe_terms([understanding.get("matched_author")])
        return {
            "mode": "author_name",
            "fields": ["authors"],
            "terms": terms,
        }

    if route == "vector" or search_mode == "vector":
        terms = _vector_highlight_terms(
            query=query,
            search_query=search_query,
            understanding=understanding,
        )
        return {
            "mode": "keyword" if terms else "none",
            "fields": ["title", "abstract"],
            "terms": terms,
        }

    return {
        "mode": "none",
        "fields": [],
        "terms": [],
    }


def _vector_highlight_terms(
    *,
    query: str,
    search_query: Optional[str],
    understanding: Dict[str, Any],
) -> List[str]:
    corrected_query = understanding.get("corrected_query")
    normalized_query = understanding.get("normalized_query")
    effective_query = search_query or corrected_query or normalized_query or query

    terms: List[str] = []
    terms.extend(_correction_terms(understanding, corrected_query=corrected_query))
    terms.extend(_candidate_keyword_terms(understanding))

    if not terms:
        terms.extend(_token_terms(effective_query))

    return _dedupe_terms(terms)[:MAX_HIGHLIGHT_TERMS]


def _correction_terms(understanding: Dict[str, Any], *, corrected_query: Optional[str]) -> Iterable[str]:
    corrections = understanding.get("corrections") or []
    for correction in corrections:
        if not isinstance(correction, dict):
            continue
        if corrected_query and correction.get("auto_apply") is False:
            continue
        corrected = correction.get("corrected")
        if corrected:
            yield from _term_with_tokens(str(corrected))


def _candidate_keyword_terms(understanding: Dict[str, Any]) -> Iterable[str]:
    candidates = understanding.get("candidates") or []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        keyword = candidate.get("keyword")
        if not keyword:
            continue
        score = candidate.get("score")
        if score is None or float(score or 0.0) >= 0.90:
            yield from _term_with_tokens(str(keyword))


def _term_with_tokens(term: str) -> Iterable[str]:
    yield term
    yield from _token_terms(term)


def _token_terms(query: Optional[str]) -> Iterable[str]:
    normalized = QueryNormalizer().normalize(query or "").normalized_query
    for token in QueryCorrector._normalize_term(normalized).split():
        if len(token) < MIN_TOKEN_CHARS:
            continue
        if token in PHRASE_STOPWORDS:
            continue
        yield token


def _dedupe_terms(terms: Iterable[Any]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for term in terms:
        text = QueryNormalizer().normalize(str(term or "")).normalized_query
        normalized = QueryCorrector._normalize_term(text)
        if not text or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(text)

    return sorted(deduped, key=lambda item: (-len(item), item.lower()))
