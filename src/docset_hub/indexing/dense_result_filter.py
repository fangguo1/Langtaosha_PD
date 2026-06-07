"""Hard-rule filters for dense retrieval candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text

from .paper_keyword_lookup import TOKEN_RE
from .query_phrase_analyzer import QueryPhraseNormalizer


DENSE_DEFAULT_MIN_SIMILARITY = 0.46
DENSE_FILTER_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "based",
    "between",
    "for",
    "from",
    "how",
    "into",
    "its",
    "of",
    "on",
    "or",
    "the",
    "their",
    "through",
    "to",
    "using",
    "with",
}


@dataclass(frozen=True)
class DenseResultFilterReport:
    """Summary of the dense hard-rule prune step."""

    initial_count: int
    kept_count: int
    score_pruned_count: int
    keyword_pruned_count: int
    min_similarity: float
    query_terms: List[str]
    matched_paper_ids: List[int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_dense_keyword_filter_terms(query: str) -> List[str]:
    """Build normalized query terms used for DB-backed keyword presence checks."""

    normalizer = QueryPhraseNormalizer()
    normalized_query = normalizer.normalize_phrase(query)
    if not normalized_query:
        return []

    terms: List[str] = []
    if len(normalized_query) >= 3:
        terms.append(normalized_query)

    for raw_token in TOKEN_RE.findall(normalized_query):
        token = normalizer.normalize_phrase(raw_token)
        if len(token) < 3 or token in DENSE_FILTER_STOPWORDS:
            continue
        terms.append(token)
        if "-" in token:
            terms.append(token.replace("-", " "))
            terms.extend(
                part
                for part in token.split("-")
                if len(part) >= 3 and part not in DENSE_FILTER_STOPWORDS
            )

    return list(dict.fromkeys(terms))


def filter_dense_results_by_hard_rules(
    metadata_db: Any,
    query: str,
    results: Sequence[Mapping[str, Any]],
    min_similarity: float = DENSE_DEFAULT_MIN_SIMILARITY,
    keyword_sources: Optional[Sequence[str]] = None,
) -> Tuple[List[Dict[str, Any]], DenseResultFilterReport]:
    """Prune dense candidates by similarity and query-term keyword evidence.

    Rule 1: drop candidates whose dense similarity is lower than
    ``min_similarity``.
    Rule 2: among the remaining candidates, keep only papers whose
    ``paper_keywords`` contain at least one normalized query term.
    """

    candidate_results = [dict(item) for item in results]
    score_kept = [
        item
        for item in candidate_results
        if _coerce_float(item.get("similarity_score", item.get("similarity"))) >= min_similarity
    ]
    query_terms = build_dense_keyword_filter_terms(query)

    if not query_terms:
        report = DenseResultFilterReport(
            initial_count=len(candidate_results),
            kept_count=len(score_kept),
            score_pruned_count=len(candidate_results) - len(score_kept),
            keyword_pruned_count=0,
            min_similarity=float(min_similarity),
            query_terms=[],
            matched_paper_ids=[],
        )
        return [_annotate_dense_filter(item, report, matched_keywords=[]) for item in score_kept], report

    paper_ids = [_coerce_int(item.get("paper_id")) for item in score_kept]
    candidate_paper_ids = [paper_id for paper_id in paper_ids if paper_id is not None]
    keyword_matches = find_dense_keyword_matches(
        metadata_db=metadata_db,
        paper_ids=candidate_paper_ids,
        query_terms=query_terms,
        keyword_sources=keyword_sources,
    )
    matched_paper_ids = set(keyword_matches)
    kept_results = [
        _annotate_dense_filter(
            item,
            matched_keywords=keyword_matches.get(_coerce_int(item.get("paper_id")) or -1, []),
        )
        for item in score_kept
        if _coerce_int(item.get("paper_id")) in matched_paper_ids
    ]
    report = DenseResultFilterReport(
        initial_count=len(candidate_results),
        kept_count=len(kept_results),
        score_pruned_count=len(candidate_results) - len(score_kept),
        keyword_pruned_count=len(score_kept) - len(kept_results),
        min_similarity=float(min_similarity),
        query_terms=query_terms,
        matched_paper_ids=sorted(matched_paper_ids),
    )
    kept_results = [
        _annotate_dense_filter(
            item,
            report,
            matched_keywords=(item.get("retrieval_debug") or {})
            .get("dense_hard_filter", {})
            .get("matched_keywords", []),
        )
        for item in kept_results
    ]
    return kept_results, report


def find_dense_keyword_matches(
    metadata_db: Any,
    paper_ids: Sequence[int],
    query_terms: Sequence[str],
    keyword_sources: Optional[Sequence[str]] = None,
) -> Dict[int, List[Dict[str, Any]]]:
    """Return paper_ids with DB keyword rows matching any query term."""

    normalized_terms = _normalize_query_terms(query_terms)
    normalized_paper_ids = list(dict.fromkeys(_coerce_int(value) for value in paper_ids))
    normalized_paper_ids = [value for value in normalized_paper_ids if value is not None]
    if not normalized_terms or not normalized_paper_ids:
        return {}

    params: Dict[str, Any] = {}
    paper_values = []
    for index, paper_id in enumerate(normalized_paper_ids):
        param_name = f"paper_id_{index}"
        params[param_name] = paper_id
        paper_values.append(f"(:{param_name})")

    term_values = []
    for index, term in enumerate(normalized_terms):
        term_param = f"term_{index}"
        phrase_param = f"phrase_term_{index}"
        like_param = f"like_term_{index}"
        phrase_like_param = f"phrase_like_term_{index}"
        phrase_term = term.replace("-", " ")
        params[term_param] = term
        params[phrase_param] = phrase_term
        params[like_param] = f"%{_escape_like(term)}%"
        params[phrase_like_param] = f"%{_escape_like(phrase_term)}%"
        term_values.append(
            f"(:{term_param}, :{phrase_param}, :{like_param}, :{phrase_like_param})"
        )

    filters = []
    if keyword_sources:
        placeholders = []
        for index, source in enumerate(keyword_sources):
            param_name = f"keyword_source_{index}"
            params[param_name] = source
            placeholders.append(f":{param_name}")
        filters.append(f"pk.source IN ({', '.join(placeholders)})")
    filter_sql = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = text(
        f"""
        WITH candidate_papers(paper_id) AS (
            VALUES {", ".join(paper_values)}
        ),
        query_terms(term, phrase_term, like_term, phrase_like_term) AS (
            VALUES {", ".join(term_values)}
        )
        SELECT
            pk.paper_id,
            qt.term AS query_term,
            pk.keyword,
            pk.keyword_type,
            pk.source AS keyword_source,
            COALESCE(pk.weight, 1.0) AS keyword_weight
        FROM candidate_papers cp
        JOIN paper_keywords pk
          ON pk.paper_id = cp.paper_id
        JOIN query_terms qt
          ON lower(pk.keyword) = qt.term
          OR replace(lower(pk.keyword), '-', ' ') = qt.phrase_term
          OR lower(pk.keyword) LIKE qt.like_term ESCAPE '\\'
          OR replace(lower(pk.keyword), '-', ' ') LIKE qt.phrase_like_term ESCAPE '\\'
        {filter_sql}
        ORDER BY pk.paper_id, keyword_weight DESC, lower(pk.keyword) ASC
        """
    )

    with metadata_db.engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().fetchall()

    matches: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        paper_id = int(row["paper_id"])
        bucket = matches.setdefault(paper_id, [])
        if len(bucket) >= 8:
            continue
        bucket.append(
            {
                "query_term": row.get("query_term"),
                "keyword": row.get("keyword"),
                "keyword_type": row.get("keyword_type"),
                "keyword_source": row.get("keyword_source"),
                "keyword_weight": float(row.get("keyword_weight") or 0.0),
            }
        )
    return matches


def _normalize_query_terms(query_terms: Sequence[str]) -> List[str]:
    normalizer = QueryPhraseNormalizer()
    terms = []
    for term in query_terms:
        normalized = normalizer.normalize_phrase(term)
        if normalized and len(normalized) >= 3:
            terms.append(normalized)
    return list(dict.fromkeys(terms))


def _annotate_dense_filter(
    item: Dict[str, Any],
    report: Optional[DenseResultFilterReport] = None,
    matched_keywords: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    annotated = dict(item)
    retrieval_debug = dict(annotated.get("retrieval_debug") or {})
    payload = {
        "matched_keywords": list(matched_keywords or []),
    }
    if report is not None:
        payload.update(report.to_dict())
    retrieval_debug["dense_hard_filter"] = payload
    annotated["retrieval_debug"] = retrieval_debug
    return annotated


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _escape_like(value: str) -> str:
    return (
        value
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
