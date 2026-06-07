#!/usr/bin/env python3
"""Scholar-like search web app."""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Flask,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from sqlalchemy import text

# 项目根目录（Langtaosha_PD）
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from config.config_loader import get_db_engine, init_config
from src.docset_hub.indexing import (
    CompositeSpanMatcher,
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    MetadataDBPhraseLexicon,
    PaperIndexer,
    QueryPhraseAnalyzer,
    RemoteOntologySpanMatcher,
    SpanMatcherError,
    SpanMatcherExecutor,
    SpanMatchResult,
    build_search_highlight,
)


def _resolve_config_path() -> Path:
    """优先使用环境变量，否则使用腾讯后端配置。"""
    default_cfg = ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"
    return Path(
        os.environ.get(
            "PD_BACKEND_CONFIG",
            os.environ.get("PD_TEST_CONFIG", str(default_cfg)),
        )
    )


CONFIG_PATH = _resolve_config_path()
init_config(CONFIG_PATH)

indexer = PaperIndexer(
    config_path=CONFIG_PATH,
    enable_vectorization=True,
)

app = Flask(
    __name__,
    root_path=str(ROOT),
    template_folder="templates",
)

LANGTAOSHA_SOURCES = ("langtaosha",)
BIORXIV_SOURCES = ("biorxiv_history", "biorxiv_daily")
SOURCE_SEARCH_TOP_K = 100
DEFAULT_INDEXER_SEARCH_TYPE = "hybrid_retrieval"
STUDY_SESSION_COOKIE = "study_session_id"
STUDY_SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30
FEEDBACK_REVIEW_DATA_PATH = ROOT / "docs" / "implementation_log" / "20260511" / "user_study_case_study_searches_20260511.jsonl"
DEFAULT_SPAN_SCISPACY_MODEL = "en_core_sci_lg"
DEFAULT_ONTOLOGY_LINKER_URL = "http://127.0.0.1:8765"
SPAN_MATCH_DISPLAY_THRESHOLD = 0.9
_SPAN_MATCHER_CONTEXT: Optional[Dict[str, Any]] = None
SHOW_PAGE_ROOT = ROOT.parent / "zhan"
FUTURE_PAGE_ROOT = ROOT.parent / "sandbox"
GRANT_TRENDS_REVIEWS_PATH = (
    SHOW_PAGE_ROOT
    / "outputs"
    / "iterative_clustering"
    / "2021_qwen36_extended_v14_clustering_v1"
    / "hierarchy_viewer_min2_iter03_recovery"
    / "hierarchy_node_reviews.json"
)
GRANT_TRENDS_REVIEW_DECISIONS = frozenset({"accept", "reject", "recategorize"})
API_SERVICE_NAME = "langtaosha-api"
DEFAULT_FRONTEND_ALLOWED_ORIGINS = (
    "http://localhost:5004",
    "http://127.0.0.1:5004",
)
LEGACY_PAGE_PROXY_HEADER = "X-Langtaosha-Legacy-Page-Proxy"
PUBLIC_API_PATHS = {"/api/health"}
PROTECTED_API_PREFIXES = (
    "/api/ready",
    "/api/scholar/search",
)


def _request_id() -> str:
    return getattr(g, "request_id", uuid.uuid4().hex)


def _parse_allowed_origins() -> List[str]:
    raw_value = os.environ.get("FRONTEND_ALLOWED_ORIGINS", "").strip()
    if not raw_value:
        return list(DEFAULT_FRONTEND_ALLOWED_ORIGINS)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _resolve_cors_origin(origin: Optional[str]) -> Optional[str]:
    allowed_origins = _parse_allowed_origins()
    if "*" in allowed_origins:
        return origin or "*"
    if origin and origin in allowed_origins:
        return origin
    return None


def _configured_api_tokens() -> List[str]:
    raw_values = [
        os.environ.get("API_AUTH_TOKEN", ""),
        os.environ.get("API_AUTH_TOKENS", ""),
    ]
    tokens: List[str] = []
    for raw_value in raw_values:
        tokens.extend(
            item.strip()
            for item in raw_value.split(",")
            if item.strip()
        )
    return tokens


def _extract_bearer_token() -> Optional[str]:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if not auth_header:
        return None
    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _api_path_requires_auth(path: str) -> bool:
    if request.method == "OPTIONS":
        return False
    if path in PUBLIC_API_PATHS:
        return False
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in PROTECTED_API_PREFIXES)


def _api_token_authorized() -> bool:
    configured_tokens = _configured_api_tokens()
    if not configured_tokens:
        return True
    token = _extract_bearer_token()
    return bool(token and token in configured_tokens)


@app.before_request
def assign_request_id():
    g.request_id = (
        request.headers.get("X-Request-Id")
        or request.headers.get("X-Correlation-Id")
        or uuid.uuid4().hex
    )
    if request.path.startswith("/api/"):
        if _api_path_requires_auth(request.path) and not _api_token_authorized():
            return _api_error(
                "missing or invalid API token",
                status_code=401,
                code="UNAUTHORIZED",
            )
        return None
    if os.environ.get("ALLOW_DIRECT_LEGACY_PAGES", "0") == "1":
        return None
    if request.headers.get(LEGACY_PAGE_PROXY_HEADER) == "1":
        return None
    return "Not Found", 404


@app.after_request
def attach_api_headers(response):
    response.headers["X-Request-Id"] = _request_id()

    if request.path.startswith("/api/"):
        allowed_origin = _resolve_cors_origin(request.headers.get("Origin"))
        if allowed_origin:
            response.headers["Access-Control-Allow-Origin"] = allowed_origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Headers"] = (
            "Authorization, Content-Type, X-Request-Id, X-Correlation-Id"
        )
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"

    return response


def _api_success(payload: Optional[Dict[str, Any]] = None, status_code: int = 200):
    body = dict(payload or {})
    body.setdefault("success", True)
    body["request_id"] = _request_id()
    return jsonify(body), status_code


def _api_error(
    message: str,
    status_code: int = 500,
    code: str = "INTERNAL_ERROR",
    extra: Optional[Dict[str, Any]] = None,
):
    body: Dict[str, Any] = {
        "success": False,
        "error": message,
        "error_code": code,
        "error_detail": {
            "code": code,
            "message": message,
            "request_id": _request_id(),
        },
        "request_id": _request_id(),
    }
    if extra:
        body.update(extra)
    return jsonify(body), status_code


def _extract_doi(metadata: Dict[str, Any]) -> Optional[str]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")

    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id and source.get("doi"):
                return source.get("doi")

    for source in sources:
        if source.get("doi"):
            return source.get("doi")
    return None


def _extract_authors(metadata: Dict[str, Any]) -> str:
    author_items = metadata.get("authors") or []
    names = [item.get("name", "").strip() for item in author_items if item.get("name")]
    return ", ".join(names)


def _normalize_source_label(source_name: Optional[str]) -> str:
    if not source_name:
        return "-"
    if source_name.startswith("biorxiv_"):
        return "Biorxiv"
    if source_name == "langtaosha":
        return "Langtaosha"
    return source_name


def _normalize_source_key(source_name: Optional[str]) -> str:
    if not source_name:
        return "unknown"
    if source_name.startswith("biorxiv_"):
        return "biorxiv"
    if source_name == "langtaosha":
        return "langtaosha"
    return source_name.lower()


def _get_preferred_source(metadata: Dict[str, Any]) -> Dict[str, Any]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")

    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id:
                return source

    if sources:
        return sources[0]
    return {}


def _extract_paper_link(metadata: Dict[str, Any], doi: Optional[str]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("source_url"):
        return preferred_source.get("source_url")

    source_name = preferred_source.get("source_name")
    if source_name and source_name.startswith("biorxiv_") and doi:
        return f"https://www.biorxiv.org/content/{doi}"

    return None


def _build_link(source_name: Optional[str], source_url: Optional[str], doi: Optional[str]) -> Optional[str]:
    if source_url:
        return source_url
    if source_name and source_name.startswith("biorxiv_") and doi:
        return f"https://www.biorxiv.org/content/{doi}"
    return None


def _extract_authors_from_json(authors_json: Any) -> str:
    if not authors_json or not isinstance(authors_json, list):
        return ""
    names = []
    for item in authors_json:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            if name:
                names.append(name)
    return ", ".join(names)


def _format_date_ymd(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.strftime("%Y-%m-%d")
    except Exception:
        s = str(value)
        if len(s) >= 10:
            return s[:10]
        return s


def _get_similarity_score(item: Dict[str, Any]) -> Optional[float]:
    score = item.get("similarity_score", item.get("similarity"))
    if score is None:
        return None
    try:
        return float(score)
    except (TypeError, ValueError):
        return None


def _is_positive_score(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _format_reason_score(value: Any) -> Optional[float]:
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _dedupe_texts(values: List[str], limit: int = 8) -> List[str]:
    seen = set()
    result = []
    for value in values:
        text_value = str(value or "").strip()
        if not text_value:
            continue
        key = text_value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text_value)
        if len(result) >= limit:
            break
    return result


def _extract_keyword_reason_concepts(retrieval_debug: Dict[str, Any]) -> List[str]:
    matched_concepts = (
        retrieval_debug.get("keyword_lookup_matched_concepts")
        or (retrieval_debug.get("keyword_lookup_debug") or {}).get("matched_concepts")
        or []
    )
    labels: List[str] = []

    for concept in matched_concepts:
        if not isinstance(concept, dict):
            labels.append(str(concept))
            continue

        label = (
            concept.get("concept_label")
            or concept.get("canonical")
            or concept.get("text")
            or concept.get("normalized_text")
            or concept.get("concept_id")
        )
        if label:
            labels.append(str(label))

        matched_keywords = concept.get("matched_keywords") or []
        for keyword_item in matched_keywords:
            if isinstance(keyword_item, dict):
                keyword = keyword_item.get("keyword") or keyword_item.get("query_term")
            else:
                keyword = keyword_item
            if keyword:
                labels.append(str(keyword))

    return _dedupe_texts(labels)


def _build_retrieval_reasons(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build display-ready evidence tags from hybrid retrieval debug payload."""
    retrieval_debug = item.get("retrieval_debug") or {}
    if not isinstance(retrieval_debug, dict):
        return []

    matched_retrievers = set(retrieval_debug.get("matched_retrievers") or [])
    reasons: List[Dict[str, Any]] = []

    dense_score = retrieval_debug.get("dense_score")
    if "dense" in matched_retrievers or _is_positive_score(dense_score):
        reason = {
            "key": "dense_score_qualified",
            "label": "dense_score_qualified",
        }
        formatted_score = _format_reason_score(dense_score)
        if formatted_score is not None:
            reason["score"] = formatted_score
        reasons.append(reason)

    sparse_score = retrieval_debug.get("sparse_score")
    if "sparse" in matched_retrievers or _is_positive_score(sparse_score):
        reason = {
            "key": "sparse_score_qualified",
            "label": "sparse_score_qualified",
        }
        formatted_score = _format_reason_score(sparse_score)
        if formatted_score is not None:
            reason["score"] = formatted_score
        reasons.append(reason)

    keyword_score = retrieval_debug.get("keyword_lookup_score")
    if "keyword_lookup" in matched_retrievers or _is_positive_score(keyword_score):
        concepts = _extract_keyword_reason_concepts(retrieval_debug)
        reason = {
            "key": "keyword_score_qualified",
            "label": "keyword_score_qualified",
            "matched_concepts": concepts,
        }
        formatted_score = _format_reason_score(keyword_score)
        if formatted_score is not None:
            reason["score"] = formatted_score
        reasons.append(reason)

    return reasons


def _resolve_search_sources(source_list: Optional[List[str]]) -> List[str]:
    sources = source_list if source_list else list(indexer.default_sources)
    valid_sources = set(indexer.default_sources)
    resolved_sources: List[str] = []
    invalid_sources: List[str] = []

    for source in sources:
        if source not in valid_sources:
            invalid_sources.append(source)
            continue
        if source not in resolved_sources:
            resolved_sources.append(source)

    if invalid_sources:
        raise ValueError(
            f"source_list 包含未知 source: {invalid_sources}; "
            f"合法 sources: {indexer.default_sources}"
        )

    return resolved_sources


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


def _get_span_matcher_context() -> Dict[str, Any]:
    global _SPAN_MATCHER_CONTEXT
    if _SPAN_MATCHER_CONTEXT is not None:
        return _SPAN_MATCHER_CONTEXT

    paper_sources = _parse_csv_items(
        os.environ.get("PAPER_SOURCES"),
        default=list(indexer.default_sources),
    )
    keyword_sources = _parse_csv_items(os.environ.get("KEYWORD_SOURCE"))
    lexicon = MetadataDBPhraseLexicon(
        metadata_db=indexer.metadata_db,
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

    _SPAN_MATCHER_CONTEXT = {
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
    return _SPAN_MATCHER_CONTEXT


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


def run_span_matcher_test(query: str) -> Dict[str, Any]:
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    context = _get_span_matcher_context()
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


def _dedupe_search_results(result_groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged_results: List[Dict[str, Any]] = []
    seen_work_ids = set()

    for results in result_groups:
        for item in results:
            work_id = item.get("work_id")
            if work_id:
                if work_id in seen_work_ids:
                    continue
                seen_work_ids.add(work_id)
            merged_results.append(item)

    return merged_results


def _prioritized_vector_search(
    query: str,
    source_list: Optional[List[str]],
    per_source_top_k: int = SOURCE_SEARCH_TOP_K,
) -> List[Dict[str, Any]]:
    resolved_sources = _resolve_search_sources(source_list)
    langtaosha_sources = [
        source for source in resolved_sources
        if source in LANGTAOSHA_SOURCES
    ]
    biorxiv_sources = [
        source for source in resolved_sources
        if source in BIORXIV_SOURCES
    ]
    other_sources = [
        source for source in resolved_sources
        if source not in LANGTAOSHA_SOURCES and source not in BIORXIV_SOURCES
    ]

    langtaosha_results = []
    if langtaosha_sources:
        langtaosha_results = indexer.search(
            query=query,
            source_list=langtaosha_sources,
            top_k=per_source_top_k,
            hydrate=True,
            search_type=DEFAULT_INDEXER_SEARCH_TYPE,
        )

    biorxiv_results = []
    if biorxiv_sources:
        biorxiv_results = indexer.search(
            query=query,
            source_list=biorxiv_sources,
            top_k=per_source_top_k,
            hydrate=True,
            search_type=DEFAULT_INDEXER_SEARCH_TYPE,
        )

    other_results = []
    if other_sources:
        other_results = indexer.search(
            query=query,
            source_list=other_sources,
            top_k=per_source_top_k,
            hydrate=True,
            search_type=DEFAULT_INDEXER_SEARCH_TYPE,
        )

    return _dedupe_search_results(
        [langtaosha_results, biorxiv_results, other_results]
    )


def _map_search_item(
    item: Dict[str, Any],
    highlight: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metadata = item.get("metadata") or item
    doi = _extract_doi(metadata)
    preferred_source = _get_preferred_source(metadata)
    raw_source_name = preferred_source.get("source_name") or item.get("source_name")
    online_at_raw = metadata.get("online_at")
    similarity_score = _get_similarity_score(item)
    retrieval_reasons = _build_retrieval_reasons(item)
    mapped = {
        "work_id": item.get("work_id"),
        "paper_id": item.get("paper_id"),
        "source_name": item.get("source_name"),
        "similarity": similarity_score,
        "similarity_score": similarity_score,
        "title": metadata.get("canonical_title"),
        "abstract": metadata.get("canonical_abstract"),
        "authors": _extract_authors(metadata),
        "doi": doi,
        "online_date": _format_date_ymd(online_at_raw),
        "source": _normalize_source_label(raw_source_name),
        "source_key": _normalize_source_key(raw_source_name),
        "link": _extract_paper_link(metadata, doi),
        "retrieval_reasons": retrieval_reasons,
        "retrieval_reason_tags": [reason["key"] for reason in retrieval_reasons],
    }
    if highlight:
        mapped["highlight"] = highlight
    return mapped


def _build_query_notice(
    query: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    search_mode: str,
) -> Optional[Dict[str, Any]]:
    if search_mode == "vector":
        return {
            "type": "vector",
            "message": "已按原 query 执行向量检索。",
        }

    if not understanding:
        return None

    intent = understanding.get("intent")
    route = understanding.get("route")
    matched_author = understanding.get("matched_author") or search_query
    suggested_author = understanding.get("suggested_author")
    corrected_query = understanding.get("corrected_query")
    normalized_query = understanding.get("normalized_query") or query

    if intent == "author_name" and route == "metadata_author" and matched_author:
        return {
            "type": "author_name",
            "message": f"已识别为作者名，正在根据作者 {matched_author} 完成搜索。",
            "action_label": "改用向量检索",
            "fallback_mode": "vector",
            "fallback_query": query,
        }

    if intent == "author_name" and route == "author_suggestion" and suggested_author:
        return {
            "type": "author_suggestion",
            "message": f'未找到 "{query}" 的高置信作者匹配，是否搜索作者 {suggested_author}？',
            "action_label": f"搜索作者 {suggested_author}",
            "fallback_mode": "smart",
            "fallback_query": suggested_author,
        }

    if corrected_query and corrected_query != normalized_query:
        return {
            "type": "query_correction",
            "message": f"已识别到可能的拼写错误，实际搜索 query 为: {corrected_query}",
            "action_label": "使用原 query 检索",
            "fallback_mode": "vector",
            "fallback_query": query,
        }

    return None


def _normalize_top_k(top_k: Any) -> int:
    try:
        normalized_top_k = int(top_k)
    except (TypeError, ValueError):
        normalized_top_k = 100
    if normalized_top_k <= 0:
        return 100
    return min(normalized_top_k, 100)


def _parse_source_list(source_list_raw: Optional[str]) -> Optional[List[str]]:
    source_list_text = (source_list_raw or "").strip()
    if not source_list_text:
        return None
    return [x.strip() for x in source_list_text.split(",") if x.strip()]


def _optional_limited_text(value: Any, max_length: Optional[int] = None) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    if max_length is not None:
        return text_value[:max_length]
    return text_value


def _normalize_participant_id(value: Any) -> Optional[str]:
    participant_id = _optional_limited_text(value)
    if participant_id and len(participant_id) > 120:
        raise ValueError("participant_id 不能超过 120 个字符")
    return participant_id


def _normalize_study_session_id(value: Any) -> Optional[str]:
    study_session_id = _optional_limited_text(value)
    if study_session_id and len(study_session_id) > 80:
        raise ValueError("study_session_id 不能超过 80 个字符")
    return study_session_id


def _get_or_create_study_session_id(candidate: Any = None) -> Tuple[str, bool]:
    study_session_id = _normalize_study_session_id(candidate)
    if study_session_id:
        return study_session_id, False
    return f"s_{uuid.uuid4().hex}", True


def _attach_study_session_cookie(response, study_session_id: str):
    response.set_cookie(
        STUDY_SESSION_COOKIE,
        study_session_id,
        max_age=STUDY_SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="Lax",
    )
    return response


def _json_payload(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _coerce_required_int(payload: Dict[str, Any], field_name: str) -> int:
    raw_value = payload.get(field_name)
    if raw_value is None or raw_value == "":
        raise ValueError(f"{field_name} 不能为空")
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} 必须是整数") from exc
    if value <= 0:
        raise ValueError(f"{field_name} 必须大于 0")
    return value


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_search_mode(search_mode: Any) -> str:
    normalized_mode = (str(search_mode or "smart")).strip().lower()
    if normalized_mode not in {"smart", "vector"}:
        raise ValueError("mode 只能是 smart 或 vector")
    return normalized_mode


def run_scholar_search(
    query: str,
    top_k: int = 100,
    source_list: Optional[List[str]] = None,
    search_mode: str = "smart",
) -> Dict[str, Any]:
    """Run the existing PaperIndexer search flow and return the API body."""
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    normalized_top_k = _normalize_top_k(top_k)
    normalized_mode = _validate_search_mode(search_mode)

    if normalized_mode == "vector":
        results = _prioritized_vector_search(
            query=normalized_query,
            source_list=source_list,
            per_source_top_k=normalized_top_k,
        )
        search_query = normalized_query
        understanding = {
            "original_query": normalized_query,
            "normalized_query": normalized_query,
            "intent": "semantic_search",
            "route": "vector",
            "corrected_query": None,
            "matched_author": None,
            "suggested_author": None,
            "confidence": 0.0,
            "candidates": [],
            "corrections": [],
            "reason": "forced_vector_search",
        }
    else:
        understanding_result = indexer.query_understanding.analyze(normalized_query)
        understanding = understanding_result.to_dict()

        if understanding_result.route == "none":
            results = []
            search_query = None
        elif understanding_result.route == "metadata_author":
            resolved_sources = _resolve_search_sources(source_list)
            search_query = (
                understanding_result.matched_author
                or understanding_result.normalized_query
            )
            results = indexer.metadata_db.search_by_author(
                author_name=search_query,
                limit=normalized_top_k,
                source_list=resolved_sources,
                fuzzy=True,
            )
        elif understanding_result.route == "author_suggestion":
            results = []
            search_query = None
        else:
            search_query = (
                understanding_result.corrected_query
                or understanding_result.normalized_query
            )
            results = _prioritized_vector_search(
                query=search_query,
                source_list=source_list,
                per_source_top_k=normalized_top_k,
            )

    highlight = build_search_highlight(
        query=normalized_query,
        search_query=search_query,
        understanding=understanding,
        search_mode=normalized_mode,
    )
    mapped_results = [_map_search_item(item, highlight=highlight) for item in results]
    notice = _build_query_notice(
        query=normalized_query,
        search_query=search_query,
        understanding=understanding,
        search_mode=normalized_mode,
    )
    return {
        "success": True,
        "query": normalized_query,
        "search_query": search_query,
        "search_mode": normalized_mode,
        "query_understanding": understanding,
        "result_policy": {
            "langtaosha_top_k": normalized_top_k,
            "biorxiv_top_k": normalized_top_k,
            "dedupe_key": "work_id",
            "default_frontend_source": "langtaosha",
            "search_type": DEFAULT_INDEXER_SEARCH_TYPE,
            "display": "show_langtaosha_first_then_biorxiv",
        },
        "notice": notice,
        "count": len(mapped_results),
        "results": mapped_results,
    }


def insert_user_study_search_event(
    study_session_id: str,
    participant_id: str,
    search_data: Dict[str, Any],
) -> Tuple[int, int]:
    engine = get_db_engine(db_key="metadata_db")
    understanding = search_data.get("query_understanding") or {}
    payload = {
        "query_understanding": understanding,
        "notice": search_data.get("notice"),
        "result_policy": search_data.get("result_policy"),
    }

    with engine.begin() as conn:
        query_index = conn.execute(
            text(
                """
                SELECT COALESCE(MAX(query_index), 0) + 1
                FROM user_study_events
                WHERE event_type = 'search'
                  AND study_session_id = :study_session_id
                """
            ),
            {"study_session_id": study_session_id},
        ).scalar_one()

        event_id = conn.execute(
            text(
                """
                INSERT INTO user_study_events (
                    event_type,
                    study_session_id,
                    participant_id,
                    query_index,
                    query,
                    search_mode,
                    search_query,
                    query_understanding_route,
                    result_count,
                    payload
                )
                VALUES (
                    'search',
                    :study_session_id,
                    :participant_id,
                    :query_index,
                    :query,
                    :search_mode,
                    :search_query,
                    :query_understanding_route,
                    :result_count,
                    CAST(:payload AS JSONB)
                )
                RETURNING id
                """
            ),
            {
                "study_session_id": study_session_id,
                "participant_id": participant_id,
                "query_index": query_index,
                "query": search_data.get("query"),
                "search_mode": search_data.get("search_mode"),
                "search_query": search_data.get("search_query"),
                "query_understanding_route": understanding.get("route"),
                "result_count": search_data.get("count"),
                "payload": _json_payload(payload),
            },
        ).scalar_one()

    return int(event_id), int(query_index)


def _extract_year_from_result(result: Dict[str, Any]) -> Optional[str]:
    online_date = result.get("online_date")
    if not online_date:
        return None
    value = str(online_date)
    if len(value) >= 4 and value[:4].isdigit():
        return value[:4]
    return None


def insert_user_study_search_results(
    study_session_id: str,
    participant_id: str,
    query_index: int,
    search_event_id: int,
    results: List[Dict[str, Any]],
) -> int:
    if not results:
        return 0

    rows = []
    for index, result in enumerate(results):
        rows.append(
            {
                "search_event_id": search_event_id,
                "study_session_id": study_session_id,
                "participant_id": participant_id,
                "query_index": query_index,
                "result_rank": index + 1,
                "work_id": _optional_limited_text(result.get("work_id"), 200),
                "paper_id": _coerce_optional_int(result.get("paper_id")),
                "title": _optional_limited_text(result.get("title")),
                "source": _optional_limited_text(result.get("source"), 64),
                "source_key": _optional_limited_text(result.get("source_key"), 64),
                "year": _extract_year_from_result(result),
                "online_date": _optional_limited_text(result.get("online_date"), 32),
                "similarity_score": _coerce_optional_float(
                    result.get("similarity_score", result.get("similarity"))
                ),
                "payload": _json_payload(result),
            }
        )

    engine = get_db_engine(db_key="metadata_db")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO user_study_search_results (
                    search_event_id,
                    study_session_id,
                    participant_id,
                    query_index,
                    result_rank,
                    work_id,
                    paper_id,
                    title,
                    source,
                    source_key,
                    year,
                    online_date,
                    similarity_score,
                    payload
                )
                VALUES (
                    :search_event_id,
                    :study_session_id,
                    :participant_id,
                    :query_index,
                    :result_rank,
                    :work_id,
                    :paper_id,
                    :title,
                    :source,
                    :source_key,
                    :year,
                    :online_date,
                    :similarity_score,
                    CAST(:payload AS JSONB)
                )
                ON CONFLICT (search_event_id, result_rank) DO NOTHING
                """
            ),
            rows,
        )
    return int(result.rowcount or 0)


def _validate_feedback_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("请求体必须是 JSON object")

    study_session_id = _normalize_study_session_id(
        payload.get("study_session_id") or request.cookies.get(STUDY_SESSION_COOKIE)
    )
    if not study_session_id:
        raise ValueError("study_session_id 不能为空")

    feedback = _optional_limited_text(payload.get("feedback"), 32)
    if feedback not in {"relevant", "not_relevant"}:
        raise ValueError("feedback 必须是 relevant 或 not_relevant")

    reason_text = _optional_limited_text(payload.get("reason_text"))
    if feedback == "not_relevant" and not reason_text:
        raise ValueError("feedback 为 not_relevant 时 reason_text 不能为空")
    if feedback == "relevant":
        reason_text = None

    return {
        "study_session_id": study_session_id,
        "participant_id": _normalize_participant_id(payload.get("participant_id")),
        "search_event_id": _coerce_required_int(payload, "search_event_id"),
        "query_index": _coerce_optional_int(payload.get("query_index")),
        "query": _optional_limited_text(payload.get("query")),
        "search_mode": _optional_limited_text(payload.get("search_mode"), 32),
        "search_query": _optional_limited_text(payload.get("search_query")),
        "result_rank": _coerce_required_int(payload, "result_rank"),
        "work_id": _optional_limited_text(payload.get("work_id"), 200),
        "paper_id": _coerce_optional_int(payload.get("paper_id")),
        "title": _optional_limited_text(payload.get("title")),
        "source": _optional_limited_text(payload.get("source"), 64),
        "year": _optional_limited_text(payload.get("year"), 16),
        "similarity_score": _coerce_optional_float(payload.get("similarity_score")),
        "feedback": feedback,
        "reason_text": reason_text,
        "payload": payload,
    }


def insert_user_study_feedback_event(feedback_data: Dict[str, Any]) -> int:
    engine = get_db_engine(db_key="metadata_db")
    with engine.begin() as conn:
        event_id = conn.execute(
            text(
                """
                INSERT INTO user_study_events (
                    event_type,
                    study_session_id,
                    participant_id,
                    query_index,
                    search_event_id,
                    query,
                    search_mode,
                    search_query,
                    result_rank,
                    work_id,
                    paper_id,
                    title,
                    source,
                    year,
                    similarity_score,
                    feedback,
                    reason_text,
                    payload
                )
                VALUES (
                    'result_feedback',
                    :study_session_id,
                    :participant_id,
                    :query_index,
                    :search_event_id,
                    :query,
                    :search_mode,
                    :search_query,
                    :result_rank,
                    :work_id,
                    :paper_id,
                    :title,
                    :source,
                    :year,
                    :similarity_score,
                    :feedback,
                    :reason_text,
                    CAST(:payload AS JSONB)
                )
                RETURNING id
                """
            ),
            {
                **feedback_data,
                "payload": _json_payload(feedback_data.get("payload") or {}),
            },
        ).scalar_one()
    return int(event_id)


# Recommendation prototype: thin, removable wrapper around the existing search stack.
def _normalize_recommend_top_k(top_k: Any) -> int:
    try:
        normalized_top_k = int(top_k)
    except (TypeError, ValueError):
        normalized_top_k = 5
    if normalized_top_k <= 0:
        return 5
    return min(normalized_top_k, 20)


def _read_seed_paper(work_id: Optional[str], paper_id_raw: Any) -> Dict[str, Any]:
    normalized_work_id = (work_id or "").strip()
    paper_id: Optional[int] = None
    if paper_id_raw not in (None, ""):
        try:
            paper_id = int(paper_id_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("paper_id ?????") from exc
        if paper_id <= 0:
            raise ValueError("paper_id ???? 0")

    if not normalized_work_id and paper_id is None:
        raise ValueError("???? work_id ? paper_id")

    seed = indexer.read(work_id=normalized_work_id or None, paper_id=paper_id)
    if not seed:
        raise ValueError("?????? seed ??")
    return seed


def _build_recommend_query(seed: Dict[str, Any]) -> str:
    title = (seed.get("canonical_title") or "").strip()
    abstract = (seed.get("canonical_abstract") or "").strip()
    if not title and not abstract:
        raise ValueError("seed ??????????????????")
    if len(abstract) > 1200:
        abstract = abstract[:1200]
    return " ".join(part for part in [title, abstract] if part).strip()


def _same_paper(candidate: Dict[str, Any], seed: Dict[str, Any]) -> bool:
    seed_work_id = seed.get("work_id")
    seed_paper_id = seed.get("paper_id")
    if seed_work_id and candidate.get("work_id") == seed_work_id:
        return True
    if seed_paper_id is not None and candidate.get("paper_id") == seed_paper_id:
        return True
    return False


def build_recommendations(
    work_id: Optional[str] = None,
    paper_id: Any = None,
    top_k: int = 5,
    source_list: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build a removable similar-paper recommendation prototype."""
    normalized_top_k = _normalize_recommend_top_k(top_k)
    seed = _read_seed_paper(work_id=work_id, paper_id_raw=paper_id)
    query_text = _build_recommend_query(seed)

    raw_candidates = _prioritized_vector_search(
        query=query_text,
        source_list=source_list,
        per_source_top_k=min(max(normalized_top_k + 5, 10), 30),
    )

    recommendations: List[Dict[str, Any]] = []
    for candidate in raw_candidates:
        mapped = _map_search_item(candidate)
        if _same_paper(mapped, seed):
            continue
        recommendations.append(mapped)
        if len(recommendations) >= normalized_top_k:
            break

    return {
        "success": True,
        "mode": "similar_paper_prototype",
        "seed": _map_search_item(seed),
        "query_terms": {
            "title": seed.get("canonical_title"),
            "abstract_used": bool(seed.get("canonical_abstract")),
        },
        "count": len(recommendations),
        "results": recommendations,
    }


def _get_daily_new_papers(limit: int = 10) -> List[Dict[str, Any]]:
    limit = max(1, min(limit, 20))
    engine = get_db_engine(db_key="metadata_db")
    sql = text(
        """
        SELECT
            p.paper_id,
            p.work_id,
            p.canonical_title AS title,
            COALESCE(p.online_at, ps.online_at) AS online_at,
            ps.source_name,
            ps.source_url,
            ps.doi,
            paa.authors
        FROM papers p
        LEFT JOIN paper_author_affiliation paa ON paa.paper_id = p.paper_id
        LEFT JOIN LATERAL (
            SELECT
                ps1.source_name,
                ps1.source_url,
                ps1.doi,
                ps1.online_at,
                ps1.paper_source_id
            FROM paper_sources ps1
            WHERE ps1.paper_id = p.paper_id
            ORDER BY
                CASE WHEN ps1.paper_source_id = p.canonical_source_id THEN 0 ELSE 1 END,
                ps1.online_at DESC NULLS LAST,
                ps1.paper_source_id DESC
            LIMIT 1
        ) ps ON TRUE
        WHERE COALESCE(p.online_at, ps.online_at) IS NOT NULL
        ORDER BY COALESCE(p.online_at, ps.online_at) DESC
        LIMIT :limit
        """
    )
    rows: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        result = conn.execute(sql, {"limit": limit})
        for row in result.mappings():
            source_name = row.get("source_name")
            doi = row.get("doi")
            source_url = row.get("source_url")
            online_at = row.get("online_at")
            rows.append(
                {
                    "paper_id": row.get("paper_id"),
                    "work_id": row.get("work_id"),
                    "title": row.get("title"),
                    "authors": _extract_authors_from_json(row.get("authors")),
                    "online_at": online_at.isoformat() if online_at else None,
                    "online_date": _format_date_ymd(online_at),
                    "source": _normalize_source_label(source_name),
                    "source_key": _normalize_source_key(source_name),
                    "link": _build_link(source_name, source_url, doi),
                }
            )
    return rows


def load_feedback_review_searches() -> List[Dict[str, Any]]:
    if not FEEDBACK_REVIEW_DATA_PATH.exists():
        raise FileNotFoundError(f"feedback review data not found: {FEEDBACK_REVIEW_DATA_PATH}")

    searches: List[Dict[str, Any]] = []
    with FEEDBACK_REVIEW_DATA_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            searches.append(json.loads(line))
    return searches


@app.route("/")
def index() -> str:
    return render_template("welcome.html")


@app.route("/show_page")
@app.route("/show_page/")
def show_page_index():
    return send_from_directory(SHOW_PAGE_ROOT, "index.html")


@app.route("/show_page/image.png")
def show_page_image():
    return send_from_directory(SHOW_PAGE_ROOT, "image.png")


@app.route("/show_page/assets/<path:filename>")
def show_page_assets(filename: str):
    return send_from_directory(SHOW_PAGE_ROOT / "assets", filename)


@app.route("/show_page/grant_trends/")
@app.route("/show_page/grant_trends")
def show_page_grant_trends():
    return send_from_directory(SHOW_PAGE_ROOT / "grant_trends", "index.html")


@app.route("/grant_trends/")
@app.route("/grant_trends")
def grant_trends_page():
    return send_from_directory(SHOW_PAGE_ROOT / "grant_trends", "index.html")


@app.route("/show_page/grant_trends/image.png")
def show_page_grant_trends_image():
    return send_from_directory(SHOW_PAGE_ROOT / "grant_trends", "image.png")


@app.route("/grant_trends/image.png")
def grant_trends_page_image():
    return send_from_directory(SHOW_PAGE_ROOT / "grant_trends", "image.png")


@app.route("/show_page/outputs/<path:filename>")
def show_page_outputs(filename: str):
    return send_from_directory(SHOW_PAGE_ROOT / "outputs", filename)


@app.route("/grant_trends/outputs/<path:filename>")
def grant_trends_page_outputs(filename: str):
    return send_from_directory(SHOW_PAGE_ROOT / "outputs", filename)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_grant_trends_node_reviews() -> Dict[str, Any]:
    return {
        "metadata": {
            "schema_version": 1,
            "hierarchy_run_id": "2021_qwen36_extended_v14_clustering_v1",
            "artifact": (
                "hierarchy_viewer_min2_iter03_recovery/hierarchy_latest.json"
            ),
            "updated_at": _utc_now_iso(),
        },
        "reviews": {},
    }


def _load_grant_trends_node_reviews() -> Dict[str, Any]:
    if not GRANT_TRENDS_REVIEWS_PATH.exists():
        return _default_grant_trends_node_reviews()
    with GRANT_TRENDS_REVIEWS_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("hierarchy_node_reviews.json must be a JSON object")
    reviews = payload.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError("hierarchy_node_reviews.json reviews must be an object")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "metadata": metadata,
        "reviews": reviews,
    }


def _save_grant_trends_node_reviews(payload: Dict[str, Any]) -> None:
    GRANT_TRENDS_REVIEWS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with GRANT_TRENDS_REVIEWS_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def _validate_grant_trends_node_review_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("请求体必须是 JSON object")

    node_id = _optional_limited_text(payload.get("node_id"), 240)
    if not node_id:
        raise ValueError("node_id 不能为空")

    decision = _optional_limited_text(payload.get("decision"), 32)
    if decision not in GRANT_TRENDS_REVIEW_DECISIONS:
        raise ValueError("decision 必须是 accept、reject 或 recategorize")

    target_label = _optional_limited_text(payload.get("target_label"), 240)
    if decision == "recategorize" and not target_label:
        raise ValueError("recategorize 需要 target_label")
    if decision != "recategorize":
        target_label = None

    node_label = _optional_limited_text(payload.get("node_label"), 240)
    node_type = _optional_limited_text(payload.get("node_type"), 64)

    return {
        "node_id": node_id,
        "decision": decision,
        "target_label": target_label,
        "node_label": node_label,
        "node_type": node_type,
    }


@app.route("/grant_trends/api/node-reviews", methods=["GET"])
def grant_trends_node_reviews_get():
    try:
        payload = _load_grant_trends_node_reviews()
        return jsonify(
            {
                "success": True,
                "metadata": payload.get("metadata") or {},
                "reviews": payload.get("reviews") or {},
                "request_id": _request_id(),
            }
        )
    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "error": str(exc),
                "error_code": "NODE_REVIEWS_LOAD_FAILED",
                "request_id": _request_id(),
            }
        ), 500


@app.route("/grant_trends/api/node-reviews", methods=["POST"])
def grant_trends_node_reviews_post():
    try:
        payload = request.get_json(silent=True) or {}
        review_input = _validate_grant_trends_node_review_payload(payload)
        store = _load_grant_trends_node_reviews()
        metadata = store.get("metadata") or {}
        reviews = store.get("reviews") or {}

        review_record = {
            "decision": review_input["decision"],
            "reviewed_at": _utc_now_iso(),
            "node_label": review_input.get("node_label"),
            "node_type": review_input.get("node_type"),
        }
        if review_input.get("target_label"):
            review_record["target_label"] = review_input["target_label"]

        reviews[review_input["node_id"]] = review_record
        metadata["updated_at"] = _utc_now_iso()
        metadata.setdefault("schema_version", 1)
        metadata.setdefault(
            "hierarchy_run_id",
            "2021_qwen36_extended_v14_clustering_v1",
        )
        metadata.setdefault(
            "artifact",
            "hierarchy_viewer_min2_iter03_recovery/hierarchy_latest.json",
        )

        saved_payload = {"metadata": metadata, "reviews": reviews}
        _save_grant_trends_node_reviews(saved_payload)
        return jsonify(
            {
                "success": True,
                "node_id": review_input["node_id"],
                "review": review_record,
                "request_id": _request_id(),
            }
        )
    except ValueError as exc:
        return jsonify(
            {
                "success": False,
                "error": str(exc),
                "error_code": "INVALID_REQUEST",
                "request_id": _request_id(),
            }
        ), 400
    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "error": str(exc),
                "error_code": "NODE_REVIEWS_SAVE_FAILED",
                "request_id": _request_id(),
            }
        ), 500


@app.route("/future")
def future_page_redirect():
    return redirect(url_for("future_page_index"))


@app.route("/future/")
def future_page_index():
    return send_from_directory(FUTURE_PAGE_ROOT, "index.html")


@app.route("/future/style.css")
def future_page_style():
    return send_from_directory(FUTURE_PAGE_ROOT, "style.css")


@app.route("/future/script.js")
def future_page_script():
    return send_from_directory(FUTURE_PAGE_ROOT, "script.js")


@app.route("/future/assets/<path:filename>")
def future_page_assets(filename: str):
    return send_from_directory(FUTURE_PAGE_ROOT / "assets", filename)


@app.route("/search")
def search_page() -> str:
    query = (request.args.get("q") or "").strip()
    return render_template(
        "search.html",
        initial_query=query,
        is_study_mode=False,
        participant_id=None,
    )


@app.route("/span-matcher")
def span_matcher_page() -> str:
    query = (request.args.get("q") or "").strip()
    return render_template("span_matcher.html", initial_query=query)


@app.route("/feedback-review")
def feedback_review_page() -> str:
    return render_template("feedback_review.html")


@app.route("/study")
def study_start_page():
    try:
        study_session_id, _ = _get_or_create_study_session_id(
            request.args.get("study_session_id")
            or request.cookies.get(STUDY_SESSION_COOKIE)
        )
        participant_id = _normalize_participant_id(request.args.get("participant_id"))
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    if participant_id:
        return redirect(
            url_for(
                "study_search_page",
                participant_id=participant_id,
                study_session_id=study_session_id,
            )
        )

    response = make_response(
        render_template("study_start.html", study_session_id=study_session_id)
    )
    return _attach_study_session_cookie(response, study_session_id)


@app.route("/study/search")
def study_search_page():
    try:
        participant_id = _normalize_participant_id(request.args.get("participant_id"))
        if not participant_id:
            return redirect(url_for("study_start_page"))
        study_session_id, _ = _get_or_create_study_session_id(
            request.args.get("study_session_id")
            or request.cookies.get(STUDY_SESSION_COOKIE)
        )
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    query = (request.args.get("q") or "").strip()
    response = make_response(
        render_template(
            "search.html",
            initial_query=query,
            is_study_mode=True,
            participant_id=participant_id,
        )
    )
    return _attach_study_session_cookie(response, study_session_id)


@app.route("/api/health", methods=["GET"])
def api_health():
    return _api_success(
        {
            "status": "ok",
            "service": API_SERVICE_NAME,
        }
    )


@app.route("/api/ready", methods=["GET"])
def api_ready():
    checks = {"metadata_db": "unknown"}
    try:
        engine = get_db_engine(db_key="metadata_db")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["metadata_db"] = "ok"
        return _api_success(
            {
                "status": "ready",
                "service": API_SERVICE_NAME,
                "checks": checks,
            }
        )
    except Exception:
        checks["metadata_db"] = "failed"
        return _api_error(
            "metadata_db unavailable",
            status_code=503,
            code="READINESS_FAILED",
            extra={
                "status": "not_ready",
                "service": API_SERVICE_NAME,
                "checks": checks,
            },
        )


@app.route("/api/scholar/search", methods=["GET"])
def api_scholar_search():
    try:
        data = run_scholar_search(
            query=(request.args.get("query") or "").strip(),
            top_k=request.args.get("top_k", default=100, type=int),
            source_list=_parse_source_list(request.args.get("source_list")),
            search_mode=request.args.get("mode") or "smart",
        )
        return _api_success(data)
    except ValueError as exc:
        return _api_error(str(exc), status_code=400, code="INVALID_REQUEST")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="SEARCH_FAILED")


@app.route("/api/span-matcher", methods=["GET"])
def api_span_matcher():
    try:
        data = run_span_matcher_test(
            query=(request.args.get("query") or "").strip(),
        )
        return _api_success(data)
    except ValueError as exc:
        return _api_error(str(exc), status_code=400, code="INVALID_REQUEST")
    except SpanMatcherError as exc:
        return _api_error(str(exc), status_code=502, code="SPAN_MATCHER_FAILED")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="SPAN_MATCHER_FAILED")


@app.route("/api/study/search", methods=["GET"])
def api_study_search():
    try:
        participant_id = _normalize_participant_id(request.args.get("participant_id"))
        if not participant_id:
            return _api_error(
                "participant_id 不能为空",
                status_code=400,
                code="INVALID_REQUEST",
            )

        study_session_id, _ = _get_or_create_study_session_id(
            request.args.get("study_session_id")
            or request.cookies.get(STUDY_SESSION_COOKIE)
        )
        data = run_scholar_search(
            query=(request.args.get("query") or "").strip(),
            top_k=request.args.get("top_k", default=100, type=int),
            source_list=_parse_source_list(request.args.get("source_list")),
            search_mode=request.args.get("mode") or "smart",
        )
        search_event_id, query_index = insert_user_study_search_event(
            study_session_id=study_session_id,
            participant_id=participant_id,
            search_data=data,
        )
        result_snapshot_count = insert_user_study_search_results(
            study_session_id=study_session_id,
            participant_id=participant_id,
            query_index=query_index,
            search_event_id=search_event_id,
            results=data.get("results") or [],
        )
        data["study"] = {
            "study_session_id": study_session_id,
            "participant_id": participant_id,
            "query_index": query_index,
            "search_event_id": search_event_id,
            "result_snapshot_count": result_snapshot_count,
        }
        response, _ = _api_success(data)
        return _attach_study_session_cookie(response, study_session_id)
    except ValueError as exc:
        return _api_error(str(exc), status_code=400, code="INVALID_REQUEST")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="STUDY_SEARCH_FAILED")


@app.route("/api/study/feedback", methods=["POST"])
def api_study_feedback():
    try:
        payload = request.get_json(silent=True) or {}
        feedback_data = _validate_feedback_payload(payload)
        event_id = insert_user_study_feedback_event(feedback_data)
        return _api_success({"event_id": event_id})
    except ValueError as exc:
        return _api_error(str(exc), status_code=400, code="INVALID_REQUEST")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="FEEDBACK_FAILED")


@app.route("/api/study/feedback-review-data", methods=["GET"])
def api_feedback_review_data():
    try:
        searches = load_feedback_review_searches()
        return _api_success(
            {
                "count": len(searches),
                "source": str(FEEDBACK_REVIEW_DATA_PATH.relative_to(ROOT)),
                "searches": searches,
            }
        )
    except FileNotFoundError as exc:
        return _api_error(str(exc), status_code=404, code="NOT_FOUND")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="FEEDBACK_REVIEW_FAILED")


@app.route("/api/recommend", methods=["GET"])
def api_recommend():
    try:
        data = build_recommendations(
            work_id=request.args.get("work_id"),
            paper_id=request.args.get("paper_id"),
            top_k=request.args.get("top_k", default=5, type=int),
            source_list=_parse_source_list(request.args.get("source_list")),
        )
        return _api_success(data)
    except ValueError as exc:
        return _api_error(str(exc), status_code=400, code="INVALID_REQUEST")
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="RECOMMEND_FAILED")


@app.route("/api/scholar/daily_new", methods=["GET"])
def api_daily_new():
    limit = request.args.get("limit", default=10, type=int)
    if limit is None or limit <= 0:
        limit = 10
    try:
        papers = _get_daily_new_papers(limit=limit)
        return _api_success({"count": len(papers), "results": papers})
    except Exception as exc:
        return _api_error(str(exc), status_code=500, code="DAILY_NEW_FAILED")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5173"))
    print("=" * 60)
    print("Scholar Search Web 启动")
    print("=" * 60)
    print(f"配置文件: {CONFIG_PATH}")
    print(f"访问地址: http://localhost:{port}")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=True)
