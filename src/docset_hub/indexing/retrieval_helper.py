"""Post-recall helpers for PaperIndexer retrieval pipelines."""

from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from .coverage_engine import (
    analyze_document_coverage,
    analyze_document_coverage_loose,
    summarize_expanded_sparse_matches,
)
from .dense_result_filter import DENSE_DEFAULT_MIN_SIMILARITY, filter_dense_results_by_hard_rules
from .paper_keyword_lookup import PaperKeywordLookupResult

if TYPE_CHECKING:
    from ..storage.metadata_db import MetadataDB
    from ..storage.vector_db import SearchResult


DEFAULT_HYBRID_RETRIEVAL_WEIGHTS = {
    "dense": 0.4,
    "sparse": 0.4,
    "keyword_lookup": 0.2,
}


@dataclass(frozen=True)
class RankedResult:
    work_id: str
    paper_id: Optional[int]
    source_name: str
    score: float
    text_type: str
    retriever: str
    rank: int
    retrieval_debug: Dict[str, Any] = field(default_factory=dict)
    matched_spans: Optional[List[Dict[str, Any]]] = None
    total_span_count: Optional[int] = None
    matched_span_count: Optional[int] = None


class RetrievalTimings:
    """Optional millisecond timing sink for L2 retrieval pipelines."""

    def __init__(self, sink: Optional[Dict[str, float]] = None) -> None:
        self._sink = sink

    def record(self, name: str, started: float) -> None:
        if self._sink is None:
            return
        self._sink[name] = round((time.perf_counter() - started) * 1000.0, 3)


@contextmanager
def timed_section(timings: Optional[RetrievalTimings], name: str) -> Iterator[None]:
    if timings is None:
        yield
        return
    started = time.perf_counter()
    try:
        yield
    finally:
        timings.record(name, started)


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def from_search_result(
    result: SearchResult,
    *,
    retriever: str,
    rank: int,
) -> RankedResult:
    return RankedResult(
        work_id=str(result.work_id or ""),
        paper_id=result.paper_id,
        source_name=str(result.source_name or ""),
        score=float(result.score or 0.0),
        text_type=str(result.text_type or ""),
        retriever=retriever,
        rank=rank,
        retrieval_debug=dict(result.retrieval_debug or {}),
    )


def from_expanded_sparse_candidate(
    candidate: Any,
    *,
    rank: int,
) -> RankedResult:
    matched_span_count = int(getattr(candidate, "matched_span_count", 0) or 0)
    total_span_count = int(getattr(candidate, "total_span_count", 0) or 0)
    coverage_ratio = float(getattr(candidate, "coverage_ratio", 0.0) or 0.0)
    matched_spans = list(getattr(candidate, "matched_spans", []) or [])
    retrieval_debug = dict(getattr(candidate, "retrieval_debug", {}) or {})
    retrieval_debug.setdefault("retriever", "expanded_sparse")
    retrieval_debug.setdefault("matched_span_count", matched_span_count)
    retrieval_debug.setdefault("total_span_count", total_span_count)
    retrieval_debug.setdefault("coverage_ratio", coverage_ratio)
    retrieval_debug.setdefault("matched_spans", matched_spans)
    return RankedResult(
        work_id=str(getattr(candidate, "work_id", "") or ""),
        paper_id=getattr(candidate, "paper_id", None),
        source_name="",
        score=coverage_ratio,
        text_type="",
        retriever="expanded_sparse",
        rank=rank,
        retrieval_debug=retrieval_debug,
        matched_spans=matched_spans,
        total_span_count=total_span_count,
        matched_span_count=matched_span_count,
    )


def from_keyword_lookup_result(
    result: PaperKeywordLookupResult,
    *,
    rank: int,
) -> RankedResult:
    retrieval_debug = dict(result.retrieval_debug or {})
    retrieval_debug.setdefault("matched_concepts", result.matched_concepts)
    retrieval_debug.setdefault("matched_concept_count", result.matched_concept_count)
    retrieval_debug.setdefault("total_concept_count", result.total_concept_count)
    return RankedResult(
        work_id=str(result.work_id or ""),
        paper_id=result.paper_id,
        source_name="",
        score=float(result.keyword_lookup_score or 0.0),
        text_type="",
        retriever="keyword_lookup",
        rank=rank,
        retrieval_debug=retrieval_debug,
    )


def ranked_result_to_dense_filter_payload(result: RankedResult) -> Dict[str, Any]:
    return {
        "work_id": result.work_id,
        "paper_id": result.paper_id,
        "source_name": result.source_name,
        "similarity": result.score,
        "similarity_score": result.score,
        "text_type": result.text_type,
        "retrieval_debug": dict(result.retrieval_debug or {}),
    }


def filter_dense_results(
    hits: Sequence[RankedResult],
    *,
    query: str,
    metadata_db: MetadataDB,
    min_similarity: float = DENSE_DEFAULT_MIN_SIMILARITY,
    keyword_sources: Optional[Sequence[str]] = None,
) -> Tuple[List[RankedResult], Dict[str, Any]]:
    """Apply dense hard-rule filter and re-rank survivors."""
    payloads = [ranked_result_to_dense_filter_payload(hit) for hit in hits]
    filtered_payloads, report = filter_dense_results_by_hard_rules(
        metadata_db=metadata_db,
        query=query,
        results=payloads,
        min_similarity=min_similarity,
        keyword_sources=keyword_sources,
    )
    payload_by_work_id = {str(item.get("work_id") or ""): item for item in filtered_payloads}
    filtered: List[RankedResult] = []
    rank = 0
    for hit in hits:
        payload = payload_by_work_id.get(hit.work_id)
        if payload is None:
            continue
        rank += 1
        retrieval_debug = dict(hit.retrieval_debug or {})
        retrieval_debug.setdefault("dense_hard_filter_report", report.to_dict())
        filtered.append(
            RankedResult(
                work_id=hit.work_id,
                paper_id=hit.paper_id,
                source_name=hit.source_name,
                score=safe_float(payload.get("similarity_score", payload.get("similarity"))),
                text_type=hit.text_type,
                retriever=hit.retriever,
                rank=rank,
                retrieval_debug=retrieval_debug,
            )
        )
    return filtered, report.to_dict()


def filter_positive_score_results(
    hits: Sequence[RankedResult],
    *,
    drop_non_positive: bool = True,
) -> List[RankedResult]:
    filtered: List[RankedResult] = []
    rank = 0
    for hit in hits:
        score = safe_float(hit.score)
        if not math.isfinite(score):
            continue
        if drop_non_positive and score <= 0:
            continue
        rank += 1
        filtered.append(
            RankedResult(
                work_id=hit.work_id,
                paper_id=hit.paper_id,
                source_name=hit.source_name,
                score=score,
                text_type=hit.text_type,
                retriever=hit.retriever,
                rank=rank,
                retrieval_debug=dict(hit.retrieval_debug or {}),
                matched_spans=list(hit.matched_spans) if hit.matched_spans is not None else None,
                total_span_count=hit.total_span_count,
                matched_span_count=hit.matched_span_count,
            )
        )
    return filtered


def filter_keyword_lookup_results(hits: Sequence[RankedResult]) -> List[RankedResult]:
    return filter_positive_score_results(hits, drop_non_positive=True)


def extract_keyword_texts(metadata: Mapping[str, Any]) -> List[str]:
    raw_keywords = metadata.get("paper_keywords") or metadata.get("keywords") or []
    if isinstance(raw_keywords, str):
        raw_keywords = [raw_keywords]
    texts: List[str] = []
    for entry in raw_keywords:
        if isinstance(entry, Mapping):
            value = entry.get("keyword") or entry.get("text") or entry.get("name")
        else:
            value = entry
        text = " ".join(str(value or "").strip().split())
        if text and text not in texts:
            texts.append(text)
    return texts


def build_coverage_document_fields(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "title": metadata.get("canonical_title") or metadata.get("title") or "",
        "abstract": metadata.get("canonical_abstract") or metadata.get("abstract") or "",
        "paper_keywords": extract_keyword_texts(metadata),
    }


def expanded_sparse_fields_from_ranked_result(result: RankedResult) -> Dict[str, Any]:
    """Build API coverage fields from expanded_sparse RankedResult recall evidence."""
    coverage_ratio = float(result.score or 0.0)
    matched_span_count = int(result.matched_span_count or 0)
    total_span_count = int(result.total_span_count or 0)
    matched_spans = list(result.matched_spans or [])
    return {
        "similarity": coverage_ratio,
        "coverage_ratio": coverage_ratio,
        "coverage": {
            "coverage_ratio": coverage_ratio,
            "matched_span_count": matched_span_count,
            "total_span_count": total_span_count,
            "matched_spans": matched_spans,
        },
        "matched_span_count": matched_span_count,
        "total_span_count": total_span_count,
        "matched_spans": matched_spans,
    }


def build_expanded_sparse_present_fields(
    result: RankedResult,
    *,
    plan: Any,
) -> Dict[str, Any]:
    coverage = summarize_expanded_sparse_matches(
        plan=plan,
        matched_spans=list(result.matched_spans or []),
    )
    return {
        "similarity": float(coverage.coverage_ratio or 0.0),
        "coverage_ratio": float(coverage.coverage_ratio or 0.0),
        "coverage": coverage.to_dict(),
        "matched_span_count": int(coverage.matched_span_count or 0),
        "total_span_count": int(coverage.total_span_count or 0),
        "matched_spans": list(coverage.matched_spans or []),
    }


def annotate_strict_coverage(
    results: List[Dict[str, Any]],
    *,
    plan: Any,
) -> None:
    for item in results:
        metadata = dict(item.get("metadata") or {})
        coverage = analyze_document_coverage(
            plan=plan,
            document_fields=build_coverage_document_fields(metadata),
        )
        item["coverage_ratio"] = float(coverage.coverage_ratio or 0.0)
        item["coverage"] = coverage.to_dict()
        item["matched_span_count"] = int(coverage.matched_span_count or 0)
        item["total_span_count"] = int(coverage.total_span_count or 0)
        item["matched_spans"] = list(coverage.matched_spans or [])


def annotate_loose_coverage(
    results: List[Dict[str, Any]],
    *,
    plan: Any,
) -> None:
    for item in results:
        metadata = dict(item.get("metadata") or {})
        loose = analyze_document_coverage_loose(
            plan=plan,
            document_fields=build_coverage_document_fields(metadata),
        )
        item["loose_coverage_ratio"] = float(loose.coverage_ratio or 0.0)
        item["loose_coverage"] = loose.to_dict()
        item["loose_matched_span_count"] = int(loose.matched_span_count or 0)
        item["loose_total_span_count"] = int(loose.total_span_count or 0)
        item["loose_matched_spans"] = list(loose.matched_spans or [])


def ranked_result_to_lightweight_dict(result: RankedResult) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "work_id": result.work_id,
        "paper_id": result.paper_id,
        "source_name": result.source_name,
        "similarity": result.score,
        "text_type": result.text_type,
        "retrieval_debug": dict(result.retrieval_debug or {}),
    }
    if result.matched_spans is not None:
        item["matched_spans"] = list(result.matched_spans)
    if result.matched_span_count is not None:
        item["matched_span_count"] = result.matched_span_count
    if result.total_span_count is not None:
        item["total_span_count"] = result.total_span_count
    return item


def to_lightweight_dicts(hits: Sequence[RankedResult]) -> List[Dict[str, Any]]:
    return [ranked_result_to_lightweight_dict(hit) for hit in hits]


def hydrate_results(
    hits: Sequence[RankedResult],
    *,
    metadata_db: MetadataDB,
) -> List[Dict[str, Any]]:
    hydrated: List[Dict[str, Any]] = []
    for hit in hits:
        try:
            paper_info = metadata_db.read_paper_by_work_id(hit.work_id)
            if not paper_info:
                logging.warning("搜索结果的 metadata 不存在: work_id=%s", hit.work_id)
                continue
            hydrated.append(
                {
                    **ranked_result_to_lightweight_dict(hit),
                    "paper_id": paper_info.get("paper_id", hit.paper_id),
                    "source_name": hit.source_name or paper_info.get("source_name"),
                    "metadata": dict(paper_info),
                }
            )
        except Exception as exc:  # noqa: BLE001
            logging.error(
                "补全 metadata 失败: work_id=%s, error=%s",
                hit.work_id,
                exc,
                exc_info=True,
            )
    return hydrated


def present_search_results(
    hits: Sequence[RankedResult],
    *,
    metadata_db: MetadataDB,
    hydrate: bool = True,
) -> List[Dict[str, Any]]:
    if hydrate:
        rows = hydrate_results(hits, metadata_db=metadata_db)
    else:
        rows = to_lightweight_dicts(hits)
    for row, hit in zip(rows, hits):
        if hit.retriever == "expanded_sparse":
            row.update(expanded_sparse_fields_from_ranked_result(hit))
    return rows


def hits_to_branch_results(hits: Sequence[RankedResult]) -> List[Dict[str, Any]]:
    branch_results: List[Dict[str, Any]] = []
    for hit in hits:
        branch_results.append(
            {
                "work_id": hit.work_id,
                "paper_id": hit.paper_id,
                "source_name": hit.source_name,
                "text_type": hit.text_type,
                "raw_score": hit.score,
                "retriever": hit.retriever,
                "rank": hit.rank,
                "payload": ranked_result_to_lightweight_dict(hit),
                "retrieval_debug": dict(hit.retrieval_debug or {}),
            }
        )
    return branch_results


def retrieval_dedupe_key(
    work_id: str,
    paper_id: Any,
    retriever: str,
    rank: int,
) -> str:
    if work_id:
        return f"work:{work_id}"
    if paper_id not in (None, ""):
        return f"paper:{paper_id}"
    return f"{retriever}:{rank}"


def resolve_hybrid_retrieval_weights(
    retrieval_weights: Optional[Mapping[str, float]] = None,
    *,
    default_weights: Optional[Mapping[str, float]] = None,
) -> Dict[str, float]:
    weights = dict(default_weights or DEFAULT_HYBRID_RETRIEVAL_WEIGHTS)
    for key, value in (retrieval_weights or {}).items():
        if key not in weights:
            continue
        try:
            weights[key] = max(0.0, float(value))
        except (TypeError, ValueError):
            continue
    if not any(value > 0 for value in weights.values()):
        return dict(default_weights or DEFAULT_HYBRID_RETRIEVAL_WEIGHTS)
    return weights


def weighted_rrf_merge(
    branch_results: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    top_k: int,
    weights: Mapping[str, float],
    rrf_k: float,
    branch_failures: Optional[Mapping[str, str]] = None,
) -> List[RankedResult]:
    merged: Dict[str, Dict[str, Any]] = {}
    failures = dict(branch_failures or {})
    effective_rrf_k = float(rrf_k)

    for retriever, results in branch_results.items():
        branch_weight = max(0.0, safe_float(weights.get(retriever, 0.0)))
        if branch_weight <= 0:
            continue
        for fallback_rank, branch_result in enumerate(results, start=1):
            rank = int(branch_result.get("rank") or fallback_rank)
            if rank <= 0:
                continue
            work_id = str(branch_result.get("work_id") or "")
            paper_id = branch_result.get("paper_id")
            key = retrieval_dedupe_key(
                work_id=work_id,
                paper_id=paper_id,
                retriever=retriever,
                rank=rank,
            )
            entry = merged.setdefault(
                key,
                {
                    "work_id": work_id,
                    "paper_id": paper_id,
                    "source_name": str(branch_result.get("source_name") or ""),
                    "text_type": str(branch_result.get("text_type") or ""),
                    "rrf_score": 0.0,
                    "retrieval_debug": {
                        "matched_retrievers": [],
                        "rrf_k": effective_rrf_k,
                        "retrieval_weights": dict(weights),
                    },
                },
            )
            entry["rrf_score"] += branch_weight / (effective_rrf_k + rank)

            if not entry.get("work_id") and work_id:
                entry["work_id"] = work_id
            if not entry.get("paper_id") and paper_id:
                entry["paper_id"] = paper_id
            if not entry.get("source_name") and branch_result.get("source_name"):
                entry["source_name"] = str(branch_result.get("source_name") or "")
            if not entry.get("text_type") and branch_result.get("text_type"):
                entry["text_type"] = str(branch_result.get("text_type") or "")

            debug = entry["retrieval_debug"]
            matched_retrievers = debug["matched_retrievers"]
            if retriever not in matched_retrievers:
                matched_retrievers.append(retriever)
            raw_score = safe_float(branch_result.get("raw_score"))
            debug[f"{retriever}_rank"] = rank
            debug[f"{retriever}_score"] = raw_score
            branch_debug = dict(branch_result.get("retrieval_debug") or {})
            if branch_debug:
                debug[f"{retriever}_debug"] = branch_debug
            payload = branch_result.get("payload")
            if retriever == "keyword_lookup" and isinstance(payload, Mapping):
                debug["keyword_lookup_matched_concepts"] = list(payload.get("matched_concepts") or [])

    fused: List[RankedResult] = []
    for index, entry in enumerate(merged.values(), start=1):
        debug = dict(entry["retrieval_debug"])
        if failures:
            debug["branch_failures"] = failures
        fused.append(
            RankedResult(
                work_id=str(entry.get("work_id") or ""),
                paper_id=entry.get("paper_id"),
                source_name=str(entry.get("source_name") or ""),
                score=float(entry.get("rrf_score") or 0.0),
                text_type=str(entry.get("text_type") or ""),
                retriever="hybrid",
                rank=index,
                retrieval_debug=debug,
            )
        )

    fused.sort(
        key=lambda result: (
            result.score,
            len((result.retrieval_debug or {}).get("matched_retrievers", [])),
            result.work_id,
        ),
        reverse=True,
    )
    return fused[: max(1, int(top_k))]


def run_retrievers_parallel(
    retrievers: Mapping[str, Callable[[str, Sequence[str], int], List[RankedResult]]],
    *,
    query: str,
    source_list: Sequence[str],
    top_k: int,
    max_workers: Optional[int] = None,
) -> Tuple[Dict[str, List[RankedResult]], Dict[str, str]]:
    branch_results: Dict[str, List[RankedResult]] = {}
    branch_failures: Dict[str, str] = {}
    worker_count = max_workers or min(3, max(1, len(retrievers)))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(retriever, query, list(source_list), top_k): name
            for name, retriever in retrievers.items()
        }
        for future in as_completed(futures):
            branch_name = futures[future]
            try:
                branch_results[branch_name] = future.result()
            except Exception as exc:
                branch_failures[branch_name] = str(exc)
                logging.warning(
                    "retrieval branch 失败: branch=%s, error=%s",
                    branch_name,
                    exc,
                    exc_info=True,
                )
    return branch_results, branch_failures
