from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


TOPIC_ROUTES = {"vector", "semantic", "hybrid_retrieval", "hybrid", "dense", "sparse"}


@dataclass(frozen=True)
class FeedbackRecord:
    query_text: str
    normalized_query: str
    route: str | None
    feedback_label: int
    annotator_id: str
    source_event_id: int
    source_search_event_id: int
    created_at: str
    origin_environment: str
    origin_work_id: str
    origin_rank: int | None
    origin_search_mode: str | None
    origin_search_query: str | None
    doi: str | None = None
    source_name: str | None = None
    source_record_id: str | None = None
    title: str | None = None


@dataclass(frozen=True)
class IdentityResolution:
    origin_work_id: str
    resolved_work_id: str | None
    match_type: str
    match_evidence: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedJudgment:
    normalized_query: str
    query_text: str
    relevance: int
    annotator_count: int
    annotator_ids: tuple[str, ...]
    origin_environment: str
    origin_work_id: str
    resolved_work_id: str
    identity_match_type: str
    identity_match_evidence: dict[str, str]
    source_event_id: int
    source_search_event_id: int
    origin_rank: int | None
    origin_search_mode: str | None
    origin_search_query: str | None


@dataclass(frozen=True)
class TargetIdentityIndex:
    existing_work_ids: set[str] = field(default_factory=set)
    doi_to_work_ids: dict[str, list[str]] = field(default_factory=dict)
    source_identity_to_work_ids: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    title_to_work_ids: dict[str, list[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class FeedbackResolutionReport:
    selected_count: int
    conflict_count: int


def select_topic_feedback(
    feedback_rows: list[FeedbackRecord],
    *,
    include_unknown_route: bool = False,
) -> list[FeedbackRecord]:
    selected: list[FeedbackRecord] = []
    for row in feedback_rows:
        if row.route in TOPIC_ROUTES:
            selected.append(row)
        elif include_unknown_route and row.route is None:
            selected.append(row)
    return selected


def _parse_created_at(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _normalize_doi(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().lower()


def resolve_document_identity(
    record: FeedbackRecord,
    index: TargetIdentityIndex,
) -> IdentityResolution:
    if record.origin_work_id in index.existing_work_ids:
        return IdentityResolution(
            origin_work_id=record.origin_work_id,
            resolved_work_id=record.origin_work_id,
            match_type="exact_work_id",
            match_evidence={"work_id": record.origin_work_id},
        )

    doi = _normalize_doi(record.doi)
    if doi:
        matches = index.doi_to_work_ids.get(doi, [])
        if len(matches) == 1:
            return IdentityResolution(
                origin_work_id=record.origin_work_id,
                resolved_work_id=matches[0],
                match_type="doi",
                match_evidence={"doi": doi},
            )
        if len(matches) > 1:
            return IdentityResolution(
                origin_work_id=record.origin_work_id,
                resolved_work_id=None,
                match_type="ambiguous",
                match_evidence={"doi": doi},
            )

    if record.source_name and record.source_record_id:
        key = (record.source_name, record.source_record_id)
        matches = index.source_identity_to_work_ids.get(key, [])
        if len(matches) == 1:
            return IdentityResolution(
                origin_work_id=record.origin_work_id,
                resolved_work_id=matches[0],
                match_type="source_identity",
                match_evidence={
                    "source_name": record.source_name,
                    "source_record_id": record.source_record_id,
                },
            )
        if len(matches) > 1:
            return IdentityResolution(
                origin_work_id=record.origin_work_id,
                resolved_work_id=None,
                match_type="ambiguous",
                match_evidence={
                    "source_name": record.source_name,
                    "source_record_id": record.source_record_id,
                },
            )

    return IdentityResolution(
        origin_work_id=record.origin_work_id,
        resolved_work_id=None,
        match_type="unresolved",
        match_evidence={},
    )


def resolve_feedback_with_report(
    feedback_rows: list[FeedbackRecord],
) -> tuple[list[ResolvedJudgment], FeedbackResolutionReport]:
    latest_by_annotator: dict[tuple[str, str, str], FeedbackRecord] = {}
    for row in feedback_rows:
        key = (row.normalized_query, row.origin_work_id, row.annotator_id)
        previous = latest_by_annotator.get(key)
        if previous is None:
            latest_by_annotator[key] = row
            continue
        if (_parse_created_at(row.created_at), row.source_event_id) > (
            _parse_created_at(previous.created_at),
            previous.source_event_id,
        ):
            latest_by_annotator[key] = row

    grouped: dict[tuple[str, str], list[FeedbackRecord]] = {}
    for row in latest_by_annotator.values():
        grouped.setdefault((row.normalized_query, row.origin_work_id), []).append(row)

    resolved: list[ResolvedJudgment] = []
    conflict_count = 0
    for rows in grouped.values():
        positive_votes = sum(1 for row in rows if row.feedback_label > 0)
        negative_votes = sum(1 for row in rows if row.feedback_label <= 0)
        if positive_votes == negative_votes:
            conflict_count += 1
            continue
        winner = 1 if positive_votes > negative_votes else 0
        sample = max(rows, key=lambda row: (_parse_created_at(row.created_at), row.source_event_id))
        annotator_ids = tuple(sorted({row.annotator_id for row in rows}))
        resolved.append(
            ResolvedJudgment(
                normalized_query=sample.normalized_query,
                query_text=sample.query_text,
                relevance=winner,
                annotator_count=len(rows),
                annotator_ids=annotator_ids,
                origin_environment=sample.origin_environment,
                origin_work_id=sample.origin_work_id,
                resolved_work_id=sample.origin_work_id,
                identity_match_type="exact_work_id",
                identity_match_evidence={"work_id": sample.origin_work_id},
                source_event_id=sample.source_event_id,
                source_search_event_id=sample.source_search_event_id,
                origin_rank=sample.origin_rank,
                origin_search_mode=sample.origin_search_mode,
                origin_search_query=sample.origin_search_query,
            )
        )

    report = FeedbackResolutionReport(
        selected_count=len(resolved),
        conflict_count=conflict_count,
    )
    return resolved, report
