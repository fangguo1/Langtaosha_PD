from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .contracts import TestbedQuery
from .feedback_importer import (
    FeedbackRecord,
    IdentityResolution,
    ResolvedJudgment,
    TargetIdentityIndex,
    resolve_document_identity,
)


class FeedbackSourceRepository:
    def __init__(self, engine: Engine, origin_environment: str) -> None:
        self.engine = engine
        self.origin_environment = origin_environment

    def load_raw_feedback(self, include_unknown_route: bool = False) -> list[FeedbackRecord]:
        del include_unknown_route  # filtering is handled in the importer layer
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        f.id AS source_event_id,
                        f.search_event_id AS source_search_event_id,
                        f.query,
                        f.search_query,
                        f.search_mode,
                        f.result_rank,
                        f.feedback,
                        f.participant_id,
                        f.work_id,
                        f.created_at,
                        s.query_understanding_route,
                        sr.payload AS result_payload
                    FROM user_study_events f
                    JOIN user_study_events s ON s.id = f.search_event_id
                    LEFT JOIN user_study_search_results sr
                      ON sr.search_event_id = f.search_event_id
                     AND sr.result_rank = f.result_rank
                    WHERE f.event_type = 'result_feedback'
                      AND f.work_id IS NOT NULL
                      AND f.query IS NOT NULL
                      AND btrim(f.query) != ''
                      AND f.feedback IN ('relevant', 'not_relevant')
                    ORDER BY f.created_at, f.id
                    """
                )
            ).mappings().all()

        records: list[FeedbackRecord] = []
        for row in rows:
            payload = row.get("result_payload")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            payload = payload or {}
            records.append(
                FeedbackRecord(
                    query_text=str(row["query"]),
                    normalized_query=" ".join(str(row["query"]).strip().lower().split()),
                    route=row.get("query_understanding_route"),
                    feedback_label=1 if row["feedback"] == "relevant" else 0,
                    annotator_id=str(row.get("participant_id") or "unknown"),
                    source_event_id=int(row["source_event_id"]),
                    source_search_event_id=int(row["source_search_event_id"]),
                    created_at=row["created_at"].isoformat(),
                    origin_environment=self.origin_environment,
                    origin_work_id=str(row["work_id"]),
                    origin_rank=row.get("result_rank"),
                    origin_search_mode=row.get("search_mode"),
                    origin_search_query=row.get("search_query"),
                    doi=_extract_result_doi(payload),
                    source_name=str(payload.get("source_name") or payload.get("source") or "") or None,
                    source_record_id=_extract_source_record_id(payload),
                    title=str(payload.get("title") or "") or None,
                )
            )
        return records


class TestbedRepository:
    def __init__(self, engine: Engine, config_fingerprint: dict[str, Any]) -> None:
        self.engine = engine
        self.config_fingerprint = config_fingerprint

    def resolve_target_identities(self, records: Sequence[FeedbackRecord]) -> list[IdentityResolution]:
        identity_index = self._build_identity_index()
        return [resolve_document_identity(record, identity_index) for record in records]

    def upsert_queries_and_judgments(self, resolved: Sequence[ResolvedJudgment]) -> dict[str, int]:
        query_ids: dict[str, int] = {}
        with self.engine.begin() as conn:
            for judgment in resolved:
                query_key = judgment.normalized_query
                if query_key not in query_ids:
                    query_id = conn.execute(
                        text(
                            """
                            INSERT INTO retrieval_testbed_queries (
                                query_text, normalized_query, query_type, status, metadata
                            )
                            VALUES (:query_text, :normalized_query, 'topic', 'active', '{}'::jsonb)
                            ON CONFLICT (normalized_query, query_type)
                            DO UPDATE SET query_text = EXCLUDED.query_text
                            RETURNING query_id
                            """
                        ),
                        {
                            "query_text": judgment.query_text,
                            "normalized_query": judgment.normalized_query,
                        },
                    ).scalar_one()
                    query_ids[query_key] = int(query_id)

                conn.execute(
                    text(
                        """
                        INSERT INTO retrieval_testbed_judgments (
                            query_id,
                            work_id,
                            relevance,
                            judgment_source,
                            source_event_id,
                            source_search_event_id,
                            annotator_id,
                            origin_rank,
                            origin_search_mode,
                            origin_search_query,
                            origin_environment,
                            origin_work_id,
                            identity_match_type,
                            identity_match_evidence
                        )
                        VALUES (
                            :query_id,
                            :work_id,
                            :relevance,
                            'user_feedback',
                            :source_event_id,
                            :source_search_event_id,
                            NULL,
                            :origin_rank,
                            :origin_search_mode,
                            :origin_search_query,
                            :origin_environment,
                            :origin_work_id,
                            :identity_match_type,
                            CAST(:identity_match_evidence AS JSONB)
                        )
                        ON CONFLICT (query_id, work_id)
                        DO UPDATE SET
                            relevance = EXCLUDED.relevance,
                            source_event_id = EXCLUDED.source_event_id,
                            source_search_event_id = EXCLUDED.source_search_event_id,
                            origin_rank = EXCLUDED.origin_rank,
                            origin_search_mode = EXCLUDED.origin_search_mode,
                            origin_search_query = EXCLUDED.origin_search_query,
                            origin_environment = EXCLUDED.origin_environment,
                            origin_work_id = EXCLUDED.origin_work_id,
                            identity_match_type = EXCLUDED.identity_match_type,
                            identity_match_evidence = EXCLUDED.identity_match_evidence,
                            updated_at = CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "query_id": query_ids[query_key],
                        "work_id": judgment.resolved_work_id,
                        "relevance": judgment.relevance,
                        "source_event_id": judgment.source_event_id,
                        "source_search_event_id": judgment.source_search_event_id,
                        "origin_rank": judgment.origin_rank,
                        "origin_search_mode": judgment.origin_search_mode,
                        "origin_search_query": judgment.origin_search_query,
                        "origin_environment": judgment.origin_environment,
                        "origin_work_id": judgment.origin_work_id,
                        "identity_match_type": judgment.identity_match_type,
                        "identity_match_evidence": json.dumps(judgment.identity_match_evidence, ensure_ascii=False),
                    },
                )

        positive_count = sum(1 for judgment in resolved if judgment.relevance > 0)
        return {
            "query_count": len(query_ids),
            "judgment_count": len(resolved),
            "positive_count": positive_count,
            "negative_count": len(resolved) - positive_count,
        }

    def freeze_version(self, name: str, metadata: dict[str, Any]) -> int:
        with self.engine.begin() as conn:
            version_id = conn.execute(
                text(
                    """
                    INSERT INTO retrieval_testbed_versions (
                        name,
                        status,
                        selection_policy,
                        metadata,
                        testbed_config_fingerprint,
                        frozen_at
                    )
                    VALUES (
                        :name,
                        'frozen',
                        '{}'::jsonb,
                        CAST(:metadata AS JSONB),
                        CAST(:fingerprint AS JSONB),
                        CURRENT_TIMESTAMP
                    )
                    RETURNING testbed_version_id
                    """
                ),
                {
                    "name": name,
                    "metadata": json.dumps(metadata, ensure_ascii=False),
                    "fingerprint": json.dumps(self.config_fingerprint, ensure_ascii=False),
                },
            ).scalar_one()

            rows = conn.execute(
                text(
                    """
                    SELECT q.query_id, j.work_id, j.relevance
                    FROM retrieval_testbed_judgments j
                    JOIN retrieval_testbed_queries q ON q.query_id = j.query_id
                    WHERE q.status = 'active'
                    """
                )
            ).mappings().all()
            for row in rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO retrieval_testbed_version_items (
                            testbed_version_id, query_id, work_id, relevance
                        )
                        VALUES (:version_id, :query_id, :work_id, :relevance)
                        """
                    ),
                    {
                        "version_id": int(version_id),
                        "query_id": int(row["query_id"]),
                        "work_id": row["work_id"],
                        "relevance": int(row["relevance"]),
                    },
                )

            summary = conn.execute(
                text(
                    """
                    SELECT
                        COUNT(DISTINCT query_id) AS query_count,
                        COUNT(*) AS judgment_count,
                        SUM(CASE WHEN relevance > 0 THEN 1 ELSE 0 END) AS positive_count,
                        SUM(CASE WHEN relevance <= 0 THEN 1 ELSE 0 END) AS negative_count
                    FROM retrieval_testbed_version_items
                    WHERE testbed_version_id = :version_id
                    """
                ),
                {"version_id": int(version_id)},
            ).mappings().one()

            conn.execute(
                text(
                    """
                    UPDATE retrieval_testbed_versions
                    SET
                        query_count = :query_count,
                        judgment_count = :judgment_count,
                        positive_count = :positive_count,
                        negative_count = :negative_count
                    WHERE testbed_version_id = :version_id
                    """
                ),
                {
                    "version_id": int(version_id),
                    "query_count": int(summary["query_count"] or 0),
                    "judgment_count": int(summary["judgment_count"] or 0),
                    "positive_count": int(summary["positive_count"] or 0),
                    "negative_count": int(summary["negative_count"] or 0),
                },
            )
            return int(version_id)

    def load_frozen_queries(self, testbed_version_id: int) -> list[TestbedQuery]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        q.query_id,
                        q.query_text,
                        v.work_id,
                        v.relevance
                    FROM retrieval_testbed_version_items v
                    JOIN retrieval_testbed_queries q ON q.query_id = v.query_id
                    WHERE v.testbed_version_id = :version_id
                    ORDER BY q.query_id, v.work_id
                    """
                ),
                {"version_id": testbed_version_id},
            ).mappings().all()

        grouped: dict[int, dict[str, Any]] = {}
        for row in rows:
            bucket = grouped.setdefault(
                int(row["query_id"]),
                {"query_text": row["query_text"], "judgments": {}},
            )
            bucket["judgments"][str(row["work_id"])] = int(row["relevance"])

        return [
            TestbedQuery(query_id=query_id, query_text=data["query_text"], judgments=data["judgments"])
            for query_id, data in grouped.items()
        ]

    def create_run(self, payload: dict[str, Any]) -> int:
        with self.engine.begin() as conn:
            return int(
                conn.execute(
                    text(
                        """
                        INSERT INTO retrieval_evaluation_runs (
                            testbed_version_id,
                            strategy_name,
                            strategy_config,
                            config_path,
                            evaluation_config_fingerprint,
                            corpus_snapshot,
                            index_version,
                            requested_top_k,
                            status,
                            aggregate_metrics
                        )
                        VALUES (
                            :testbed_version_id,
                            :strategy_name,
                            CAST(:strategy_config AS JSONB),
                            :config_path,
                            CAST(:evaluation_config_fingerprint AS JSONB),
                            CAST(:corpus_snapshot AS JSONB),
                            CAST(:index_version AS JSONB),
                            :requested_top_k,
                            'running',
                            '{}'::jsonb
                        )
                        RETURNING run_id
                        """
                    ),
                    {
                        "testbed_version_id": payload["testbed_version_id"],
                        "strategy_name": payload["strategy_name"],
                        "strategy_config": json.dumps(payload.get("strategy_config") or {}, ensure_ascii=False),
                        "config_path": payload.get("config_path"),
                        "evaluation_config_fingerprint": json.dumps(
                            payload.get("evaluation_config_fingerprint") or self.config_fingerprint,
                            ensure_ascii=False,
                        ),
                        "corpus_snapshot": json.dumps(payload.get("corpus_snapshot") or {}, ensure_ascii=False),
                        "index_version": json.dumps(payload.get("index_version") or {}, ensure_ascii=False),
                        "requested_top_k": payload["requested_top_k"],
                    },
                ).scalar_one()
            )

    def record_results(self, run_id: int, query_id: int, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO retrieval_evaluation_results (
                        run_id, query_id, work_id, rank, score, is_judged, relevance, retrieval_debug
                    )
                    VALUES (
                        :run_id,
                        :query_id,
                        :work_id,
                        :rank,
                        :score,
                        :is_judged,
                        :relevance,
                        CAST(:retrieval_debug AS JSONB)
                    )
                    """
                ),
                [
                    {
                        "run_id": run_id,
                        "query_id": query_id,
                        "work_id": row["work_id"],
                        "rank": row["rank"],
                        "score": row.get("score"),
                        "is_judged": row["is_judged"],
                        "relevance": row.get("relevance"),
                        "retrieval_debug": json.dumps(row.get("retrieval_debug") or {}, ensure_ascii=False),
                    }
                    for row in rows
                ],
            )

    def record_query_metrics(
        self,
        run_id: int,
        query_id: int,
        metrics: dict[str, Any],
        error_summary: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO retrieval_evaluation_query_metrics (
                        run_id, query_id, metrics, error_summary
                    )
                    VALUES (
                        :run_id,
                        :query_id,
                        CAST(:metrics AS JSONB),
                        :error_summary
                    )
                    ON CONFLICT (run_id, query_id)
                    DO UPDATE SET
                        metrics = EXCLUDED.metrics,
                        error_summary = EXCLUDED.error_summary
                    """
                ),
                {
                    "run_id": run_id,
                    "query_id": query_id,
                    "metrics": json.dumps(metrics, ensure_ascii=False),
                    "error_summary": error_summary,
                },
            )

    def complete_run(
        self,
        run_id: int,
        aggregate_metrics: dict[str, Any],
        status: str,
        error_summary: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE retrieval_evaluation_runs
                    SET
                        aggregate_metrics = CAST(:aggregate_metrics AS JSONB),
                        status = :status,
                        error_summary = :error_summary,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "aggregate_metrics": json.dumps(aggregate_metrics, ensure_ascii=False),
                    "status": status,
                    "error_summary": error_summary,
                },
            )

    def _build_identity_index(self) -> TargetIdentityIndex:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        p.work_id,
                        ps.source_name,
                        ps.source_record_id,
                        ps.doi
                    FROM papers p
                    JOIN paper_sources ps ON ps.paper_id = p.paper_id
                    WHERE p.work_id IS NOT NULL
                    """
                )
            ).mappings().all()

        existing_work_ids: set[str] = set()
        doi_to_work_ids: dict[str, list[str]] = defaultdict(list)
        source_identity_to_work_ids: dict[tuple[str, str], list[str]] = defaultdict(list)
        for row in rows:
            work_id = str(row["work_id"])
            existing_work_ids.add(work_id)
            doi = str(row["doi"]).strip().lower() if row.get("doi") else ""
            if doi:
                doi_to_work_ids[doi].append(work_id)
            if row.get("source_name") and row.get("source_record_id"):
                source_identity_to_work_ids[
                    (str(row["source_name"]), str(row["source_record_id"]))
                ].append(work_id)
        return TargetIdentityIndex(
            existing_work_ids=existing_work_ids,
            doi_to_work_ids=dict(doi_to_work_ids),
            source_identity_to_work_ids=dict(source_identity_to_work_ids),
        )


def list_missing_testbed_tables(engine: Engine) -> list[str]:
    required_tables = [
        "retrieval_testbed_queries",
        "retrieval_testbed_judgments",
        "retrieval_testbed_versions",
        "retrieval_testbed_version_items",
        "retrieval_evaluation_runs",
        "retrieval_evaluation_results",
        "retrieval_evaluation_query_metrics",
    ]
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(:table_names)
                """
            ),
            {"table_names": required_tables},
        ).scalars().all()
    existing = set(rows)
    return [table_name for table_name in required_tables if table_name not in existing]


def _extract_result_doi(payload: dict[str, Any]) -> str | None:
    doi = payload.get("doi")
    if doi:
        return str(doi)
    metadata = payload.get("metadata") or {}
    if metadata.get("sources"):
        for source in metadata["sources"]:
            if source.get("doi"):
                return str(source["doi"])
    return None


def _extract_source_record_id(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("metadata") or {}
    if metadata.get("sources"):
        for source in metadata["sources"]:
            if source.get("source_record_id"):
                return str(source["source_record_id"])
    return None
