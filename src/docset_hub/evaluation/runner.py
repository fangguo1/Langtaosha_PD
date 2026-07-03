from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .contracts import RankedDocument, TestbedQuery
from .metrics import aggregate_query_metrics, evaluate_query


class RetrievalEvaluationRunner:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def run_queries(
        self,
        *,
        strategy: Any,
        queries: Sequence[TestbedQuery],
        top_k: int,
        ks: Sequence[int] = (5, 10),
        run_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = dict(run_metadata or {})
        run_payload = {
            "strategy_name": getattr(strategy, "name", strategy.__class__.__name__),
            "requested_top_k": top_k,
            "metadata": metadata,
        }
        run_payload.update(metadata)
        run_id = self.repository.create_run(run_payload)
        completed_query_metrics: list[dict[str, Any]] = []
        query_failures: list[str] = []
        per_query: list[dict[str, Any]] = []

        for query in queries:
            try:
                ranked_documents = strategy.search(query.query_text, top_k)
                rows = self._serialize_ranked_documents(ranked_documents, query.judgments)
                self.repository.record_results(run_id, query.query_id, rows)
                metrics = evaluate_query([item.work_id for item in ranked_documents], query.judgments, ks=ks)
                completed_query_metrics.append(metrics)
                self.repository.record_query_metrics(run_id, query.query_id, metrics, error_summary=None)
                per_query.append(
                    {
                        "query_id": query.query_id,
                        "query_text": query.query_text,
                        "metrics": metrics,
                        "results": [self._externalize_row(row) for row in rows],
                    }
                )
            except Exception as exc:  # noqa: BLE001
                query_failures.append(f"query_id={query.query_id}: {exc}")
                self.repository.record_query_metrics(run_id, query.query_id, {}, error_summary=str(exc))
                per_query.append(
                    {
                        "query_id": query.query_id,
                        "query_text": query.query_text,
                        "metrics": {},
                        "results": [],
                        "error_summary": str(exc),
                    }
                )

        aggregate_metrics = aggregate_query_metrics(completed_query_metrics)
        status = "completed"
        error_summary = None if not query_failures else "; ".join(query_failures)
        self.repository.complete_run(
            run_id,
            aggregate_metrics=aggregate_metrics,
            status=status,
            error_summary=error_summary,
        )
        return {
            "run_id": run_id,
            "status": status,
            "aggregate_metrics": aggregate_metrics,
            "query_failures": query_failures,
            "per_query": per_query,
        }

    @staticmethod
    def _serialize_ranked_documents(
        ranked_documents: Sequence[RankedDocument],
        judgments: dict[str, int],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for document in ranked_documents:
            relevance = judgments.get(document.work_id)
            rows.append(
                {
                    "work_id": document.work_id,
                    "rank": document.rank,
                    "score": document.score,
                    "is_judged": relevance is not None,
                    "relevance": relevance,
                    "retrieval_debug": document.retrieval_debug,
                }
            )
        return rows

    @staticmethod
    def _externalize_row(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "work_id": row["work_id"],
            "rank": row["rank"],
            "score": row.get("score"),
            "label": row.get("relevance"),
            "is_judged": row["is_judged"],
            "retrieval_debug": row.get("retrieval_debug") or {},
        }
