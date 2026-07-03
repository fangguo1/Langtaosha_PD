from __future__ import annotations

from dataclasses import dataclass

from src.docset_hub.evaluation.contracts import RankedDocument, TestbedQuery as EvalQuery
from src.docset_hub.evaluation.runner import RetrievalEvaluationRunner


class FakeStrategy:
    def __init__(self, name: str, responses: dict[str, list[RankedDocument] | Exception]):
        self.name = name
        self.responses = responses
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, top_k: int) -> list[RankedDocument]:
        self.calls.append((query, top_k))
        response = self.responses[query]
        if isinstance(response, Exception):
            raise response
        return response


@dataclass
class FakeRepository:
    runs: list[dict]
    results: list[dict]
    query_metrics: list[dict]
    completed_runs: list[dict]

    def create_run(self, payload: dict) -> int:
        self.runs.append(payload)
        return len(self.runs)

    def record_results(self, run_id: int, query_id: int, rows: list[dict]) -> None:
        self.results.append({"run_id": run_id, "query_id": query_id, "rows": rows})

    def record_query_metrics(self, run_id: int, query_id: int, metrics: dict, error_summary: str | None = None) -> None:
        self.query_metrics.append(
            {
                "run_id": run_id,
                "query_id": query_id,
                "metrics": metrics,
                "error_summary": error_summary,
            }
        )

    def complete_run(self, run_id: int, aggregate_metrics: dict, status: str, error_summary: str | None = None) -> None:
        self.completed_runs.append(
            {
                "run_id": run_id,
                "aggregate_metrics": aggregate_metrics,
                "status": status,
                "error_summary": error_summary,
            }
        )


def _repo() -> FakeRepository:
    return FakeRepository(runs=[], results=[], query_metrics=[], completed_runs=[])


def test_runner_searches_complete_corpus_and_scores_frozen_judgments():
    strategy = FakeStrategy(
        "dense",
        {
            "query": [
                RankedDocument(work_id="NEW", rank=1, score=0.9),
                RankedDocument(work_id="W1", rank=2, score=0.8),
                RankedDocument(work_id="W3", rank=3, score=0.7),
            ]
        },
    )
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    query = EvalQuery(query_id=1, query_text="query", judgments={"W1": 1, "W2": 1, "W3": 0})
    outcome = runner.run_queries(strategy=strategy, queries=[query], top_k=3, ks=(1, 3))

    assert strategy.calls == [("query", 3)]
    assert outcome["aggregate_metrics"]["known_positive_recall@3"] == 0.5
    assert outcome["aggregate_metrics"]["known_negative_count@3"] == 1.0
    assert repository.results[0]["rows"][1]["work_id"] == "W1"
    assert repository.results[0]["rows"][1]["is_judged"] is True


def test_runner_records_query_failure_and_continues():
    strategy = FakeStrategy(
        "dense",
        {
            "bad": RuntimeError("search failed"),
            "good": [RankedDocument(work_id="W1", rank=1, score=1.0)],
        },
    )
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    queries = [
        EvalQuery(query_id=1, query_text="bad", judgments={"W9": 1}),
        EvalQuery(query_id=2, query_text="good", judgments={"W1": 1}),
    ]
    outcome = runner.run_queries(strategy=strategy, queries=queries, top_k=3, ks=(1, 3))

    assert outcome["status"] == "completed"
    assert len(repository.query_metrics) == 2
    assert repository.query_metrics[0]["error_summary"] == "search failed"
    assert repository.query_metrics[1]["metrics"]["known_positive_recall@1"] == 1.0


def test_runner_persists_ranked_results_and_aggregate_metrics():
    strategy = FakeStrategy(
        "hybrid_retrieval",
        {
            "q1": [RankedDocument(work_id="W1", rank=1, score=0.8, retrieval_debug={"matched_retrievers": ["dense"]})],
            "q2": [RankedDocument(work_id="W2", rank=1, score=0.7, retrieval_debug={"matched_retrievers": ["sparse"]})],
        },
    )
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    queries = [
        EvalQuery(query_id=1, query_text="q1", judgments={"W1": 1}),
        EvalQuery(query_id=2, query_text="q2", judgments={"W2": 0}),
    ]
    outcome = runner.run_queries(strategy=strategy, queries=queries, top_k=5, ks=(1, 5))

    assert len(repository.results) == 2
    assert repository.results[0]["rows"][0]["retrieval_debug"] == {"matched_retrievers": ["dense"]}
    assert repository.query_metrics[0]["metrics"]["known_positive_recall@1"] == 1.0
    assert repository.completed_runs[0]["aggregate_metrics"]["query_count"] == 2
    assert outcome["per_query"][0]["query_text"] == "q1"
    assert outcome["per_query"][0]["results"][0] == {
        "work_id": "W1",
        "rank": 1,
        "score": 0.8,
        "label": 1,
        "is_judged": True,
        "retrieval_debug": {"matched_retrievers": ["dense"]},
    }


def test_runner_returns_unjudged_results_with_null_label():
    strategy = FakeStrategy(
        "dense",
        {
            "query": [
                RankedDocument(work_id="W1", rank=1, score=0.9),
                RankedDocument(work_id="NEW", rank=2, score=0.8),
            ]
        },
    )
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    query = EvalQuery(query_id=1, query_text="query", judgments={"W1": 1})
    outcome = runner.run_queries(strategy=strategy, queries=[query], top_k=2, ks=(1, 2))

    assert outcome["per_query"][0]["results"][1]["label"] is None
    assert outcome["per_query"][0]["results"][1]["is_judged"] is False


def test_runner_merges_run_metadata_into_create_run_payload():
    strategy = FakeStrategy("dense", {"q1": [RankedDocument(work_id="W1", rank=1, score=1.0)]})
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    query = EvalQuery(query_id=1, query_text="q1", judgments={"W1": 1})
    runner.run_queries(
        strategy=strategy,
        queries=[query],
        top_k=5,
        run_metadata={"testbed_version_id": 9, "config_path": "use.yaml"},
    )

    assert repository.runs[0]["strategy_name"] == "dense"
    assert repository.runs[0]["requested_top_k"] == 5
    assert repository.runs[0]["testbed_version_id"] == 9
    assert repository.runs[0]["config_path"] == "use.yaml"
