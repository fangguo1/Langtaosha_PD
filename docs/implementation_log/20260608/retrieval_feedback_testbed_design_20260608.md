# Langtaosha Topic Retrieval Feedback Testbed Design

**Date:** 2026-06-08

## 1. Goal

Build a long-lived retrieval regression testbed from labels collected during normal Langtaosha usage.

The first version includes only `topic/semantic` queries. Author queries, author suggestions, and other non-topic routes are excluded because they require different relevance definitions and retrieval methods.

The testbed answers:

> Can a candidate retrieval method find and rank the documents that users previously labeled relevant better than the current or historical method?

It does not claim to measure complete, unbiased corpus recall. Labels only exist for documents exposed by historical Langtaosha retrieval flows, so the benchmark is explicitly a **known-judgment historical feedback regression testbed**.

## 2. Confirmed Decisions

- Collect labels through the existing Study Mode feedback flow.
- Do not construct a multi-system candidate pool in the first version.
- Run every evaluated method against the complete configured corpus.
- Compare returned `work_id` values with known labeled documents.
- Use `work_id` as the testbed document identity because it is the stable cross-storage identifier.
- Preserve `user_study_events` and `user_study_search_results` as immutable raw evidence.
- Export selected feedback as a frozen JSON testbed artifact.
- Evaluate only topic/semantic queries in version one.
- Treat unjudged returned documents as unknown, not negative.
- Import historical pilot labels from `config_tecent_backend_server_mimic.yaml`.
- Store future Study Mode labels and run production-facing evaluations with `config_tecent_backend_server_use.yaml`.
- Keep version-one import and evaluation inside one environment at a time: `mimic -> mimic` or `use -> use`.

## 3. Scope

### Included

- Import topic/semantic queries and result feedback from Study Mode.
- Resolve repeated or changed feedback into one current judgment per query and document.
- Create immutable testbed dataset versions.
- Evaluate multiple `PaperIndexer` retrieval methods through a common strategy adapter.
- Persist experiment configuration, ranked results, per-query metrics, and aggregate metrics.
- Report known-positive recall, known-negative exposure, MRR, and judgment coverage.

### Excluded

- Author retrieval evaluation.
- Multi-model pooling and active-learning judgment selection.
- Training data generation.
- Online A/B testing.
- Treating unjudged documents as irrelevant.
- Claiming complete gold recall over the corpus.
- Building a web dashboard in the first version.

## 4. Configuration and Environment Compatibility

Historical and future labels may come from different configured environments:

| Role | Configuration | Metadata database | Vector database | Collection prefix |
| --- | --- | --- | --- | --- |
| Historical feedback source | `config_tecent_backend_server_mimic.yaml` | `langtaosha_mimic` | `langtaosha_mimic` | `lt_mimic_` / `lt_mimic_bm25_` |
| Future feedback and evaluation target | `config_tecent_backend_server_use.yaml` | `langtaosha_use` | `langtaosha_use` | `lt_` / `lt_bm25_` |

Version one still records which environment produced a testbed artifact:

```text
source_environment
evaluation_target_config
```

The historical importer reads labels from one metadata database and writes a frozen JSON artifact for that same environment. The same is true for future use-environment feedback. Cross-environment identity reconciliation is deferred out of version one.

Before future labels are collected from use:

1. Apply the existing Study Mode migrations and the testbed migration to `langtaosha_use`.
2. Start the backend with `PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_use.yaml`.
3. Verify a Study Mode search and feedback event are written to `langtaosha_use`, not `langtaosha_mimic`.

The current backend default still points to mimic when `PD_BACKEND_CONFIG` is absent. Production deployment must therefore set the environment variable explicitly; the testbed must not infer that an unlabeled/default backend is using use.

### 4.1 Environment Boundary

`work_id` is the runtime evaluation key, but only within one environment.

Version one does not transfer labels across independently populated metadata databases. The supported operating modes are:

1. `mimic` feedback -> `mimic` JSON testbed -> `mimic` retrieval evaluation
2. `use` feedback -> `use` JSON testbed -> `use` retrieval evaluation

If cross-environment replay is needed later, it should be added as a separate reconciliation workflow rather than kept in the default import path.

### 4.2 Config Fingerprints

Testbed JSON artifacts and evaluation outputs store a non-secret config fingerprint containing:

```text
config_role
metadata_db.name
vector_db.database
vector_db.collection_prefix
vector_db.sparse_collection_prefix
vector_db.embedding_model
default_sources
```

Passwords, API keys, hosts, and usernames are not persisted in testbed metadata or reports.

The fingerprint makes corpus/index differences visible. Metrics from runs with different fingerprints may be compared, but reports must state that both retrieval method and corpus/index state may contribute to changes.

### 4.3 CLI Configuration Contract

Import and evaluation commands use explicit configuration arguments:

```text
--config-path
--evaluation-config-path
```

Defaults:

```text
historical mimic import:
  config-path = mimic.yaml

future use import:
  config-path = use.yaml

evaluation:
  evaluation-config = use.yaml
```

The commands must print database names and config roles before writing. They must reject accidental production writes when the target role was not explicitly confirmed.

Canonical version-one commands:

```bash
python scripts/import_retrieval_feedback_testbed.py \
  --config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-database langtaosha_mimic \
  --origin-environment mimic \
  --freeze-version-name topic-v1 \
  --output-report local_data/retrieval_testbed/import_topic_v1_mimic.json

python scripts/run_retrieval_testbed.py \
  --evaluation-config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-evaluation-database langtaosha_mimic \
  --testbed-json local_data/retrieval_testbed/import_topic_v1_mimic.json \
  --strategies dense sparse hybrid hybrid_retrieval \
  --top-k 10 \
  --ks 5 10 \
  --output-dir local_data/retrieval_testbed/runs/topic_v1_mimic
```

## 5. Data Model

Raw Study Mode tables remain unchanged:

```text
user_study_events
user_study_search_results
```

The testbed adds two primary artifact layers in version one:

1. raw Study Mode tables in PostgreSQL
2. frozen JSON testbed artifacts on disk

Existing testbed/evaluation tables may still be kept for optional archival support, but the JSON artifact is the primary evaluation input.

### 5.1 `retrieval_testbed_queries`

Stores the logical query identity and its evaluation eligibility.

```text
query_id
query_text
normalized_query
query_type = topic
status = active | excluded
exclusion_reason
metadata
created_at
updated_at
```

`query_text` is not the primary key. Two identical strings may later need separate intent definitions. The importer may initially deduplicate by normalized query text while retaining source search-event IDs in metadata.

### 5.2 `retrieval_testbed_judgments`

Stores the current materialized judgment for one query-document pair.

```text
query_id
work_id
relevance = 0 | 1
judgment_source = user_feedback
source_event_id
source_search_event_id
annotator_id
origin_rank
origin_search_mode
origin_search_query
origin_environment
origin_work_id
identity_match_type
identity_match_evidence
created_at
updated_at
```

The unique key is `(query_id, work_id)`.

Initial mapping:

```text
relevant     -> 1
not_relevant -> 0
```

When multiple feedback events exist for the same participant, query, and document, the latest event wins. When different participants disagree, version one uses majority vote; ties exclude the pair from the materialized dataset and record the conflict in import metadata.

### 5.3 `retrieval_testbed_versions`

Freezes a reproducible set of queries and judgments.

```text
testbed_version_id
name
status = draft | frozen
selection_policy
query_count
judgment_count
positive_count
negative_count
created_at
frozen_at
metadata
testbed_config_fingerprint
```

### 5.4 `retrieval_testbed_version_items`

Copies the selected query-document judgments into a frozen version.

```text
testbed_version_id
query_id
work_id
relevance
```

Version-one experiments primarily read the frozen JSON artifact, never mutable current judgments.

### 5.5 Experiment Tables

`retrieval_evaluation_runs` stores:

```text
run_id
testbed_version_id
strategy_name
strategy_config
config_path
evaluation_config_fingerprint
corpus_snapshot
index_version
requested_top_k
status
aggregate_metrics
started_at
completed_at
error_summary
```

`retrieval_evaluation_results` stores:

```text
run_id
query_id
work_id
rank
score
is_judged
relevance
retrieval_debug
```

`retrieval_evaluation_query_metrics` stores per-query metrics and counts.

## 6. Import Policy

The importer reads `user_study_events` and selects:

- `event_type = 'result_feedback'`;
- non-empty `query` and `work_id`;
- feedback in `relevant` or `not_relevant`;
- topic/semantic searches only.

Topic eligibility uses the originating search event:

```text
query_understanding_route NOT IN ('metadata_author', 'author_suggestion', 'none')
```

Missing routes are accepted only when the query is explicitly imported with `--include-unknown-route`; the default is exclusion.

The importer is idempotent at the artifact level: rerunning with the same configuration and feedback state rewrites the same JSON shape deterministically.

Version-one import flow:

```text
Study Mode events in one environment
  -> topic feedback selection
  -> majority-vote resolved judgments
  -> query -> labeled work_ids JSON
  -> frozen testbed artifact
```

## 7. Search Strategy Interface

Evaluation code must not contain method-specific parsing. Each retrieval method is wrapped by a strategy adapter with one contract:

```python
@dataclass(frozen=True)
class RankedDocument:
    work_id: str
    rank: int
    score: float | None
    retrieval_debug: dict


class SearchStrategy(Protocol):
    name: str
```

Every strategy implements `search(query: str, top_k: int) -> list[RankedDocument]`.

Version-one strategies:

| Strategy | Underlying call |
| --- | --- |
| `dense` | `PaperIndexer.search(search_type="dense", hydrate=False)` |
| `sparse` | `PaperIndexer.search(search_type="sparse", hydrate=False)` |
| `hybrid` | `PaperIndexer.search(search_type="hybrid", hydrate=False)` |
| `hybrid_retrieval` | `PaperIndexer.hybrid_retrieval_search(hydrate=False)` |

`smart_search` and the formal API-equivalent production flow are deferred because they include query routing and currently do not share the same result contract. They can be added later as separate adapters without changing the evaluator.

Every strategy searches the complete configured corpus. Judged documents are never passed in as the candidate set.

## 8. Metrics

For one query, let:

- `P` be all known positive `work_id` values in the frozen testbed version;
- `N` be all known negative `work_id` values;
- `R@K` be the strategy's top-K returned `work_id` values.

Version-one metrics:

```text
known_positive_recall@K = |R@K intersect P| / |P|
known_negative_count@K = |R@K intersect N|
known_negative_rate@K = |R@K intersect N| / K
known_positive_mrr = reciprocal rank of the first work_id in P
judged_count@K = |R@K intersect (P union N)|
judged_rate@K = judged_count@K / K
unjudged_count@K = K - judged_count@K
```

Aggregate metrics are macro-averaged across eligible queries. Queries without known positives are excluded from known-positive recall and MRR aggregates, but remain visible in diagnostic counts.

The default report uses `K = 5, 10`.

## 9. Components

```text
Study Mode tables
  -> import_retrieval_feedback_testbed.py
  -> frozen testbed JSON
  -> SearchStrategy adapters
  -> RetrievalEvaluator
  -> comparison JSON + Markdown analysis
```

Suggested ownership:

```text
src/docset_hub/evaluation/contracts.py
src/docset_hub/evaluation/json_testbed.py
src/docset_hub/evaluation/feedback_importer.py
src/docset_hub/evaluation/search_strategies.py
src/docset_hub/evaluation/metrics.py
src/docset_hub/evaluation/runner.py
scripts/import_retrieval_feedback_testbed.py
scripts/run_retrieval_testbed.py
```

Evaluation logic stays outside `PaperIndexer`; `PaperIndexer` remains a retrieval implementation dependency.

## 10. Failure Handling

- Invalid feedback rows are skipped and counted by reason.
- Feedback without `work_id` is excluded because stable evaluation matching is impossible.
- A strategy failure for one query is recorded without aborting other queries.
- A run is `completed` only when every selected query has either results or a recorded query-level failure.
- Frozen JSON testbed artifacts are immutable by convention.
- Missing returned `work_id` values are treated as malformed strategy output and recorded.
- Duplicate returned `work_id` values retain their first rank.
- A run records its evaluation config fingerprint and refuses to silently reuse a strategy initialized from another config.

## 11. Verification

Unit tests use mocks and synthetic judgments for:

- topic-route filtering;
- latest-feedback and majority-vote resolution;
- frozen JSON artifact shape and round-trip loading;
- adapter normalization across `PaperIndexer` methods;
- metric calculations;
- duplicate results, unknown results, no-positive queries, and strategy failures.
- config fingerprint redaction and mismatch detection.

Script tests verify CLI argument handling and orchestration with mocked repositories and strategies.

One optional integration test may run against the test configuration to verify that a frozen testbed version can invoke real `PaperIndexer` retrieval. It must not require production configuration or destructive cleanup.

## 12. Success Criteria

Version one is complete when:

1. Existing Study Mode topic feedback can be imported idempotently into a frozen JSON artifact.
2. Dense, sparse, hybrid, and three-way hybrid retrieval can run through the same evaluator.
3. Results are matched by `work_id` against frozen judgments loaded from JSON.
4. Per-query and aggregate `@5` and `@10` metrics are exported.
5. Reports clearly label metrics as known-judgment historical-feedback regression metrics.
6. Every testbed JSON artifact and evaluation run records a non-secret config fingerprint.
