# Search Hydration and Dense Filter Performance Plan 20260630

## Goal

Reduce hybrid search latency by:

1. Replacing search-result hydration with a batch lightweight summary path.
2. Simplifying dense hard filtering to keep only candidates whose dense similarity is at least the configured threshold.

## Current Evidence

Observed query log for `deep learning`:

- Dense recall: `118.172ms`
- Dense hard filter: `267.461ms`
- Retrieval total: `386.142ms`
- Hydration/presentation for 100 results: `512.111ms`
- Total: `904.298ms`

The dense filter currently queries `paper_keywords` and builds per-hit keyword debug payloads. Hydration currently reads full paper details per result through `read_paper_by_work_id -> get_paper_info_by_paper_id`, which loads source metadata and references that search result cards do not need.

## Scope

### In Scope

- Add a batch lightweight metadata method for search cards.
- Make `present_search_results(..., hydrate=True)` use the batch method when available.
- Keep fallback behavior for tests/mocks that only expose `read_paper_by_work_id`.
- Simplify dense filtering to similarity threshold only.
- Update focused unit tests.

### Out of Scope

- Changing full-detail paper pages.
- Removing `get_paper_info_by_paper_id`.
- Database index or migration work.
- Frontend UI changes.

## Design

### Dense Filter

`filter_dense_results_by_hard_rules(...)` will:

1. Copy candidate result mappings.
2. Keep candidates where `similarity_score` or `similarity` is at least `min_similarity`.
3. Return a report with:
   - `initial_count`
   - `kept_count`
   - `score_pruned_count`
   - `keyword_pruned_count = 0`
   - `query_terms = []`
   - `matched_paper_ids = []`

The existing keyword helper functions can remain for now if other code imports them, but they will not be called by dense filtering.

### Lightweight Search Summary

Add `MetadataDB.get_paper_summaries_by_work_ids(work_ids: List[str]) -> Dict[str, Dict[str, Any]]`.

The method will return enough data for search result cards:

- `paper_id`
- `work_id`
- `canonical_title`
- `canonical_abstract`
- `canonical_language`
- `canonical_publisher`
- `submitted_at`
- `online_at`
- `published_at`
- `canonical_source_id`
- `merge_status`
- `authors`
- `keywords`
- `sources`

It will not return:

- `raw_metadata_json`
- `normalized_json`
- `references`
- parser/schema metadata

Implementation uses batch SQL:

1. Fetch all `papers` rows by `work_id`.
2. Fetch all `paper_sources` rows by `paper_id`.
3. Fetch all `paper_author_affiliation` rows by `paper_id`.
4. Fetch all `paper_keywords` rows by `paper_id`.

### Hydration

`hydrate_results(...)` will:

1. Collect unique non-empty `work_id`s in result order.
2. If `metadata_db` exposes `get_paper_summaries_by_work_ids`, call it once.
3. If the batch method is absent, use the existing cached per-work-id fallback.
4. Preserve result ordering and existing output shape:
   - top-level lightweight retrieval fields
   - `metadata` containing the summary/full metadata dict

## Test Plan

Run from:

```bash
cd <repo-root>
```

Commands:

```bash
python3 -m py_compile \
  src/docset_hub/indexing/dense_result_filter.py \
  src/docset_hub/indexing/retrieval_helper.py \
  src/docset_hub/storage/metadata_db.py \
  tests/indexing/test_dense_result_filter.py \
  tests/indexing/test_retrieval_helper.py \
  tests/storage/test_metadata_db.py
```

```bash
source /home/wnlab/miniconda3/etc/profile.d/conda.sh && \
conda activate langtaosha-pd-test && \
python -m pytest \
  tests/indexing/test_dense_result_filter.py \
  tests/indexing/test_retrieval_helper.py
```

If storage tests require unavailable DB config/services, report that explicitly and rely on unit tests plus compilation for this pass.

## Implementation Tasks

1. Update dense filter tests to assert similarity-only filtering.
2. Simplify `filter_dense_results_by_hard_rules`.
3. Keep `filter_dense_results` preserving filtered payload debug but avoid global report duplication per hit.
4. Add retrieval helper tests for batch summary hydration and duplicate work-id fallback caching.
5. Implement batch summary hydration in `hydrate_results`.
6. Add `MetadataDB.get_paper_summaries_by_work_ids`.
7. Run focused verification and record results.
