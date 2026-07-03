# Author Name Suggestion Retrieval Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce noisy SQL recall in `suggest_author_names()` for multi-token author queries by requiring at least one exact token match in the normalized author name.

**Architecture:** Keep the existing Python reranking pipeline unchanged. Only tighten the SQL candidate recall stage for multi-token queries by switching from substring matching to normalized token-boundary matching, while preserving the current OR-based recall logic.

**Tech Stack:** Python, SQLAlchemy `text()`, PostgreSQL regex matching, pytest

---

## File Structure

### Modified files

- `src/docset_hub/storage/metadata_db.py`
  Update author-name normalization SQL and multi-token recall conditions inside `suggest_author_names()`.
- `tests/storage/test_metadata_db_author_search.py`
  Add a focused regression test for noisy multi-token author suggestions.

---

### Task 1: Add a Failing Regression Test for Multi-Token Noisy Recall

**Files:**
- Modify: `tests/storage/test_metadata_db_author_search.py`
- Reference: `src/docset_hub/storage/metadata_db.py:2592-2680`

- [ ] **Step 1: Add a test that proves substring-only surname noise is filtered for multi-token queries**

```python
def test_suggest_author_names_multi_token_query_filters_substring_only_noise(
    metadata_db,
    transformer,
    test_papers,
):
    paper_ids = []
    try:
        cases = [
            ("Niang Yan", "niang-yan-a", "langtaosha"),
            ("Niang Yan", "niang-yan-b", "biorxiv_daily"),
            ("Aghayan S", "aghayan-a", "langtaosha"),
            ("Aghayan S", "aghayan-b", "biorxiv_daily"),
        ]

        for author_name, suffix, source_name in cases:
            if source_name == "langtaosha":
                payload = _payload_with_author(test_papers["langtaosha"][0], source_name, author_name)
                payload = _unique_langtaosha_payload(payload, suffix)
            else:
                payload = _payload_with_author(test_papers["biorxiv_daily"][0], source_name, author_name)
                payload = _unique_biorxiv_payload(payload, suffix)
            paper_ids.append(_insert_real_payload(metadata_db, transformer, payload, source_name))

        suggestions = metadata_db.suggest_author_names("niang yan", limit=10)
        names = [item["name"] for item in suggestions]

        assert "Niang Yan" in names
        assert "Aghayan S" not in names
    finally:
        for paper_id in paper_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)
```

- [ ] **Step 2: Run the focused test to verify it fails before implementation**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/storage/test_metadata_db_author_search.py -v
```

Expected:

```text
FAIL because the current SQL substring recall still returns substring-only noise such as Aghayan
```

- [ ] **Step 3: Commit the red-state test**

```bash
git add tests/storage/test_metadata_db_author_search.py
git commit -m "test: cover multi-token author suggestion noise"
```

---

### Task 2: Tighten Multi-Token SQL Recall in `suggest_author_names()`

**Files:**
- Modify: `src/docset_hub/storage/metadata_db.py`
- Test: `tests/storage/test_metadata_db_author_search.py`

- [ ] **Step 1: Extend normalized author SQL so hyphens become token separators**

```python
normalized_author_sql = (
    "btrim(regexp_replace("
    "lower(replace(replace(replace(author_name, ',', ' '), '.', ' '), '-', ' ')), "
    "'\\s+', ' ', 'g'"
    "))"
)
```

- [ ] **Step 2: For multi-token recall, replace substring matching with token-boundary matching**

```python
conditions = []
token_match_parts = []

for idx, token in enumerate(recall_tokens):
    param_name = f"token_{idx}"

    conditions.append(f"{normalized_author_sql} ~ :{param_name}")
    params[param_name] = rf"(^| ){re.escape(token)}( |$)"

    token_match_parts.append(
        f"""
        CASE
            WHEN {normalized_author_sql} ~ :{param_name}
            THEN 1
            ELSE 0
        END
        """
    )
```

- [ ] **Step 3: Keep the existing OR logic so at least one exact token match is enough**

```python
where_clause = " OR ".join(conditions)
```

- [ ] **Step 4: Keep the rest of the ranking pipeline unchanged**

```python
ORDER BY exact_priority ASC, token_match_count DESC
LIMIT :candidate_limit
```

- [ ] **Step 5: Run the focused test suite**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/storage/test_metadata_db_author_search.py -v
```

Expected:

```text
PASS with multi-token author noise filtered while the existing suggestion ranking still works
```

- [ ] **Step 6: Commit the implementation**

```bash
git add src/docset_hub/storage/metadata_db.py tests/storage/test_metadata_db_author_search.py
git commit -m "feat: narrow multi-token author suggestion recall"
```

---

## Self-Review

- Scope check: This plan only covers the requested SQL recall optimization in `suggest_author_names()`.
- Behavior check: Python reranking stays unchanged.
- Recall rule: Multi-token queries require at least one exact normalized token match; OR logic stays in place.
