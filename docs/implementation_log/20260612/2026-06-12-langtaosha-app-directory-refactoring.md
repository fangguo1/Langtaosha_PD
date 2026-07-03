# Langtaosha App Directory Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the current flat `app/` package into `app/routes/`, `app/pages/`, and `app/dev/` while preserving the existing Flask behavior and preparing a later move of business logic into `src/docset_hub/services`.

**Architecture:** This is a structural refactor first, not a framework migration. The design document names FastAPI, but the current application is Flask, so this plan keeps Flask and focuses on single-direction dependencies: `app` registers HTTP/page/dev entry points, while retrieval and persistence logic remains in `src/docset_hub`. The later service-layer extraction is intentionally split into a follow-up plan because `app/main.py` currently mixes route handlers, search orchestration, study logging, and page/static routes.

**Tech Stack:** Python 3.11, Flask, pytest, existing `src/docset_hub` indexing/storage/logging modules.

---

## Analysis

The design document at `docs/implementation_log/20260612/Langtaosha App Directory Refactoring Design_0612.md` defines a three-layer architecture:

```text
Application Layer -> Domain Layer -> Infrastructure Layer
```

Current state mostly matches the desired package roots, but not the desired app boundaries:

```text
app/
├── main.py                    # production Flask app, route handlers, search orchestration, study logging
├── main_develop.py            # dev-only Flask app composition
├── paper_indexer_api.py       # dev search API
├── span_matcher_page.py       # debug page + API helper
├── expanded_compare_page.py   # debug page + API helper
├── feedback_review_page.py    # review page + API helper
├── feedback_review_app.py     # standalone feedback review app
├── retrieval_compare_page.py  # debug page
├── develop_api_proxy.py       # dev proxy
├── run_feedback_review.py     # CLI wrapper
└── legacy/
```

Important mismatch: the design says `app/main.py` creates FastAPI, but the codebase uses Flask. Do not migrate Flask to FastAPI in this refactor. Treat framework migration as a separate product and compatibility project.

Sensitive import paths:

```text
tests/app/test_span_matcher_page.py      imports app.span_matcher_page and monkeypatches app.span_matcher_page.*
tests/app/test_expanded_compare_page.py  imports app.expanded_compare_page and monkeypatches app.expanded_compare_page.*
tests/app/test_feedback_review_page.py   imports app.feedback_review_page
tests/app/test_feedback_review_app.py    imports app.feedback_review_app
tests/app/test_search_api_contract.py    imports app.main
app/main_develop.py                      imports all current app modules
app/expanded_compare_page.py             imports app.span_matcher_page._serialize_semantic_plan
```

Recommended compatibility strategy:

```text
1. Move modules into their target directories.
2. Leave thin compatibility modules at the old import paths for one branch.
3. Update application code and tests to use the new paths.
4. Keep monkeypatch targets working through the transition where practical.
5. Remove compatibility modules in a later cleanup after external references are updated.
```

Target app layout for this plan:

```text
app/
├── __init__.py
├── main.py
├── routes/
│   ├── __init__.py
│   ├── paper.py
│   ├── scholar.py
│   └── study.py
├── pages/
│   ├── __init__.py
│   ├── expanded_compare_page.py
│   ├── feedback_review_page.py
│   ├── retrieval_compare_page.py
│   └── span_matcher_page.py
├── dev/
│   ├── __init__.py
│   ├── develop_api_proxy.py
│   ├── feedback_review_app.py
│   ├── main_develop.py
│   └── run_feedback_review.py
└── legacy/
```

Out of scope for this plan:

```text
FastAPI migration
New deployments/span_matcher service
New deployments/ontology_linker service
New deployments/llm_gateway service
Full SearchService extraction from app/main.py
Replacing every direct requests usage in src/docset_hub
```

Those are valid future steps, but mixing them into this directory refactor would create too much behavioral risk.

## File Structure

Move:

```text
app/span_matcher_page.py -> app/pages/span_matcher_page.py
app/expanded_compare_page.py -> app/pages/expanded_compare_page.py
app/feedback_review_page.py -> app/pages/feedback_review_page.py
app/retrieval_compare_page.py -> app/pages/retrieval_compare_page.py
app/paper_indexer_api.py -> app/routes/paper.py
app/develop_api_proxy.py -> app/dev/develop_api_proxy.py
app/main_develop.py -> app/dev/main_develop.py
app/feedback_review_app.py -> app/dev/feedback_review_app.py
app/run_feedback_review.py -> app/dev/run_feedback_review.py
```

Create:

```text
app/routes/__init__.py
app/pages/__init__.py
app/dev/__init__.py
app/span_matcher_page.py
app/expanded_compare_page.py
app/feedback_review_page.py
app/retrieval_compare_page.py
app/paper_indexer_api.py
app/develop_api_proxy.py
app/main_develop.py
app/feedback_review_app.py
app/run_feedback_review.py
tests/app/test_app_directory_imports.py
```

Modify:

```text
app/main.py
tests/app/test_span_matcher_page.py
tests/app/test_expanded_compare_page.py
tests/app/test_feedback_review_page.py
tests/app/test_feedback_review_app.py
README.md
frontend/README.md
docs/api/frontend_api_0602.md
docs/api/frontend_api_0602_xiongye.md
docs/core/shared/docset_hub/indexing/SPAN_MATCHER_README.md
```

## Task 1: Add Target Packages And Import Contract Tests

**Files:**
- Create: `app/routes/__init__.py`
- Create: `app/pages/__init__.py`
- Create: `app/dev/__init__.py`
- Create: `tests/app/test_app_directory_imports.py`

- [ ] **Step 1: Create package directories**

Run:

```bash
mkdir -p app/routes app/pages app/dev
touch app/routes/__init__.py app/pages/__init__.py app/dev/__init__.py
```

- [ ] **Step 2: Write failing import contract tests**

Create `tests/app/test_app_directory_imports.py`:

```python
from __future__ import annotations

import importlib


def test_target_app_packages_are_importable():
    for module_name in (
        "app.routes",
        "app.pages",
        "app.dev",
    ):
        assert importlib.import_module(module_name).__name__ == module_name


def test_target_page_modules_are_importable_after_refactor():
    for module_name in (
        "app.pages.span_matcher_page",
        "app.pages.expanded_compare_page",
        "app.pages.feedback_review_page",
        "app.pages.retrieval_compare_page",
    ):
        assert importlib.import_module(module_name).__name__ == module_name


def test_target_route_modules_are_importable_after_refactor():
    assert importlib.import_module("app.routes.paper").__name__ == "app.routes.paper"


def test_target_dev_modules_are_importable_after_refactor():
    for module_name in (
        "app.dev.develop_api_proxy",
        "app.dev.feedback_review_app",
        "app.dev.main_develop",
    ):
        assert importlib.import_module(module_name).__name__ == module_name
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_app_directory_imports.py -q
```

Expected: FAIL because the moved modules do not exist yet.

- [ ] **Step 4: Commit package scaffolding and failing contract test**

Run:

```bash
git add app/routes/__init__.py app/pages/__init__.py app/dev/__init__.py tests/app/test_app_directory_imports.py
git commit -m "test: document app directory import targets"
```

## Task 2: Move Debug And Review Pages Into app/pages

**Files:**
- Move: `app/span_matcher_page.py -> app/pages/span_matcher_page.py`
- Move: `app/expanded_compare_page.py -> app/pages/expanded_compare_page.py`
- Move: `app/feedback_review_page.py -> app/pages/feedback_review_page.py`
- Move: `app/retrieval_compare_page.py -> app/pages/retrieval_compare_page.py`
- Create: `app/span_matcher_page.py`
- Create: `app/expanded_compare_page.py`
- Create: `app/feedback_review_page.py`
- Create: `app/retrieval_compare_page.py`
- Modify: `app/pages/expanded_compare_page.py`
- Modify: page tests under `tests/app/`

- [ ] **Step 1: Move page modules**

Run:

```bash
git mv app/span_matcher_page.py app/pages/span_matcher_page.py
git mv app/expanded_compare_page.py app/pages/expanded_compare_page.py
git mv app/feedback_review_page.py app/pages/feedback_review_page.py
git mv app/retrieval_compare_page.py app/pages/retrieval_compare_page.py
```

- [ ] **Step 2: Update intra-page import**

In `app/pages/expanded_compare_page.py`, replace:

```python
from app.span_matcher_page import _serialize_semantic_plan
```

with:

```python
from app.pages.span_matcher_page import _serialize_semantic_plan
```

- [ ] **Step 3: Add compatibility modules for old import paths**

Create `app/span_matcher_page.py`:

```python
from app.pages.span_matcher_page import *  # noqa: F401,F403
```

Create `app/expanded_compare_page.py`:

```python
from app.pages.expanded_compare_page import *  # noqa: F401,F403
```

Create `app/feedback_review_page.py`:

```python
from app.pages.feedback_review_page import *  # noqa: F401,F403
```

Create `app/retrieval_compare_page.py`:

```python
from app.pages.retrieval_compare_page import *  # noqa: F401,F403
```

- [ ] **Step 4: Update test imports and monkeypatch targets**

In `tests/app/test_span_matcher_page.py`, replace imports and monkeypatch strings:

```python
from app.pages.span_matcher_page import (
    register_span_matcher_api_routes,
    register_span_matcher_page_routes,
    run_span_matcher_test,
)
```

```python
monkeypatch.setattr(
    "app.pages.span_matcher_page.run_span_matcher_test",
    ...
)
```

```python
monkeypatch.setattr(
    "app.pages.span_matcher_page.SpanMatcherPipeline.from_profile",
    fake_from_profile,
)
```

In `tests/app/test_expanded_compare_page.py`, replace imports and monkeypatch strings:

```python
from app.pages.expanded_compare_page import (
    register_expanded_compare_api_routes,
    register_expanded_compare_page_routes,
)
```

```python
monkeypatch.setattr(
    "app.pages.expanded_compare_page.build_expanded_sparse_query_rows",
    ...
)
```

```python
monkeypatch.setattr(
    "app.pages.expanded_compare_page.match_papers_by_expanded_sparse_plan",
    ...
)
```

In `tests/app/test_feedback_review_page.py`, replace:

```python
from app.feedback_review_page import (
```

with:

```python
from app.pages.feedback_review_page import (
```

- [ ] **Step 5: Run page tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/app/test_span_matcher_page.py \
  tests/app/test_expanded_compare_page.py \
  tests/app/test_feedback_review_page.py \
  tests/app/test_app_directory_imports.py \
  -q
```

Expected: PASS.

- [ ] **Step 6: Commit page move**

Run:

```bash
git add app tests/app/test_span_matcher_page.py tests/app/test_expanded_compare_page.py tests/app/test_feedback_review_page.py
git commit -m "refactor: move app pages into pages package"
```

## Task 3: Move Dev Entrypoints Into app/dev

**Files:**
- Move: `app/develop_api_proxy.py -> app/dev/develop_api_proxy.py`
- Move: `app/main_develop.py -> app/dev/main_develop.py`
- Move: `app/feedback_review_app.py -> app/dev/feedback_review_app.py`
- Move: `app/run_feedback_review.py -> app/dev/run_feedback_review.py`
- Create: `app/develop_api_proxy.py`
- Create: `app/main_develop.py`
- Create: `app/feedback_review_app.py`
- Create: `app/run_feedback_review.py`
- Modify: `app/dev/main_develop.py`
- Modify: `app/dev/feedback_review_app.py`
- Modify: `app/dev/run_feedback_review.py`
- Modify: `tests/app/test_feedback_review_app.py`

- [ ] **Step 1: Move dev modules**

Run:

```bash
git mv app/develop_api_proxy.py app/dev/develop_api_proxy.py
git mv app/main_develop.py app/dev/main_develop.py
git mv app/feedback_review_app.py app/dev/feedback_review_app.py
git mv app/run_feedback_review.py app/dev/run_feedback_review.py
```

- [ ] **Step 2: Update dev imports**

In `app/dev/main_develop.py`, replace current `app.*` imports with:

```python
from app.dev.develop_api_proxy import register_develop_api_cors, register_develop_api_proxy
from app.pages.expanded_compare_page import (
    register_expanded_compare_api_routes,
    register_expanded_compare_page_routes,
)
from app.pages.feedback_review_page import (
    register_feedback_review_api_routes,
    register_feedback_review_page_routes,
)
from app.routes.paper import register_paper_indexer_api_routes
from app.pages.retrieval_compare_page import register_retrieval_compare_page_routes
from app.pages.span_matcher_page import (
    register_span_matcher_api_routes,
    register_span_matcher_page_routes,
)
```

In `app/dev/feedback_review_app.py`, replace:

```python
from app.feedback_review_page import register_feedback_review_routes
```

with:

```python
from app.pages.feedback_review_page import register_feedback_review_routes
```

In `app/dev/run_feedback_review.py`, replace:

```python
from app.feedback_review_app import main
```

with:

```python
from app.dev.feedback_review_app import main
```

- [ ] **Step 3: Add compatibility modules for old dev import paths**

Create `app/develop_api_proxy.py`:

```python
from app.dev.develop_api_proxy import *  # noqa: F401,F403
```

Create `app/main_develop.py`:

```python
from app.dev.main_develop import *  # noqa: F401,F403
```

Create `app/feedback_review_app.py`:

```python
from app.dev.feedback_review_app import *  # noqa: F401,F403
```

Create `app/run_feedback_review.py`:

```python
from app.dev.run_feedback_review import *  # noqa: F401,F403
```

- [ ] **Step 4: Update feedback review app tests**

In `tests/app/test_feedback_review_app.py`, replace:

```python
from app import feedback_review_app as feedback_app_module
```

with:

```python
from app.dev import feedback_review_app as feedback_app_module
```

- [ ] **Step 5: Run dev and review tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/app/test_feedback_review_app.py \
  tests/app/test_app_directory_imports.py \
  -q
```

Expected: PASS.

- [ ] **Step 6: Verify dev app imports**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile \
  app/dev/main_develop.py \
  app/dev/develop_api_proxy.py \
  app/dev/feedback_review_app.py \
  app/dev/run_feedback_review.py
```

Expected: exit code 0.

- [ ] **Step 7: Commit dev move**

Run:

```bash
git add app tests/app/test_feedback_review_app.py
git commit -m "refactor: move development entrypoints into dev package"
```

## Task 4: Move Paper Indexer API Into app/routes

**Files:**
- Move: `app/paper_indexer_api.py -> app/routes/paper.py`
- Create: `app/paper_indexer_api.py`
- Modify: `app/dev/main_develop.py`

- [ ] **Step 1: Move paper API route module**

Run:

```bash
git mv app/paper_indexer_api.py app/routes/paper.py
```

- [ ] **Step 2: Add compatibility module**

Create `app/paper_indexer_api.py`:

```python
from app.routes.paper import *  # noqa: F401,F403
```

- [ ] **Step 3: Confirm dev app imports new route path**

In `app/dev/main_develop.py`, confirm this import exists:

```python
from app.routes.paper import register_paper_indexer_api_routes
```

- [ ] **Step 4: Run API route tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/app/test_app_directory_imports.py \
  tests/app/test_expanded_compare_page.py \
  tests/app/test_span_matcher_page.py \
  -q
```

Expected: PASS.

- [ ] **Step 5: Commit route move**

Run:

```bash
git add app tests/app/test_app_directory_imports.py
git commit -m "refactor: move paper indexer API into routes package"
```

## Task 5: Register Extracted Page Modules In Production app/main.py

**Files:**
- Modify: `app/main.py`
- Test: `tests/app/test_search_api_contract.py`
- Test: `tests/app/test_span_matcher_page.py`

- [ ] **Step 1: Update production imports**

In `app/main.py`, replace:

```python
from app.feedback_review_page import register_feedback_review_routes
```

with:

```python
from app.pages.feedback_review_page import register_feedback_review_routes
from app.pages.span_matcher_page import (
    register_span_matcher_api_routes,
    register_span_matcher_page_routes,
    run_span_matcher_test,
)
```

- [ ] **Step 2: Remove duplicated production span matcher route functions**

Delete the production-local implementations that duplicate `app.pages.span_matcher_page` behavior:

```text
_parse_csv_items
_env_int
_env_float
_load_span_scispacy_pipeline
_get_span_matcher_context
_serialize_span_aliases
_filter_span_results_for_display
_serialize_selected_candidate
run_span_matcher_test
span_matcher_page
api_span_matcher
```

Then, after the Flask `app` object and `_api_success` / `_api_error` are defined, register the extracted routes:

```python
register_span_matcher_page_routes(app)
register_span_matcher_api_routes(
    app,
    _api_success,
    _api_error,
    paper_indexer=indexer,
)
```

- [ ] **Step 3: Keep feedback review registration on the new import path**

Confirm the existing feedback review registration still calls:

```python
register_feedback_review_routes(
    app,
    _api_success,
    _api_error,
)
```

- [ ] **Step 4: Run production API contract tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/app/test_search_api_contract.py \
  tests/app/test_span_matcher_page.py \
  -q
```

Expected: PASS.

- [ ] **Step 5: Compile production app**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile app/main.py
```

Expected: exit code 0.

- [ ] **Step 6: Commit production registration cleanup**

Run:

```bash
git add app/main.py
git commit -m "refactor: register span matcher routes from pages package"
```

## Task 6: Add Route Package Placeholders For Future Scholar And Study Split

**Files:**
- Create: `app/routes/scholar.py`
- Create: `app/routes/study.py`
- Modify: `tests/app/test_app_directory_imports.py`

- [ ] **Step 1: Create explicit future route modules**

Create `app/routes/scholar.py`:

```python
"""Scholar search route package placeholder.

The production handlers still live in app.main while SearchService extraction is planned.
"""
```

Create `app/routes/study.py`:

```python
"""Study route package placeholder.

The production handlers still live in app.main while study service extraction is planned.
"""
```

- [ ] **Step 2: Extend import contract tests**

Add to `test_target_route_modules_are_importable_after_refactor()`:

```python
for module_name in (
    "app.routes.paper",
    "app.routes.scholar",
    "app.routes.study",
):
    assert importlib.import_module(module_name).__name__ == module_name
```

- [ ] **Step 3: Run import contract tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_app_directory_imports.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit route placeholders**

Run:

```bash
git add app/routes/scholar.py app/routes/study.py tests/app/test_app_directory_imports.py
git commit -m "chore: reserve scholar and study route modules"
```

## Task 7: Update Documentation And Entrypoint Commands

**Files:**
- Modify: `README.md`
- Modify: `frontend/README.md`
- Modify: `docs/api/frontend_api_0602.md`
- Modify: `docs/api/frontend_api_0602_xiongye.md`
- Modify: `docs/core/shared/docset_hub/indexing/SPAN_MATCHER_README.md`
- Modify: `docs/implementation_log/20260612/Langtaosha App Directory Refactoring Design_0612.md`

- [ ] **Step 1: Update production command references**

Keep production command references as:

```bash
python app/main.py
```

Keep gunicorn references as:

```bash
gunicorn -w 2 -b 0.0.0.0:5173 app.main:app
```

- [ ] **Step 2: Update development command references**

Replace old development command references:

```bash
python app/main_develop.py
python app/run_feedback_review.py
```

with:

```bash
python -m app.dev.main_develop
python -m app.dev.run_feedback_review
```

- [ ] **Step 3: Update span matcher docs**

Replace references:

```text
app/span_matcher_page.py
app/expanded_compare_page.py
```

with:

```text
app/pages/span_matcher_page.py
app/pages/expanded_compare_page.py
```

- [ ] **Step 4: Add implementation note to the design document**

Append this section to `docs/implementation_log/20260612/Langtaosha App Directory Refactoring Design_0612.md`:

````markdown
## 2026-06-12 Implementation Note

The first implementation phase keeps the current Flask runtime. The design's FastAPI wording describes the desired responsibility of `app/main.py`, not an immediate framework migration.

Initial scope:

```text
app/pages/   debug and review pages
app/routes/  formal API route modules where already extracted
app/dev/     development-only entrypoints and proxy helpers
```

Deferred scope:

```text
SearchService extraction
StudyService extraction
remote service clients
FastAPI migration
```
````

- [ ] **Step 5: Run documentation grep checks**

Run:

```bash
grep -RIn "python app/main_develop.py\\|python app/run_feedback_review.py\\|app/span_matcher_page.py\\|app/expanded_compare_page.py" \
  README.md frontend/README.md docs/api docs/core docs/implementation_log/20260612
```

Expected: no stale references except in historical implementation logs where preserving history is intentional.

- [ ] **Step 6: Commit docs**

Run:

```bash
git add README.md frontend/README.md docs/api/frontend_api_0602.md docs/api/frontend_api_0602_xiongye.md docs/core/shared/docset_hub/indexing/SPAN_MATCHER_README.md docs/implementation_log/20260612/Langtaosha\ App\ Directory\ Refactoring\ Design_0612.md
git commit -m "docs: document app directory refactor paths"
```

## Task 8: Final Verification

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run focused app tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/app/test_app_directory_imports.py \
  tests/app/test_search_api_contract.py \
  tests/app/test_span_matcher_page.py \
  tests/app/test_expanded_compare_page.py \
  tests/app/test_feedback_review_page.py \
  tests/app/test_feedback_review_app.py \
  -q
```

Expected: PASS.

- [ ] **Step 2: Compile moved app modules**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile \
  app/main.py \
  app/routes/paper.py \
  app/routes/scholar.py \
  app/routes/study.py \
  app/pages/span_matcher_page.py \
  app/pages/expanded_compare_page.py \
  app/pages/feedback_review_page.py \
  app/pages/retrieval_compare_page.py \
  app/dev/main_develop.py \
  app/dev/develop_api_proxy.py \
  app/dev/feedback_review_app.py \
  app/dev/run_feedback_review.py
```

Expected: exit code 0.

- [ ] **Step 3: Check git status**

Run:

```bash
git status --short --branch
```

Expected: clean except intentional uncommitted work if the executor is preparing a final commit.

- [ ] **Step 4: Commit final verification marker if needed**

If Task 8 changed no files, do not create an empty commit. If verification led to small fixes, commit them:

```bash
git add app tests docs README.md frontend/README.md
git commit -m "chore: finish app directory refactor verification"
```

## Follow-Up Plan: Service Layer Extraction

After this directory refactor lands, write a separate plan for extracting production business logic out of `app/main.py`:

```text
src/docset_hub/services/search_service.py
src/docset_hub/services/study_service.py
src/docset_hub/services/recommendation_service.py
```

The first candidate is `SearchService`, because `tests/app/test_search_api_contract.py` already exercises `run_scholar_search()` as a pure-ish function. A good follow-up target is:

```text
app/routes/scholar.py
    -> SearchService.search(...)
    -> PaperIndexer / MetadataDB / record_frontend_search_request
```

Do not start that extraction until the import-only refactor above is green and committed.

## Self-Review

Spec coverage:

```text
Application layer structure: covered by Tasks 1-7.
app/main.py as startup/registration layer: partially covered by Task 5; full cleanup deferred to service-layer follow-up.
app/routes: covered for already extracted paper API and placeholders for scholar/study.
app/pages: covered by Task 2.
app/dev: covered by Task 3.
Domain layer responsibility: preserved; no new business logic is added to app.
clients/deployments: explicitly deferred because they are infrastructure/service extraction work, not directory refactor work.
```

Known risk:

```text
Compatibility shims keep old imports working, but monkeypatches against old modules may not affect the moved implementation if tests still patch old paths. Update tests to patch app.pages.* directly.
app/main.py is large and imports many helpers; Task 5 should stay narrow and avoid rewriting search/study/recommendation flows.
Historical docs may intentionally mention old paths; do not rewrite historical implementation logs unless they are active docs.
```

## 2026-06-12 Inline Execution Result

Executed inline in `feature/span-matcher-expanded-sparse-retrieval`.

Commits:

```text
669ee48 refactor: organize app pages routes and dev modules
c62246c refactor: delegate span matcher routes from main app
```

Implemented:

```text
app/pages/
app/routes/
app/dev/
app/main_prev.py
```

`app/main.py` now delegates `/span-matcher` and `/api/span-matcher` registration to `app.pages.span_matcher_page`, while retaining a compatibility wrapper for `run_span_matcher_test(query)`.
