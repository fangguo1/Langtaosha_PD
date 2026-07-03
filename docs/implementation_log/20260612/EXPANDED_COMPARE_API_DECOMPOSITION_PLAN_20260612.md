# Expanded Compare API 拆解实施计划（前后端分离改造）

> **For agentic workers:** 按 Task 顺序逐项执行，每个 Task 内严格按 Step 顺序（先写失败测试 → 实现 → 通过 → commit）。Steps 使用 checkbox（`- [ ]`）跟踪进度。

**Goal:** 删除 `/api/expanded-compare` 聚合 API，将其能力拆解为通用检索 API（`/api/search` 新增 `expanded_sparse` 检索类型与可选 coverage 注解）和 dev 内部 API（`/api/semantic-plan`），由前端页面自行组合，符合前后端分离与三层架构原则。

**Architecture:** 业务编排从 `app/pages/expanded_compare_page.py` 下沉到 Domain 层（`PaperIndexer` 公开方法），routes 层只做参数校验和透传；semantic plan 序列化收敛到 `src/docset_hub/indexing/query_semantic_plan.py`；`/api/semantic-plan` 只在 develop API app（5006 端口）注册，production `app/main.py` 不注册。

**Tech Stack:** Flask、pytest（monkeypatch + FakeIndexer 模式）、原生 JS（模板内联）。

---

## 决策记录（来自 2026-06-12 讨论）

1. **expanded sparse 是第四种检索类型**，进入 `PaperIndexer.search(search_type="expanded_sparse")`，不另开专用 API。
2. **`/api/semantic-plan` 定位为 dev 内部 API**，与 `/api/span-matcher` 同级，只在 `main_develop._register_develop_api_routes` 注册。它是 develop API app 内的一个 endpoint，**不是**独立进程服务（区别于 ontology linker 8765）。
3. **禁止 app 层调用 `indexer._build_query_semantic_plan` 私有方法**，先升格为公开方法再被 API 包装。
4. **不做 plan 缓存**。接受一次 compare 页面查询触发约 4 次 semantic plan 构建（每次含远程 ontology HTTP 调用）。后续若有性能问题再加 LRU。
5. **`develop_api_proxy` 无需改动**——它对 `/api/*` 通配转发。
6. **范围外**：根目录 `app/expanded_compare_page.py` 等 star-import shim 文件由 app 目录重构计划（`2026-06-12-langtaosha-app-directory-refactoring.md`）处理，本计划不动。

## 改造前后对比

```text
改造前（聚合 API）:
  expanded_compare.html --1次--> /api/expanded-compare
                                   └─ app/pages/expanded_compare_page.py
                                      ├─ indexer.search(dense)
                                      ├─ indexer.search(sparse)
                                      ├─ indexer._build_query_semantic_plan()   ← 私有方法越界
                                      ├─ match_papers_by_expanded_sparse_plan() ← Domain 编排写在 page
                                      └─ coverage + 序列化

改造后（前端组合）:
  expanded_compare.html --并发4次-->
      /api/search?search_type=dense&include_coverage=1      ┐
      /api/search?search_type=sparse&include_coverage=1     ├ app/routes/paper.py（通用）
      /api/search?search_type=expanded_sparse               ┘
      /api/semantic-plan                                     app/dev/semantic_plan_api.py（dev-only）
  全部编排逻辑在 src/docset_hub/indexing/paper_indexer.py（Domain）
```

## 文件结构（本计划锁定的分解）

| 文件 | 动作 | 职责 |
|------|------|------|
| `src/docset_hub/indexing/query_semantic_plan.py` | 修改 | 新增 `serialize_semantic_plan(plan)`（plan 唯一序列化出口） |
| `src/docset_hub/indexing/__init__.py` | 修改 | 导出 `serialize_semantic_plan` |
| `src/docset_hub/indexing/paper_indexer.py` | 修改 | `_build_query_semantic_plan` 升格公开；新增 `expanded_sparse_search`；`search()` 支持 `expanded_sparse` 与 `include_coverage` |
| `app/routes/paper.py` | 修改 | `SUPPORTED_SEARCH_TYPES` 增加 `expanded_sparse`；新增 `keyword_sources`、`include_coverage` 参数 |
| `app/dev/semantic_plan_api.py` | 新建 | `GET /api/semantic-plan`（dev-only） |
| `app/dev/main_develop.py` | 修改 | 注册 semantic-plan API；移除 expanded-compare API 注册 |
| `app/pages/expanded_compare_page.py` | 修改 | 只保留页面路由，删除聚合 API 与全部序列化辅助函数 |
| `app/pages/span_matcher_page.py` | 修改 | 改用 Domain 的 `serialize_semantic_plan` |
| `templates/expanded_compare.html` | 修改 | JS 改为并发调 4 个 API 并在前端组合 |
| `tests/indexing/test_query_semantic_plan.py` | 修改 | 新增 serializer 测试 |
| `tests/indexing/test_expanded_sparse_retrieval.py` | 修改 | 方法改名适配；新增 `expanded_sparse_search` / dispatch / coverage 注解测试 |
| `tests/app/test_semantic_plan_api.py` | 新建 | dev API 合同测试 |
| `tests/app/test_expanded_compare_page.py` | 修改 | 删除聚合 API 测试，仅保留页面渲染测试 |
| `tests/app/test_paper_routes.py` | 新建 | `/api/search` 新参数合同测试 |

所有命令在 worktree 根目录执行：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
```

---

## Task 1: Domain 层提供 `serialize_semantic_plan`

**Files:**
- Modify: `src/docset_hub/indexing/query_semantic_plan.py`（文件末尾追加）
- Modify: `src/docset_hub/indexing/__init__.py`
- Modify: `app/pages/span_matcher_page.py:66-112`（删除本地实现，改 import）
- Modify: `app/pages/expanded_compare_page.py:7`（改 import，line 112 改调用名）
- Test: `tests/indexing/test_query_semantic_plan.py`

- [ ] **Step 1: 写失败测试**

在 `tests/indexing/test_query_semantic_plan.py` 末尾追加（文件已有 `SimpleNamespace` 风格可循；若文件未 import SimpleNamespace 则在顶部加 `from types import SimpleNamespace`）：

```python
def test_serialize_semantic_plan_serializes_spans_terms_and_children():
    from src.docset_hub.indexing.query_semantic_plan import serialize_semantic_plan

    plan = SimpleNamespace(
        original_query="renal adhesion",
        normalized_query="renal adhesion",
        spans=[
            SimpleNamespace(
                span_id="s1",
                surface_text="renal",
                normalized_text="renal",
                start=0,
                end=5,
                canonical_text="Renal",
                own_terms=SimpleNamespace(
                    tier1=[SimpleNamespace(text="renal", match_mode="exact")],
                    tier2=[SimpleNamespace(text="kidney", match_mode="exact")],
                ),
                children=[
                    SimpleNamespace(
                        span_id="s1.1",
                        surface_text="ren",
                        normalized_text="ren",
                        start=0,
                        end=3,
                        canonical_text="Ren",
                        own_terms=SimpleNamespace(
                            tier1=[SimpleNamespace(text="ren", match_mode="prefix")],
                            tier2=[],
                        ),
                    )
                ],
            )
        ],
    )

    payload = serialize_semantic_plan(plan)

    assert payload["original_query"] == "renal adhesion"
    assert payload["spans"][0]["span_id"] == "s1"
    assert payload["spans"][0]["own_terms"]["tier1"] == [{"text": "renal", "match_mode": "exact"}]
    assert payload["spans"][0]["own_terms"]["tier2"] == [{"text": "kidney", "match_mode": "exact"}]
    assert payload["spans"][0]["children"][0]["span_id"] == "s1.1"
    assert payload["spans"][0]["children"][0]["own_terms"]["tier1"] == [
        {"text": "ren", "match_mode": "prefix"}
    ]
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py::test_serialize_semantic_plan_serializes_spans_terms_and_children -v
```

预期：FAIL（`ImportError: cannot import name 'serialize_semantic_plan'`）。

- [ ] **Step 3: 实现**

把 `app/pages/span_matcher_page.py` 66-112 行的 `_serialize_semantic_plan` 整体搬到 `src/docset_hub/indexing/query_semantic_plan.py` 末尾，改名为公开函数（实现逻辑逐字保留）：

```python
def serialize_semantic_plan(plan: Any) -> Dict[str, Any]:
    """Serialize a QuerySemanticPlan into a JSON-safe dict (API/页面共用出口)."""

    def serialize_terms(terms: Any) -> List[Dict[str, Any]]:
        return [
            {
                "text": getattr(term, "text", ""),
                "match_mode": getattr(term, "match_mode", "exact"),
            }
            for term in list(terms or [])
        ]

    def serialize_child(child: Any) -> Dict[str, Any]:
        return {
            "span_id": child.span_id,
            "surface_text": child.surface_text,
            "normalized_text": child.normalized_text,
            "start": child.start,
            "end": child.end,
            "canonical_text": child.canonical_text,
            "own_terms": {
                "tier1": serialize_terms(getattr(child.own_terms, "tier1", [])),
                "tier2": serialize_terms(getattr(child.own_terms, "tier2", [])),
            },
        }

    return {
        "original_query": plan.original_query,
        "normalized_query": plan.normalized_query,
        "spans": [
            {
                "span_id": span.span_id,
                "surface_text": span.surface_text,
                "normalized_text": span.normalized_text,
                "start": span.start,
                "end": span.end,
                "canonical_text": span.canonical_text,
                "own_terms": {
                    "tier1": serialize_terms(getattr(span.own_terms, "tier1", [])),
                    "tier2": serialize_terms(getattr(span.own_terms, "tier2", [])),
                },
                "children": [
                    serialize_child(child)
                    for child in list(getattr(span, "children", []) or [])
                ],
            }
            for span in plan.spans
        ],
    }
```

注意 `query_semantic_plan.py` 顶部 typing import 需包含 `Any, Dict, List`（缺则补）。

`src/docset_hub/indexing/__init__.py` 两处修改：

```python
from .query_semantic_plan import (
    QuerySemanticPlan,
    SemanticChildSpan,
    SemanticSpanGroup,
    SemanticTerm,
    SemanticTermBucket,
    build_query_semantic_plan,
    serialize_semantic_plan,
)
```

`__all__` 列表追加 `'serialize_semantic_plan',`。

`app/pages/span_matcher_page.py`：删除 66-112 行本地 `_serialize_semantic_plan`，顶部 import 区加：

```python
from src.docset_hub.indexing import serialize_semantic_plan
```

第 167 行调用改为 `"semantic_plan": serialize_semantic_plan(result.semantic_plan),`。

`app/pages/expanded_compare_page.py`：第 7 行 `from app.pages.span_matcher_page import _serialize_semantic_plan` 改为 `from src.docset_hub.indexing import serialize_semantic_plan`，第 112 行调用名同步改（此文件 Task 7 会大幅删减，这里只为保持测试绿色）。

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py tests/app/test_span_matcher_page.py tests/app/test_expanded_compare_page.py -v
```

预期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/indexing/query_semantic_plan.py src/docset_hub/indexing/__init__.py app/pages/span_matcher_page.py app/pages/expanded_compare_page.py tests/indexing/test_query_semantic_plan.py
git commit -m "refactor: move semantic plan serialization into domain layer"
```

---

## Task 2: `_build_query_semantic_plan` 升格为公开方法

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py:721`（方法改名）、`:748`（内部调用点）
- Modify: `app/pages/expanded_compare_page.py:75`（调用点）
- Modify: `tests/indexing/test_expanded_sparse_retrieval.py:332,355`
- Modify: `tests/app/test_expanded_compare_page.py:120,223`（FakeIndexer 方法名）

- [ ] **Step 1: 先改测试（TDD：测试先红）**

`tests/indexing/test_expanded_sparse_retrieval.py`：

- 第 332 行 `plan = indexer._build_query_semantic_plan(` → `plan = indexer.build_query_semantic_plan(`
- 第 353-357 行 monkeypatch 目标改名：

```python
    monkeypatch.setattr(
        indexer,
        "build_query_semantic_plan",
        lambda query, source_list, keyword_sources=None: plan,
    )
```

`tests/app/test_expanded_compare_page.py` 两处 FakeIndexer：`def _build_query_semantic_plan(self, **kwargs):` → `def build_query_semantic_plan(self, **kwargs):`

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py tests/app/test_expanded_compare_page.py -v
```

预期：FAIL（`AttributeError: ... has no attribute 'build_query_semantic_plan'` 等）。

- [ ] **Step 3: 实现改名**

`src/docset_hub/indexing/paper_indexer.py`：

- 第 721 行 `def _build_query_semantic_plan(` → `def build_query_semantic_plan(`，并补充 docstring：

```python
    def build_query_semantic_plan(
        self,
        query: str,
        source_list: List[str],
        keyword_sources: Optional[Sequence[str]] = None,
        profile_name: str = "ontology_plus_keyword",
    ):
        """构建查询语义计划（公开 Domain 能力，供检索分支与 dev API 使用）。

        Returns:
            QuerySemanticPlan，无可用 selected_concepts 时返回 None。
        """
```

- 第 748 行 `plan = self._build_query_semantic_plan(` → `plan = self.build_query_semantic_plan(`

`app/pages/expanded_compare_page.py` 第 75 行 `indexer._build_query_semantic_plan(` → `indexer.build_query_semantic_plan(`

确认无遗漏调用点：

```bash
grep -rn "_build_query_semantic_plan" --include="*.py" . | grep -v ".worktrees"
```

预期：无输出（worktree 内执行时直接 `grep -rn "_build_query_semantic_plan" --include="*.py" src/ app/ tests/ scripts/`，预期无输出）。

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py tests/app/test_expanded_compare_page.py tests/indexing/test_span_matcher.py -v
```

预期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/indexing/paper_indexer.py app/pages/expanded_compare_page.py tests/indexing/test_expanded_sparse_retrieval.py tests/app/test_expanded_compare_page.py
git commit -m "refactor: promote build_query_semantic_plan to public PaperIndexer API"
```

---

## Task 3: `PaperIndexer.expanded_sparse_search` + `search_type="expanded_sparse"`

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py`（`search()` 签名与 dispatch；新增公开方法；import）
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: 写失败测试**

在 `tests/indexing/test_expanded_sparse_retrieval.py` 末尾追加（沿用本文件 `PaperIndexer.__new__` + monkeypatch 惯例；`_plan()` 为文件内已有 helper）：

```python
def test_expanded_sparse_search_returns_coverage_annotated_results(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.default_sources = ["langtaosha"]

    class FakeMetadataDB:
        def read_paper_by_work_id(self, work_id):
            return {
                "paper_id": 101,
                "work_id": work_id,
                "source_name": "langtaosha",
                "canonical_title": "Kidney adhesion paper",
                "canonical_abstract": "Renal epithelial adhesion study.",
                "paper_keywords": [{"keyword": "kidney"}],
            }

    indexer.metadata_db = FakeMetadataDB()
    plan = _plan()

    monkeypatch.setattr(
        indexer,
        "build_query_semantic_plan",
        lambda query, source_list, keyword_sources=None: plan,
    )
    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.match_papers_by_expanded_sparse_plan",
        lambda metadata_db, plan, source_list, keyword_sources=None, top_k=50: [
            type(
                "Candidate",
                (),
                {
                    "paper_id": 101,
                    "work_id": "W101",
                    "matched_span_count": 1,
                    "total_span_count": 2,
                    "coverage_ratio": 0.5,
                    "matched_spans": [
                        {
                            "span_id": "s1",
                            "canonical_text": "Renal",
                            "matched_terms": ["kidney"],
                            "matched_scopes": ["parent"],
                            "own_term_matched": True,
                            "matched_child_count": 0,
                            "total_child_count": 0,
                            "span_score": 1.0,
                        }
                    ],
                    "retrieval_debug": {"retriever": "expanded_sparse"},
                },
            )()
        ],
    )

    results = indexer.expanded_sparse_search(
        query="adhesion protein in kidney",
        source_list=["langtaosha"],
        top_k=5,
        hydrate=True,
    )

    assert len(results) == 1
    assert results[0]["work_id"] == "W101"
    assert results[0]["paper_id"] == 101
    assert results[0]["similarity"] == results[0]["coverage_ratio"]
    assert results[0]["coverage"]["matched_span_count"] == 1
    assert results[0]["matched_spans"][0]["matched_terms"] == ["kidney"]
    assert results[0]["metadata"]["canonical_title"] == "Kidney adhesion paper"
    assert results[0]["source_name"] == "langtaosha"


def test_search_dispatches_expanded_sparse_without_vector_db(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.default_sources = ["langtaosha"]
    indexer.metadata_db = object()
    indexer.vector_db = None  # expanded_sparse 不依赖向量库
    captured = {}

    def fake_expanded_sparse_search(**kwargs):
        captured.update(kwargs)
        return [{"work_id": "W1"}]

    monkeypatch.setattr(indexer, "expanded_sparse_search", fake_expanded_sparse_search)

    results = indexer.search(
        query="renal adhesion",
        source_list=["langtaosha"],
        top_k=7,
        search_type="expanded_sparse",
    )

    assert results == [{"work_id": "W1"}]
    assert captured["query"] == "renal adhesion"
    assert captured["top_k"] == 7
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -v -k "expanded_sparse_search or dispatches_expanded_sparse"
```

预期：FAIL（`AttributeError: 'PaperIndexer' object has no attribute 'expanded_sparse_search'`）。

- [ ] **Step 3: 实现**

`src/docset_hub/indexing/paper_indexer.py`：

(a) 顶部 import 区确认/追加（`match_papers_by_expanded_sparse_plan` 已有；coverage 若未导入则加）：

```python
from .coverage_engine import analyze_document_coverage, summarize_expanded_sparse_matches
```

(b) `search()`（第 335 行起）签名追加两个参数（`include_coverage` 在 Task 4 用，本任务先占位）：

```python
    def search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True,
        search_type: str = "dense",
        keyword_sources: Optional[Sequence[str]] = None,
        include_coverage: bool = False,
    ) -> List[Dict[str, Any]]:
```

docstring 的 search_type 行改为：`search_type: 检索类型，支持 dense / sparse / hybrid / hybrid_retrieval / expanded_sparse`。

(c) 在 `if not self.vector_db:` 检查**之前**插入 dispatch（expanded_sparse 走 MetadataDB，不需要向量库）：

```python
        if search_type == "expanded_sparse":
            return self.expanded_sparse_search(
                query=query,
                source_list=source_list,
                top_k=top_k,
                hydrate=hydrate,
                keyword_sources=keyword_sources,
            )

        if not self.vector_db:
            raise ValueError("向量数据库未启用，无法执行搜索")
```

(d) 新增公开方法（放在 `hybrid_retrieval_search` 之后）：

```python
    def expanded_sparse_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True,
        keyword_sources: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Expanded sparse 检索：semantic plan 展开词项匹配 + span coverage 评分。

        结果按 coverage_ratio 作为 similarity 返回，形状与 search() 其他
        检索类型保持一致（metadata 嵌套，hydrate 可关）。
        """
        resolved_source_list = self._resolve_source_list(source_list)
        plan = self.build_query_semantic_plan(
            query=query,
            source_list=resolved_source_list,
            keyword_sources=keyword_sources,
        )
        if plan is None:
            return []

        candidates = match_papers_by_expanded_sparse_plan(
            metadata_db=self.metadata_db,
            plan=plan,
            source_list=resolved_source_list,
            keyword_sources=keyword_sources,
            top_k=top_k,
        )

        results: List[Dict[str, Any]] = []
        for candidate in candidates:
            coverage = summarize_expanded_sparse_matches(
                plan=plan,
                matched_spans=list(getattr(candidate, "matched_spans", []) or []),
            )
            item: Dict[str, Any] = {
                "work_id": getattr(candidate, "work_id", None),
                "paper_id": getattr(candidate, "paper_id", None),
                "similarity": float(coverage.coverage_ratio or 0.0),
                "coverage_ratio": float(coverage.coverage_ratio or 0.0),
                "coverage": coverage.to_dict(),
                "matched_span_count": int(coverage.matched_span_count or 0),
                "total_span_count": int(coverage.total_span_count or 0),
                "matched_spans": list(coverage.matched_spans or []),
                "retrieval_debug": dict(getattr(candidate, "retrieval_debug", {}) or {}),
            }
            if hydrate:
                metadata: Dict[str, Any] = {}
                work_id = item["work_id"]
                if work_id:
                    try:
                        metadata = dict(self.metadata_db.read_paper_by_work_id(work_id) or {})
                    except Exception:  # noqa: BLE001
                        metadata = {}
                item["metadata"] = metadata
                item["source_name"] = metadata.get("source_name")
            results.append(item)
        return results
```

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -v
```

预期：全部 PASS（含既有用例）。

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/indexing/paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: add expanded_sparse search type to PaperIndexer"
```

---

## Task 4: dense/sparse 结果的可选 coverage 注解（`include_coverage`）

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py`（`search()` 内 hydrate 后注解；新增私有 helper）
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: 写失败测试**

```python
def test_search_annotates_dense_results_with_coverage_when_requested(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.default_sources = ["langtaosha"]
    indexer.metadata_db = object()
    plan = _plan()

    class FakeSearchResult:
        work_id = "W11"
        paper_id = 11
        source_name = "langtaosha"
        score = 0.77
        text_type = "abstract"
        retrieval_debug = {}

    class FakeVectorDB:
        def search(self, query, source_list, top_k, search_type):
            return [FakeSearchResult()]

    indexer.vector_db = FakeVectorDB()
    monkeypatch.setattr(
        indexer,
        "_resolve_source_list",
        lambda source_list: ["langtaosha"],
    )
    monkeypatch.setattr(
        indexer,
        "_hydrate_search_results",
        lambda search_results: [
            {
                "work_id": "W11",
                "paper_id": 11,
                "source_name": "langtaosha",
                "similarity": 0.77,
                "metadata": {
                    "canonical_title": "Kidney adhesion paper",
                    "canonical_abstract": "Renal epithelial adhesion study.",
                    "paper_keywords": [{"keyword": "kidney"}],
                },
            }
        ],
    )
    monkeypatch.setattr(
        indexer,
        "build_query_semantic_plan",
        lambda query, source_list, keyword_sources=None: plan,
    )

    results = indexer.search(
        query="adhesion protein in kidney",
        search_type="dense",
        include_coverage=True,
    )

    assert "coverage_ratio" in results[0]
    assert "coverage" in results[0]
    assert "matched_spans" in results[0]
    assert results[0]["total_span_count"] == len(plan.spans)


def test_search_skips_coverage_annotation_by_default(monkeypatch):
    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.default_sources = ["langtaosha"]
    indexer.metadata_db = object()

    class FakeVectorDB:
        def search(self, query, source_list, top_k, search_type):
            return []

    indexer.vector_db = FakeVectorDB()
    monkeypatch.setattr(indexer, "_resolve_source_list", lambda source_list: ["langtaosha"])
    monkeypatch.setattr(indexer, "_hydrate_search_results", lambda search_results: [])

    called = {"plan": False}

    def fail_plan(**kwargs):
        called["plan"] = True

    monkeypatch.setattr(indexer, "build_query_semantic_plan", fail_plan)

    indexer.search(query="renal adhesion", search_type="dense")

    assert called["plan"] is False
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -v -k "coverage_when_requested or skips_coverage"
```

预期：第一条 FAIL（结果中无 `coverage_ratio`），第二条 PASS（守护用例）。

- [ ] **Step 3: 实现**

`paper_indexer.py` 中 `search()` 的 hydrate 分支改为：

```python
            # 3. 可选：补全 metadata
            if hydrate:
                hydrated = self._hydrate_search_results(search_results)
                if include_coverage and search_type in ("dense", "sparse"):
                    self._annotate_results_with_coverage(
                        results=hydrated,
                        query=query,
                        source_list=resolved_source_list,
                        keyword_sources=keyword_sources,
                    )
                return hydrated
```

新增两个私有 helper（放在 `expanded_sparse_search` 之后）：

```python
    def _annotate_results_with_coverage(
        self,
        *,
        results: List[Dict[str, Any]],
        query: str,
        source_list: List[str],
        keyword_sources: Optional[Sequence[str]] = None,
    ) -> None:
        """对 hydrate 后的检索结果就地附加 span coverage 字段（dev 对比用）。"""
        plan = self.build_query_semantic_plan(
            query=query,
            source_list=source_list,
            keyword_sources=keyword_sources,
        )
        if plan is None:
            return
        for item in results:
            metadata = dict(item.get("metadata") or {})
            coverage = analyze_document_coverage(
                plan=plan,
                document_fields={
                    "title": metadata.get("canonical_title") or metadata.get("title") or "",
                    "abstract": metadata.get("canonical_abstract") or metadata.get("abstract") or "",
                    "paper_keywords": self._extract_keyword_texts(metadata),
                },
            )
            item["coverage_ratio"] = float(coverage.coverage_ratio or 0.0)
            item["coverage"] = coverage.to_dict()
            item["matched_span_count"] = int(coverage.matched_span_count or 0)
            item["total_span_count"] = int(coverage.total_span_count or 0)
            item["matched_spans"] = list(coverage.matched_spans or [])

    @staticmethod
    def _extract_keyword_texts(metadata: Mapping[str, Any]) -> List[str]:
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
```

确认 `Mapping` 已在 paper_indexer.py 顶部 typing import 中（已有，`_resolve_hybrid_retrieval_weights` 在用）。

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -v
```

预期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/indexing/paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: support optional coverage annotation in PaperIndexer.search"
```

---

## Task 5: `/api/search` 路由扩展

**Files:**
- Modify: `app/routes/paper.py`
- Test: 新建 `tests/app/test_paper_routes.py`

- [ ] **Step 1: 写失败测试**

新建 `tests/app/test_paper_routes.py`（`_json_success` / `_json_error` 复用 `tests/app/test_expanded_compare_page.py` 的实现，逐字拷贝即可——按仓库测试规范，跨文件共享优先，但这两个 helper 已在多个 app 测试内联，保持一致）：

```python
from __future__ import annotations

import json

from flask import Flask

from app.routes.paper import register_paper_indexer_api_routes


def _json_success(app):
    def api_success(payload=None, status_code=200):
        return (
            app.response_class(
                json.dumps({"success": True, **(payload or {})}),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_success


def _json_error(app):
    def api_error(message, status_code=500, code="ERR", extra=None):
        return (
            app.response_class(
                json.dumps(
                    {"success": False, "error": message, "error_code": code, **(extra or {})}
                ),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_error


class FakeIndexer:
    def __init__(self):
        self.captured = {}

    def search(self, **kwargs):
        self.captured.update(kwargs)
        return [{"work_id": "W1", "similarity": 0.9}]


def test_api_search_accepts_expanded_sparse_type():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get("/api/search?query=renal&search_type=expanded_sparse")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["search_type"] == "expanded_sparse"
    assert indexer.captured["search_type"] == "expanded_sparse"


def test_api_search_passes_keyword_sources_and_include_coverage():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=dense"
        "&keyword_sources=paper_metadata,mesh&include_coverage=1"
    )

    assert response.status_code == 200
    assert indexer.captured["keyword_sources"] == ["paper_metadata", "mesh"]
    assert indexer.captured["include_coverage"] is True


def test_api_search_defaults_include_coverage_false():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    app.test_client().get("/api/search?query=renal")

    assert indexer.captured["include_coverage"] is False
    assert indexer.captured["keyword_sources"] is None
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/app/test_paper_routes.py -v
```

预期：FAIL（`search_type 只能是 dense, sparse, hybrid, hybrid_retrieval`；`search()` 未收到新参数）。

- [ ] **Step 3: 实现**

`app/routes/paper.py`：

```python
SUPPORTED_SEARCH_TYPES = ("dense", "sparse", "hybrid", "hybrid_retrieval", "expanded_sparse")


def _parse_bool_flag(raw_value: Optional[str]) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}
```

`api_search` 内 `indexer.search(...)` 调用改为：

```python
            results = indexer.search(
                query=query,
                source_list=_parse_source_list(request.args.get("source_list")),
                top_k=top_k,
                hydrate=_parse_hydrate(request.args.get("hydrate")),
                search_type=search_type,
                keyword_sources=_parse_source_list(request.args.get("keyword_sources")),
                include_coverage=_parse_bool_flag(request.args.get("include_coverage")),
            )
```

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/app/test_paper_routes.py -v
```

预期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add app/routes/paper.py tests/app/test_paper_routes.py
git commit -m "feat: expose expanded_sparse and coverage options on /api/search"
```

---

## Task 6: dev API `/api/semantic-plan`

**Files:**
- Create: `app/dev/semantic_plan_api.py`
- Modify: `app/dev/main_develop.py`（import + `_register_develop_api_routes` 注册）
- Test: 新建 `tests/app/test_semantic_plan_api.py`

- [ ] **Step 1: 写失败测试**

新建 `tests/app/test_semantic_plan_api.py`：

```python
from __future__ import annotations

import json
from types import SimpleNamespace

from flask import Flask

from app.dev.semantic_plan_api import register_semantic_plan_api_routes


def _json_success(app):
    def api_success(payload=None, status_code=200):
        return (
            app.response_class(
                json.dumps({"success": True, **(payload or {})}),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_success


def _json_error(app):
    def api_error(message, status_code=500, code="ERR", extra=None):
        return (
            app.response_class(
                json.dumps(
                    {"success": False, "error": message, "error_code": code, **(extra or {})}
                ),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_error


def _make_plan():
    return SimpleNamespace(
        original_query="renal adhesion",
        normalized_query="renal adhesion",
        spans=[
            SimpleNamespace(
                span_id="s1",
                surface_text="renal",
                normalized_text="renal",
                start=0,
                end=5,
                canonical_text="Renal",
                own_terms=SimpleNamespace(
                    tier1=[SimpleNamespace(text="renal", match_mode="exact")],
                    tier2=[SimpleNamespace(text="kidney", match_mode="exact")],
                ),
                children=[],
            )
        ],
    )


def test_semantic_plan_api_returns_plan_rows_and_highlight_terms(monkeypatch):
    app = Flask(__name__)
    captured = {}
    plan = _make_plan()

    class FakeIndexer:
        default_sources = ["langtaosha"]

        def build_query_semantic_plan(self, **kwargs):
            captured.update(kwargs)
            return plan

    monkeypatch.setattr(
        "app.dev.semantic_plan_api.build_expanded_sparse_query_rows",
        lambda received_plan: [
            {"term": "kidney", "match_mode": "exact"},
            {"term": "kidney", "match_mode": "exact"},
            {"term": "renal", "match_mode": "prefix"},
        ],
    )

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=renal%20adhesion")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["source_list"] == ["langtaosha"]
    assert payload["semantic_plan"]["spans"][0]["span_id"] == "s1"
    assert len(payload["expanded_query_rows"]) == 3
    assert payload["highlight_terms"] == [
        {"text": "kidney", "match_mode": "exact"},
        {"text": "renal", "match_mode": "prefix"},
    ]


def test_semantic_plan_api_rejects_empty_query():
    app = Flask(__name__)

    class FakeIndexer:
        default_sources = ["langtaosha"]

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=")

    assert response.status_code == 400


def test_semantic_plan_api_returns_empty_payload_when_plan_is_none():
    app = Flask(__name__)

    class FakeIndexer:
        default_sources = ["langtaosha"]

        def build_query_semantic_plan(self, **kwargs):
            return None

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=unknown%20term")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["semantic_plan"] is None
    assert payload["expanded_query_rows"] == []
    assert payload["highlight_terms"] == []
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/app/test_semantic_plan_api.py -v
```

预期：FAIL（`ModuleNotFoundError: No module named 'app.dev.semantic_plan_api'`）。

- [ ] **Step 3: 实现**

新建 `app/dev/semantic_plan_api.py`：

```python
"""Dev-only semantic plan inspection API.

只在 develop API app（main_develop）注册，production app/main.py 不挂载。
定位与 /api/span-matcher 一致：错误分析与人工调试用，不承诺接口稳定性。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from flask import request

from src.docset_hub.indexing import (
    build_expanded_sparse_query_rows,
    serialize_semantic_plan,
)


def _parse_csv_items(raw_value: Optional[str]) -> Optional[List[str]]:
    if raw_value is None:
        return None
    items = [item.strip() for item in raw_value.split(",") if item.strip()]
    return items or None


def _extract_highlight_terms(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, str]]:
    seen = set()
    terms: List[Dict[str, str]] = []
    for row in rows:
        text = " ".join(str(row.get("term") or "").strip().lower().split())
        match_mode = str(row.get("match_mode") or "exact")
        if not text:
            continue
        key = (text, match_mode)
        if key in seen:
            continue
        seen.add(key)
        terms.append({"text": text, "match_mode": match_mode})
    return terms


def register_semantic_plan_api_routes(
    app,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
) -> None:
    @app.route("/api/semantic-plan", methods=["GET"])
    def api_semantic_plan():
        try:
            query = (request.args.get("query") or "").strip()
            if not query:
                return api_error("query 不能为空", status_code=400, code="INVALID_REQUEST")

            source_list = _parse_csv_items(request.args.get("source_list"))
            keyword_sources = _parse_csv_items(request.args.get("keyword_sources"))
            profile_name = (request.args.get("profile") or "ontology_plus_keyword").strip()

            plan = indexer.build_query_semantic_plan(
                query=query,
                source_list=source_list or list(getattr(indexer, "default_sources", []) or []),
                keyword_sources=keyword_sources,
                profile_name=profile_name,
            )
            if plan is None:
                return api_success(
                    {
                        "query": query,
                        "semantic_plan": None,
                        "expanded_query_rows": [],
                        "highlight_terms": [],
                    }
                )

            rows = build_expanded_sparse_query_rows(plan)
            return api_success(
                {
                    "query": query,
                    "semantic_plan": serialize_semantic_plan(plan),
                    "expanded_query_rows": rows,
                    "highlight_terms": _extract_highlight_terms(rows),
                }
            )
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="SEMANTIC_PLAN_FAILED")
```

`app/dev/main_develop.py`：

import 区追加：

```python
from app.dev.semantic_plan_api import register_semantic_plan_api_routes
```

`_register_develop_api_routes` 末尾（`register_span_matcher_api_routes` 之后）追加：

```python
    register_semantic_plan_api_routes(
        app,
        resolved_indexer,
        _api_success,
        _api_error,
    )
```

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/app/test_semantic_plan_api.py tests/app/test_app_directory_imports.py -v
```

预期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add app/dev/semantic_plan_api.py app/dev/main_develop.py tests/app/test_semantic_plan_api.py
git commit -m "feat: add dev-only /api/semantic-plan endpoint"
```

---

## Task 7: 删除 `/api/expanded-compare` 聚合 API

**Files:**
- Modify: `app/pages/expanded_compare_page.py`（只留页面路由）
- Modify: `app/dev/main_develop.py`（移除注册）
- Modify: `tests/app/test_expanded_compare_page.py`（只留渲染测试）

- [ ] **Step 1: 先改测试**

`tests/app/test_expanded_compare_page.py` 整体替换为：

```python
from __future__ import annotations

from flask import Flask

from app.pages.expanded_compare_page import register_expanded_compare_page_routes


def test_expanded_compare_page_renders():
    app = Flask(__name__, template_folder="../../templates")
    register_expanded_compare_page_routes(app)

    response = app.test_client().get("/expanded-compare?q=renal%20adhesion")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Expanded Sparse Compare" in html
    assert "renal adhesion" in html


def test_expanded_compare_module_no_longer_exports_api_registrar():
    import app.pages.expanded_compare_page as page_module

    assert not hasattr(page_module, "register_expanded_compare_api_routes")
```

- [ ] **Step 2: 运行确认失败**

```bash
python3 -m pytest tests/app/test_expanded_compare_page.py -v
```

预期：`test_expanded_compare_module_no_longer_exports_api_registrar` FAIL。

- [ ] **Step 3: 实现删除**

`app/pages/expanded_compare_page.py` 整体替换为（聚合 API、序列化辅助、coverage 拼装全部删除）：

```python
from __future__ import annotations

from flask import render_template, request


DEFAULT_TOP_K = 10


def register_expanded_compare_page_routes(app) -> None:
    @app.route("/expanded-compare")
    def expanded_compare_page() -> str:
        initial_query = (request.args.get("q") or "").strip()
        return render_template(
            "expanded_compare.html",
            initial_query=initial_query,
            default_top_k=DEFAULT_TOP_K,
        )
```

`app/dev/main_develop.py`：

- import 改为只导入页面注册函数：

```python
from app.pages.expanded_compare_page import register_expanded_compare_page_routes
```

- `_register_develop_api_routes` 中删除整段：

```python
    register_expanded_compare_api_routes(
        app,
        resolved_indexer,
        _api_success,
        _api_error,
    )
```

- [ ] **Step 4: 运行确认通过**

```bash
python3 -m pytest tests/app/ -v
```

预期：全部 PASS（注意 `tests/app/test_app_directory_imports.py` 仍应通过——它只 import 模块）。

- [ ] **Step 5: Commit**

```bash
git add app/pages/expanded_compare_page.py app/dev/main_develop.py tests/app/test_expanded_compare_page.py
git commit -m "refactor: remove /api/expanded-compare aggregate endpoint"
```

---

## Task 8: 前端改造 `templates/expanded_compare.html`

**Files:**
- Modify: `templates/expanded_compare.html`（`runCompare` 函数与新增 adapter，约 508-541 行；`renderResults` 不变）

页面改为并发调 4 个 API 并在前端组合。每路检索失败独立显示在对应列（原 `errors` dict 行为保留）。

- [ ] **Step 1: 实现 JS 改造**

在 `renderResults` 之后、原 `runCompare` 位置，新增 adapter 并整体替换 `runCompare`：

```javascript
    function firstText(...values) {
      for (const value of values) {
        const candidate = Array.isArray(value) ? value.find(Boolean) : value;
        if (candidate) return String(candidate);
      }
      return "";
    }

    function extractKeywords(metadata) {
      const raw = metadata.paper_keywords || metadata.keywords || [];
      const items = typeof raw === "string" ? [raw] : raw;
      const keywords = [];
      for (const entry of items) {
        const value = (entry && typeof entry === "object")
          ? (entry.keyword || entry.text || entry.name)
          : entry;
        const text = String(value || "").trim();
        if (text && !keywords.includes(text)) keywords.push(text);
      }
      return keywords;
    }

    function adaptSearchItem(item, rank) {
      const metadata = item.metadata || {};
      return {
        rank,
        score: Number(item.similarity ?? item.score ?? 0),
        coverage_ratio: item.coverage_ratio,
        coverage: item.coverage,
        matched_span_count: item.matched_span_count,
        total_span_count: item.total_span_count,
        matched_spans: item.matched_spans || [],
        title: firstText(metadata.canonical_title, metadata.title),
        abstract: firstText(metadata.canonical_abstract, metadata.abstract),
        keywords: extractKeywords(metadata),
      };
    }

    async function fetchJson(url) {
      const response = await fetch(url);
      const payload = await response.json();
      if (!response.ok || !payload.success) {
        throw new Error(payload.error || `HTTP ${response.status}`);
      }
      return payload;
    }

    function buildSearchUrl(query, searchType, topK, withCoverage) {
      const params = new URLSearchParams({
        query,
        top_k: topK,
        search_type: searchType,
      });
      if (withCoverage) params.set("include_coverage", "1");
      return `/api/search?${params.toString()}`;
    }

    async function runCompare() {
      const query = queryInput.value.trim();
      if (!query) {
        statusEl.textContent = "query required";
        return;
      }
      runButton.disabled = true;
      statusEl.textContent = "running...";
      denseResults.innerHTML = "";
      sparseResults.innerHTML = "";
      expandedResults.innerHTML = "";

      const topK = topKInput.value || "10";
      const planParams = new URLSearchParams({ query });

      const [planOutcome, denseOutcome, sparseOutcome, expandedOutcome] =
        await Promise.allSettled([
          fetchJson(`/api/semantic-plan?${planParams.toString()}`),
          fetchJson(buildSearchUrl(query, "dense", topK, true)),
          fetchJson(buildSearchUrl(query, "sparse", topK, true)),
          fetchJson(buildSearchUrl(query, "expanded_sparse", topK, false)),
        ]);

      let terms = [];
      let rowCount = 0;
      if (planOutcome.status === "fulfilled") {
        terms = planOutcome.value.highlight_terms || [];
        rowCount = planOutcome.value.expanded_query_rows?.length || 0;
        renderTerms(terms);
        if (planOutcome.value.semantic_plan) {
          renderPlan(planOutcome.value.semantic_plan);
        } else {
          planGrid.innerHTML = `<div class="empty">No semantic plan</div>`;
        }
      } else {
        renderTerms([]);
        planGrid.innerHTML =
          `<div class="error">${escapeHtml(planOutcome.reason?.message || planOutcome.reason)}</div>`;
      }

      const renderColumn = (outcome, container, countEl, mode) => {
        if (outcome.status === "fulfilled") {
          const items = (outcome.value.results || []).map(adaptSearchItem);
          renderResults(container, countEl, items, terms, mode);
        } else {
          renderResults(
            container, countEl, [], terms, mode,
            outcome.reason?.message || String(outcome.reason),
          );
        }
      };
      renderColumn(denseOutcome, denseResults, denseCount, "dense");
      renderColumn(sparseOutcome, sparseResults, sparseCount, "sparse");
      renderColumn(expandedOutcome, expandedResults, expandedCount, "expanded");

      statusEl.textContent = `${query} | ${rowCount} DB rows`;
      runButton.disabled = false;
    }
```

注意：

- `adaptSearchItem` 给结果编 rank：`(outcome.value.results || []).map(adaptSearchItem)` 中 map 的第二个参数是 0 基索引，改为 `(item, index) => adaptSearchItem(item, index + 1)`。落实现时用这个写法。
- `planGrid` 等 DOM 引用沿用模板顶部已有的 `const` 声明，不重复声明。
- 原 `runCompare` 的 try/catch 整体移除（错误已按列独立处理），不要遗留旧的 `/api/expanded-compare` fetch。

- [ ] **Step 2: 渲染测试回归**

```bash
python3 -m pytest tests/app/test_expanded_compare_page.py -v
```

预期：PASS（模板仍可渲染）。

- [ ] **Step 3: 手动验证（需要真实服务，按 repo-test-skill 显式报告可用性）**

```bash
SKIP_SCISPACY=1 python3 app/dev/main_develop.py both
```

浏览器打开 `http://127.0.0.1:5005/expanded-compare?q=renal%20adhesion`，确认：三列结果渲染、plan 面板渲染、某一路 API 故障时只有该列显示错误。无浏览器时用 curl 验证 API 面：

```bash
curl -s "http://127.0.0.1:5006/api/semantic-plan?query=renal%20adhesion" | python3 -m json.tool | head -30
curl -s "http://127.0.0.1:5006/api/search?query=renal%20adhesion&search_type=expanded_sparse&top_k=5" | python3 -m json.tool | head -30
curl -s "http://127.0.0.1:5006/api/search?query=renal%20adhesion&search_type=dense&include_coverage=1&top_k=3" | python3 -m json.tool | head -40
```

若 MetadataDB / ontology linker / 向量服务不可用，记录「手动验证未执行及原因」，不要静默跳过。

- [ ] **Step 4: Commit**

```bash
git add templates/expanded_compare.html
git commit -m "refactor: compose expanded compare page from generic search and semantic-plan APIs"
```

---

## Task 9: 回归验证

- [ ] **Step 1: 全量相关测试**

```bash
python3 -m pytest tests/indexing/ tests/app/ -v
```

预期：全部 PASS。若 `tests/indexing/` 中有依赖真实服务的标记用例失败，确认其在本改造前同样失败（基线对比），并在汇报中说明。

- [ ] **Step 2: 残留检查**

```bash
grep -rn "expanded-compare" app/ src/ templates/ --include="*.py" --include="*.html" | grep -v "expanded_compare_page\|/expanded-compare\b" || true
grep -rn "register_expanded_compare_api_routes\|_serialize_semantic_plan\|_build_query_semantic_plan" app/ src/ tests/ scripts/ --include="*.py"
```

预期：第二条无输出（旧符号彻底消失）。

- [ ] **Step 3: 集成测试（可选，真实服务可用时）**

```bash
python3 -m pytest tests/integration/test_expanded_sparse_retrieval_real_services.py -v
```

按 repo-test-skill：失败或跳过须在汇报中显式说明（服务/配置不可用）。

- [ ] **Step 4: 最终汇报核对**

- 列出实际执行过的 compile/pytest 命令与结果
- 说明手动验证（浏览器/curl）是否完成
- 确认无真实服务测试数据残留（本计划测试均为 mock，正常无残留）

---

## Self-Review 记录

1. **覆盖核对**：聚合 API 的 6 项职责全部有去处——dense/sparse 检索（已有 `/api/search`）、plan 构建（Task 2 公开 + Task 6 API）、expanded 检索（Task 3）、coverage 注解（Task 4）、序列化（Task 1 Domain + Task 8 前端 adapter）、按路错误隔离（Task 8 `Promise.allSettled`）。
2. **类型一致性**：`build_query_semantic_plan(query, source_list, keyword_sources, profile_name)` 签名在 Task 2/3/4/6 中一致；`expanded_sparse_search` 返回 dict 形状与 `/api/search` 既有 dense 结果形状对齐（`similarity` + 嵌套 `metadata`）。
3. **已知取舍**：一次 compare 触发约 4 次 plan 构建（dense coverage、sparse coverage、expanded、semantic-plan 各一次），决策记录第 4 条已确认接受；`hydrate=False` 时 `include_coverage` 不生效（注解依赖 metadata），属预期行为。
4. **顺序依赖**：Task 2 依赖 Task 1（expanded_compare_page 的 import 改动）；Task 5 依赖 Task 3/4（search 签名）；Task 7 依赖 Task 5/6（前端有替代 API 后才删聚合 API）；Task 8 依赖 Task 5/6/7。不可乱序执行。
