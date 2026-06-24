# Coverage Loose Suffix Match 实现计划

**日期**: 2026-06-24  
**状态**: Plan  
**目标**: 新增 `analyze_document_coverage_loose()`，在 strict coverage 之外提供带 suffix 匹配的单文档 coverage 分析。  
**约束**: **不修改** 现有 `analyze_document_coverage()` 及其调用链。

---

## 1. 背景

Query `developmental disorder`、文档含 `neurodevelopmental disorders` 时：

- strict `analyze_document_coverage()`：`developmental` 嵌在 `neurodevelopmental` 内，exact/prefix 均因词边界规则 **不命中**，`coverage_ratio = 0`。
- 期望：在 **dev 分析 / compare 辅助** 场景下，允许一种更宽松的「词干嵌合」命中，但不影响现有 strict 语义。

---

## 2. 设计原则

| 原则 | 说明 |
|------|------|
| **零侵入 strict** | `analyze_document_coverage()` 函数体、签名、返回值形状 **不改** |
| **独立函数** | 新增 `analyze_document_coverage_loose()`，自包含逻辑 |
| **可共享私有 helper** | 允许新增 `_tokenize_field()`、`_term_suffix_matches()` 等 **新** 私有函数；strict 路径 **不调用** 它们 |
| **Guard 1 only** | suffix 仅对 **单词 term、len≥8** 生效；不做 Guard 2（min prefix 长度） |
| **Python only** | 本阶段不进 MetadataDB SQL、不改 `PaperIndexer.search(include_coverage=)` |

---

## 3. 两个 API 对比

| | `analyze_document_coverage` | `analyze_document_coverage_loose` |
|--|----------------------------|-------------------------------------|
| 状态 | 已有，**不动** | **新增** |
| exact | ✅ | ✅ |
| prefix | ✅ | ✅ |
| suffix（Guard 1） | ❌ | ✅ |
| 返回类型 | `CoverageReport` | 同 `CoverageReport` |
| 生产默认 | `include_coverage` 继续用它 | **不默认接入** |

---

## 4. Suffix 规则（Guard 1）

在 loose 函数内，对每个 term **先走与 strict 相同的 exact/prefix 判断**（复制 `_term_matches_field` 逻辑或调用现有 private 函数 `_term_matches_field`，**不修改该函数**）。

若 strict 匹配失败，再尝试 suffix：

```text
1. term.match_mode == "exact"（prefix 词项不走 suffix）
2. normalized_term 为单词（无空格）
3. len(normalized_term) >= 8                    ← Guard 1
4. field 拆词：words = re.findall(r'[a-z0-9]+', field_value)
5. 存在 word 满足：
     word != normalized_term
     word.endswith(normalized_term)
     len(normalized_term) / len(word) >= 0.5
```

**预期 case**：

| term | field word | loose |
|------|------------|-------|
| `developmental` | `neurodevelopmental` | ✅ suffix |
| `renal` | `adrenal` | ❌ len=5 < 8 |
| `renal`（prefix mode） | `renalac` | ✅ 仍走 prefix，非 suffix |

---

## 5. 评分（loose 内部固定）

suffix 命中 parent scope 时：

- 计为 **match**（`span_score > 0`）
- `span_score = 0.5`（常量 `LOOSE_SUFFIX_PARENT_SPAN_SCORE`）
- exact / prefix 命中 parent：`span_score = 1.0`（与 strict 一致）

同一 span 若既有 exact 又有 suffix，取 **max → 1.0**。

child partial 逻辑与 strict 相同：parent 无命中时，`matched_children / total_children`。

`matched_spans[]` 可选增加 debug 字段（仅 loose 写入）：

```python
"match_kinds": ["suffix"]  # 或 ["exact"], ["prefix"], ["exact", "suffix"]
```

strict 的 `CoverageReport` payload **不变**。

---

## 6. 实现方案（简单、不 refactor）

### 6.1 新增公开 API

```python
def analyze_document_coverage_loose(
    *,
    plan: QuerySemanticPlan,
    document_fields: Mapping[str, Any],
) -> CoverageReport:
    ...
```

### 6.2 新增私有 helper（strict 不用）

```python
SUFFIX_MIN_TERM_LENGTH = 8
SUFFIX_MIN_WORD_RATIO = 0.5
LOOSE_SUFFIX_PARENT_SPAN_SCORE = 0.5

def _tokenize_field_words(field_value: str) -> List[str]: ...

def _term_suffix_matches_guard1(term: SemanticTerm, words: Sequence[str]) -> bool: ...

def _loose_term_match_kind(term, field_value, field_words) -> str:
    # "none" | "exact" | "prefix" | "suffix"
    ...
```

### 6.3 loose 主流程（独立复制 strict 结构）

`analyze_document_coverage_loose` **自行实现** 与 strict 相同的 span 循环结构（约 60 行），差异仅在 term 匹配处调用 `_loose_term_match_kind`，以及 parent score 用 0.5/1.0 分支。

**刻意不做**：把 strict 抽成 `_analyze_document_coverage(allow_suffix=...)`。  
接受少量 duplication，换取 strict 零 diff。

复用已有、**不修改** 的 helper：

- `_normalize_document_fields`
- `_iter_span_terms`
- `_term_matches_field`（只读调用）
- `_prune_subsumed_terms`
- `_build_report`

### 6.4 导出

`src/docset_hub/indexing/__init__.py` 增加：

```python
analyze_document_coverage_loose
```

---

## 7. 文件改动清单

| 文件 | 动作 |
|------|------|
| `src/docset_hub/indexing/coverage_engine.py` | 追加 loose 函数 + 新 private helper；**strict 函数零改动** |
| `src/docset_hub/indexing/__init__.py` | export |
| `tests/indexing/test_coverage_engine.py` | 仅 **新增** loose 测试 |
| `docs/core/shared/docset_hub/indexing/COVERAGE_ENGINE_README.md` | 追加「Loose coverage」小节（实现后） |

**不改**：

- `paper_indexer.py`
- `metadata_db.py`
- `analyze_document_coverage` 的任何调用方

---

## 8. 测试计划

### 8.1 回归

```bash
python3 -m pytest tests/indexing/test_coverage_engine.py -q
```

现有 strict 测试 **不应改断言**；若失败说明误改了 shared helper。

### 8.2 新增 loose 测试

| 测试名 | 断言 |
|--------|------|
| `test_loose_coverage_matches_developmental_suffix_in_neurodevelopmental` | tier1 `developmental disorder` + title `neurodevelopmental disorders` → loose ratio=0.5，match_kinds 含 suffix；**strict 同 case ratio=0** |
| `test_loose_coverage_rejects_renal_in_adrenal_via_guard1` | tier1 `renal` + `adrenal` → loose ratio=0 |
| `test_loose_coverage_loose_gte_strict` | 任意 plan：loose.coverage_ratio >= strict.coverage_ratio |

---

## 9. 实施步骤（2 个 commit）

```text
Commit 1: feat: add analyze_document_coverage_loose with suffix Guard 1
  - coverage_engine.py（仅追加）
  - __init__.py export
  - 3 个新测试

Commit 2: docs: document loose coverage in COVERAGE_ENGINE_README
```

---

## 10. 后续（不在本计划）

- compare 页展示 loose coverage 列
- `PaperIndexer` opt-in 参数
- MetadataDB / expanded sparse SQL 对齐（单独项目）

---

## 11. 相关文档

- [Coverage Engine README](../../core/shared/docset_hub/indexing/COVERAGE_ENGINE_README.md)
- [Query Semantic Plan README](../../core/shared/docset_hub/indexing/QUERY_SEMANTIC_PLAN_README.md)
- TODO：`docs/daily_development_log/To_do_list.md`（developmental disorder dense case）
