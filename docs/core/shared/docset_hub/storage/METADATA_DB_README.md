# MetadataDB - 元数据库操作类

**位置**: `src/docset_hub/storage/metadata_db.py`
**版本**: v2.2
**更新日期**: 2026-06-06

---

## 概述

`MetadataDB` 用于把 `MetadataTransformer` 产出的标准化论文元数据写入 PostgreSQL，并提供多源论文管理、读取查询、作者检索、关键词召回、embedding 状态管理等能力。

当前实现的核心特点：

- 支持一篇论文对应多个 `paper_sources`
- 写入前统一判定 `same_source / cross_source / no_match`
- `insert / update / upsert` 三种写入语义统一走同一套 resolve + apply 流程
- canonical source 支持自动选择和手动指定
- `paper_keywords` 支持多来源共存与大小写不敏感写入
- 支持 generated keywords 回写与 query term 候选召回
- 支持作者检索、作者候选推荐和 author enrichment 状态记录
- 支持 embedding 三态管理：`pending / succeeded / failed`

---

## 阅读顺序

本 README 已按 `metadata_db.py` 当前源码顺序组织：

1. 初始化与类内常量
2. 通用工具函数
3. 对外写入 API
4. 写入主流程与命中判定
5. canonical 与 source 校验相关内部方法
6. 底层持久化写入方法
7. generated keywords 与关键词召回
8. 完整读取与作者检索
9. embedding 状态管理
10. 删除、条件搜索、work_id 相关方法
11. author enrichment 与轻量 getter

---

## 1. 初始化与类内常量

### `__init__(config_path, db_key='metadata_db')`

初始化数据库连接，并缓存 `default_sources` 用于 source 合法性校验。

### `GENERATED_KEYWORD_SOURCE`

默认 generated keyword 来源：

```text
scispacy-en_core_sci_lg-generated
```

### `ALLOWED_GENERATED_KEYWORD_TYPES`

允许写入的 generated keyword 类型：

```text
domain, concept, method, task, disease, gene, protein,
model, dataset, metric, organism, chemical
```

---

## 2. 通用工具函数

这部分出现在文件最前面，主要给后续写入、检索、作者匹配和返回结构复用。

### 作者名归一化与打分

```python
normalize_author_name(name: str) -> str
author_match_score(query: str, author_name: str) -> float
```

用途：

- 把作者名统一成小写、去逗号/句点、压缩空格
- 为 `suggest_author_names()` 提供 0.0-1.0 的匹配分数
- 优先使用 `rapidfuzz`，缺失时回退到 `SequenceMatcher`

归一化规则：

```text
lowercase -> comma/dot 转空格 -> collapse spaces
```

### 关键词与 source 过滤工具

```python
_normalize_keyword(keyword: str) -> str
_source_filter_sql(source_list, params, table_alias='ps') -> str
```

用途：

- 规范化 keyword 文本，避免多余空白
- 为作者检索等查询构造 `source_name IN (...)` 过滤片段

### 统一返回与轻量查询工具

```python
_build_write_result(...)
_get_canonical_source_id(conn, paper_id)
_get_work_id_by_paper_id(conn, paper_id)
```

用途：

- 统一构造 `insert/update/upsert` 的结构化返回结果
- 读取当前 `canonical_source_id`
- 根据 `paper_id` 反查 `work_id`

---

## 3. 对外写入 API

这三个方法是 `MetadataDB` 最核心的 public write surface。

### `insert_paper(db_payload, upsert_key) -> Dict[str, Any]`

幂等插入。

语义：

- `same_source` 命中时，按 `version + online_at` 判断覆盖或跳过
- `cross_source / no_match` 时插入新 source，必要时新建 paper

典型场景：

- 批量导入
- 可重复执行的 ingestion

### `update_paper(db_payload, upsert_key, canonical_source_id=None, auto_select_canonical=True) -> Dict[str, Any]`

强制更新。

语义：

- 仅 `same_source` 命中时允许更新
- `cross_source / no_match` 时返回 reject 状态

典型场景：

- 明确要更新已有同源记录
- 需要手动指定 canonical source

### `upsert_paper(db_payload, upsert_key, canonical_source_id=None, auto_select_canonical=True) -> Dict[str, Any]`

插入或更新。

语义：

- `same_source` 命中时强制更新
- `cross_source / no_match` 时插入

典型场景：

- API 写接口
- 不确定论文是否已存在时

### 写入返回结构

三个 public write API 都返回统一结构：

```python
{
    "ok": True,
    "mode": "insert|update|upsert",
    "status_code": "INSERT_SKIP_SAME_SOURCE",
    "paper_id": 123,
    "work_id": "W...",
    "paper_source_id": 456,
    "resolve": {
        "match_type": "same_source|cross_source|no_match",
        "matched_paper_id": 123,
        "matched_paper_source_id": 456,
    },
    "apply": {
        "action": "insert|update|skip|reject",
        "reason": "same_source_update|cross_source_append|...",
    },
    "canonical": {
        "strategy": "manual|auto_online_at|None",
        "before_canonical_source_id": 455,
        "canonical_source_id": 456,
        "changed": True,
    },
}
```

常见 `status_code`：

- `INSERT_NEW_PAPER`
- `INSERT_APPEND_SOURCE`
- `INSERT_UPDATE_SAME_SOURCE`
- `INSERT_SKIP_SAME_SOURCE`
- `UPDATE_SAME_SOURCE`
- `UPDATE_NOT_ALLOWED_NON_SAME_SOURCE`
- `UPSERT_NEW_PAPER`
- `UPSERT_APPEND_SOURCE`
- `UPSERT_UPDATE_SAME_SOURCE`

---

## 4. 写入主流程与命中判定

这部分是多源写入架构的核心引擎。

### `_resolve_and_apply(...)`

统一写入主流程，负责：

1. 校验 `source_name`
2. 调用 `_resolve_match_by_identity()`
3. 根据 `mode=insert/update/upsert` 执行业务分流
4. 执行 canonical 处理
5. 返回统一结果结构

### `_resolve_match_by_identity(conn, upsert_key)`

基于 identity bundle 判定命中类型。

判定顺序：

1. `same_source`
2. `cross_source`
3. `no_match`

同 source 参与匹配的字段：

- 当前 source 对应的 `source_record_id`
- `doi`
- `arxiv_id`
- `pubmed_id`
- `semantic_scholar_id`

跨 source 参与匹配的字段：

- `doi`
- `arxiv_id`
- `pubmed_id`
- `semantic_scholar_id`

返回示意：

```python
{"match_type": "same_source", "paper_id": 1, "paper_source_id": 101, "version": "v2", "online_at": "..."}
{"match_type": "cross_source", "paper_id": 1}
{"match_type": "no_match"}
```

### 当前写入语义

- `insert_paper`
  - `same_source`: 比较版本与日期，决定 `update` 或 `skip`
  - `cross_source`: 追加 source
  - `no_match`: 新建 paper
- `update_paper`
  - 只有 `same_source` 才允许更新
- `upsert_paper`
  - `same_source`: 强制更新
  - 其余：插入

---

## 5. canonical 与 source 校验相关内部方法

这一组在源码里紧跟写入主流程，用于保证 source 合法、canonical 可控。

### source 信息辅助与校验

```python
_get_current_source_identifier(upsert_key)
_validate_source_name(source_name)
_validate_source_consistency(db_payload, upsert_key)
```

职责：

- 提取当前 source 的 source identifier 供日志使用
- 校验 `source_name` 是否属于 `default_sources`
- 校验 `db_payload.paper_sources.source_name` 与 `upsert_key.source_name` 一致

### canonical 选择

```python
_set_canonical_source_by_online_at(conn, paper_id)
_set_canonical_source_by_user(conn, paper_id, canonical_source_id)
_apply_canonical_strategy(conn, paper_id, canonical_source_id, auto_select_canonical)
```

默认规则：

- 自动 canonical 选择 `online_at` 最晚的 source 作为主来源

手动规则：

- `canonical_source_id` 必须属于当前 `paper_id`

典型触发时机：

- 同 source 覆盖更新后
- cross source 追加新来源后
- `update/upsert` 执行完成后

---

## 6. 底层持久化写入方法

这一组是真正的落表执行层。

### paper 与 source 主记录

```python
_get_or_create_paper_from_payload(conn, db_payload) -> int
_insert_source_record_from_payload(conn, paper_id, db_payload) -> int
_update_source_record_from_payload(conn, paper_source_id, db_payload) -> None
```

职责：

- 创建 `papers` 记录，并在缺失时自动生成 `work_id`
- 插入 `paper_sources`
- 更新 `paper_sources`
- 当被更新的 source 正好是 canonical source 时，同步更新 `papers` 表中的 canonical 字段

### 关联写入编排

```python
_apply_source_update(conn, paper_id, paper_source_id, db_payload)
_apply_insert_side_effects(conn, paper_id, paper_source_id, db_payload)
```

职责：

- 更新路径下，重写 authors / keywords / references / source metadata
- 插入路径下，写入 authors / keywords / references / source metadata

### source metadata

```python
_upsert_source_metadata_from_payload(conn, paper_source_id, db_payload)
```

把原始 metadata 和 normalized metadata upsert 到 `paper_source_metadata`。

### authors / keywords / references

```python
_insert_author_affiliation_from_payload(conn, paper_id, db_payload)
_insert_keywords_from_payload(conn, paper_id, db_payload)
_upsert_keyword_case_insensitive(conn, paper_id, keyword_type, keyword, weight, source)
_insert_references_from_payload(conn, paper_id, paper_source_id, db_payload)
```

其中关键词写入语义很重要：

- 原始 metadata keyword 缺失 source 时兜底为 `paper_metadata`
- 同一 `paper_id + keyword_type + source` 内，keyword identity 大小写不敏感
- 首次插入保留展示大小写
- 后续大小写变体会更新同一行的 `weight`

---

## 7. generated keywords 与关键词召回

这部分在源码中位于关联写入方法之后，属于 query understanding / keyword retrieval 能力。

### `upsert_generated_keywords(paper_id, keywords, source=..., allowed_types=None)`

用于幂等写入模型生成的结构化关键词。

写入规则：

- 先校验 `paper_id` 必须存在
- 仅允许白名单 `keyword_type`
- 空 keyword 跳过
- `weight` 被裁剪到 `0.0-1.0`
- 同批次按 `(keyword_type, lower(keyword), lower(source))` 去重
- 逐条复用 `_upsert_keyword_case_insensitive()`

返回示例：

```python
{
    "success": True,
    "paper_id": 123,
    "source": "scispacy-en_core_sci_lg-generated",
    "inserted": 8,
    "updated": 2,
    "skipped": 1,
    "errors": [],
}
```

### `has_keywords_from_source(paper_id, source=...) -> bool`

判断某篇论文是否已拥有指定来源的关键词。常用于 `INSERT_SKIP_SAME_SOURCE` 后补齐 generated keywords。

### `suggest_query_terms(...)`

从 `paper_keywords` 中为 query correction 召回候选词。

默认优先 sources：

- `scispacy-en_core_sci_lg-generated`
- `scispacy-en_ner_bionlp13cg_md-generated`
- `scispacy-en_core_sci_lg-generated-test`
- `scispacy-en_ner_bionlp13cg_md-generated-test`

召回路由：

- substring recall
- prefix recall
- trigram recall

当数据库安装 `pg_trgm` 时会启用 trigram 扩召；否则自动回退到 substring + prefix。

推荐索引：

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_paper_keywords_lower_keyword_trgm
ON paper_keywords
USING gin (lower(keyword) gin_trgm_ops);
```

返回字段：

```python
{
    "keyword": "machine learning",
    "keyword_type": "method",
    "source": "scispacy-en_core_sci_lg-generated",
    "doc_count": 12,
    "avg_weight": 0.91,
}
```

### 关键词论文召回

```python
lookup_papers_by_keyword_terms(...)
lookup_papers_by_keyword_lookup_terms(...)
```

用途：

- 根据 query understanding 产出的 term/group 结构召回论文
- 返回 `matched_concepts`、`keyword_lookup_score`、`retrieval_debug`

关联内部 helper：

```python
_normalize_keyword_lookup_terms(...)
_keyword_lookup_in_filter(...)
_keyword_lookup_paper_source_filter(...)
_keyword_lookup_row_to_dict(...)
_keyword_lookup_plan_row_to_dict(...)
_has_pg_trgm()
```

两类召回差异：

- `lookup_papers_by_keyword_terms()` 以 concept 为单位聚合
- `lookup_papers_by_keyword_lookup_terms()` 支持 grouped selected/sub-concept 召回，并对 support score 做 cap

---

## 8. 完整读取与作者检索

源码中的“更新的查询方法”从这里开始。

### 完整读取

```python
get_paper_info_by_paper_id(paper_id)
read_paper(paper_id)
```

`get_paper_info_by_paper_id()` 会组装完整论文对象，包含：

- `papers` 主记录
- 全部 `paper_sources`
- 每个 source 对应的 `paper_source_metadata`
- `authors`
- `keywords`
- `references`

返回结构核心字段：

```python
{
    "paper_id": 1,
    "work_id": "W...",
    "canonical_title": "...",
    "canonical_source_id": 101,
    "sources": [...],
    "authors": [...],
    "keywords": [...],
    "references": [...],
}
```

`read_paper()` 只是它的别名。

### 作者检索

```python
search_by_author(author_name, limit=100, source_list=None, fuzzy=True)
```

实现特点：

- 只匹配 `paper_author_affiliation.authors[].name`
- 避免 `authors::text ILIKE` 误命中 affiliation 或 JSON 字段名
- 支持 `source_list` 过滤
- 命中后返回完整 paper info
- 按 `online_at DESC NULLS LAST, paper_id DESC` 排序

### 作者候选推荐

```python
suggest_author_names(query, limit=5)
```

流程：

1. 先用 query token 做 SQL 初筛
2. 聚合 `paper_count`
3. Python 层计算 normalized score
4. 去重后按得分排序

排序规则：

1. `score DESC`
2. `paper_count DESC`
3. `name ASC`

---

## 9. Embedding 状态管理

这一组专门服务 embedding/backfill 流程。

### `get_source_name_by_paper_source_id(paper_source_id)`

根据 `paper_source_id` 反查 `source_name`。

### `upsert_embedding_status_pending(...)`

写入或重置 `embedding_status = pending`。

写入字段包括：

- `paper_id`
- `work_id`
- `canonical_source_id`
- `source_name`
- `text_type`
- `last_attempt_at`

### `mark_embedding_succeeded(paper_id)`

把状态更新为 `succeeded`，并：

- `attempt_count + 1`
- 清空错误
- 设置 `last_success_at`

### `mark_embedding_failed(paper_id, error_message)`

把状态更新为 `failed`，并：

- `attempt_count + 1`
- 记录最近错误
- 更新时间戳

### `list_embedding_candidates(source_name=None, statuses=None, limit=100, offset=0)`

查询待处理候选，默认返回：

- `pending`
- `failed`

常用于 embedding backfill 批处理。

---

## 10. 删除、条件搜索、work_id 相关方法

这一段在源码里位于 embedding 状态管理后面。

### 删除

```python
delete_paper_by_paper_id(paper_id) -> bool
delete_paper_by_work_id(work_id) -> bool
```

注意：

- 删除 `papers` 主记录会依赖外键 `ON DELETE CASCADE` 自动清理关联数据
- 方法会先检查记录是否存在

### 条件搜索

```python
search_by_condition(title=None, author=None, category=None, year=None, limit=100)
```

当前已实现：

- `title` 模糊搜索
- `year` 精确匹配 `online_at` 年份

当前未实现：

- `author`
- `category`

### work_id 相关读取

```python
get_paper_info_by_work_id(work_id)
read_paper_by_work_id(work_id)
get_papers_by_work_ids(work_ids, include_sources=True)
```

`work_id` 的定位：

- 对外稳定标识符
- 适合 API、Vector DB、跨系统数据交换
- 相比内部 `paper_id` 更适合系统间关联

---

## 11. Author Enrichment 与轻量 getter

这部分在源码最后，主要服务 Semantic Scholar author enrichment 流程。

### 轻量 getter

```python
get_authors_by_paper_id(paper_id)
get_keywords_by_paper_id(paper_id)
get_references_by_paper_id(paper_id)
```

用途：

- 只取局部关联数据
- 避免每次都走完整 `get_paper_info_by_paper_id()`

### 更新 enrichment 结果

```python
update_author_enrichment(paper_id, authors, semantic_scholar_paper_id=None, doi=None)
```

职责：

- 回写 enriched authors JSON
- 可选同步更新 `paper_sources.semantic_scholar_id`

### 遍历待 enrichment 的论文

```python
iter_papers_for_author_enrichment(
    source_names=None,
    limit=None,
    only_missing=True,
    target_date=None,
    skip_recorded_status=False,
)
```

筛选条件包括：

- 必须有 DOI
- 必须有 authors JSON
- 可按 source 限制
- 可仅返回缺失 enrichment 的记录
- 可按日期过滤
- 可跳过已有终态 status 的记录

### 进度表与状态记录

```python
ensure_author_enrichment_status_table()
record_author_enrichment_status(item)
```

用途：

- 为大规模 Semantic Scholar 回填建立轻量状态表
- 记录每次 enrichment 尝试结果
- 支持 resumable backfill

---

## 数据模型摘要

```text
papers
  ├─ paper_id
  ├─ work_id
  ├─ canonical_title / canonical_abstract
  └─ canonical_source_id -> paper_sources.paper_source_id

paper_sources
  ├─ paper_source_id
  ├─ paper_id
  ├─ source_name
  ├─ source_record_id
  ├─ version
  └─ online_at

paper_source_metadata
  └─ paper_source_id -> raw_metadata_json / normalized_json

paper_author_affiliation
  └─ paper_id -> authors JSONB

paper_keywords
  └─ (paper_id, keyword_type, keyword, source)

paper_references
  └─ paper_id + paper_source_id + reference_order
```

### ID 语义

- `paper_id`: 内部主键
- `work_id`: 对外稳定标识符
- `paper_source_id`: 单条来源记录 ID
- `canonical_source_id`: 当前主来源指针

### 多 source 写入语义

- 同 source 命中
  - `insert`: 覆盖或跳过
  - `update`: 强制更新
  - `upsert`: 强制更新
- 跨 source 命中
  - 追加新的 `paper_sources`
  - 重算 canonical
- 未命中
  - 新建 `papers`
  - 插入首条 `paper_sources`

### `paper_keywords` 多来源语义

同一篇论文的同一个 keyword 可以保留多个来源，例如：

```text
(123, "concept", "CRISPR", "biorxiv")
(123, "concept", "CRISPR", "scispacy-en_core_sci_lg-generated")
```

同一来源内大小写不敏感，不允许 `CRISPR` 与 `crispr` 并存。

相关 migration：

- `database/migrations/20260426_paper_keywords_multisource.sql`
- `database/migrations/20260427_paper_keywords_case_insensitive.sql`

---

## 使用约束

### 必须先经过 `MetadataTransformer`

```python
transformer = MetadataTransformer()
result = transformer.transform_file(file_path, "biorxiv")
write_result = metadata_db.insert_paper(result.db_payload, result.upsert_key)
```

不再支持直接把原始 payload 当成旧式输入写入。

### `paper_keywords` 必须带 source

- 原始 metadata keyword 缺失时兜底为 `paper_metadata`
- generated keyword 由调用方显式传入模型来源
- 不同来源的同名 keyword 应共存

---

## 相关文档

- `src/docset_hub/storage/metadata_db.py`
- `docs/core/shared/docset_hub/storage/VECTOR_DB_README.md`
- `docs/core/shared/docset_hub/indexing/PAPER_INDEXER_README.md`
- `scripts/backfill_generated_keywords.py`
- `tests/storage/test_metadata_db_author_search.py`
- `tests/storage/test_generated_keywords.py`
- `tests/storage/test_paper_keywords_multisource.py`

---

## 变更说明

本次 README 重构重点是：

- 按 `metadata_db.py` 当前函数顺序重排章节
- 把“新增方法”“辅助方法”这类时间/宽泛标签改成更贴近源码阅读路径的说明
- 保留现有多源写入、作者检索、关键词召回、work_id、embedding 状态等关键信息
