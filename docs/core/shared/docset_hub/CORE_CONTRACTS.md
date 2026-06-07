# Docset Hub 核心契约

本文档定义跨 `metadata`、`storage`、`indexing` 和 `orchestrator` 使用的稳定语义。实现细节可以变化，但跨模块协作应遵守这些契约。

## 1. Source 契约

`source_name` 表示一条来源记录所属的数据源。当前正式来源包括：

- `langtaosha`
- `biorxiv_history`
- `biorxiv_daily`

传递路径：

```text
caller
  -> MetadataRouter
  -> SourceAdapter
  -> NormalizedRecord.source_name
  -> db_payload.paper_sources.source_name
  -> upsert_key.source_name
  -> source-specific VectorDB collection
```

约束：

- 调用方应显式提供 `source_name`。
- `db_payload` 与 `upsert_key` 中的 `source_name` 必须一致。
- `source_name` 必须存在于当前配置和对应模块的允许列表中。
- 来源名描述数据来源，不描述执行环境或抓取批次。

## 2. 论文身份契约

| 身份 | 作用域 | 职责 |
| --- | --- | --- |
| `source_record_id` | 单个 source | 来源侧记录身份，用于同来源幂等写入 |
| `paper_source_id` | MetadataDB | 某篇论文与某个来源记录的关联身份 |
| `paper_id` | MetadataDB | PostgreSQL 内部论文身份 |
| `work_id` | 跨存储 | PostgreSQL 与 Dense/Sparse 文档之间的主要关联身份 |
| `canonical_source_id` | MetadataDB | 当前提供 canonical metadata 的 `paper_source_id` |

约束：

- 同一来源记录重跑不应生成重复 `paper_source`。
- 跨来源识别为同一论文时，应共享 `paper_id` 和 `work_id`。
- VectorDB 文档必须携带 `work_id`，以便 hydrate 完整 metadata。
- 搜索结果中的 `work_id` 在 MetadataDB 缺失时属于一致性异常。
- canonical source 变化时，需要重新评估 Dense、Sparse 和关键词资产。

## 3. Metadata 转换契约

Metadata 转换流：

```text
raw payload
  -> input adapter
  -> explicit source routing
  -> source adapter
  -> NormalizedRecord
  -> normalizer
  -> DB payload + upsert key
```

`NormalizedRecord` 是来源适配层与数据库映射层之间的核心中间契约。最低要求：

- `source_name` 非空；
- `raw_metadata` 非空并可追溯；
- `core.title` 非空；
- 作者序号在单条记录内唯一。

Metadata 模块只产生写入意图，不决定最终 `paper_id`、`work_id` 或 canonical source。

## 4. Metadata 写入契约

`MetadataDB` 在写入前解析记录身份：

```text
same_source
cross_source
no_match
```

`PaperIndexer.index_dict()` 当前统一使用 `MetadataDB.insert_paper()`。其语义不是简单 SQL insert，而是：

- 同来源命中：依据版本与时间策略更新或跳过；
- 跨来源命中：向现有论文追加 source；
- 未命中：创建新论文；
- 写入完成后：计算 canonical source。

写入结果必须提供足够信息供后续阶段决策，至少包括：

```text
status_code
paper_id
work_id
canonical.changed
canonical.canonical_source_id
```

## 5. 可索引文本契约

Dense 与 Sparse 当前共享索引文本构造规则：

```text
canonical title + canonical abstract
  -> canonical title
  -> current source title + abstract
  -> current source title
  -> skip
```

在当前 `PaperIndexer` 实现中，canonical 字段为空时回退到本次 source payload。

约束：

- title 与 abstract 均为空时必须显式跳过。
- Dense 与 Sparse 应基于同一份 canonical 文本语义。
- `text_type` 当前为 `abstract` 或 `title`，用于解释索引文本来源。
- canonical source 变化后，旧索引是否被清理和新索引是否完成必须可验证。

## 6. 索引触发契约

当前 `PaperIndexer` 根据 MetadataDB 写入状态决定是否刷新 Dense 和 Sparse：

| 写入状态 | 索引行为 |
| --- | --- |
| `INSERT_NEW_PAPER` | 建立索引 |
| `INSERT_APPEND_SOURCE` | 仅 canonical 变化时建立索引 |
| `INSERT_UPDATE_SAME_SOURCE` | 仅更新来源为 canonical source 时建立索引 |
| 其他状态 | 默认跳过 |

关键词扩充使用相似但不完全相同的触发规则。架构 review 时应分别验证三类检索资产的覆盖情况。

## 7. 检索结果契约

底层 VectorDB 搜索结果使用 `SearchResult`：

```text
source_name
work_id
score
text_type
paper_id
retrieval_debug
```

约束：

- Dense、Sparse 和 RRF 融合后的 `score` 含义不同。
- RRF 融合结果中的 `score` 不能解释为普通语义相似度。
- 融合结果应保留 `retrieval_debug`，包括命中的分支、分支 rank 和原始分数。
- hydrate 后应保留检索证据，并增加完整 metadata。

## 8. 失败与降级契约

建议统一使用以下语义：

| 状态 | 含义 |
| --- | --- |
| `succeeded` | 阶段完成且满足成功标准 |
| `degraded` | 主流程完成，但一个或多个可选能力失败 |
| `failed` | 阻断数据成为可检索资产或阻断正式搜索 |
| `skipped_expected` | 根据输入或配置按预期跳过 |
| `skipped_suspicious` | 被跳过但需要人工检查 |
| `dry_run` | 仅验证命令与输入 |

当前实现尚未全面使用这些名称。Review 时应根据实际返回码和结果解释状态，而不能只依赖字符串 `ok` 或 `skipped`。

## 9. 跨存储一致性契约

必须能够检测以下异常：

- PostgreSQL 有可索引论文，但 Dense 文档缺失；
- PostgreSQL 有可索引论文，但 Sparse 文档缺失；
- VectorDB 有 `work_id`，但 PostgreSQL 无对应 metadata；
- canonical source 已变化，但索引仍使用旧文本；
- embedding 长期处于 `pending` 或失败无错误摘要；
- 新增论文缺少预期关键词资产。
