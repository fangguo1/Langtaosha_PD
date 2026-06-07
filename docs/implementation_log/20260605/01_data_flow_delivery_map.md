# 数据流交付地图

**日期**: 2026-06-05
**状态**: Delivery Review
**目标**: 证明外部论文数据能够稳定进入系统，并最终成为可被正式搜索 API 召回的检索资产。

## 1. 范围与交付结论

本文件覆盖以下数据流：

```text
bioRxiv / Langtaosha
  -> Fetch / Crawl
  -> Raw JSONL
  -> MetadataTransformer
  -> MetadataDB
  -> Keyword Enrichment
  -> Author Enrichment
  -> Dense / Sparse Index
  -> Consistency Check
  -> Search Verification
```

本文件不负责描述在线 query 检索逻辑。在线搜索链路见：

- `02_search_flow_delivery_map.md`
- `03_release_acceptance_checklist.md`

当前总体状态：`DEGRADED`

主要原因：

- 每日 orchestrator 已能够执行抓取、入库和作者补全，并生成 manifest。
- 当前 manifest 将所有 `skipped` 状态视为可接受，可能隐藏 `returncode != 0` 的可选步骤失败。
- 尚需形成固定的 PostgreSQL、Dense VectorDB、Sparse VectorDB 和关键词覆盖一致性报告。
- 尚需将“脚本执行成功”进一步验证为“当日新增论文可以通过正式 API 被召回”。

## 2. 正式数据流

### 2.1 调度入口

正式每日任务入口：

```text
scripts/run_daily_orchestrator.py
  -> src/docset_hub/orchestrator/daily_pipeline.py
```

默认处理目标日期为执行当天的前一天。

### 2.2 主链路

```text
DailyPipeline.run()
  -> fetch_biorxiv
  -> fetch_langtaosha
  -> ensure raw JSONL exists
  -> ingest biorxiv_daily
  -> ingest langtaosha
  -> enrich biorxiv_daily authors
  -> enrich langtaosha authors
  -> write manifest and docs log
```

入库脚本：

```text
scripts/backfill_source_records.py
  -> PaperIndexer.index_dict()
  -> MetadataTransformer.transform_dict()
  -> MetadataDB insert
  -> dense vectorization
  -> sparse vectorization
  -> keyword enrichment
```

## 3. 数据流节点交付表

状态定义：

- `PASS`: 已满足当前交付要求。
- `DEGRADED`: 主流程可用，但存在已知降级或验证缺口。
- `FAIL`: 阻断数据成为可检索资产。
- `PENDING`: 尚未完成本轮验收。
- `NOT_IN_SCOPE`: 本次明确不交付。

| 节点 | 输入 | 输出 | 当前状态 | 成功标准 | 验证方式 |
| --- | --- | --- | --- | --- | --- |
| bioRxiv 抓取 | `target_date` | bioRxiv JSONL | `PASS` | 命令成功，记录数与文件一致 | manifest、原始文件行数 |
| Langtaosha 抓取 | `target_date` | Langtaosha JSONL | `DEGRADED` | 能区分正常无数据与抓取异常 | manifest、stderr、源站抽查 |
| Raw JSONL 落盘 | 抓取结果 | 按 source/date 存储的 JSONL | `PASS` | 文件存在，记录可解析 | JSONL 解析检查 |
| Metadata 转换 | source raw payload | 统一 DB payload | `PASS` | source 契约正确，失败样本可定位 | transformer tests、失败清单 |
| MetadataDB 入库 | DB payload | `paper_id`、`work_id` | `PASS` | 幂等写入，单条失败不阻断批次 | 入库统计、SQL 抽查 |
| Dense 索引 | canonical title/abstract | Dense VectorDB 文档 | `DEGRADED` | 应索引论文存在 dense 文档 | embedding 状态、一致性检查 |
| Sparse 索引 | canonical title/abstract | Sparse VectorDB 文档 | `PENDING` | 应索引论文存在 sparse 文档 | sparse coverage 报告 |
| Keyword enrichment | title/abstract | `paper_keywords` | `DEGRADED` | 达到约定覆盖率，失败可回填 | keyword coverage 报告 |
| Author enrichment | paper metadata / Semantic Scholar | 作者补全结果 | `DEGRADED` | 失败不阻断主入库，失败原因可见 | enrichment manifest |
| 数据一致性 | PG、VectorDB、关键词库 | 差异报告 | `PENDING` | 缺失、孤儿、积压均可统计 | 固定检查脚本 |
| 可检索性验证 | 当日新增样本 | 正式 API 搜索结果 | `PENDING` | 抽样论文可由正式 API 召回 | smoke query 报告 |

## 4. 核心数据契约

### 4.1 Source 契约

`source_name` 必须由调用方显式提供，并贯穿：

```text
caller
  -> MetadataRouter
  -> SourceAdapter
  -> NormalizedRecord
  -> db_payload.paper_sources.source_name
  -> upsert_key.source_name
```

当前正式 source：

- `langtaosha`
- `biorxiv_history`
- `biorxiv_daily`

验收要求：

- `source_name` 必须存在于配置的 `default_sources`。
- payload 与 `upsert_key` 中的 `source_name` 必须一致。
- 同一来源记录重跑时不应产生重复 paper。

### 4.2 论文身份契约

核心身份字段：

- `paper_id`: MetadataDB 内部身份。
- `work_id`: 跨 MetadataDB 与 VectorDB 的主要关联身份。
- `paper_source_id`: 来源记录身份。
- `canonical_source_id`: 当前 canonical metadata 来源。

验收要求：

- VectorDB 文档必须能够通过 `work_id` 回填 MetadataDB 数据。
- 一个搜索结果不能因为 metadata 缺失而无法展示。
- canonical source 变化后，应明确是否触发重新索引。

### 4.3 可索引文本契约

当前向量文本优先级：

```text
canonical title + canonical abstract
  -> canonical title
  -> current source title + abstract
  -> current source title
  -> no vectorization
```

缺少可索引文本时必须记录为可解释的跳过，不能默认为成功索引。

## 5. 状态与失败语义

### 5.1 建议统一状态

```text
succeeded
degraded
failed
skipped_expected
skipped_suspicious
dry_run
```

含义：

| 状态 | 含义 |
| --- | --- |
| `succeeded` | 阶段完成并满足成功标准 |
| `degraded` | 主链路完成，但可选能力失败或结果低于阈值 |
| `failed` | 阻断数据成为可检索资产 |
| `skipped_expected` | 根据输入或配置按预期跳过 |
| `skipped_suspicious` | 被跳过但需要人工检查 |
| `dry_run` | 仅验证命令和输入，不产生正式数据 |

### 5.2 当前需修正的状态风险

当前 `DailyPipeline` 将 `ok`、`skipped`、`dry_run` 都视为整体成功。近期日志中存在作者补全命令 `returncode=1`，但步骤被标记为 `skipped`，最终 pipeline 仍为 `ok`。

交付前建议至少做到：

- `returncode != 0` 的可选步骤使整体状态变为 `degraded`。
- `reason=empty_file` 仅在确认源站当天确实无数据时标记为 `skipped_expected`。
- manifest 汇总 required step 与 optional step 的成功、失败和跳过数量。

## 6. 必须统计的指标

### 6.1 每次任务指标

| 指标 | 说明 | 当前交付要求 |
| --- | --- | --- |
| `fetched_count` | 抓取记录数 | 必须输出 |
| `raw_valid_count` | 可解析原始记录数 | 必须输出 |
| `transformed_count` | 成功标准化记录数 | 必须输出 |
| `metadata_inserted_count` | 新增 metadata 数 | 必须输出 |
| `metadata_existing_count` | 幂等跳过或已存在数 | 必须输出 |
| `metadata_failed_count` | metadata 失败数 | 必须输出 |
| `dense_indexed_count` | dense 写入成功数 | 必须输出 |
| `sparse_indexed_count` | sparse 写入成功数 | 必须输出 |
| `keyword_covered_count` | 有关键词数据的论文数 | 必须输出 |
| `author_enriched_count` | 作者补全成功数 | 可降级 |
| `searchable_sample_count` | 抽样可召回数量 | 必须输出 |
| `consistency_error_count` | 一致性错误数量 | 必须输出 |

### 6.2 建议交付阈值

以下阈值需要在首次完整报告后由负责人确认：

| 指标 | 建议阈值 | 状态 |
| --- | --- | --- |
| Metadata 转换成功率 | `>= 99%` | `PENDING` |
| Metadata 入库成功率 | `>= 99%` | `PENDING` |
| Dense 索引覆盖率 | `>= 98%` | `PENDING` |
| Sparse 索引覆盖率 | `>= 95%` | `PENDING` |
| 当日新增可检索率 | `>= 95%` | `PENDING` |
| 超过 24 小时的 embedding pending | `0` 或有明确解释 | `PENDING` |
| PG 存在但 VectorDB 缺失 | 低于约定阈值 | `PENDING` |

## 7. 一致性检查矩阵

| 检查项 | 异常含义 | 是否阻断交付 | 当前状态 |
| --- | --- | --- | --- |
| PG paper 有 `work_id`，Dense VectorDB 无文档 | 语义检索可能无法召回 | 是 | `PENDING` |
| PG paper 有 `work_id`，Sparse VectorDB 无文档 | BM25 分支覆盖不完整 | 条件阻断 | `PENDING` |
| VectorDB 有 `work_id`，PG 无 metadata | 结果无法 hydrate | 是 | `PENDING` |
| embedding 长期 `pending` | 索引积压或任务异常 | 条件阻断 | `PENDING` |
| embedding `failed` 无错误摘要 | 无法补偿和定位 | 是 | `PENDING` |
| 新增 paper 无 keyword | keyword lookup 覆盖下降 | 可降级 | `PENDING` |
| 作者补全失败 | 作者详情不完整 | 可降级 | `DEGRADED` |

## 8. 每日运行检查表

每日检查应输出一条结论，不只保存原始 manifest。

| 检查项 | 结果 | 证据/备注 |
| --- | --- | --- |
| 调度任务按时执行 | `PENDING` |  |
| bioRxiv 抓取量合理 | `PENDING` |  |
| Langtaosha 抓取量合理或有零数据解释 | `PENDING` |  |
| MetadataDB 入库量与抓取量可对账 | `PENDING` |  |
| Dense/Sparse 索引无异常积压 | `PENDING` |  |
| Keyword coverage 无显著下降 | `PENDING` |  |
| 作者补全失败已记录 | `PENDING` |  |
| PG/VectorDB 一致性通过 | `PENDING` |  |
| 当日新增抽样可被 API 召回 | `PENDING` |  |

## 9. 端到端验收案例

每个 source 至少选择 3 篇当日新增论文：

```text
raw record
  -> source_name
  -> paper_id / work_id
  -> canonical metadata
  -> paper_keywords
  -> dense document
  -> sparse document
  -> API search result
```

验收记录模板：

| Source | Paper/Work ID | Metadata | Keyword | Dense | Sparse | API 召回 | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `biorxiv_daily` |  |  |  |  |  |  | `PENDING` |
| `biorxiv_daily` |  |  |  |  |  |  | `PENDING` |
| `biorxiv_daily` |  |  |  |  |  |  | `PENDING` |
| `langtaosha` |  |  |  |  |  |  | `PENDING` |
| `langtaosha` |  |  |  |  |  |  | `PENDING` |
| `langtaosha` |  |  |  |  |  |  | `PENDING` |

## 10. 补偿与恢复

交付前必须确认以下操作有明确执行方式：

| 异常 | 补偿方式 | 当前状态 |
| --- | --- | --- |
| 某日抓取失败 | 按日期重新抓取 | `PASS` |
| 原始记录解析失败 | 从失败清单重跑 | `PENDING` |
| MetadataDB 单条失败 | 按 source record 重跑 | `DEGRADED` |
| Dense/Sparse 写入失败 | 从 pending/failed 回填 | `DEGRADED` |
| Keyword enrichment 失败 | 按 paper/source 回填 | `DEGRADED` |
| Author enrichment 失败 | 非阻断重跑 | `DEGRADED` |
| PG/VectorDB 不一致 | 输出差异并定向修复 | `PENDING` |

## 11. 已知风险与非本次范围

### 已知风险

- `skipped` 状态可能掩盖非零 return code。
- Langtaosha 当日零数据尚需区分正常无数据与抓取异常。
- 数据流统计目前分散在命令输出和 manifest，尚未收束为单一验收报告。
- Dense、Sparse、Keyword 与 MetadataDB 的覆盖率尚未形成统一快照。

### 非本次范围

- 完整作者消歧。
- 完整外部作者数据源治理。
- 完整告警平台。
- 全量历史数据重新索引。

## 12. 最终签收

| 项目 | 内容 |
| --- | --- |
| 数据流总体状态 | `DEGRADED` |
| 阻断项 | 一致性报告、可检索性验收尚未完成 |
| 可接受降级 | Author enrichment 部分失败但不阻断论文检索 |
| 负责人 | `PENDING` |
| 验收日期 | `PENDING` |
