# Orchestrator 模块架构

`src/docset_hub/orchestrator` 是每日数据交付的进程级调度层。它组织抓取、入库和作者补全脚本，并保存运行证据。

当前核心实现是 `DailyPipeline`。

## 1. 核心职责

Orchestrator 负责：

- 根据目标日期组织每日任务；
- 调用 bioRxiv 和 Langtaosha 抓取脚本；
- 确保预期 Raw JSONL 路径存在；
- 调用批量入库脚本；
- 可选执行作者补全；
- 保存 stdout、stderr、manifest 和 docs log；
- 支持 `dry_run` 和可选 vector stage。

它不负责：

- 来源 payload 字段映射；
- 论文身份解析；
- Dense、Sparse 或关键词算法；
- 在线搜索；
- 底层数据库事务。

## 2. 当前执行路径

```text
scripts/run_daily_orchestrator.py
  -> DailyPipeline(DailyPipelineConfig)
  -> DailyPipeline.run()
```

`DailyPipeline.run()` 当前顺序：

```text
fetch_biorxiv
fetch_langtaosha
ensure biorxiv raw file
ensure langtaosha raw file
ingest biorxiv_daily
ingest langtaosha
enrich biorxiv_daily authors
enrich langtaosha authors
write manifest and docs log
```

## 3. 进程边界

Orchestrator 当前通过 `subprocess.run()` 调用脚本：

| 阶段 | 脚本 |
| --- | --- |
| bioRxiv 抓取 | `scripts/bioarxiv/biorxiv_api.py` |
| Langtaosha 抓取 | `scripts/langtaosha/langtaosha_scrape.py` |
| 入库 | `scripts/backfill_source_records.py` |
| 作者补全 | `scripts/backfill_semantic_scholar_authors.py` |

因此阶段间契约主要由以下内容组成：

- command arguments；
- 文件路径；
- process return code；
- stdout / stderr；
- 阶段 manifest。

## 4. 配置与输出

`DailyPipelineConfig` 控制：

- `project_root`
- `config_path`
- `target_date`
- `dry_run`
- `run_vector_stage`
- `run_author_enrichment`
- `python_executable`

主要输出：

```text
local_data/daily_orchestrator/<target_date>/manifest.json
local_data/daily_orchestrator/<target_date>/logs/
docs/daily_orchestrator_log/
```

## 5. 当前状态语义

当前步骤状态包括：

```text
ok
failed
skipped
dry_run
```

整体状态包括：

```text
ok
partial_failure
failed
```

当前 `allow_failure=True` 的命令即使 `returncode != 0` 也会被标记为 `skipped`，而整体 pipeline 将 `skipped` 视为成功。这意味着 manifest 的 `ok` 可能包含可选步骤异常。

Review 时应同时检查：

- `status`
- `returncode`
- `reason`
- stdout / stderr
- 子任务 manifest

## 6. 架构改进方向

1. 区分 `skipped_expected`、`skipped_suspicious` 和 `degraded`。
2. 为 required 与 optional step 建立明确策略。
3. 从脚本日志中提取结构化阶段指标。
4. 增加 PostgreSQL、Dense、Sparse 和关键词一致性阶段。
5. 增加当日新增论文的正式 API 可召回验证。
6. 明确重试、补偿和断点续跑语义。

## 7. Review 重点

1. 每个命令的输入输出是否形成稳定契约。
2. 空文件表示“正常无数据”还是“抓取异常”。
3. 非零返回码是否会被错误标记为成功。
4. manifest 是否足以回答每个阶段处理了多少记录。
5. 重跑是否幂等，失败后能否只补偿失败阶段。
6. 任务成功是否最终验证数据可检索。

## 8. 相关文档

- [Docset Hub 总览](../README.md)
- [核心契约](../CORE_CONTRACTS.md)
- [数据写入与索引流](../flows/DATA_INGESTION_FLOW.md)
