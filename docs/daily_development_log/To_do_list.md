# To Do List

## 2026-06-30

- [ ] 2026-06-30 01:04:58 CST - 完善 search log 管理：梳理 `/api/scholar/search` 请求日志在 `frontend_search_request_logs` 数据库摘要表与 `local_data/search_api_logs` JSONL 文件中的记录、查询、失败告警和清理策略，确保 search-use 线上请求可追踪、可审计、可维护。

## 2026-06-29

- [ ] 2026-06-29 16:05:00 CST - 优化 `expanded_sparse` 检索的数据库性能问题：基于诊断报告 [expanded_sparse_search_perf_diagnosis_20260629.md](../implementation_log/20260629/expanded_sparse_search_perf_diagnosis_20260629.md)，优先拆分 `title_matches` / `abstract_matches` / `keyword_matches` 耗时埋点，随后减少跨 tier 重复 term 扫描，并重构 `papers.canonical_title` 与 `papers.canonical_abstract` 的匹配策略，避免 repeated full-table regex scan。
- [ ] 2026-06-29 15:28:40 CST - 优化 `build_query_semantic_plan` 的性能问题：为 SciSpaCy pipeline 增加进程内缓存，避免 `SpanMatcherPipeline.from_profile()` 每次请求重复执行 `spacy.load("en_core_sci_lg")`，重点验证 search-use API 场景下二次查询延迟是否明显下降。

## 2026-06-25

- [ ] 2026-06-25 16:28:37 CST - 推进 `search-use` 前后端分离方案：在现有 `app/pages`、`app/routes`、`PaperIndexer` 分层基础上，先冻结 API contract，再评估将 Flask 页面迁移为独立 `frontend/` 工程，并明确开发代理、部署代理与页面渲染职责切分。
