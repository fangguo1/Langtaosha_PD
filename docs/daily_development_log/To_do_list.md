# To Do List

## 2026-06-11

- [ ] 2026-06-11 09:26:07 CST - 分析 `expanded_compare` 中 query `developmental disorder` 的 dense case：当前 dense 能召回包含 `neurodevelopmental disorders` 的论文，但 `analyze_document_coverage()` 基于 `title`/`abstract`/`paper_keywords` 的 exact or prefix term 匹配返回 `coverage=0`；需要确认 semantic plan 实际 term、词边界规则，以及是否应支持这类复合词/派生词命中。
- [ ] 2026-06-11 00:15:10 CST - 实现 retrieval testbed 的 `--candidate-scope labeled` 模式；按 `docs/implementation_log/20260610/retrieval_testbed_labeled_candidate_scope_implementation_plan_20260610.md` 执行，确保每个 query 只在自己的 labeled `work_id` 集合内检索/排序，并保留默认 `corpus` 全库评估行为。
