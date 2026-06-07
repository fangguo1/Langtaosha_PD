# 智能搜索 MVP 发布验收清单

**日期**: 2026-06-05
**状态**: Pre-release Review
**目标交付日期**: 2026-06-10
**用途**: 汇总数据流、搜索功能流、API、运维和回归测试证据，形成最终 `GO / CONDITIONAL GO / NO-GO` 决策。

## 1. 关联交付物

- `01_data_flow_delivery_map.md`
- `02_search_flow_delivery_map.md`
- `docs/discussion/smart_search_14_day_plan_0528.md`
- `docs/api/frontend_handoff.md`

本清单不重复描述实现细节。所有验收项必须提供可复查证据。

## 2. 状态定义

| 状态 | 含义 |
| --- | --- |
| `PASS` | 满足本次交付要求 |
| `DEGRADED` | 可以交付，但存在明确、可接受、可监控的降级 |
| `FAIL` | 阻断本次交付 |
| `PENDING` | 尚未完成验收 |
| `NOT_IN_SCOPE` | 本次明确不交付 |

总体发布结论：

| 结论 | 条件 |
| --- | --- |
| `GO` | 所有阻断项为 `PASS`，非阻断问题有明确负责人 |
| `CONDITIONAL GO` | 无 `FAIL`，存在少量可接受 `DEGRADED`，并有监控和补偿方案 |
| `NO-GO` | 任一阻断项为 `FAIL`，或关键项仍为 `PENDING` |

当前建议结论：`NO-GO`

原因：当前仍处于验收准备阶段，数据一致性、正式搜索回归集、完整 Search Trace 和故障演练尚未完成。

## 3. 发布范围

### 3.1 本次承诺交付

- 智能搜索正式 API。
- Query Understanding 基础路由。
- 作者名搜索与作者建议。
- Dense、Sparse/BM25、Keyword Lookup 三路混合召回。
- 单检索分支失败后的降级能力。
- API Request ID、稳定错误结构和前端交接文档。
- 每日 bioRxiv / Langtaosha 抓取和入库 orchestrator。
- 基础作者补全能力。
- 用户测试反馈和固定搜索回归集。

### 3.2 明确不在本次范围

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| 完整作者消歧 | `NOT_IN_SCOPE` | 第一版保留来源与置信度 |
| 完整 MeSH/UMLS 体系 | `NOT_IN_SCOPE` | 第一版使用关键词增强能力 |
| 复杂学习排序 | `NOT_IN_SCOPE` | 当前使用 weighted RRF 和规则过滤 |
| 大规模 benchmark | `NOT_IN_SCOPE` | 使用固定回归集和用户测试 |
| 完整告警平台 | `NOT_IN_SCOPE` | 第一版保证日志和人工检查方式 |
| Streaming search | `NOT_IN_SCOPE` | 第一版一次性 JSON 响应 |
| 正式分页协议 | `NOT_IN_SCOPE` | 第一版一次性结果列表 |

## 4. Go / No-Go 阻断标准

出现以下任一情况，发布结论必须为 `NO-GO`：

- 正式搜索 API 无法稳定调用。
- API、用户测试和回放脚本验证的不是同一条正式搜索链路。
- 当日新增数据无法成为可检索资产。
- MetadataDB 与 VectorDB 存在无法解释的大量不一致。
- 三路检索全部失败时仍返回正常成功结果。
- 单个检索分支失败会导致整个服务不可用。
- API 契约在交付期间仍发生破坏性变化。
- 核心回归 query 相比基线出现严重退化。
- 关键失败请求无法通过 `request_id` 定位。
- 无法执行回滚或补偿操作。

以下情况可以考虑 `CONDITIONAL GO`：

- Author enrichment 部分失败，但论文基础检索正常。
- Ontology linker 不可用，但正式混合检索可降级运行。
- 管理面板不完整，但日志和查询脚本能够完成问题定位。
- Sparse 或 Keyword Lookup 部分覆盖不足，但已量化影响并可回填。

## 5. 数据流验收

详细证据见 `01_data_flow_delivery_map.md`。

| ID | 验收项 | 状态 | 是否阻断 | 验收证据 | 负责人 |
| --- | --- | --- | --- | --- | --- |
| D01 | 每日 orchestrator 按时执行并生成 manifest | `PASS` | 是 | `docs/daily_orchestrator_log/` | `PENDING` |
| D02 | bioRxiv 抓取量、原始文件行数和入库量可对账 | `PENDING` | 是 | 每日数据流报告 | `PENDING` |
| D03 | Langtaosha 零数据可区分正常无数据与抓取异常 | `PENDING` | 是 | 源站抽查、stderr、manifest | `PENDING` |
| D04 | MetadataTransformer 对正式 source 契约稳定 | `PASS` | 是 | transformer tests | `PENDING` |
| D05 | MetadataDB 入库幂等，单条失败不阻断批次 | `DEGRADED` | 是 | backfill 统计、失败样本 | `PENDING` |
| D06 | Dense 索引覆盖率达到交付阈值 | `PENDING` | 是 | Dense coverage 报告 | `PENDING` |
| D07 | Sparse 索引覆盖率达到交付阈值 | `PENDING` | 条件阻断 | Sparse coverage 报告 | `PENDING` |
| D08 | Keyword coverage 达到交付阈值 | `PENDING` | 条件阻断 | Keyword coverage 报告 | `PENDING` |
| D09 | PG 与 Dense/Sparse VectorDB 一致性可检查 | `PENDING` | 是 | 一致性报告 | `PENDING` |
| D10 | 当日新增论文抽样可由正式 API 召回 | `PENDING` | 是 | Search smoke 报告 | `PENDING` |
| D11 | `returncode != 0` 的可选步骤不会使整体显示完全成功 | `FAIL` | 是 | orchestrator 状态语义检查 | `PENDING` |
| D12 | 数据失败有补抓、回填和定向重跑方式 | `DEGRADED` | 是 | 运维命令与演练记录 | `PENDING` |

## 6. 搜索功能流验收

详细证据见 `02_search_flow_delivery_map.md`。

| ID | 验收项 | 状态 | 是否阻断 | 验收证据 | 负责人 |
| --- | --- | --- | --- | --- | --- |
| S01 | 正式入口明确为 `GET /api/scholar/search` | `PASS` | 是 | API 文档 | `PENDING` |
| S02 | API、用户测试和回放脚本使用同一正式链路 | `PENDING` | 是 | 调用链审计 | `PENDING` |
| S03 | 默认正式检索策略冻结为 `hybrid_retrieval` | `PASS` | 是 | 配置与代码审计 | `PENDING` |
| S04 | Query Understanding 核心路由回归通过 | `PENDING` | 是 | 固定回归集 | `PENDING` |
| S05 | 作者名查询与姓名变体回归通过 | `PENDING` | 是 | 作者 query 报告 | `PENDING` |
| S06 | Dense、Sparse、Keyword Lookup 三路正常运行 | `PENDING` | 是 | 分支 smoke test | `PENDING` |
| S07 | 单个检索分支失败时其他分支继续返回 | `PENDING` | 是 | 故障注入测试 | `PENDING` |
| S08 | 全部检索分支失败时返回明确错误 | `PENDING` | 是 | 故障注入测试 | `PENDING` |
| S09 | Weighted RRF 与检索权重有版本化记录 | `DEGRADED` | 条件阻断 | 配置快照 | `PENDING` |
| S10 | 低相关结果和多概念 query 达到质量阈值 | `PENDING` | 是 | 固定回归集与用户评测 | `PENDING` |
| S11 | Metadata hydrate 失败不会返回不可展示结果 | `PENDING` | 是 | 故障测试 | `PENDING` |
| S12 | Search Trace 可按 request_id 还原完整链路 | `PENDING` | 是 | Trace 抽查 | `PENDING` |

## 7. API 与前端联调验收

| ID | 验收项 | 状态 | 是否阻断 | 验收证据 | 负责人 |
| --- | --- | --- | --- | --- | --- |
| A01 | `/api/health` 可用于服务存活检查 | `PASS` | 是 | curl / 部署检查 | `PENDING` |
| A02 | `/api/ready` 可用于服务就绪检查 | `DEGRADED` | 是 | 当前仅检查 MetadataDB | `PENDING` |
| A03 | 搜索 API 请求参数和响应 schema 已冻结 | `DEGRADED` | 是 | `docs/api/frontend_handoff.md` | `PENDING` |
| A04 | 错误响应结构稳定并包含 request_id | `PASS` | 是 | API 错误测试 | `PENDING` |
| A05 | API 鉴权和 CORS 行为符合交付要求 | `PASS` | 是 | 外部测试记录 | `PENDING` |
| A06 | `top_k` 的 per-source 语义已被前端理解 | `PENDING` | 条件阻断 | 联调确认 | `PENDING` |
| A07 | hybrid `similarity` 不被前端误展示为语义百分比 | `PENDING` | 条件阻断 | 联调确认 | `PENDING` |
| A08 | 前端可展示作者建议、纠错提示和错误信息 | `PENDING` | 是 | 联调记录 | `PENDING` |
| A09 | 主搜索流程端到端联调通过 | `PENDING` | 是 | 联调验收记录 | `PENDING` |

## 8. 可观测性与运维验收

| ID | 验收项 | 状态 | 是否阻断 | 验收证据 | 负责人 |
| --- | --- | --- | --- | --- | --- |
| O01 | 每个 API 请求有 request_id | `PASS` | 是 | API 响应 | `PENDING` |
| O02 | 能统计搜索成功数、错误率和 result count | `PENDING` | 是 | 日志或报表 | `PENDING` |
| O03 | 能统计搜索总耗时和慢请求 | `PENDING` | 是 | latency 报表 | `PENDING` |
| O04 | 能查看 query route 和 correction | `DEGRADED` | 是 | Study Mode / trace | `PENDING` |
| O05 | 能查看各检索分支耗时、候选数和失败 | `PENDING` | 是 | Search Trace | `PENDING` |
| O06 | 能查看过滤前后结果数和过滤原因 | `PENDING` | 条件阻断 | Search Trace | `PENDING` |
| O07 | 能查看每日抓取、入库、失败和跳过数量 | `DEGRADED` | 是 | orchestrator manifest | `PENDING` |
| O08 | 能查看 embedding pending/failed 积压 | `PENDING` | 是 | 数据检查报告 | `PENDING` |
| O09 | 能定位 PG/VectorDB 不一致 | `PENDING` | 是 | 一致性报告 | `PENDING` |
| O10 | 关键异常有人工处理 Runbook | `PENDING` | 是 | 运维说明 | `PENDING` |

## 9. 固定回归集验收

### 9.1 必测 Query

| 类型 | Query | 主要验收点 | 状态 |
| --- | --- | --- | --- |
| 作者名 | `xiao fan` | 正确作者路由 | `PENDING` |
| 作者名变体 | `fan xiao` | 姓名顺序变体 | `PENDING` |
| 精确术语 | `CAR-T` | 精确召回 | `PENDING` |
| 拼写错误 | `machi learning` | 纠错与回退 | `PENDING` |
| 固定短语 | `brain computer interface` | 短语完整性 | `PENDING` |
| 多概念组合 | `obesity and macrophage` | 两个概念同时覆盖 | `PENDING` |
| 关系型 query | `enhancer-promoter interaction` | 关系短语不丢失 | `PENDING` |
| 多概念组合 | `genome 3D structure regulation` | 低相关结果控制 | `PENDING` |
| 宽泛主题 | `synapse` | 控制结果发散 | `PENDING` |
| 固定短语 | `developmental disorder` | 短语与主题约束 | `PENDING` |

### 9.2 质量指标

| 指标 | 交付阈值 | 实际结果 | 状态 |
| --- | --- | --- | --- |
| 核心 query route 正确率 | 负责人确认，建议 `>= 95%` |  | `PENDING` |
| 作者名核心案例正确率 | `100%` |  | `PENDING` |
| Top 3 人工相关率 | 负责人确认，建议 `>= 80%` |  | `PENDING` |
| 多概念 query 部分命中错误 | 明显低于当前基线 |  | `PENDING` |
| 单分支失败后的可用率 | `100%` |  | `PENDING` |
| 全部分支失败正确报错率 | `100%` |  | `PENDING` |
| API P95 latency | 负责人确认 |  | `PENDING` |

## 10. 故障演练

| 场景 | 预期结果 | 状态 | 证据 |
| --- | --- | --- | --- |
| MetadataDB 不可用 | `/api/ready` 返回 503，搜索明确报错 | `PENDING` |  |
| Dense 分支不可用 | Sparse + Keyword Lookup 继续 | `PENDING` |  |
| Sparse 分支不可用 | Dense + Keyword Lookup 继续 | `PENDING` |  |
| Keyword Lookup 不可用 | Dense + Sparse 继续 | `PENDING` |  |
| 全部检索分支不可用 | 返回 `SEARCH_FAILED` 和 request_id | `PENDING` |  |
| Author enrichment 失败 | 数据主链路为 degraded，不阻断检索 | `PENDING` |  |
| 当日源数据为空 | 能区分正常无数据与异常断流 | `PENDING` |  |
| VectorDB 文档无法 hydrate | 不返回不可展示结果并记录异常 | `PENDING` |  |

## 11. 已知问题与风险接受

发布前，每个已知问题必须具有明确状态、影响范围、负责人和后续日期。

| 风险 | 影响 | 当前决策 | 负责人 | 后续日期 |
| --- | --- | --- | --- | --- |
| Author enrichment 可选步骤失败仍可能显示 pipeline `ok` | 隐性数据质量下降 | 必须修正或明确降级 | `PENDING` | `PENDING` |
| 正式 API 与 `PaperIndexer.smart_search()` 语义分叉 | 测试与线上行为可能不同 | 必须收束或明确边界 | `PENDING` | `PENDING` |
| Search Trace 尚不完整 | 线上问题难定位 | 必须补齐最小 trace | `PENDING` | `PENDING` |
| `top_k` 为 per-source | 前端结果数和排序理解偏差 | 文档冻结并联调确认 | `PENDING` | `PENDING` |
| Source 分组优先展示 | 可能弱化全局相关排序 | 产品负责人接受风险 | `PENDING` | `PENDING` |
| 宽泛 query 低相关结果较多 | 用户体验下降 | 通过回归集确定阈值 | `PENDING` | `PENDING` |

## 12. 发布步骤

### 12.1 发布前

- [ ] 冻结代码版本和配置版本。
- [ ] 保存 retrieval 权重、阈值和 source 配置快照。
- [ ] 执行数据一致性检查。
- [ ] 执行固定 query 回归集。
- [ ] 执行 API smoke test。
- [ ] 执行单分支故障演练。
- [ ] 确认前端使用冻结后的 API schema。
- [ ] 确认回滚命令和负责人。

### 12.2 发布后观察

- [ ] 检查 `/api/health` 和 `/api/ready`。
- [ ] 检查搜索错误率和 P95 latency。
- [ ] 检查 branch failure 和降级次数。
- [ ] 检查零结果 query 和异常高结果数 query。
- [ ] 检查每日数据任务状态。
- [ ] 检查 embedding pending/failed。
- [ ] 抽查正式 API 搜索结果。

## 13. 回滚与补偿

| 场景 | 处理方式 | 当前状态 |
| --- | --- | --- |
| 新搜索策略质量明显退化 | 恢复上一版 retrieval 配置或代码 | `PENDING` |
| API schema 破坏前端 | 恢复兼容响应结构 | `PENDING` |
| 单个 source 数据异常 | 暂时从 source_list 移除并补抓 | `PENDING` |
| Dense/Sparse 索引异常 | 使用可用分支降级并定向回填 | `PENDING` |
| 新增数据入库错误 | 停止当日任务，按 manifest 定向修复 | `PENDING` |

## 14. 最终发布结论

| 项目 | 内容 |
| --- | --- |
| 发布版本/Commit | `PENDING` |
| 配置版本 | `PENDING` |
| 验收日期 | `PENDING` |
| 数据流状态 | `DEGRADED` |
| 搜索功能流状态 | `DEGRADED` |
| API 联调状态 | `PENDING` |
| 回归测试状态 | `PENDING` |
| 故障演练状态 | `PENDING` |
| 最终结论 | `NO-GO` |
| 阻断问题 | 数据一致性、Search Trace、固定回归集、故障演练 |
| 已接受风险 | `PENDING` |
| 发布负责人 | `PENDING` |
| 回滚负责人 | `PENDING` |

### 签字确认

| 角色 | 姓名 | 结论 | 日期 |
| --- | --- | --- | --- |
| 系统负责人/架构师 |  |  |  |
| 后端负责人 |  |  |  |
| 前端负责人 |  |  |  |
| 测试/验收负责人 |  |  |  |
