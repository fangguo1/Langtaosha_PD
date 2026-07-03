# PaperIndexer Public L1/L2 Retrieval Real Test Plan

## Goal

为 `paper_indexer.py` 重构后的 retrieval 链路补充真实数据库、真实向量库、真实数据的集成测试，范围只覆盖公开 `L1` 与 `L2`：

- `L1`: `dense_search`, `sparse_search`, `expanded_sparse_search`
- `L2`: `search`

本计划明确排除：

- 内部 hybrid 支路 `_keyword_lookup_search`
- `L2 hybrid_retrieval_search`
- `L3 smart_search`
- scholar 路由层返回结构测试

测试配置固定使用：

- `src/config/config_tecent_backend_server_mimic.yaml`

测试数据优先使用：

- mimic 库中已有真实数据进行只读检索验证
- 仓库 `test_data/` 中的真实 payload 仅在必须补充“可控命中样本”时使用

---

## Why This Test Exists

本轮 `paper_indexer` 重构把 retrieval 逻辑重新分层：

- `L1` 只负责 raw recall，统一返回 `List[RankedResult]`
- `L2` 负责 source resolve、helper pipeline、结果呈现与 hybrid 编排

因此现阶段最需要验证的不是 mock contract，而是以下真实链路是否仍然成立：

1. `L1` 是否还能从真实 VectorDB / MetadataDB 拉回结构正确的 `RankedResult`
2. `L2` 是否还能把真实 recall 结果正确转成 API-shaped dict
3. `expanded_sparse` 的 `keyword_sources` 是否在真实链路里继续影响召回
4. `hybrid_retrieval_search` 是否还能在真实数据上完成多路编排，而不是只在 mock 测试里成立

---

## Scope Adjustment

经真实 mimic 数据探查后，第一版真实库测试进一步收敛：

- 保留公开 `L1` 的真实召回验证
- 保留 `L2 search(search_type=...)` 的真实结果呈现验证
- 暂不把 `hybrid_retrieval_search()` 纳入第一版真实测试

原因：

1. 当前真实返回里 hybrid 前排结果明显偏 sparse 主导
2. 第一版更适合先稳定验证分层重构后的单路 recall contract
3. hybrid 的真实多路融合断言需要单独设计更稳的 query 与更细的 debug 预期

因此本计划中的 `L2` 范围应理解为：

- `search(search_type="dense")`
- `search(search_type="sparse")`
- `search(search_type="expanded_sparse")`

---

## Constraints And Working Agreement

本计划遵守以下仓库约束：

- 按 `CODEX_WORKFLOW.md`，实现计划与变更跟踪文档写入 `docs/implementation_log/`
- 按 `tests/conftest.py` 复用共享真实 payload fixture，不重复实现 `load_test_papers()`
- 按仓库 `TEST_SKILL.md`，真实服务测试归类为 integration tests
- 真实服务测试必须显式说明 cleanup 策略
- 不允许静默切到 production config

同时需要尊重当前协作边界：

- Codex 可以编写与静态检查这些测试
- 真实网络环境下的最终执行与稳定性验证，应由用户在本地 shell / Conda 环境中完成

---

## Test Placement

建议新增两个 integration 文件，而不是把真实服务测试混进现有 `tests/indexing/` mock 套件：

- `tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py`
- `tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py`

原因：

1. 这次验证的是跨组件真实链路，已经超出单模块 unit test 范围
2. 现有 `tests/indexing/test_paper_indexer_search_type.py` 与 `test_paper_indexer_three_way_hybrid_retrieval.py` 仍然适合保留为 mock/contract tests
3. 真实 DB / VectorDB 依赖不应该污染日常快速单测反馈

---

## Config Strategy

测试文件中应定义独立的 mimic config 解析顺序，避免硬编码不可覆盖：

1. `--config-path` CLI 参数
2. 测试专用环境变量
3. 默认回退到 `src/config/config_tecent_backend_server_mimic.yaml`

推荐环境变量名：

- `PAPER_INDEXER_REAL_RETRIEVAL_TEST_CONFIG`

推荐默认路径：

- `src/config/config_tecent_backend_server_mimic.yaml`

注意：

- 不要回退到 `_test.yaml`
- 不要静默切换到其他数据库
- 若 mimic config 不存在，应直接 `pytest.skip`

---

## Shared Fixtures Design

建议新增以下 fixture：

### 1. `real_retrieval_config_path`

职责：

- 解析真实测试配置路径
- 校验文件存在
- 为后续 fixture 提供统一 `Path`

scope:

- `session`

### 2. `real_retrieval_indexer`

职责：

- `_reset_config()`
- `init_config(..., force_reload=True)`
- 构建 `PaperIndexer`
- 启用 `vectorization`
- 关闭不必要的 side flow

建议初始化方式：

- `enable_vectorization=True`
- `enable_keyword_enrichment=False`

scope:

- `session`

### 3. `retrieval_probe`

职责：

- 在真正断言前先检查 mimic 环境是否具备最小测试前置条件

建议检查：

- `metadata_db` 可连接
- `vector_db` 已启用
- `default_sources` 非空
- 至少一个 source 存在 metadata
- 至少一个 source 能返回 dense 或 sparse 结果
- `expanded_sparse` 所依赖的 keyword 数据表中存在可用记录

行为：

- 前置条件不满足时使用 `pytest.skip(...)`
- 不把环境问题误判为代码失败

scope:

- `module`

### 4. `sample_queries`

职责：

- 为 L1/L2 提供稳定度尽可能高的一组查询

推荐来源顺序：

1. 先从 mimic 库真实 keyword / title token / abstract token 中动态选取
2. 只有在动态选取不可控时，才从 `test_papers` 中抽取 token 生成查询

建议返回结构：

```python
{
    "dense_or_sparse_query": "...",
    "expanded_sparse_query": "...",
    "source_list": ["biorxiv_history"],
    "keyword_sources": ["paper_metadata"],
}
```

scope:

- `module`

---

## L1 Test Plan

文件：

- `tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py`

### Test 1: `dense_search` returns real `RankedResult` rows

目标：

- 验证公开 `L1 dense_search` 在真实向量库上可返回 `RankedResult`

断言：

- 返回值类型是 `list`
- 至少一个元素是 `RankedResult`
- `retriever == "dense"`
- `rank` 从 1 开始连续增长
- `score` 为有限浮点数
- `work_id` 非空
- `source_name` 属于请求的 `source_list`

不建议断言：

- 第一名固定是某个 `work_id`

原因：

- mimic 数据会演化，固定排序断言太脆弱

### Test 2: `sparse_search` returns real `RankedResult` rows

目标：

- 验证公开 `L1 sparse_search` 在真实 sparse collection 上可工作

断言：

- 返回非空
- 元素类型为 `RankedResult`
- `retriever == "sparse"`
- `rank` 连续
- 至少一个结果 `score > 0`
- `work_id` 与 `source_name` 合法

### Test 3: `expanded_sparse_search` returns span-aware ranked results

目标：

- 验证 `expanded_sparse_search` 真实走通：
  - semantic plan build
  - MetadataDB expanded sparse lookup
  - `ExpandedSparseCandidate -> RankedResult`

断言：

- 返回非空
- `retriever == "expanded_sparse"`
- 至少一个结果 `matched_span_count > 0`
- `total_span_count >= matched_span_count`
- `score == coverage_ratio` 语义成立
  - 实际上通过断言 `score > 0`
  - 以及 `retrieval_debug` 中存在 coverage 相关字段来侧面验证
- `matched_spans` 为非空列表或至少存在该字段

### Test 4: `expanded_sparse_search` respects `keyword_sources`

目标：

- 验证 `keyword_sources` 仍然是 recall-time 参数，而不是被错误移出 L1 语义

做法：

- 使用同一 query 和 source_list
- 分别调用：
  - `keyword_sources=["paper_metadata"]`
  - `keyword_sources=None`

断言建议：

- 两次调用都不报错
- 至少一组结果非空
- 若两组都非空，比较结果集或 top work ids，允许“不完全相同”
- 不把“必须不同”写成硬断言，避免 mimic 数据状态导致偶发误伤

更稳妥的 contract：

- 断言该参数可透传到真实链路并保持行为稳定
- 若环境允许，再增加一个更强的差异性断言

---

## L2 Test Plan

文件：

- `tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py`

### Test 1: `search(search_type="dense")` presents lightweight rows

目标：

- 验证 `L2 search()` 能消费真实 `dense_search()` 结果并输出轻量 dict

调用：

- `hydrate=False`

断言：

- 返回非空 `List[Dict]`
- 每项包含：
  - `work_id`
  - `paper_id`
  - `source_name`
  - `similarity`
  - `retrieval_debug`
- `retrieval_debug` 中可识别 dense 来源
- 不要求 `metadata` 存在

### Test 2: `search(search_type="dense", hydrate=True)` hydrates metadata

目标：

- 验证 `present_search_results(..., hydrate=True)` 在真实 metadata 库上可用

断言：

- 返回非空
- 第一项包含 `metadata`
- `metadata` 中至少存在：
  - `canonical_title` 或 `title`
  - `sources`

### Test 3: `search(search_type="sparse")` presents sparse rows

目标：

- 验证真实 sparse recall 经 L2 转换后结构稳定

断言：

- 返回非空
- `similarity` 为数值
- `retrieval_debug` 保留 sparse 分支信息

### Test 4: `search(search_type="expanded_sparse")` presents coverage-facing fields

目标：

- 验证 expanded sparse 经过 helper 呈现后，输出结构符合新链路预期

断言方向：

- 返回非空
- 每项仍为 dict
- 至少包含：
  - `work_id`
  - `similarity`
  - `retrieval_debug`
- 若 helper 当前对外暴露 `matched_spans` / `coverage` / `matched_span_count` 等字段，则断言这些字段存在且自洽

注意：

- 这里的断言应跟随当前 `present_search_results()` 实际输出 contract
- 不要臆造 API 字段名

### Deferred: `hybrid_retrieval_search()`

第一版不纳入真实库测试。

保留原因记录：

- 当前真实 mimic 数据下，hybrid 前排结果更接近 sparse 主导召回
- 若要稳定断言 hybrid，需要先单独筛选“多分支共同命中”的 query
- 这部分仍可继续由现有 mock 测试覆盖编排 contract

---

## Query Selection Strategy

这是整个真实测试稳定性的关键。

### 原则

优先找“高概率稳定命中”的 query，而不是手写领域词猜测：

1. 对 dense / sparse：
   - 从真实 metadata 的 title / abstract / keyword 中抽取辨识度较高的词组
2. 对 expanded_sparse：
   - 优先使用 mimic 库里真实存在的 keyword phrase
   - 尽量选择双词或多词 query，便于出现 span/group 匹配

### 第一版固定 query

以下 query 已基于 mimic 库真实数据和当前 worktree 中的 `PaperIndexer` 实跑验证，可直接进入第一版测试：

1. `acute kidney injury`
2. `renal fibrosis`
3. `lung cancer`

对应真实样本 work_id：

- `acute kidney injury`
  - `W019db3ae-a05a-7c81-b772-5ebd3344c9c5`
  - `W019db3ae-b742-72ea-9909-761d2272ee7b`
  - `W019db3af-3d39-7d9e-b03a-0043515f51f5`
- `renal fibrosis`
  - `W019db3ad-c1fb-7178-acdf-eac92fe46751`
  - `W019db3af-1988-7718-9f70-abc8e74f952a`
  - `W019db3af-8420-7c5b-9dc3-0cae07a43372`
- `lung cancer`
  - `W019db3ae-49bd-7b63-9171-d4e5f79a5b42`
  - `W019db3ae-7347-7e6b-bb3a-9e5357d991cf`
  - `W019db3ad-be5f-7076-bc81-3d0a863157ea`
  - `W019db3af-9629-756a-9de1-e42af5808ece`

这些 query 的当前观察结果：

- `dense_search`、`sparse_search`、`expanded_sparse_search` 均返回非空
- `search(search_type="dense"|"sparse"|"expanded_sparse")` 均返回非空
- `expanded_sparse` 返回中存在稳定的 `coverage_ratio=1.0`、`matched_span_count=1`

### 推荐实现

在 fixture 中做少量 probe：

- 先从 `paper_keywords` 或已有 metadata 查询 20-50 个候选短语
- 选择一个在 `dense_search`、`sparse_search`、`expanded_sparse_search` 中至少一部分可命中的 query

这样能降低以下风险：

- 仓库长期演进后，硬编码 query 失效
- mimic 数据增删后固定 query 变成空结果

---

## Read-Only First Strategy

本轮测试默认采用只读验证，不主动往 mimic 库写入数据。

优点：

- 避免真实服务测试中的 cleanup 复杂度
- 避免污染已有 mimic 数据集
- 更贴近“重构后 retrieval 是否仍兼容线上样本”的目标

只有当只读测试无法稳定构造 expanded sparse 命中样本时，才考虑追加第二阶段：

- 基于 `test_papers` 生成唯一 payload
- `index_dict(...)` 写入少量样本
- 用唯一 token 做 retrieval
- 使用 `finally` 或 `yield` fixture 清理 metadata 与 vector docs

当前优先级：

- 第一版不写库

---

## Cleanup Policy

### 第一版计划

- 只读测试，无数据写入
- 原则上无需 cleanup

### 若后续升级为写入型测试

必须实现：

- 测试专属唯一 token / DOI / title
- 记录每次插入得到的 `paper_id` 与 `work_id`
- `finally` 或 `yield` fixture 中清理：
  - metadata rows
  - dense vector docs
  - sparse vector docs

禁止行为：

- 删除整张 collection
- 按 source 批量清理
- 清理非测试生成的数据

---

## Assertion Philosophy

真实 retrieval 测试应强调 contract 与行为，不强调脆弱排序。

### 应该强断言

- 返回类型
- 核心字段存在
- 分支标识正确
- `rank` 连续
- `score` 合法
- hydrate 行为存在差异
- expanded sparse 的 span evidence 字段自洽
- hybrid 的 `matched_retrievers` 存在

### 应避免的脆弱断言

- 固定第一名 `work_id`
- 固定总结果数
- 固定 dense / sparse / hybrid 的完全一致排序
- 假设 mimic 数据永远不会变化

---

## Execution Plan

### Phase 1: Environment And Probe Fixture

目标：

- 建立 mimic config fixture
- 建立 PaperIndexer fixture
- 建立只读 probe

产出：

- 测试文件基础骨架可运行
- 环境不满足时能明确 skip

### Phase 2: Public L1 Real Retrieval Tests

目标：

- 补齐 `dense_search`
- 补齐 `sparse_search`
- 补齐 `expanded_sparse_search`
- 补齐 `keyword_sources` 透传测试

产出：

- `tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py`

### Phase 3: Public L2 Real Retrieval Tests

目标：

- 补齐 `search(...dense...)`
- 补齐 `search(...sparse...)`
- 补齐 `search(...expanded_sparse...)`

产出：

- `tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py`

### Phase 4: Local Static Validation

Codex 可执行：

```bash
python3 -m py_compile tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py
python3 -m py_compile tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py
```

### Phase 5: User Real Environment Validation

建议由用户在仓库根目录、`langtaosha` 环境中执行：

```bash
python3 -m pytest tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py -m integration -v
python3 -m pytest tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py -m integration -v
```

如需显式指定配置：

```bash
python3 -m pytest tests/integration/test_paper_indexer_public_l1_retrieval_real_mimic.py -m integration -v --config-path src/config/config_tecent_backend_server_mimic.yaml
python3 -m pytest tests/integration/test_paper_indexer_public_l2_retrieval_real_mimic.py -m integration -v --config-path src/config/config_tecent_backend_server_mimic.yaml
```

---

## Risks

### 1. mimic 数据本身会变

影响：

- 固定 query 可能从命中变为空

应对：

- 用 probe/dynamic query selection 降低脆弱性

### 2. VectorDB 与 MetadataDB 状态不同步

影响：

- L1 有结果但 hydrate 失败
- L2 hydrate 不稳定

应对：

- 明确区分 `hydrate=False` 与 `hydrate=True` 测试
- 把 hydrate 场景单独列为 L2 contract

### 3. expanded sparse 依赖 keyword 数据完整性

影响：

- 某些 query 在 dense/sparse 可命中，但在 expanded sparse 为空

应对：

- 通过 probe 先筛选可用 query
- 必要时退回写入型受控样本方案

### 4. Codex 当前环境不一定具备真实内网可达性

影响：

- 文档和静态检查可以完成
- 真正 integration run 可能必须由用户本地执行

应对：

- final report 中明确区分 `Codex verified` 和 `User to verify`

---

## Recommended Next Step

按本计划先实现第一版只读 integration tests，顺序如下：

1. 先写 mimic config/path/indexer/probe fixtures
2. 先完成 `L1 dense/sparse/expanded_sparse`
3. 再完成 `L2 search/hybrid_retrieval_search`
4. 最后再根据真实运行情况决定是否需要补写入型稳定样本测试
5. hybrid 如需进入真实测试，单独开第二版计划

这条路径最保守，也最符合本次“验证 retrieval 重构链路”的目标。
