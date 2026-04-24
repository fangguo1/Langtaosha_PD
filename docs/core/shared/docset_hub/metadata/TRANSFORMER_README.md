# Metadata 流水线

本模块负责把不同来源的论文元数据转换成统一结构，并产出可直接写入数据库的 payload。
位置为'docset_hub/metatata'


## 核心结论

- 推荐入口是 `MetadataTransformer`
- `source_name` 现在必须由调用方显式传入，不再自动识别
- `source_name` 会贯穿 Router、Source Adapter、`NormalizedRecord`、DB payload，最终写入 `paper_sources`
- `MetadataDB` 在写库前会再次校验 `source_name` 的合法性和一致性，避免脏数据进入数据库

## 快速开始

```python
from docset_hub.metadata.transformer import MetadataTransformer

transformer = MetadataTransformer()

result = transformer.transform_file(
    input_path="/path/to/paper.json",
    source_name="biorxiv_daily",
)

if result.success:
    db_payload = result.db_payload
    upsert_key = result.upsert_key
else:
    print(result.error)
```

如果数据已经在内存里，可以直接跳过 input adapter：

```python
result = transformer.transform_dict(
    raw_payload=paper_dict,
    source_name="langtaosha",
)
```

## 流水线概览

默认流程如下：

```text
input file / dict
    -> input_adapter
    -> router
    -> source_adapter
    -> normalizer
    -> db_mapper
    -> db_payload + upsert_key
```

各模块职责：

- `input_adapters`：解析 `.json` 或 `.jsonl`
- `router`：校验调用方传入的 `source_name`
- `source_adapters`：把来源字段映射到统一契约
- `normalizer`：清洗 DOI、日期、语言等值格式
- `db_mapper`：生成 `papers`、`paper_sources` 等表的写库 payload

## `source_name` 传递机制

这是当前版本最重要的约束之一。

### 1. 调用方必须显式提供

```python
result = transformer.transform_file(
    input_path="/path/to/paper.json",
    source_name="biorxiv_history",
)
```

不再支持根据 URL、字段特征或平台自动猜测来源。

### 2. Router 先做第一层校验

`MetadataRouter.route(payload, source_name=...)` 会验证来源名称是否在 `SUPPORTED_SOURCES` 中；不合法会抛出 `RoutingError`。

### 3. Transformer 会把 `source_name` 传给对应的 Source Adapter

这次调整后：

- `BiorxivSourceAdapter(source_name=...)`
- `LangtaoshaSourceAdapter(source_name=...)`

不再在 adapter 内部硬编码或推断来源名。这样同一个 bioRxiv adapter 就可以区分：

- `biorxiv`
- `biorxiv_history`
- `biorxiv_daily`

### 4. `source_name` 会进入统一中间结构和 DB payload

传递链路如下：

```text
调用方 source_name
    -> MetadataRouter
    -> SourceAdapter(source_name=...)
    -> NormalizedRecord.source_name
    -> db_payload["paper_sources"]["source_name"]
    -> upsert_key["source_name"]
```

### 5. 写库前会再做第二层校验

`MetadataDB` 初始化时会加载配置中的 `default_sources`，写库时会执行两类校验：

- 合法性校验：`source_name` 必须存在于 `default_sources`
- 一致性校验：`db_payload.paper_sources.source_name` 必须与 `upsert_key.source_name` 一致

校验入口在：

- `insert_paper(...)`
- `update_paper(...)`
- `upsert_paper(...)`

内部查询与插入逻辑里也保留了兜底校验。

## 当前已实现的来源

对默认 `MetadataTransformer` 来说，当前已接通的 source adapter 有：

- `langtaosha`
- `biorxiv_history`
- `biorxiv_daily`

其中：

- `biorxiv_history`
- `biorxiv_daily`

都复用 `BiorxivSourceAdapter`，区别只在调用方传入的 `source_name`，以及最终落库到 `paper_sources.source_name` 的值。

## 输出结果

`MetadataTransformer` 成功后返回 `TransformResult`，常用字段如下：

- `success`
- `db_payload`
- `upsert_key`
- `work_id`
- `error`

`db_payload` 主要包含：

- `papers`
- `paper_sources`
- `paper_source_metadata`
- `paper_author_affiliation`
- `paper_keywords`
- `paper_references`

`upsert_key` 主要用于 `MetadataDB` 判断同一来源记录是否已存在。

说明：`work_id` 不再由 `MetadataTransformer` 生成。该字段由 `MetadataDB` 在新建 `paper`（`INSERT_NEW_PAPER` / `UPSERT_NEW_PAPER`）时分配，因此转换结果中的 `work_id` 可能为 `None`。

## 写库语义

`MetadataDB` 的三种主入口：

- `insert_paper(db_payload, upsert_key)`：只插入，不覆盖已存在来源记录
- `update_paper(db_payload, upsert_key)`：仅在来源记录已存在时更新
- `upsert_paper(db_payload, upsert_key)`：不存在则插入，存在则更新

来源记录存在性判断采用同源回退策略：

1. `source_name + source_record_id`
2. `source_name + doi`
3. `source_name + arxiv_id`
4. `source_name + pubmed_id`

也就是说，`source_name` 不只是标签，它直接参与去重和更新判定。

## 最小示例：转换并写库

```python
from pathlib import Path

from docset_hub.metadata.transformer import MetadataTransformer
from docset_hub.storage.metadata_db import MetadataDB

config_path = Path("src/config/config_tecent_backend_server_test.yaml")

transformer = MetadataTransformer()
metadata_db = MetadataDB(config_path=config_path)

result = transformer.transform_file(
    input_path="/path/to/biorxiv_daily.json",
    source_name="biorxiv_daily",
)

if result.success:
    paper_id = metadata_db.upsert_paper(
        db_payload=result.db_payload,
        upsert_key=result.upsert_key,
    )
    print(paper_id)
else:
    print(result.error)
```

## 测试覆盖

本轮改动后的测试重点包括：

- `load_test_papers()` 同时加载 `langtaosha`、`biorxiv_history`、`biorxiv_daily`
- `TestSourceValidation` 覆盖 `source_name` 合法性与一致性校验
- `TestPaperSourcesTable` 验证 `paper_sources.source_name` 正确落库
- 多源插入、更新、upsert 与 canonical source 相关测试

如果你正在扩展新来源，建议至少同步检查三处：

1. `MetadataRouter.SUPPORTED_SOURCES`
2. `MetadataTransformer.SOURCE_ADAPTERS`
3. 配置中的 `default_sources`
