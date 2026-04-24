# Langtaosha PD

`Langtaosha_PD` 是一个面向科研文献数据的后端工程，覆盖元数据清洗、MetadataDB 存储、VectorDB 检索、索引构建与查询服务。当前仓库已经按模块重组文档：`docs/core` 负责核心说明，`docs/implementation_log` 负责较大改动的计划与实现记录。

## What This Repo Contains

- `src/docset_hub/metadata`: 元数据转换、输入适配、source 适配
- `src/docset_hub/storage`: MetadataDB / VectorDB 访问层
- `src/docset_hub/indexing`: paper indexer 与检索流程
- `app/`: Flask 入口与 Web 层代码
- `scripts/`: 建库、清库、导入、回填、诊断脚本
- `tests/`: unit、mock、integration 测试
- `docs/core/`: 核心模块说明与本地协作约定
- `docs/implementation_log/`: 计划文档、实现记录、阶段总结

## Documentation Layout

- `docs/core/local/`: 本仓库本地约定与重要实现说明
  - [`CODEX_WORKFLOW.md`](/home/wnlab/langtaosha/Langtaosha_PD/docs/core/local/CODEX_WORKFLOW.md)
  - `tencent_vector_db_embedding_manual.md`
- `docs/core/shared/`: 与 `src/` / `tests/` 结构对应的核心模块说明
  - [`VECTOR_DB_README.md`](/home/wnlab/langtaosha/Langtaosha_PD/docs/core/shared/docset_hub/storage/VECTOR_DB_README.md)
  - [`METADATA_DB_README.md`](/home/wnlab/langtaosha/Langtaosha_PD/docs/core/shared/docset_hub/storage/METADATA_DB_README.md)
  - [`TRANSFORMER_README.md`](/home/wnlab/langtaosha/Langtaosha_PD/docs/core/shared/docset_hub/metadata/TRANSFORMER_README.md)
  - [`TEST_SKILL.md`](/home/wnlab/langtaosha/Langtaosha_PD/docs/core/shared/tests/TEST_SKILL.md)
- `docs/implementation_log/`: 大改动前的 plan 与实现日志

修改或使用核心模块前，先阅读对应的 `docs/core/...` 说明。大范围或跨模块改动前，先在 `docs/implementation_log/` 下创建 `GOAL_YYYYMMDD.md` 计划文件。

## Project Structure

```text
Langtaosha_PD/
├── app/                     # Flask 应用入口
├── database/                # 数据库 schema / migrations
├── docs/
│   ├── core/
│   │   ├── local/           # 本地工作流与重要手册
│   │   └── shared/          # 与 src/tests 对应的模块说明
│   ├── implementation_log/  # plan、实现记录、阶段总结
│   └── migrations/          # 迁移相关文档
├── examples/                # 示例脚本
├── local_data/              # 本地数据与配置目录
├── scripts/                 # 建库、清库、导入、回填、诊断脚本
├── src/
│   ├── config/              # YAML 配置加载
│   └── docset_hub/
│       ├── metadata/
│       ├── storage/
│       ├── indexing/
│       └── crud/
├── static/                  # 静态资源
├── templates/               # Web 模板
└── tests/                   # unit / mock / integration tests
```

## Environment

推荐环境：

```bash
conda activate langtaosha
pip install -r requirements.txt
pip install -e .
```

默认从仓库根目录运行脚本：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
```

## Common Commands

启动 Web 服务：

```bash
python app/main.py
```

初始化数据库：

```bash
python scripts/setup_databases.py --config-path src/config/config_tecent_backend_server_test.yaml
```

清理数据库：

```bash
python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_test.yaml --confirm
```

导入 biorxiv JSONL：

```bash
python scripts/import_from_biorxiv_jsonl.py
```

运行向量库导入：

```bash
python scripts/run_vector_db_import_from_jsonl.py
```

## Testing Boundary

- Codex 适合执行 unit tests、mock tests、静态检查与不依赖内网服务的本地验证。
- 依赖真实内网服务的 integration tests，建议在你的 shell 中运行。
- 当前真实环境依赖包括：
  - Tencent VectorDB (`172.21.*`)
  - PostgreSQL metadata_db (`172.21.*`)

常用测试命令：

```bash
pytest tests/storage/test_vector_db.py -q
pytest tests/indexing/test_paper_indexer.py -q
```

如果本机配置了代理，跑真实内网测试前建议先清掉：

```bash
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY
```

## Notes

- `local_data/`、`test_data/`、`mimic_data/` 提供本地样例数据与测试数据目录。
- `scripts/_vdb_diag_tmp.py` 可用于临时诊断 VectorDB HTTP 行为。
- 大改动完成后，建议同步更新相应模块文档与 `docs/implementation_log/` 记录。
