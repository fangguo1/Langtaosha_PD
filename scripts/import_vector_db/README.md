# 大规模文献向量化系统

实现从 PostgreSQL 到 FAISS 向量索引的完整流水线，支持千万级文献的向量化和存储。

## 架构概述

本系统基于执行计划 v4 设计，采用三层架构：

1. **Step 1: PostgreSQL → Arrow Batches**（导出器）
   - 从 PostgreSQL 导出未处理的文献（`embedding_status = 0`）
   - 按 range-sharding 分配到不同 shard
   - 导出为 Apache Arrow 格式文件
   - 更新状态为 `1` (exported)

2. **Step 2: Arrow Batches → Embeddings**（并行计算）
   - 多个 embedding worker 并行处理 batch 文件
   - 使用 GritLM 模型计算 embedding
   - 输出为 NPZ 格式文件
   - 不更新 PostgreSQL（仅文件系统操作）

3. **Step 3: Embeddings → FAISS Index**（索引构建）
   - 每个 shard 一个 writer 进程（串行写入）
   - 将 embedding 文件写入 FAISS 索引
   - 定期 checkpoint，支持断点续传
   - 完成后更新状态为 `2` (ready)

## 目录结构

```
import_vector_db/
├── __init__.py                 # 包初始化
├── 002_add_embedding_status.sql    # 数据库迁移脚本
├── export_batches.py           # Step 1: 导出器
├── embed_worker.py             # Step 2: Embedding Worker
├── shard_writer.py             # Step 3: Shard Writer
├── manifest_manager.py         # Manifest 管理器
├── run_pipeline.py             # 主控脚本
├── test_pipeline.py            # 测试脚本
└── README.md                   # 本文件
```

## 安装依赖

```bash
pip install pyarrow numpy faiss-cpu  # 或 faiss-gpu（如果有 GPU）
# GritLM 模型会自动从 HuggingFace 下载，或指定本地路径
```

## 数据库准备

首先执行数据库迁移脚本，添加 `embedding_status` 字段：

```bash
# 连接到 PostgreSQL 数据库
psql -h <host> -U <user> -d <database>

# 执行迁移脚本
\i scripts/import_vector_db/002_add_embedding_status.sql
```

或者使用 Python 脚本执行：

```python
from sqlalchemy import text
from config.db_config import get_engine

engine = get_engine()
with open('scripts/import_vector_db/002_add_embedding_status.sql', 'r') as f:
    sql = f.read()

with engine.connect() as conn:
    conn.execute(text(sql))
    conn.commit()
```

## 使用方法

### 方法 1: 使用主控脚本（推荐）

执行完整流水线：

```bash
python scripts/import_vector_db/run_pipeline.py \
    --step all \
    --base-dir /mnt/lit_platform \
    --batch-size 10000 \
    --papers-per-shard 2500000
```

分步执行：

```bash
# Step 1: 导出 Arrow Batches
python scripts/import_vector_db/run_pipeline.py --step 1 --base-dir /mnt/lit_platform

# Step 2: 计算 Embeddings（处理所有 shard）
python scripts/import_vector_db/run_pipeline.py --step 2 --base-dir /mnt/lit_platform

# Step 2: 处理特定 shard
python scripts/import_vector_db/run_pipeline.py --step 2 --base-dir /mnt/lit_platform --shard-id 0

# Step 3: 构建 FAISS 索引
python scripts/import_vector_db/run_pipeline.py --step 3 --base-dir /mnt/lit_platform
```

### 方法 2: 使用单独脚本

#### Step 1: 导出 Arrow Batches

```bash
python scripts/import_vector_db/export_batches.py \
    --base-dir /mnt/lit_platform \
    --batch-size 10000 \
    --papers-per-shard 2500000
```

#### Step 2: 计算 Embeddings

处理单个 batch 文件：

```bash
python scripts/import_vector_db/embed_worker.py \
    --batch-file /mnt/lit_platform/batches/shard_000/batch_000001.arrow \
    --output-dir /mnt/lit_platform/embeddings \
    --model-path /path/to/gritlm-model
```

处理整个 shard：

```bash
python scripts/import_vector_db/embed_worker.py \
    --shard-id 0 \
    --batches-dir /mnt/lit_platform/batches \
    --output-dir /mnt/lit_platform/embeddings
```

#### Step 3: 构建 FAISS 索引

```bash
python scripts/import_vector_db/shard_writer.py \
    --shard-id 0 \
    --embeddings-dir /mnt/lit_platform/embeddings/shard_000 \
    --faiss-dir /mnt/lit_platform/faiss \
    --paper-id-start 1 \
    --paper-id-end 2500000
```

## 文件系统结构

执行后会生成以下目录结构：

```
/mnt/lit_platform/
├── batches/                    # Step 1 输出
│   ├── shard_000/
│   │   ├── batch_000001.arrow
│   │   ├── batch_000002.arrow
│   │   └── ...
│   ├── shard_001/
│   └── manifest.json
│
├── embeddings/                 # Step 2 输出
│   ├── shard_000/
│   │   ├── batch_000001.emb.npz
│   │   ├── batch_000002.emb.npz
│   │   └── ...
│   └── shard_001/
│
└── faiss/                      # Step 3 输出
    ├── shards/
    │   ├── shard_000.index
    │   ├── shard_000.ids.npy
    │   ├── shard_001.index
    │   └── ...
    ├── state/
    │   ├── shard_000.state.json
    │   └── ...
    └── manifest.json
```

## 测试

运行测试脚本：

```bash
# 运行所有测试
python scripts/import_vector_db/test_pipeline.py --test all

# 运行特定测试
python scripts/import_vector_db/test_pipeline.py --test export
python scripts/import_vector_db/test_pipeline.py --test embed
python scripts/import_vector_db/test_pipeline.py --test writer
```

## 配置说明

### 数据库配置

通过 `config/config.yaml` 配置数据库连接：

```yaml
db:
  host: 10.0.4.7
  name: pubmed_database_test
  user: wangyuanshi
  port: 5432
  password: "123456"
```

### 模型配置

通过命令行参数指定模型：

- `--model-path`: GritLM 模型本地路径（优先使用）
- `--model-name`: GritLM 模型名称（默认: GritLM/GritLM-7B）

或通过环境变量：

```bash
export GRITLM_MODEL_PATH=/path/to/gritlm-model
```

## 性能参数

- **batch_size**: 每个 batch 的文献数量（默认: 10000）
  - 较大的 batch 可以提高 embedding 计算效率
  - 但会增加内存占用

- **papers_per_shard**: 每个 shard 的文献数量（默认: 2500000）
  - 影响最终的 FAISS 索引数量
  - 每个 shard 一个索引文件

- **vector_dim**: 向量维度（默认: 4096，GritLM-7B）
  - 必须与模型输出的维度匹配

## 错误处理与恢复

- **Step 1**: 支持中断恢复，已导出的文件不会重复导出（状态已更新）
- **Step 2**: 支持断点续传，已处理的 batch 文件会跳过（检查输出文件是否存在）
- **Step 3**: 支持 checkpoint 恢复，已处理的 batch 会从 checkpoint 读取

## 并行执行

- **Step 2 (Embedding)**: 可以并行运行多个 worker 处理不同 shard
  ```bash
  # 在不同终端或使用任务调度器并行运行
  python embed_worker.py --shard-id 0 --output-dir /mnt/lit_platform/embeddings &
  python embed_worker.py --shard-id 1 --output-dir /mnt/lit_platform/embeddings &
  ```

- **Step 3 (Writer)**: 不同 shard 的 writer 可以并行运行
  - 但同一个 shard 只能有一个 writer（FAISS 索引不支持并发写入）

## 注意事项

1. **FAISS 索引并发安全**: 同一个 shard 的索引写入必须串行，多写会导致数据损坏
2. **状态字段语义**: `embedding_status = 2` 表示向量已写入 FAISS 索引，可被检索
3. **磁盘空间**: 确保有足够的磁盘空间存储 Arrow、NPZ 和 FAISS 索引文件
4. **GPU 内存**: Embedding 计算需要 GPU 内存，根据 batch_size 调整

## 故障排查

### 问题：导出失败，数据库连接错误

- 检查 `config/config.yaml` 中的数据库配置
- 确认数据库服务运行正常
- 检查网络连接

### 问题：Embedding 计算失败，模型加载错误

- 检查模型路径是否正确
- 确认有足够的 GPU 内存
- 检查 HuggingFace 访问（如果从网络下载模型）

### 问题：FAISS 索引构建失败

- 检查 embedding 文件是否存在且格式正确
- 确认有足够的磁盘空间
- 检查 checkpoint 文件是否损坏（可删除后重新开始）

## 许可证

（根据项目许可证）


