"""大规模文献向量化模块

实现从 PostgreSQL 到 FAISS 向量索引的完整流水线：
- Step 1: PostgreSQL → Arrow Batches（导出器）
- Step 2: Arrow Batches → Embedding Results（并行计算）
- Step 3: Embedding Results → FAISS Shard Index（索引构建）
"""

__version__ = "1.0.0"

