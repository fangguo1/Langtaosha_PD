"""向量数据库操作类

基于腾讯云 VectorDB 的服务端自动 embedding 模式。
负责将文档文本向量化并提供语义检索能力。

架构设计:
- VectorDBClient: HTTP 适配层，封装腾讯云 API 调用
- VectorDB: 业务层，负责 source 与 collection 映射和业务逻辑

参考文档:
- docs/vector_db_building_plan_0415.md
- docs/tencent_vectordb_embedding_manual.md
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from config.config_loader import init_config, get_vector_db_config
from .vector_db_client import (
    VectorDBClient,
    VectorDBError,
    VectorDBClientError,
    VectorDBServerError
)
from .sparse_encoder import BM25SparseEncoder


@dataclass
class SearchResult:
    """搜索结果

    Attributes:
        source_name: 来源名称
        work_id: 作品 ID
        score: 相似度分数
        text_type: 文本类型
        paper_id: 论文 ID
    """
    source_name: str
    work_id: str
    score: float
    text_type: str
    paper_id: Optional[str] = None
    retrieval_debug: Optional[Dict[str, Any]] = None


class VectorDB:
    """向量数据库操作类

    负责管理向量索引和语义检索，支持多 source 隔离。

    使用腾讯云服务端自动 embedding 模式:
    - 创建 collection 时配置 embedding
    - 写入时直接提交原始文本
    - 搜索时使用 embeddingItems

    Attributes:
        config: 向量数据库配置
        client: VectorDBClient 实例
        database: 数据库名称
        collection_prefix: Collection 名称前缀
        allowed_sources: 允许的 source 列表
    """

    # Collection 命名前缀
    DEFAULT_COLLECTION_PREFIX = "lt_"

    # 文档 ID 分隔符
    ID_SEPARATOR = ":"

    def __init__(self, config_path: Optional[Path] = None):
        """初始化向量数据库操作器

        Args:
            config_path: 配置文件路径，如果为 None 则抛出错误

        Raises:
            ValueError: 配置文件未指定
            ValueError: 不支持的 embedding_source
        """
        # 确保配置已初始化
        if config_path is None:
            raise ValueError("未找到配置文件 config.yaml，请指定 config_path")
        init_config(config_path)

        # 加载配置
        self.config = get_vector_db_config()

        # 读取基础配置
        self.url = self.config.get('url')
        self.account = self.config.get('account')
        self.api_key = self.config.get('api_key')
        self.embedding_source = self.config.get('embedding_source')
        self.embedding_model = self.config.get('embedding_model')
        self.database = self.config.get('database', 'langtaosha_test')
        self.collection_prefix = self.config.get(
            'collection_prefix',
            self.DEFAULT_COLLECTION_PREFIX
        )
        self.sparse_collection_prefix = self.config.get(
            'sparse_collection_prefix',
            f"{self.collection_prefix}bm25_"
        )
        self.allowed_sources = self.config.get('allowed_sources', [])
        self.sparse_config = self.config.get('sparse', {}) or {}
        self.hybrid_config = self.config.get('hybrid', {}) or {}
        self.sparse_bm25_language = self.sparse_config.get('bm25_language', 'en')
        self.sparse_max_non_zero = int(self.sparse_config.get('max_non_zero', 1024))
        self.sparse_terminate_after = self.sparse_config.get('terminate_after', 4000)
        self.sparse_cutoff_frequency = self.sparse_config.get('cutoff_frequency', 0.1)

        # 验证配置
        self._validate_config()

        # 创建客户端
        self.client = VectorDBClient(
            url=self.url,
            account=self.account,
            api_key=self.api_key
        )
        self._ensured_collections: set[str] = set()
        self._sparse_encoder: Optional[BM25SparseEncoder] = None

        logging.info(f"✅ VectorDB 初始化成功 (database={self.database})")

    def _validate_config(self) -> None:
        """验证配置

        Raises:
            ValueError: 配置不完整或不支持
        """
        # 检查必需字段
        required_fields = ['url', 'account', 'api_key', 'embedding_source']
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"配置缺少必需字段: vector_db.{field}")

        # 检查 embedding_source 支持
        if self.embedding_source not in ['tecent_made', 'local_made']:
            raise ValueError(
                f"不支持的 embedding_source: {self.embedding_source}。"
                f"当前版本仅支持 'tecent_made'"
            )

        # 如果是 local_made，给出明确错误
        if self.embedding_source == 'local_made':
            raise NotImplementedError(
                "当前版本暂不支持 embedding_source=local_made。"
                "请使用 embedding_source=tecent_made 或等待后续版本更新。"
            )

        # 检查 embedding_model
        if not self.embedding_model:
            raise ValueError("配置缺少必需字段: vector_db.embedding_model")

        # 检查 allowed_sources
        if not self.allowed_sources:
            raise ValueError("配置缺少必需字段: vector_db.allowed_sources")

    def _get_collection_name(self, source_name: str) -> str:
        """获取 source 对应的 collection 名称

        Args:
            source_name: 来源名称

        Returns:
            str: Collection 名称

        Example:
            >>> vector_db._get_collection_name("biorxiv_history")
            'lt_biorxiv_history'
        """
        return f"{self.collection_prefix}{source_name}"

    def _get_sparse_collection_name(self, source_name: str) -> str:
        """获取 source 对应的 BM25 sparse collection 名称。"""
        return f"{self.sparse_collection_prefix}{source_name}"

    def _get_sparse_encoder(self) -> BM25SparseEncoder:
        """Lazy-load BM25 encoder so dense-only flows do not require tcvdb-text."""
        if self._sparse_encoder is None:
            self._sparse_encoder = BM25SparseEncoder(
                language=self.sparse_bm25_language,
                max_non_zero=self.sparse_max_non_zero,
            )
        return self._sparse_encoder

    def _validate_source(self, source_name: str) -> None:
        """验证 source 名称是否允许

        Args:
            source_name: 来源名称

        Raises:
            ValueError: source 不在允许列表中
        """
        if source_name not in self.allowed_sources:
            raise ValueError(
                f"不允许的 source: '{source_name}'。"
                f"允许的 source 列表: {self.allowed_sources}"
            )

    def _generate_doc_id(
        self,
        source_name: str,
        work_id: str,
        text_type: str
    ) -> str:
        """生成文档 ID

        Args:
            source_name: 来源名称
            work_id: 作品 ID
            text_type: 文本类型

        Returns:
            str: 文档 ID

        Example:
            >>> vector_db._generate_doc_id("biorxiv_history", "work_123", "abstract")
            'work_123'
        """
        # 业务约束：向量与 work_id 一对一，doc_id 仅使用 work_id。
        # 参数 source_name/text_type 保留是为了兼容现有调用方签名。
        return work_id

    def _document_exists(
        self,
        collection_name: str,
        doc_id: str,
        retries: int = 3,
        retry_delay: float = 0.5
    ) -> bool:
        """检查文档是否存在

        Args:
            collection_name: Collection 名称
            doc_id: 文档 ID
            retries: 重试次数（默认3次）
            retry_delay: 重试间隔秒数（默认0.5秒）

        Returns:
            bool: 文档存在返回 True，否则返回 False
        """
        import time

        for attempt in range(retries):
            try:
                result = self.client.query_documents(
                    database=self.database,
                    collection=collection_name,
                    ids=[doc_id],
                    output_fields=["id"],
                    limit=1,
                    read_consistency="strongConsistency"  # 使用强一致性确保读到最新数据
                )
                documents = result.get('documents', [])
                exists = len(documents) > 0
                logging.debug(f"文档存在检查: {doc_id} = {exists}")
                # 查询成功时直接返回结果；仅查询异常时才重试
                return exists

            except (VectorDBClientError, VectorDBServerError) as e:
                if attempt < retries - 1:
                    # 查询失败且不是最后一次尝试，记录警告并重试
                    logging.warning(
                        f"查询文档是否存在失败: {e} (尝试 {attempt + 1}/{retries})，"
                        f"{retry_delay}秒后重试"
                    )
                    time.sleep(retry_delay)
                else:
                    # 最后一次尝试失败，记录警告并假设文档不存在
                    logging.warning(f"查询文档是否存在失败: {e}，假设文档不存在")
                    return False

        return False

    def ensure_database(self) -> bool:
        """确保数据库存在

        如果数据库不存在则创建。

        Returns:
            bool: 成功返回 True

        Raises:
            VectorDBError: 创建失败
        """
        try:
            databases = self.client.list_databases()

            if self.database in databases:
                logging.info(f"数据库已存在: {self.database}")
                return True

            # 创建数据库
            self.client.create_database(self.database)
            logging.info(f"✅ 数据库创建成功: {self.database}")
            return True

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 创建数据库失败: {str(e)}")
            raise VectorDBError(f"创建数据库失败: {str(e)}")

    def ensure_collection(self, source_name: str) -> bool:
        """确保 source 对应的 Collection 存在

        如果 Collection 不存在则创建。

        Args:
            source_name: 来源名称

        Returns:
            bool: 成功返回 True

        Raises:
            ValueError: source 不在允许列表中
            VectorDBError: 创建失败
        """
        self._validate_source(source_name)

        try:
            collection_name = self._get_collection_name(source_name)
            if collection_name in self._ensured_collections:
                return True

            # 检查 collection 是否存在（允许失败）
            try:
                collections = self.client.list_collections(self.database)
                if collection_name in collections:
                    logging.info(f"Collection 已存在: {collection_name}")
                    self._ensured_collections.add(collection_name)
                    return True
            except Exception as e:
                logging.warning(f"无法获取 collection 列表（将尝试创建）: {e}")

            # 尝试创建 collection（如果已存在会返回错误，但这是预期的）
            try:
                self.client.create_collection(
                    database=self.database,
                    collection=collection_name,
                    embedding_field="text",
                    embedding_model=self.embedding_model
                )
                logging.info(f"✅ Collection 创建成功: {collection_name}")
            except (VectorDBClientError, VectorDBServerError) as e:
                # 如果是因为 collection 已存在导致的错误，这是正常的
                err = str(e).lower()
                if 'already exist' in err or 'already exists' in err or '已存在' in str(e):
                    logging.info(f"Collection 已存在: {collection_name}")
                else:
                    raise

            self._ensured_collections.add(collection_name)
            return True

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 创建 Collection 失败: {str(e)}")
            raise VectorDBError(f"创建 Collection 失败: {str(e)}")

    def ensure_sparse_collection(self, source_name: str) -> bool:
        """确保 source 对应的 BM25 sparse collection 存在。"""
        self._validate_source(source_name)

        try:
            collection_name = self._get_sparse_collection_name(source_name)
            if collection_name in self._ensured_collections:
                return True

            try:
                collections = self.client.list_collections(self.database)
                if collection_name in collections:
                    logging.info(f"Sparse Collection 已存在: {collection_name}")
                    self._ensured_collections.add(collection_name)
                    return True
            except Exception as e:
                logging.warning(f"无法获取 collection 列表（将尝试创建 sparse collection）: {e}")

            try:
                self.client.create_sparse_collection(
                    database=self.database,
                    collection=collection_name,
                    disk_swap_enabled=self.sparse_config.get("disk_swap_enabled")
                )
                logging.info(f"✅ Sparse Collection 创建成功: {collection_name}")
            except (VectorDBClientError, VectorDBServerError) as e:
                err = str(e).lower()
                if 'already exist' in err or 'already exists' in err or '已存在' in str(e):
                    logging.info(f"Sparse Collection 已存在: {collection_name}")
                else:
                    raise

            self._ensured_collections.add(collection_name)
            return True

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 创建 Sparse Collection 失败: {str(e)}")
            raise VectorDBError(f"创建 Sparse Collection 失败: {str(e)}")

    def get_collection_info(
        self,
        source_name: Optional[str] = None,
        collection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取指定 collection 的详细信息。

        Args:
            source_name: 来源名称（与 collection_name 二选一）
            collection_name: collection 名称（与 source_name 二选一）

        Returns:
            Dict[str, Any]: collection 详细信息。

        Raises:
            ValueError: 参数非法或 source 不在允许列表中
            VectorDBError: 查询失败
        """
        if bool(source_name) == bool(collection_name):
            raise ValueError("必须且只能提供 source_name 或 collection_name 之一")

        if source_name:
            self._validate_source(source_name)
            target_collection = self._get_collection_name(source_name)
        else:
            target_collection = collection_name  # type: ignore[assignment]

        try:
            collections = self.client.list_collections(self.database)
            if target_collection not in collections:
                return {
                    "database": self.database,
                    "collection": target_collection,
                    "exists": False
                }

            info = self.client.describe_collection(
                database=self.database,
                collection=target_collection
            ) or {}
            result = dict(info)
            result.setdefault("database", self.database)
            result.setdefault("collection", target_collection)
            result["exists"] = True
            return result
        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 查询 collection 信息失败: {str(e)}")
            raise VectorDBError(f"查询 collection 信息失败: {str(e)}")

    def get_collection_list(
        self,
        with_info: bool = False,
        source_list: Optional[List[str]] = None
    ) -> List[Any]:
        """获取数据库中的 collection 列表。

        Args:
            with_info: 是否返回详细信息（True 返回 List[Dict]）
            source_list: 可选的来源过滤列表（按 source_name 过滤）

        Returns:
            List[Any]: collection 名称列表或详细信息列表

        Raises:
            ValueError: source_list 中包含非法 source
            VectorDBError: 查询失败
        """
        if source_list is not None:
            for source_name in source_list:
                self._validate_source(source_name)
            allowed_collections = {
                self._get_collection_name(source_name) for source_name in source_list
            }
        else:
            allowed_collections = None

        try:
            if with_info:
                collections = self.client.list_collections_with_info(self.database)
                if allowed_collections is None:
                    return collections
                return [
                    item for item in collections
                    if isinstance(item, dict) and item.get("collection") in allowed_collections
                ]

            collections = self.client.list_collections(self.database)
            if allowed_collections is None:
                return collections
            return [name for name in collections if name in allowed_collections]
        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 查询 collection 列表失败: {str(e)}")
            raise VectorDBError(f"查询 collection 列表失败: {str(e)}")

    def get_vector_db_info(self) -> Dict[str, Any]:
        """获取当前 VectorDB 配置信息与运行状态摘要。"""
        try:
            databases = self.client.list_databases()
            collections = self.client.list_collections(self.database) if self.database in databases else []
            return {
                "url": self.url,
                "database": self.database,
                "database_exists": self.database in databases,
                "collection_prefix": self.collection_prefix,
                "sparse_collection_prefix": self.sparse_collection_prefix,
                "allowed_sources": list(self.allowed_sources),
                "embedding_source": self.embedding_source,
                "embedding_model": self.embedding_model,
                "collections": collections,
                "collection_count": len(collections),
            }
        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 查询 VectorDB 信息失败: {str(e)}")
            raise VectorDBError(f"查询 VectorDB 信息失败: {str(e)}")

    def add_document(
        self,
        source_name: str,
        work_id: str,
        text: str,
        text_type: str = "abstract",
        paper_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        skip_ensure_collection: bool = False
    ) -> Dict[str, Any]:
        """添加文档到向量数据库

        Args:
            source_name: 来源名称
            work_id: 作品 ID
            text: 待索引的文本内容
            text_type: 文本类型 (默认: "abstract")
            paper_id: 论文 ID (可选)
            metadata: 额外的元数据 (可选，暂不使用)
            skip_ensure_collection: 是否跳过 collection 检查 (默认: False)

        Returns:
            Dict[str, Any]: 操作结果，包含:
                - success (bool): 是否成功
                - action (str): 操作类型，'inserted' 或 'updated'
                - doc_id (str): 文档 ID
                - affected_count (int): 影响的文档数量

        Raises:
            ValueError: source 不在允许列表中
            VectorDBError: 添加失败

        Example:
            >>> result = vector_db.add_document(
            ...     source_name="biorxiv_history",
            ...     work_id="work_123",
            ...     text="这是论文的标题和摘要",
            ...     text_type="abstract",
            ...     paper_id="12345"
            ... )
            >>> print(f"操作类型: {result['action']}")
        """
        # 确保 collection 存在（可选）
        if not skip_ensure_collection:
            self.ensure_collection(source_name)

        try:
            collection_name = self._get_collection_name(source_name)
            doc_id = self._generate_doc_id(source_name, work_id, text_type)

            # 1. 先检查文档是否存在（现在 API 已修复，可以正常使用）
            doc_exists = self._document_exists(collection_name, doc_id)
            if doc_exists:
                logging.info(f"文档已存在，准备更新: {doc_id}")
            else:
                logging.info(f"文档不存在，准备插入: {doc_id}")

            # 2. 构造文档
            document = {
                "id": doc_id,
                "text": text,
                "work_id": work_id,
                "source_name": source_name,
                "text_type": text_type
            }

            if paper_id:
                document["paper_id"] = paper_id

            # 3. 执行 upsert
            result = self.client.upsert_documents(
                database=self.database,
                collection=collection_name,
                documents=[document],
                build_index=True
            )

            # 4. 记录操作类型
            action = 'updated' if doc_exists else 'inserted'
            affected_count = result.get('affectedCount', result.get('insertCount', 0))

            logging.info(
                f"✅ 文档{action}成功: source={source_name}, "
                f"work_id={work_id}, text_type={text_type}, "
                f"action={action}, affected_count={affected_count}"
            )

            return {
                'success': True,
                'action': action,
                'doc_id': doc_id,
                'affected_count': affected_count
            }

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 添加文档失败: {str(e)}")
            raise VectorDBError(f"添加文档失败: {str(e)}")

    def add_sparse_document(
        self,
        source_name: str,
        work_id: str,
        text: str,
        text_type: str = "abstract",
        paper_id: Optional[str] = None,
        sparse_vector: Optional[List[List[float]]] = None,
        skip_ensure_collection: bool = False
    ) -> Dict[str, Any]:
        """添加单篇文档到 BM25 sparse collection。"""
        results = self.add_sparse_documents(
            source_name=source_name,
            documents=[
                {
                    "work_id": work_id,
                    "text": text,
                    "text_type": text_type,
                    "paper_id": paper_id,
                    "sparse_vector": sparse_vector,
                }
            ],
            skip_ensure_collection=skip_ensure_collection,
        )
        return {
            "success": results.get("success", False),
            "action": "upserted",
            "doc_id": work_id,
            "affected_count": results.get("affected_count", 0),
        }

    def add_sparse_documents(
        self,
        source_name: str,
        documents: List[Dict[str, Any]],
        skip_ensure_collection: bool = False
    ) -> Dict[str, Any]:
        """批量添加文档到 BM25 sparse collection。

        Each document must include `work_id` and either `text` or a precomputed
        `sparse_vector`. The sparse collection stores only retrieval fields, not
        the full index text.
        """
        self._validate_source(source_name)
        if not skip_ensure_collection:
            self.ensure_sparse_collection(source_name)

        if not documents:
            return {
                "success": True,
                "action": "upserted",
                "affected_count": 0,
                "document_count": 0,
            }

        try:
            collection_name = self._get_sparse_collection_name(source_name)
            texts_to_encode: List[str] = []
            text_positions: List[int] = []
            sparse_vectors: List[Optional[List[List[float]]]] = []

            for index, document in enumerate(documents):
                provided_vector = document.get("sparse_vector")
                if provided_vector is not None:
                    sparse_vectors.append(provided_vector)
                    continue

                sparse_vectors.append(None)
                texts_to_encode.append(document.get("text", ""))
                text_positions.append(index)

            if texts_to_encode:
                encoded_vectors = self._get_sparse_encoder().encode_documents(texts_to_encode)
                for position, sparse_vector in zip(text_positions, encoded_vectors):
                    sparse_vectors[position] = sparse_vector

            upsert_documents = []
            for document, sparse_vector in zip(documents, sparse_vectors):
                work_id = document.get("work_id")
                if not work_id:
                    raise ValueError("BM25 sparse document 缺少 work_id")

                upsert_document = {
                    "id": str(work_id),
                    "sparse_vector": sparse_vector or [],
                    "work_id": str(work_id),
                    "source_name": source_name,
                    "text_type": document.get("text_type") or "abstract",
                }
                paper_id = document.get("paper_id")
                if paper_id is not None:
                    upsert_document["paper_id"] = str(paper_id)
                upsert_documents.append(upsert_document)

            result = self.client.upsert_documents(
                database=self.database,
                collection=collection_name,
                documents=upsert_documents,
                build_index=True
            )
            affected_count = result.get('affectedCount', result.get('insertCount', 0))
            logging.info(
                f"✅ Sparse 文档 upsert 成功: source={source_name}, "
                f"count={len(upsert_documents)}, affected_count={affected_count}"
            )
            return {
                "success": True,
                "action": "upserted",
                "affected_count": affected_count,
                "document_count": len(upsert_documents),
            }

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 添加 Sparse 文档失败: {str(e)}")
            raise VectorDBError(f"添加 Sparse 文档失败: {str(e)}")

    def delete_document(
        self,
        source_name: str,
        work_id: str,
        text_type: str = "abstract"
    ) -> Dict[str, Any]:
        """删除文档

        Args:
            source_name: 来源名称
            work_id: 作品 ID
            text_type: 文本类型 (默认: "abstract")

        Returns:
            Dict[str, Any]: 操作结果，包含:
                - success (bool): 是否成功
                - deleted (bool): 是否真的删除了文档（False 表示文档不存在）
                - doc_id (str): 文档 ID
                - delete_count (int): 删除的文档数量

        Raises:
            ValueError: source 不在允许列表中
            VectorDBError: 删除失败

        Example:
            >>> result = vector_db.delete_document(
            ...     source_name="biorxiv_history",
            ...     work_id="work_123",
            ...     text_type="abstract"
            ... )
            >>> if result['deleted']:
            ...     print(f"成功删除文档: {result['doc_id']}")
            ... else:
            ...     print(f"文档不存在: {result['doc_id']}")
        """
        self._validate_source(source_name)

        try:
            collection_name = self._get_collection_name(source_name)
            doc_id = self._generate_doc_id(source_name, work_id, text_type)

            # 1. 先检查文档是否存在
            doc_exists = self._document_exists(collection_name, doc_id)

            if not doc_exists:
                logging.info(
                    f"文档不存在，无需删除: source={source_name}, "
                    f"work_id={work_id}, text_type={text_type}"
                )
                return {
                    'success': True,
                    'deleted': False,
                    'doc_id': doc_id,
                    'delete_count': 0
                }

            # 2. 删除文档
            result = self.client.delete_documents(
                database=self.database,
                collection=collection_name,
                ids=[doc_id]
            )

            delete_count = result.get('affectedCount', 0)

            logging.info(
                f"✅ 文档删除成功: source={source_name}, "
                f"work_id={work_id}, text_type={text_type}, "
                f"delete_count={delete_count}"
            )

            return {
                'success': True,
                'deleted': True,
                'doc_id': doc_id,
                'delete_count': delete_count
            }

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 删除文档失败: {str(e)}")
            raise VectorDBError(f"删除文档失败: {str(e)}")

    def dense_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10
    ) -> List[SearchResult]:
        """稠密向量搜索 (Dense Search)

        基于腾讯云 VectorDB 的稠密向量检索，使用 embedding 模型进行语义搜索。

        Args:
            query: 查询文本
            source_list: 来源列表，如果为 None 则搜索所有允许的 source
            top_k: 返回结果数量

        Returns:
            List[SearchResult]: 搜索结果列表

        Raises:
            ValueError: source 不在允许列表中
            VectorDBError: 搜索失败

        Example:
            >>> results = vector_db.dense_search(
            ...     query="机器学习算法",
            ...     source_list=["biorxiv_history", "langtaosha"],
            ...     top_k=5
            ... )
            >>> for result in results:
            ...     print(f"{result.work_id}: {result.score}")
        """
        # 如果未指定 source_list，使用所有允许的 source
        if source_list is None:
            source_list = self.allowed_sources

        # 验证 source_list
        for source_name in source_list:
            self._validate_source(source_name)

        try:
            all_results = []

            # 对每个 source 进行搜索
            for source_name in source_list:
                collection_name = self._get_collection_name(source_name)

                # 检查 collection 是否存在
                collections = self.client.list_collections(self.database)
                if collection_name not in collections:
                    logging.warning(f"Collection 不存在，跳过: {collection_name}")
                    continue

                # 搜索
                result = self.client.search_documents(
                    database=self.database,
                    collection=collection_name,
                    query_text=query,
                    limit=top_k,
                    output_fields=["work_id", "paper_id", "source_name", "text_type"]
                )

                # 解析结果（使用预提取的文档列表）
                documents = result.get('_extracted_documents', [])
                for doc in documents:
                    search_result = SearchResult(
                        source_name=doc.get('source_name', source_name),
                        work_id=doc.get('work_id', ''),
                        score=doc.get('score', 0.0),
                        text_type=doc.get('text_type', ''),
                        paper_id=doc.get('paper_id')
                    )
                    all_results.append(search_result)

            # 按分数排序
            all_results.sort(key=lambda x: x.score, reverse=True)

            # 返回 top_k 结果
            return all_results[:top_k]

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ 稠密向量搜索失败: {str(e)}")
            raise VectorDBError(f"稠密向量搜索失败: {str(e)}")

    def sparse_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10
    ) -> List[SearchResult]:
        """稀疏向量搜索 (Sparse Search)

        基于腾讯云 VectorDB 的 BM25 稀疏检索，使用词频统计进行关键词搜索。

        注意：此方法需要腾讯云 VectorDB 支持 BM25 功能，当前版本暂未实现。

        Args:
            query: 查询文本
            source_list: 来源列表，如果为 None 则搜索所有允许的 source
            top_k: 返回结果数量

        Returns:
            List[SearchResult]: 搜索结果列表

        Raises:
            NotImplementedError: 当前版本暂不支持稀疏搜索
            ValueError: source 不在允许列表中
            VectorDBError: 搜索失败

        Example:
            >>> # 未来使用示例
            >>> results = vector_db.sparse_search(
            ...     query="machine learning algorithms",
            ...     source_list=["biorxiv_history"],
            ...     top_k=10
            ... )
        """
        if source_list is None:
            source_list = self.allowed_sources

        for source_name in source_list:
            self._validate_source(source_name)

        query_sparse_vector = self._get_sparse_encoder().encode_query(query)
        if not query_sparse_vector:
            return []

        try:
            all_results = []
            collections = self.client.list_collections(self.database)

            for source_name in source_list:
                collection_name = self._get_sparse_collection_name(source_name)
                if collection_name not in collections:
                    logging.warning(f"Sparse Collection 不存在，跳过: {collection_name}")
                    continue

                result = self.client.fulltext_search_documents(
                    database=self.database,
                    collection=collection_name,
                    sparse_vector=query_sparse_vector,
                    limit=top_k,
                    output_fields=["work_id", "paper_id", "source_name", "text_type"],
                    terminate_after=self.sparse_terminate_after,
                    cutoff_frequency=self.sparse_cutoff_frequency,
                )

                documents = result.get('_extracted_documents', [])
                for doc in documents:
                    all_results.append(
                        SearchResult(
                            source_name=doc.get('source_name', source_name),
                            work_id=doc.get('work_id', doc.get('id', '')),
                            score=doc.get('score', 0.0),
                            text_type=doc.get('text_type', ''),
                            paper_id=doc.get('paper_id')
                        )
                    )

            all_results.sort(key=lambda x: x.score, reverse=True)
            return all_results[:top_k]

        except (VectorDBClientError, VectorDBServerError) as e:
            logging.error(f"❌ BM25 稀疏搜索失败: {str(e)}")
            raise VectorDBError(f"BM25 稀疏搜索失败: {str(e)}")

    def hybrid_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10
    ) -> List[SearchResult]:
        """Langtaosha-side hybrid search using dense + BM25 RRF merge."""
        candidate_multiplier = int(self.hybrid_config.get("candidate_multiplier", 5))
        min_candidate_k = int(self.hybrid_config.get("min_candidate_k", 50))
        candidate_k = max(top_k * candidate_multiplier, min_candidate_k)

        dense_results = self.dense_search(
            query=query,
            source_list=source_list,
            top_k=candidate_k,
        )
        sparse_results = self.sparse_search(
            query=query,
            source_list=source_list,
            top_k=candidate_k,
        )

        return self._rrf_merge_results(
            dense_results=dense_results,
            sparse_results=sparse_results,
            top_k=top_k,
            rrf_k=float(self.hybrid_config.get("rrf_k", 60)),
            dense_weight=float(self.hybrid_config.get("dense_weight", 1.0)),
            sparse_weight=float(self.hybrid_config.get("sparse_weight", 1.0)),
        )

    @staticmethod
    def _rrf_merge_results(
        dense_results: List[SearchResult],
        sparse_results: List[SearchResult],
        top_k: int,
        rrf_k: float = 60.0,
        dense_weight: float = 1.0,
        sparse_weight: float = 1.0,
    ) -> List[SearchResult]:
        """Merge dense and sparse ranked lists with Reciprocal Rank Fusion."""
        merged: Dict[str, Dict[str, Any]] = {}

        def add_results(results: List[SearchResult], retriever: str, weight: float) -> None:
            for rank, result in enumerate(results, start=1):
                key = result.work_id or f"{retriever}:{rank}"
                entry = merged.setdefault(
                    key,
                    {
                        "result": result,
                        "rrf_score": 0.0,
                        "retrieval_debug": {"matched_retrievers": []},
                    },
                )

                entry["rrf_score"] += weight / (rrf_k + rank)
                debug = entry["retrieval_debug"]
                debug[f"{retriever}_rank"] = rank
                debug[f"{retriever}_score"] = result.score
                if retriever not in debug["matched_retrievers"]:
                    debug["matched_retrievers"].append(retriever)

                existing = entry["result"]
                if not existing.paper_id and result.paper_id:
                    existing.paper_id = result.paper_id
                if not existing.text_type and result.text_type:
                    existing.text_type = result.text_type
                if not existing.source_name and result.source_name:
                    existing.source_name = result.source_name

        add_results(dense_results, "dense", dense_weight)
        add_results(sparse_results, "sparse", sparse_weight)

        fused_results = []
        for entry in merged.values():
            result = entry["result"]
            fused_results.append(
                SearchResult(
                    source_name=result.source_name,
                    work_id=result.work_id,
                    score=entry["rrf_score"],
                    text_type=result.text_type,
                    paper_id=result.paper_id,
                    retrieval_debug=entry["retrieval_debug"],
                )
            )

        fused_results.sort(key=lambda x: x.score, reverse=True)
        return fused_results[:top_k]

    def search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        search_type: str = "dense"
    ) -> List[SearchResult]:
        """统一搜索入口

        支持稠密向量搜索（语义）和稀疏向量搜索（BM25）。

        Args:
            query: 查询文本
            source_list: 来源列表，如果为 None 则搜索所有允许的 source
            top_k: 返回结果数量
            search_type: 搜索类型
                - "dense": 稠密向量搜索（默认，基于 embedding）
                - "sparse": 稀疏向量搜索（BM25，暂未实现）
                - "hybrid": 混合搜索（暂未实现）

        Returns:
            List[SearchResult]: 搜索结果列表

        Raises:
            ValueError: search_type 不支持或 source 不在允许列表中
            VectorDBError: 搜索失败

        Example:
            >>> # 稠密向量搜索（语义搜索）
            >>> results = vector_db.search(
            ...     query="机器学习算法",
            ...     search_type="dense",
            ...     top_k=5
            ... )

            >>> # 稀疏向量搜索（BM25，暂未实现）
            >>> # results = vector_db.search(
            ... #     query="machine learning",
            ... #     search_type="sparse",
            ... #     top_k=10
            ... # )
        """
        if search_type == "dense":
            return self.dense_search(query, source_list, top_k)
        elif search_type == "sparse":
            return self.sparse_search(query, source_list, top_k)
        elif search_type == "hybrid":
            return self.hybrid_search(query, source_list, top_k)
        else:
            raise ValueError(
                f"不支持的 search_type: '{search_type}'。"
                f"支持的类型: 'dense', 'sparse', 'hybrid'"
            )
