"""论文索引器 - 新架构编排层

基于 default_sources 配置体系的新 indexer，替代旧的单 source 设计。

核心职责:
1. 读取和解析 source 配置
2. 调用 MetadataTransformer
3. 调用 MetadataDB
4. 构造向量化文本
5. 调用 VectorDB
6. 统一返回结果

使用示例:
    from docset_hub.indexing import PaperIndexer
    from pathlib import Path

    # 初始化
    indexer = PaperIndexer(config_path=Path("config.yaml"))

    # 索引字典
    result = indexer.index_dict(
        raw_payload={"title": "...", "abstract": "..."},
        source_name="langtaosha",
        mode="upsert"
    )

    # 索引文件
    result = indexer.index_file(
        input_path="/path/to/paper.json",
        source_name="langtaosha"
    )

    # 搜索
    results = indexer.search(
        query="机器学习算法",
        top_k=10
    )

    # 删除
    result = indexer.delete(
        work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
        source_name="langtaosha"
    )
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

from config import init_config, get_default_sources
from ..metadata.transformer import MetadataTransformer, TransformResult
from ..storage.metadata_db import MetadataDB
from ..storage.vector_db import VectorDB, SearchResult
from .keyword_enrichment import KeywordEnrichmentService
from .query_understanding import QueryUnderstandingService


class PaperIndexer:
    """论文索引器 - 新架构编排层

    负责编排 MetadataTransformer、MetadataDB、VectorDB 完成论文索引和检索。
    基于 default_sources 配置体系，支持多 source 工作。

    Attributes:
        config_path: 配置文件路径
        enable_vectorization: 是否启用向量化（默认 True）
        default_sources: 默认 source 列表
        transformer: 元数据转换器
        metadata_db: 元数据库
        vector_db: 向量数据库（可选）
    """

    def __init__(
        self,
        config_path: Path,
        enable_vectorization: bool = True,
        enable_keyword_enrichment: bool = True
    ):
        """初始化论文索引器

        Args:
            config_path: 配置文件路径
            enable_vectorization: 是否启用向量化（默认 True）

        Raises:
            ValueError: 配置文件不存在或配置无效
        """
        # 确保配置已初始化
        if not config_path.exists():
            raise ValueError(f"配置文件不存在: {config_path}")
        init_config(config_path)

        self.config_path = config_path
        self.enable_vectorization = enable_vectorization
        self.enable_keyword_enrichment = enable_keyword_enrichment

        # 读取 default_sources
        self.default_sources = get_default_sources()

        # 初始化各个组件
        self.transformer = MetadataTransformer()
        self.metadata_db = MetadataDB(config_path=config_path)
        self.vector_db = VectorDB(config_path=config_path) if enable_vectorization else None
        self.keyword_enrichment = (
            KeywordEnrichmentService(config_path=config_path)
            if enable_keyword_enrichment
            else None
        )
        self.query_understanding = QueryUnderstandingService(self.metadata_db)

        logging.info(
            f"✅ PaperIndexer 初始化完成: "
            f"default_sources={self.default_sources}, "
            f"enable_vectorization={enable_vectorization}, "
            f"enable_keyword_enrichment={enable_keyword_enrichment}"
        )

    # =========================================================================
    # 核心 public 接口
    # =========================================================================

    def index_dict(
        self,
        raw_payload: Dict[str, Any],
        source_name: Optional[str] = None,
        mode: str = "insert"
    ) -> Dict[str, Any]:
        """索引字典数据

        Args:
            raw_payload: 原始元数据字典
            source_name: 来源名称（如果不提供且只有一个默认 source 则自动使用）
            mode: 索引模式（当前仅支持 insert，其他值会被降级为 insert）

        Returns:
            Dict[str, Any]: 操作结果，包含:
                - success (bool): 是否成功
                - source_name (str): 来源名称
                - work_id (str): 作品 ID
                - paper_id (int): 论文 ID
                - mode (str): 操作模式
                - metadata (Dict): metadata 操作结果
                - vectorization (Dict): 向量化操作结果

        Raises:
            ValueError: 参数错误或 source 解析失败
        """
        try:
            # 0. 规范化模式（当前强制 insert-only）
            effective_mode = self._normalize_insert_mode(mode)

            # 1. 解析 source_name
            resolved_source_name = self._resolve_source_name(source_name)

            # 2. 转换数据
            transform_result = self.transformer.transform_dict(
                raw_payload=raw_payload,
                source_name=resolved_source_name
            )

            if not transform_result.success:
                return {
                    "success": False,
                    "source_name": resolved_source_name,
                    "error": f"转换失败: {transform_result.error}",
                    "mode": effective_mode
                }

            # 3. 持久化 metadata
            db_result = self._insert_metadata(
                db_payload=transform_result.db_payload,
                upsert_key=transform_result.upsert_key
            )

            if not db_result["success"]:
                return {
                    "success": False,
                    "source_name": resolved_source_name,
                    "error": f"Metadata 持久化失败: {db_result.get('error')}",
                    "mode": effective_mode
                }

            # 4. 向量化（仅 canonical_source_id 变化时触发）
            vector_result = self._handle_insert_vectorization(
                resolved_source_name=resolved_source_name,
                db_payload=transform_result.db_payload,
                db_result=db_result
            )

            keyword_enrichment_result = self._handle_keyword_enrichment(
                db_payload=transform_result.db_payload,
                db_result=db_result
            )

            # 5. 返回统一结果
            return {
                "success": True,
                "source_name": resolved_source_name,
                "work_id": db_result.get("work_id") or transform_result.work_id,
                "paper_id": db_result.get("paper_id"),
                "mode": effective_mode,
                "metadata": db_result,
                "vectorization": vector_result,
                "keyword_enrichment": keyword_enrichment_result
            }

        except Exception as e:
            logging.error(f"index_dict 失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "source_name": source_name,
                "error": str(e),
                "mode": self._normalize_insert_mode(mode)
            }

    def index_file(
        self,
        input_path: Union[str, Path],
        source_name: Optional[str] = None,
        mode: str = "insert"
    ) -> Dict[str, Any]:
        """索引文件

        Args:
            input_path: 输入文件路径
            source_name: 来源名称（如果不提供且只有一个默认 source 则自动使用）
            mode: 索引模式（当前仅支持 insert，其他值会被降级为 insert）

        Returns:
            Dict[str, Any]: 操作结果（格式同 index_dict）
        """
        try:
            # 0. 规范化模式（当前强制 insert-only）
            effective_mode = self._normalize_insert_mode(mode)

            # 1. 解析 source_name
            resolved_source_name = self._resolve_source_name(source_name)

            # 2. 转换文件
            transform_result = self.transformer.transform_file(
                input_path=input_path,
                source_name=resolved_source_name
            )

            if not transform_result.success:
                return {
                    "success": False,
                    "source_name": resolved_source_name,
                    "error": f"转换失败: {transform_result.error}",
                    "mode": effective_mode
                }

            # 3. 持久化 metadata
            db_result = self._insert_metadata(
                db_payload=transform_result.db_payload,
                upsert_key=transform_result.upsert_key
            )

            if not db_result["success"]:
                return {
                    "success": False,
                    "source_name": resolved_source_name,
                    "error": f"Metadata 持久化失败: {db_result.get('error')}",
                    "mode": effective_mode
                }

            # 4. 向量化（仅 canonical_source_id 变化时触发）
            vector_result = self._handle_insert_vectorization(
                resolved_source_name=resolved_source_name,
                db_payload=transform_result.db_payload,
                db_result=db_result
            )

            keyword_enrichment_result = self._handle_keyword_enrichment(
                db_payload=transform_result.db_payload,
                db_result=db_result
            )

            # 5. 返回统一结果
            return {
                "success": True,
                "source_name": resolved_source_name,
                "work_id": db_result.get("work_id") or transform_result.work_id,
                "paper_id": db_result.get("paper_id"),
                "mode": effective_mode,
                "metadata": db_result,
                "vectorization": vector_result,
                "keyword_enrichment": keyword_enrichment_result
            }

        except Exception as e:
            logging.error(f"index_file 失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "source_name": source_name,
                "error": str(e),
                "mode": self._normalize_insert_mode(mode)
            }

    def search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True
    ) -> List[Dict[str, Any]]:
        """搜索论文

        Args:
            query: 查询文本
            source_list: 来源列表（如果不提供则使用 default_sources）
            top_k: 返回结果数量
            hydrate: 是否补全完整 metadata（默认 True）

        Returns:
            List[Dict[str, Any]]: 搜索结果列表，每个结果包含:
                - work_id (str): 作品 ID
                - paper_id (Optional[int]): 论文 ID
                - source_name (str): 来源名称
                - similarity (float): 相似度分数
                - text_type (str): 文本类型
                - metadata (Optional[Dict]): 完整 metadata（如果 hydrate=True）

        Raises:
            ValueError: vector_db 未启用或参数错误
        """
        if not self.vector_db:
            raise ValueError("向量数据库未启用，无法执行搜索")

        try:
            # 1. 解析 source_list
            resolved_source_list = self._resolve_source_list(source_list)

            # 2. 执行向量搜索
            search_results = self.vector_db.search(
                query=query,
                source_list=resolved_source_list,
                top_k=top_k,
                search_type="dense"
            )

            # 3. 可选：补全 metadata
            if hydrate:
                return self._hydrate_search_results(search_results)
            else:
                # 返回轻量级结果
                return [
                    {
                        "work_id": result.work_id,
                        "paper_id": result.paper_id,
                        "source_name": result.source_name,
                        "similarity": result.score,
                        "text_type": result.text_type
                    }
                    for result in search_results
                ]

        except Exception as e:
            logging.error(f"search 失败: {str(e)}", exc_info=True)
            raise e

    def smart_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True,
    ) -> Dict[str, Any]:
        """Search with query understanding and route selection.

        Author-name queries are routed to MetadataDB.search_by_author().
        Semantic queries keep using vector search, optionally with a high
        confidence corrected query from paper_keywords candidates.
        """
        understanding = self.query_understanding.analyze(query)
        understanding_payload = understanding.to_dict()

        if understanding.route == "none":
            return {
                "success": False,
                "query": query,
                "search_query": None,
                "query_understanding": understanding_payload,
                "results": [],
            }

        resolved_source_list = self._resolve_source_list(source_list)
        if understanding.route == "metadata_author":
            results = self.metadata_db.search_by_author(
                author_name=understanding.matched_author or understanding.normalized_query,
                limit=top_k,
                source_list=resolved_source_list,
                fuzzy=True,
            )
            return {
                "success": True,
                "query": query,
                "search_query": understanding.matched_author or understanding.normalized_query,
                "query_understanding": understanding_payload,
                "results": results,
            }

        search_query = understanding.corrected_query or understanding.normalized_query
        results = self.search(
            query=search_query,
            source_list=resolved_source_list,
            top_k=top_k,
            hydrate=hydrate,
        )
        return {
            "success": True,
            "query": query,
            "search_query": search_query,
            "query_understanding": understanding_payload,
            "results": results,
        }

    def delete(
        self,
        work_id: str,
        source_name: Optional[str] = None,
        text_type: str = "abstract"
    ) -> Dict[str, Any]:
        """删除论文

        Args:
            work_id: 作品 ID
            source_name: 来源名称（如果不提供且只有一个默认 source 则自动使用）
            text_type: 文本类型（默认 "abstract"）

        Returns:
            Dict[str, Any]: 操作结果，包含:
                - success (bool): 是否成功
                - source_name (str): 来源名称
                - work_id (str): 作品 ID
                - metadata_deleted (bool): metadata 是否删除成功
                - vector_deleted (bool): 向量是否删除成功

        Raises:
            ValueError: source 解析失败
        """
        try:
            # 1. 解析 source_name
            resolved_source_name = self._resolve_source_name(source_name)

            # 2. 删除 metadata
            metadata_deleted = self.metadata_db.delete_paper_by_work_id(work_id)

            # 3. 删除向量（如果启用）
            vector_deleted = False
            if self.enable_vectorization and self.vector_db:
                vector_result = self.vector_db.delete_document(
                    source_name=resolved_source_name,
                    work_id=work_id,
                    text_type=text_type
                )
                vector_deleted = vector_result.get('deleted', False)

            return {
                "success": True,
                "source_name": resolved_source_name,
                "work_id": work_id,
                "metadata_deleted": metadata_deleted,
                "vector_deleted": vector_deleted
            }

        except Exception as e:
            logging.error(f"delete 失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "source_name": source_name,
                "work_id": work_id,
                "error": str(e)
            }

    def read(
        self,
        work_id: Optional[str] = None,
        paper_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """读取论文

        Args:
            work_id: 作品 ID
            paper_id: 论文 ID

        Returns:
            Optional[Dict[str, Any]]: 论文完整信息，如果不存在则返回 None

        Raises:
            ValueError: 必须提供 work_id 或 paper_id 之一
        """
        if work_id is None and paper_id is None:
            raise ValueError("必须提供 work_id 或 paper_id 之一")

        try:
            if work_id:
                return self.metadata_db.read_paper_by_work_id(work_id)
            else:
                return self.metadata_db.read_paper(paper_id)

        except Exception as e:
            logging.error(f"read 失败: {str(e)}", exc_info=True)
            raise e

    # =========================================================================
    # 私有辅助方法
    # =========================================================================

    def _resolve_source_name(self, source_name: Optional[str]) -> str:
        """解析单个 source 名称

        规则:
            1. 如果传入 source_name，直接使用
            2. 如果未传且 default_sources 长度为 1，自动使用唯一 source
            3. 如果未传且 default_sources 包含多个 source，报错

        Args:
            source_name: 来源名称

        Returns:
            str: 解析后的 source 名称

        Raises:
            ValueError: source 解析失败
        """
        if source_name:
            # 显式传入，直接使用
            if source_name not in self.default_sources:
                raise ValueError(
                    f"source_name '{source_name}' 不在 default_sources 中。"
                    f"合法的 sources: {self.default_sources}"
                )
            return source_name

        # 未传入 source_name
        if len(self.default_sources) == 1:
            # 只有一个默认 source，自动使用
            return self.default_sources[0]
        else:
            # 多个默认 source，要求显式指定
            raise ValueError(
                f"default_sources 包含多个 source ({self.default_sources})，"
                f"请显式指定 source_name"
            )

    def _resolve_source_list(self, source_list: Optional[List[str]]) -> List[str]:
        """解析 source 列表

        规则:
            1. 如果传入 source_list，直接使用（需验证合法性）
            2. 如果未传，使用 default_sources

        Args:
            source_list: 来源列表

        Returns:
            List[str]: 解析后的 source 列表

        Raises:
            ValueError: source 解析失败
        """
        if source_list:
            # 验证所有 source 都在 default_sources 中
            for source in source_list:
                if source not in self.default_sources:
                    raise ValueError(
                        f"source '{source}' 不在 default_sources 中。"
                        f"合法的 sources: {self.default_sources}"
                    )
            return source_list

        # 未传入，使用默认列表
        return self.default_sources.copy()

    def _normalize_insert_mode(self, mode: str) -> str:
        """规范化索引模式：当前仅支持 insert。"""
        if mode != "insert":
            logging.warning(
                "index_dict/index_file 当前仅支持 insert，"
                f"已将 mode={mode} 降级为 insert"
            )
        return "insert"

    def _insert_metadata(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any]
    ) -> Dict[str, Any]:
        """仅通过 insert 路径持久化 metadata。"""
        try:
            write_result = self.metadata_db.insert_paper(
                db_payload=db_payload,
                upsert_key=upsert_key
            )
            canonical = write_result.get("canonical", {}) or {}
            canonical_source_id = canonical.get("canonical_source_id")
            canonical_source_name = None
            db_work_id = write_result.get("work_id")
            paper_id = write_result.get("paper_id")
            if paper_id is not None and not db_work_id:
                paper_info = self.metadata_db.read_paper(paper_id)
                if paper_info:
                    db_work_id = paper_info.get("work_id")
            if canonical_source_id is not None:
                canonical_source_name = self.metadata_db.get_source_name_by_paper_source_id(
                    canonical_source_id
                )

            return {
                "success": True,
                "paper_id": paper_id,
                "work_id": db_work_id,
                "action": (write_result.get("apply", {}) or {}).get("action", "insert"),
                "status_code": write_result.get("status_code"),
                "canonical_changed": bool(canonical.get("changed", False)),
                "canonical_source_id": canonical_source_id,
                "canonical_source_name": canonical_source_name,
                "write_result": write_result
            }
        except Exception as e:
            logging.error(f"_insert_metadata 失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "action": "failed"
            }

    def _handle_insert_vectorization(
        self,
        resolved_source_name: str,
        db_payload: Dict[str, Any],
        db_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """insert-only 向量化编排：按状态码 + canonical 条件触发。"""
        if not self.enable_vectorization or not self.vector_db:
            return {
                "enabled": self.enable_vectorization,
                "success": False,
                "skipped": True,
                "message": "向量化未启用"
            }

        status_code = db_result.get("status_code")
        canonical_changed = bool(db_result.get("canonical_changed", False))
        canonical_source_name = db_result.get("canonical_source_name")
        is_canonical_source = canonical_source_name == resolved_source_name

        should_vectorize = False
        if status_code == "INSERT_NEW_PAPER":
            should_vectorize = True
        elif status_code == "INSERT_APPEND_SOURCE":
            # 仅在 append 后 canonical 切换到该来源时触发
            should_vectorize = canonical_changed
        elif status_code == "INSERT_UPDATE_SAME_SOURCE":
            # 仅当当前 canonical 指向该来源时触发
            should_vectorize = is_canonical_source

        if not should_vectorize:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": (
                    "跳过向量化："
                    f"status_code={status_code}, "
                    f"canonical_changed={canonical_changed}, "
                    f"is_canonical_source={is_canonical_source}"
                )
            }

        # 满足触发条件后，写 pending 并执行向量化
        paper_id = db_result.get("paper_id")
        work_id = db_result.get("work_id")
        index_text_info = self._build_index_text(db_payload)
        text_type = index_text_info.get("text_type", "abstract") or "abstract"
        canonical_source_id = db_result.get("canonical_source_id")
        canonical_source_name = db_result.get("canonical_source_name") or resolved_source_name

        if paper_id is None:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": "跳过向量化：paper_id 为空"
            }

        if not work_id:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": "跳过向量化：work_id 为空"
            }

        if not index_text_info.get("should_vectorize", False):
            return {
                "enabled": True,
                "success": True,
                "skipped": True,
                "message": "跳过向量化：title 和 abstract 均为空"
            }

        self.metadata_db.upsert_embedding_status_pending(
            paper_id=paper_id,
            work_id=work_id,
            canonical_source_id=canonical_source_id,
            source_name=canonical_source_name,
            text_type=text_type
        )

        vector_result = self._vectorize_document(
            source_name=canonical_source_name,
            work_id=work_id,
            paper_id=paper_id,
            db_payload=db_payload
        )

        if vector_result.get("success"):
            self.metadata_db.mark_embedding_succeeded(paper_id)
        else:
            self.metadata_db.mark_embedding_failed(
                paper_id=paper_id,
                error_message=vector_result.get("error", "unknown vectorization error")
            )

        return vector_result

    def _handle_keyword_enrichment(
        self,
        db_payload: Dict[str, Any],
        db_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run optional keyword enrichment after metadata persistence."""
        enable_keyword_enrichment = getattr(self, "enable_keyword_enrichment", False)
        keyword_enrichment = getattr(self, "keyword_enrichment", None)
        if not enable_keyword_enrichment or not keyword_enrichment:
            return {
                "enabled": enable_keyword_enrichment,
                "success": False,
                "skipped": True,
                "message": "关键词扩充未启用"
            }

        paper_id = db_result.get("paper_id")
        if paper_id is None:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": "跳过关键词扩充：paper_id 为空"
            }

        status_code = db_result.get("status_code")
        canonical_changed = bool(db_result.get("canonical_changed", False))
        sources = list(getattr(keyword_enrichment, "sources", None) or [keyword_enrichment.source])

        should_enrich = False
        if status_code == "INSERT_NEW_PAPER":
            should_enrich = True
        elif status_code == "INSERT_APPEND_SOURCE":
            should_enrich = canonical_changed
        elif status_code == "INSERT_UPDATE_SAME_SOURCE":
            should_enrich = True
        elif status_code == "INSERT_SKIP_SAME_SOURCE":
            should_enrich = not all(
                self.metadata_db.has_keywords_from_source(
                    paper_id=paper_id,
                    source=source
                )
                for source in sources
            )

        if not should_enrich:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "sources": sources,
                "message": f"跳过关键词扩充：status_code={status_code}"
            }

        papers_data = db_payload.get("papers", {})
        sources_data = db_payload.get("paper_sources", {})
        title = papers_data.get("canonical_title") or sources_data.get("title")
        abstract = papers_data.get("canonical_abstract") or sources_data.get("abstract")

        try:
            extraction = keyword_enrichment.extract_keywords(title=title, abstract=abstract)
            if not extraction.success:
                return {
                    "enabled": True,
                    "success": False,
                    "skipped": extraction.skipped,
                    "source": extraction.source,
                    "sources": sources,
                    "model_name": extraction.model_name,
                    "error": extraction.error,
                    "skip_reason": extraction.skip_reason,
                    "model_results": extraction.model_results,
                }

            grouped_keywords: Dict[str, List[Dict[str, Any]]] = {}
            for keyword in extraction.keywords:
                keyword_source = keyword.get("source") or extraction.source
                grouped_keywords.setdefault(keyword_source, []).append(keyword)

            write_results = {}
            totals = {"inserted": 0, "updated": 0, "skipped": 0}
            for keyword_source, keywords in grouped_keywords.items():
                write_result = self.metadata_db.upsert_generated_keywords(
                    paper_id=paper_id,
                    keywords=keywords,
                    source=keyword_source,
                )
                write_results[keyword_source] = write_result
                totals["inserted"] += write_result.get("inserted", 0)
                totals["updated"] += write_result.get("updated", 0)
                totals["skipped"] += write_result.get("skipped", 0)

            return {
                "enabled": True,
                "success": True,
                "source": extraction.source,
                "sources": list(grouped_keywords),
                "model_name": extraction.model_name,
                "inserted": totals["inserted"],
                "updated": totals["updated"],
                "skipped": totals["skipped"],
                "keyword_count": len(extraction.keywords),
                "model_results": extraction.model_results,
                "write_results": write_results,
            }
        except Exception as e:
            logging.error("keyword enrichment failed: %s", e, exc_info=True)
            return {
                "enabled": True,
                "success": False,
                "skipped": False,
                "sources": sources,
                "error": str(e),
            }

    def _build_index_text(self, db_payload: Dict[str, Any]) -> Dict[str, Any]:
        """构造向量化文本

        规则:
            1. 优先取 title + abstract
            2. 只有 title 时仅索引 title
            3. 两者都为空时跳过向量化

        Args:
            db_payload: 数据库 payload

        Returns:
            Dict[str, Any]: 包含 should_vectorize, text, text_type
        """
        # 提取 title 和 abstract
        papers_data = db_payload.get('papers', {})
        sources_data = db_payload.get('paper_sources', {})

        # 优先使用 canonical 数据，回退到 source 数据
        title = papers_data.get('canonical_title') or sources_data.get('title')
        abstract = papers_data.get('canonical_abstract') or sources_data.get('abstract')

        # 构造文本
        if title and abstract:
            text = f"{title}\n{abstract}"
            return {
                "should_vectorize": True,
                "text": text,
                "text_type": "abstract"
            }
        elif title:
            return {
                "should_vectorize": True,
                "text": title,
                "text_type": "title"
            }
        else:
            return {
                "should_vectorize": False,
                "text": "",
                "text_type": ""
            }

    def _vectorize_document(
        self,
        source_name: str,
        work_id: str,
        paper_id: Optional[int],
        db_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """向量化文档

        Args:
            source_name: 来源名称
            work_id: 作品 ID
            paper_id: 论文 ID
            db_payload: 数据库 payload

        Returns:
            Dict[str, Any]: 向量化结果
        """
        try:
            # 1. 构造向量化文本
            index_text_info = self._build_index_text(db_payload)

            if not index_text_info["should_vectorize"]:
                return {
                    "success": True,
                    "enabled": True,
                    "message": "跳过向量化：title 和 abstract 均为空"
                }

            # 2. 添加到向量数据库
            result = self.vector_db.add_document(
                source_name=source_name,
                work_id=work_id,
                text=index_text_info["text"],
                text_type=index_text_info["text_type"],
                paper_id=str(paper_id) if paper_id else None
            )

            return {
                "success": True,
                "enabled": True,
                "action": result.get("action", "unknown"),
                "message": f"向量化成功: {result.get('action')}"
            }

        except Exception as e:
            logging.error(f"_vectorize_document 失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "enabled": True,
                "error": str(e)
            }

    def _hydrate_search_results(self, search_results: List[SearchResult]) -> List[Dict[str, Any]]:
        """补全搜索结果的 metadata

        Args:
            search_results: VectorDB 搜索结果列表

        Returns:
            List[Dict[str, Any]]: 补全后的结果列表
        """
        hydrated_results = []

        for result in search_results:
            try:
                # 读取 metadata
                paper_info = self.metadata_db.read_paper_by_work_id(result.work_id)

                if paper_info:
                    # 补全结果
                    hydrated_result = {
                        "work_id": result.work_id,
                        "paper_id": paper_info.get("paper_id"),
                        "source_name": result.source_name,
                        "similarity": result.score,
                        "text_type": result.text_type,
                        "metadata": paper_info
                    }
                    hydrated_results.append(hydrated_result)
                else:
                    # metadata 不存在，记录警告
                    logging.warning(
                        f"搜索结果的 metadata 不存在: work_id={result.work_id}"
                    )

            except Exception as e:
                logging.error(
                    f"补全 metadata 失败: work_id={result.work_id}, error={str(e)}",
                    exc_info=True
                )

        return hydrated_results
