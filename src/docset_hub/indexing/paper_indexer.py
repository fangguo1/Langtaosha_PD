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
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, List, Mapping, Optional, Sequence, Union

from config import init_config, get_default_sources
from ..metadata.transformer import MetadataTransformer, TransformResult
from ..storage.metadata_db import MetadataDB
from ..storage.vector_db import VectorDB, SearchResult
from .dense_result_filter import (
    DENSE_DEFAULT_MIN_SIMILARITY,
    filter_dense_results_by_hard_rules,
)
from .coverage_engine import analyze_document_coverage, summarize_expanded_sparse_matches
from .expanded_sparse_retrieval import match_papers_by_expanded_sparse_plan
from .keyword_enrichment import KeywordEnrichmentService
from .paper_keyword_lookup import (
    PaperKeywordLookupResult,
    match_paper_keywords_with_lookup_plan,
)
from .span_matcher_pipeline import SpanMatcherPipeline, SpanMatcherProfile
from .query_understanding import QueryUnderstandingService


DEFAULT_HYBRID_RETRIEVAL_WEIGHTS = {
    "dense": 0.4,
    "sparse": 0.4,
    "keyword_lookup": 0.2,
}


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
                - sparse_vectorization (Dict): BM25 稀疏向量化操作结果

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

            sparse_vector_result = self._handle_insert_sparse_vectorization(
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
                "sparse_vectorization": sparse_vector_result,
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

            sparse_vector_result = self._handle_insert_sparse_vectorization(
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
                "sparse_vectorization": sparse_vector_result,
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
        hydrate: bool = True,
        search_type: str = "dense",
        keyword_sources: Optional[Sequence[str]] = None,
        include_coverage: bool = False,
    ) -> List[Dict[str, Any]]:
        """搜索论文

        Args:
            query: 查询文本
            source_list: 来源列表（如果不提供则使用 default_sources）
            top_k: 返回结果数量
            hydrate: 是否补全完整 metadata（默认 True）
            search_type: 检索类型，支持 dense / sparse / hybrid / hybrid_retrieval / expanded_sparse
            keyword_sources: 关键词来源过滤（expanded_sparse 与 coverage 注解使用）
            include_coverage: dense/sparse 结果附加 span coverage 字段（需 hydrate=True）

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
        if search_type == "expanded_sparse":
            return self.expanded_sparse_search(
                query=query,
                source_list=source_list,
                top_k=top_k,
                hydrate=hydrate,
                keyword_sources=keyword_sources,
            )

        if not self.vector_db:
            raise ValueError("向量数据库未启用，无法执行搜索")

        try:
            # 1. 解析 source_list
            resolved_source_list = self._resolve_source_list(source_list)

            if search_type == "hybrid_retrieval":
                return self.hybrid_retrieval_search(
                    query=query,
                    source_list=resolved_source_list,
                    top_k=top_k,
                    hydrate=hydrate,
                )

            # 2. 执行向量搜索
            search_results = self.vector_db.search(
                query=query,
                source_list=resolved_source_list,
                top_k=top_k,
                search_type=search_type
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
                        "text_type": result.text_type,
                        "retrieval_debug": result.retrieval_debug,
                    }
                    for result in search_results
                ]

        except Exception as e:
            logging.error(f"search 失败: {str(e)}", exc_info=True)
            raise e

    def hybrid_retrieval_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True,
        retrieval_weights: Optional[Dict[str, float]] = None,
        candidate_multiplier: Optional[int] = None,
        min_candidate_k: Optional[int] = None,
        rrf_k: Optional[float] = None,
        include_keyword_lookup: bool = True,
        keyword_sources: Optional[List[str]] = None,
        dense_min_similarity: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Run dense, BM25 sparse, and keyword lookup recall, then weighted RRF.

        This is the Step 3 candidate-pool builder. Dense results are first
        passed through ``dense_result_filter``. Sparse and keyword lookup
        candidates must carry positive branch evidence before they enter RRF.
        """
        if not self.vector_db:
            raise ValueError("向量数据库未启用，无法执行三路混合检索")

        resolved_source_list = self._resolve_source_list(source_list)
        top_k = max(1, int(top_k))

        hybrid_config = getattr(self.vector_db, "hybrid_config", {}) or {}
        effective_candidate_multiplier = int(
            candidate_multiplier
            if candidate_multiplier is not None
            else hybrid_config.get("candidate_multiplier", 5)
        )
        effective_min_candidate_k = int(
            min_candidate_k
            if min_candidate_k is not None
            else hybrid_config.get("min_candidate_k", 50)
        )
        candidate_k = max(top_k * effective_candidate_multiplier, effective_min_candidate_k)
        effective_rrf_k = float(rrf_k if rrf_k is not None else hybrid_config.get("rrf_k", 60))
        effective_weights = self._resolve_hybrid_retrieval_weights(retrieval_weights)
        effective_dense_min_similarity = float(
            dense_min_similarity
            if dense_min_similarity is not None
            else hybrid_config.get("dense_min_similarity", DENSE_DEFAULT_MIN_SIMILARITY)
        )

        branch_results: Dict[str, List[Dict[str, Any]]] = {}
        branch_failures: Dict[str, str] = {}
        futures = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures[
                executor.submit(
                    self._run_dense_retrieval_branch,
                    query=query,
                    source_list=resolved_source_list,
                    top_k=candidate_k,
                    keyword_sources=keyword_sources,
                    min_similarity=effective_dense_min_similarity,
                )
            ] = "dense"
            futures[
                executor.submit(
                    self._run_sparse_retrieval_branch,
                    query=query,
                    source_list=resolved_source_list,
                    top_k=candidate_k,
                )
            ] = "sparse"
            if include_keyword_lookup:
                futures[
                    executor.submit(
                        self._run_keyword_lookup_retrieval_branch,
                        query=query,
                        source_list=resolved_source_list,
                        top_k=candidate_k,
                        keyword_sources=keyword_sources,
                    )
                ] = "keyword_lookup"

            for future in as_completed(futures):
                branch_name = futures[future]
                try:
                    branch_results[branch_name] = future.result()
                except Exception as exc:
                    branch_failures[branch_name] = str(exc)
                    logging.warning(
                        "三路混合检索 branch 失败: branch=%s, error=%s",
                        branch_name,
                        exc,
                        exc_info=True,
                    )

        requested_branch_count = len(futures)
        if branch_failures and len(branch_failures) == requested_branch_count:
            raise RuntimeError(f"三路混合检索全部 branch 失败: {branch_failures}")

        merged_results = self._weighted_rrf_merge_retrieval_branches(
            branch_results=branch_results,
            top_k=top_k,
            weights=effective_weights,
            rrf_k=effective_rrf_k,
            branch_failures=branch_failures,
        )

        if hydrate:
            return self._hydrate_search_results(merged_results)
        return self._search_results_to_lightweight_dicts(merged_results)

    def expanded_sparse_search(
        self,
        query: str,
        source_list: Optional[List[str]] = None,
        top_k: int = 10,
        hydrate: bool = True,
        keyword_sources: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Expanded sparse 检索：semantic plan 展开词项匹配 + span coverage 评分。

        结果按 coverage_ratio 作为 similarity 返回，形状与 search() 其他
        检索类型保持一致（metadata 嵌套，hydrate 可关）。
        """
        resolved_source_list = self._resolve_source_list(source_list)
        plan = self.build_query_semantic_plan(
            query=query,
            source_list=resolved_source_list,
            keyword_sources=keyword_sources,
        )
        if plan is None:
            return []

        candidates = match_papers_by_expanded_sparse_plan(
            metadata_db=self.metadata_db,
            plan=plan,
            source_list=resolved_source_list,
            keyword_sources=keyword_sources,
            top_k=top_k,
        )

        results: List[Dict[str, Any]] = []
        for candidate in candidates:
            coverage = summarize_expanded_sparse_matches(
                plan=plan,
                matched_spans=list(getattr(candidate, "matched_spans", []) or []),
            )
            item: Dict[str, Any] = {
                "work_id": getattr(candidate, "work_id", None),
                "paper_id": getattr(candidate, "paper_id", None),
                "similarity": float(coverage.coverage_ratio or 0.0),
                "coverage_ratio": float(coverage.coverage_ratio or 0.0),
                "coverage": coverage.to_dict(),
                "matched_span_count": int(coverage.matched_span_count or 0),
                "total_span_count": int(coverage.total_span_count or 0),
                "matched_spans": list(coverage.matched_spans or []),
                "retrieval_debug": dict(getattr(candidate, "retrieval_debug", {}) or {}),
            }
            if hydrate:
                metadata: Dict[str, Any] = {}
                work_id = item["work_id"]
                if work_id:
                    try:
                        metadata = dict(self.metadata_db.read_paper_by_work_id(work_id) or {})
                    except Exception:  # noqa: BLE001
                        metadata = {}
                item["metadata"] = metadata
                item["source_name"] = metadata.get("source_name")
            results.append(item)
        return results

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

        if understanding.route == "author_suggestion":
            return {
                "success": True,
                "query": query,
                "search_query": None,
                "query_understanding": understanding_payload,
                "results": [],
            }

        search_query = understanding.corrected_query or understanding.normalized_query
        expansion = understanding_payload.get("expansion") or {}
        expanded_queries = expansion.get("expanded_queries") if expansion.get("status") == "ok" else []
        queries = [search_query] + [q for q in (expanded_queries or []) if q and q != search_query]
        result_batches = []
        for candidate_query in queries:
            result_batches.append(
                (
                    candidate_query,
                    self.search(
                        query=candidate_query,
                        source_list=resolved_source_list,
                        top_k=top_k,
                        hydrate=hydrate,
                    ),
                )
            )
        results = self._merge_search_result_batches(result_batches, top_k=top_k)
        return {
            "success": True,
            "query": query,
            "search_query": search_query,
            "expanded_search_queries": queries[1:],
            "query_understanding": understanding_payload,
            "results": results,
        }

    @staticmethod
    def _merge_search_result_batches(
        result_batches: List[tuple[str, List[Dict[str, Any]]]],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        """Merge search results from original and expanded queries."""
        merged: Dict[str, Dict[str, Any]] = {}
        order = 0
        for query, results in result_batches:
            for result in results:
                order += 1
                key = str(result.get("work_id") or result.get("paper_id") or order)
                score = result.get("similarity")
                existing = merged.get(key)
                if existing is None or (score is not None and score > existing.get("similarity", -1)):
                    item = dict(result)
                    item["matched_query"] = query
                    item["_merge_order"] = order
                    merged[key] = item
        values = list(merged.values())
        values.sort(key=lambda item: (item.get("similarity") is not None, item.get("similarity", -1)), reverse=True)
        for item in values:
            item.pop("_merge_order", None)
        return values[:top_k]

    def _run_dense_retrieval_branch(
        self,
        query: str,
        source_list: List[str],
        top_k: int,
        keyword_sources: Optional[Sequence[str]] = None,
        min_similarity: float = DENSE_DEFAULT_MIN_SIMILARITY,
    ) -> List[Dict[str, Any]]:
        """Run dense search and apply the DB-backed hard filter."""
        dense_results = self.vector_db.dense_search(
            query=query,
            source_list=source_list,
            top_k=top_k,
        )
        dense_payloads = [
            self._search_result_to_filter_payload(result)
            for result in dense_results
        ]
        filtered_payloads, report = filter_dense_results_by_hard_rules(
            metadata_db=self.metadata_db,
            query=query,
            results=dense_payloads,
            min_similarity=min_similarity,
            keyword_sources=keyword_sources,
        )
        return self._adapt_dense_payloads_to_branch_results(filtered_payloads, report.to_dict())

    def _run_sparse_retrieval_branch(
        self,
        query: str,
        source_list: List[str],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        """Run BM25 sparse search and keep only positive-evidence results."""
        sparse_results = self.vector_db.sparse_search(
            query=query,
            source_list=source_list,
            top_k=top_k,
        )
        return self._adapt_search_results_to_branch_results(
            sparse_results,
            retriever="sparse",
            drop_non_positive=True,
        )

    def _run_keyword_lookup_retrieval_branch(
        self,
        query: str,
        source_list: List[str],
        top_k: int,
        keyword_sources: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Run DB-backed keyword lookup recall from query span matches."""
        profile = self._build_span_matcher_profile(
            source_list=source_list,
            keyword_sources=keyword_sources,
            profile_name="keyword_only",
        )
        result = SpanMatcherPipeline.from_profile(
            profile=profile,
            metadata_db=self.metadata_db,
        ).run(query)
        if not result.selected_concepts:
            return []

        lookup_results = match_paper_keywords_with_lookup_plan(
            metadata_db=self.metadata_db,
            selected_concepts=result.selected_concepts,
            span_results=result.span_results,
            source_list=source_list,
            keyword_sources=keyword_sources,
            top_k=top_k,
            include_sub_concepts=True,
            include_substring_candidates=True,
        )
        return self._adapt_keyword_lookup_results_to_branch_results(lookup_results)

    def _build_span_matcher_profile(
        self,
        *,
        source_list: Sequence[str],
        keyword_sources: Optional[Sequence[str]] = None,
        profile_name: str = "ontology_plus_keyword",
    ) -> SpanMatcherProfile:
        if profile_name == "keyword_only":
            factory = SpanMatcherProfile.keyword_only
        elif profile_name == "ontology_only":
            factory = SpanMatcherProfile.ontology_only
        else:
            factory = SpanMatcherProfile.ontology_plus_keyword

        return factory(
            enable_scispacy=os.environ.get("SKIP_SCISPACY", "0") != "1",
            ontology_base_url=os.environ.get("ONTOLOGY_LINKER_URL", "http://127.0.0.1:8765"),
            ontology_sources=tuple(self._parse_csv(os.environ.get("ONTOLOGY_SOURCE_LIST", "umls,mesh"))),
            ontology_top_k=self._env_int("ONTOLOGY_TOP_K", 2),
            ontology_threshold=self._env_float("ONTOLOGY_THRESHOLD", 0.9),
            ontology_timeout=self._env_float("ONTOLOGY_TIMEOUT", 20.0),
            paper_sources=tuple(source_list or self.default_sources),
            keyword_sources=tuple(keyword_sources or ()),
        )

    def build_query_semantic_plan(
        self,
        query: str,
        source_list: List[str],
        keyword_sources: Optional[Sequence[str]] = None,
        profile_name: str = "ontology_plus_keyword",
    ):
        """构建查询语义计划（公开 Domain 能力，供检索分支与 dev API 使用）。

        Returns:
            QuerySemanticPlan，无可用 selected_concepts 时返回 None。
        """
        profile = self._build_span_matcher_profile(
            source_list=source_list,
            keyword_sources=keyword_sources,
            profile_name=profile_name,
        )
        result = SpanMatcherPipeline.from_profile(
            profile=profile,
            metadata_db=self.metadata_db,
        ).run(query)
        if not result.selected_concepts:
            return None
        return result.semantic_plan

    def _run_expanded_sparse_retrieval_branch(
        self,
        query: str,
        source_list: List[str],
        top_k: int,
        keyword_sources: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        plan = self.build_query_semantic_plan(
            query=query,
            source_list=source_list,
            keyword_sources=keyword_sources,
        )
        if plan is None:
            return []
        results = match_papers_by_expanded_sparse_plan(
            metadata_db=self.metadata_db,
            plan=plan,
            source_list=source_list,
            keyword_sources=keyword_sources,
            top_k=top_k,
        )
        return self._adapt_expanded_sparse_results_to_branch_results(results)

    @staticmethod
    def _resolve_hybrid_retrieval_weights(
        retrieval_weights: Optional[Mapping[str, float]] = None,
    ) -> Dict[str, float]:
        """Return non-negative branch weights for weighted RRF."""
        weights = dict(DEFAULT_HYBRID_RETRIEVAL_WEIGHTS)
        for key, value in (retrieval_weights or {}).items():
            if key not in weights:
                continue
            try:
                weights[key] = max(0.0, float(value))
            except (TypeError, ValueError):
                continue
        if not any(value > 0 for value in weights.values()):
            return dict(DEFAULT_HYBRID_RETRIEVAL_WEIGHTS)
        return weights

    @staticmethod
    def _search_result_to_filter_payload(result: SearchResult) -> Dict[str, Any]:
        """Convert a dense SearchResult into dense_result_filter input."""
        return {
            "work_id": result.work_id,
            "paper_id": result.paper_id,
            "source_name": result.source_name,
            "similarity": result.score,
            "similarity_score": result.score,
            "text_type": result.text_type,
            "retrieval_debug": dict(result.retrieval_debug or {}),
        }

    def _adapt_dense_payloads_to_branch_results(
        self,
        dense_payloads: Sequence[Mapping[str, Any]],
        filter_report: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Convert filtered dense payloads into RRF branch results."""
        branch_results = []
        rank = 0
        for item in dense_payloads:
            raw_score = self._safe_float(item.get("similarity_score", item.get("similarity")))
            if not math.isfinite(raw_score):
                continue
            rank += 1
            retrieval_debug = dict(item.get("retrieval_debug") or {})
            if filter_report is not None:
                retrieval_debug.setdefault("dense_hard_filter_report", dict(filter_report))
            branch_results.append(
                {
                    "work_id": str(item.get("work_id") or ""),
                    "paper_id": item.get("paper_id"),
                    "source_name": str(item.get("source_name") or ""),
                    "text_type": str(item.get("text_type") or ""),
                    "raw_score": raw_score,
                    "retriever": "dense",
                    "rank": rank,
                    "payload": dict(item),
                    "retrieval_debug": retrieval_debug,
                }
            )
        return branch_results

    @staticmethod
    def _adapt_expanded_sparse_results_to_branch_results(
        results: Sequence[Any],
    ) -> List[Dict[str, Any]]:
        branch_results: List[Dict[str, Any]] = []
        rank = 0
        for result in results:
            paper_id = getattr(result, "paper_id", None)
            work_id = getattr(result, "work_id", None)
            matched_span_count = int(getattr(result, "matched_span_count", 0) or 0)
            total_span_count = int(getattr(result, "total_span_count", 0) or 0)
            coverage_ratio = float(getattr(result, "coverage_ratio", 0.0) or 0.0)
            if matched_span_count <= 0:
                continue
            rank += 1
            branch_results.append(
                {
                    "paper_id": paper_id,
                    "work_id": work_id,
                    "score": coverage_ratio,
                    "raw_score": coverage_ratio,
                    "rank": rank,
                    "retrieval_debug": {
                        "retriever": "expanded_sparse",
                        "matched_span_count": matched_span_count,
                        "total_span_count": total_span_count,
                        "coverage_ratio": coverage_ratio,
                        "matched_spans": list(getattr(result, "matched_spans", []) or []),
                    },
                }
            )
        return branch_results

    def _adapt_search_results_to_branch_results(
        self,
        search_results: Sequence[SearchResult],
        retriever: str,
        drop_non_positive: bool = False,
    ) -> List[Dict[str, Any]]:
        """Convert VectorDB SearchResult objects into RRF branch results."""
        branch_results = []
        rank = 0
        for result in search_results:
            raw_score = self._safe_float(result.score)
            if not math.isfinite(raw_score):
                continue
            if drop_non_positive and raw_score <= 0:
                continue
            rank += 1
            branch_results.append(
                {
                    "work_id": result.work_id,
                    "paper_id": result.paper_id,
                    "source_name": result.source_name,
                    "text_type": result.text_type,
                    "raw_score": raw_score,
                    "retriever": retriever,
                    "rank": rank,
                    "payload": result,
                    "retrieval_debug": dict(result.retrieval_debug or {}),
                }
            )
        return branch_results

    def _adapt_keyword_lookup_results_to_branch_results(
        self,
        lookup_results: Sequence[PaperKeywordLookupResult],
    ) -> List[Dict[str, Any]]:
        """Convert keyword lookup results into positive-evidence branch results."""
        branch_results = []
        rank = 0
        for result in lookup_results:
            raw_score = self._safe_float(result.keyword_lookup_score)
            if not math.isfinite(raw_score) or raw_score <= 0:
                continue
            rank += 1
            payload = result.to_dict()
            retrieval_debug = dict(result.retrieval_debug or {})
            retrieval_debug.setdefault("matched_concepts", result.matched_concepts)
            retrieval_debug.setdefault("matched_concept_count", result.matched_concept_count)
            retrieval_debug.setdefault("total_concept_count", result.total_concept_count)
            branch_results.append(
                {
                    "work_id": result.work_id,
                    "paper_id": result.paper_id,
                    "source_name": "",
                    "text_type": "",
                    "raw_score": raw_score,
                    "retriever": "keyword_lookup",
                    "rank": rank,
                    "payload": payload,
                    "retrieval_debug": retrieval_debug,
                }
            )
        return branch_results

    def _weighted_rrf_merge_retrieval_branches(
        self,
        branch_results: Mapping[str, Sequence[Mapping[str, Any]]],
        top_k: int,
        weights: Mapping[str, float],
        rrf_k: float,
        branch_failures: Optional[Mapping[str, str]] = None,
    ) -> List[SearchResult]:
        """Merge multiple retrieval branch ranked lists with weighted RRF."""
        merged: Dict[str, Dict[str, Any]] = {}
        branch_failures = dict(branch_failures or {})
        rrf_k = float(rrf_k)

        for retriever, results in branch_results.items():
            branch_weight = max(0.0, self._safe_float(weights.get(retriever, 0.0)))
            if branch_weight <= 0:
                continue
            for fallback_rank, branch_result in enumerate(results, start=1):
                rank = int(branch_result.get("rank") or fallback_rank)
                if rank <= 0:
                    continue
                work_id = str(branch_result.get("work_id") or "")
                paper_id = branch_result.get("paper_id")
                key = self._retrieval_dedupe_key(work_id=work_id, paper_id=paper_id, retriever=retriever, rank=rank)
                entry = merged.setdefault(
                    key,
                    {
                        "work_id": work_id,
                        "paper_id": paper_id,
                        "source_name": str(branch_result.get("source_name") or ""),
                        "text_type": str(branch_result.get("text_type") or ""),
                        "rrf_score": 0.0,
                        "retrieval_debug": {
                            "matched_retrievers": [],
                            "rrf_k": rrf_k,
                            "retrieval_weights": dict(weights),
                        },
                    },
                )
                entry["rrf_score"] += branch_weight / (rrf_k + rank)

                if not entry.get("work_id") and work_id:
                    entry["work_id"] = work_id
                if not entry.get("paper_id") and paper_id:
                    entry["paper_id"] = paper_id
                if not entry.get("source_name") and branch_result.get("source_name"):
                    entry["source_name"] = str(branch_result.get("source_name") or "")
                if not entry.get("text_type") and branch_result.get("text_type"):
                    entry["text_type"] = str(branch_result.get("text_type") or "")

                debug = entry["retrieval_debug"]
                matched_retrievers = debug["matched_retrievers"]
                if retriever not in matched_retrievers:
                    matched_retrievers.append(retriever)
                raw_score = self._safe_float(branch_result.get("raw_score"))
                debug[f"{retriever}_rank"] = rank
                debug[f"{retriever}_score"] = raw_score
                branch_debug = dict(branch_result.get("retrieval_debug") or {})
                if branch_debug:
                    debug[f"{retriever}_debug"] = branch_debug
                payload = branch_result.get("payload")
                if retriever == "keyword_lookup" and isinstance(payload, Mapping):
                    debug["keyword_lookup_matched_concepts"] = list(payload.get("matched_concepts") or [])

        fused_results = []
        for entry in merged.values():
            debug = entry["retrieval_debug"]
            if branch_failures:
                debug["branch_failures"] = branch_failures
            fused_results.append(
                SearchResult(
                    source_name=str(entry.get("source_name") or ""),
                    work_id=str(entry.get("work_id") or ""),
                    score=float(entry.get("rrf_score") or 0.0),
                    text_type=str(entry.get("text_type") or ""),
                    paper_id=entry.get("paper_id"),
                    retrieval_debug=debug,
                )
            )

        fused_results.sort(
            key=lambda result: (
                result.score,
                len((result.retrieval_debug or {}).get("matched_retrievers", [])),
                result.work_id,
            ),
            reverse=True,
        )
        return fused_results[: max(1, int(top_k))]

    @staticmethod
    def _retrieval_dedupe_key(
        work_id: str,
        paper_id: Any,
        retriever: str,
        rank: int,
    ) -> str:
        if work_id:
            return f"work:{work_id}"
        if paper_id not in (None, ""):
            return f"paper:{paper_id}"
        return f"{retriever}:{rank}"

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    @staticmethod
    def _parse_csv(value: Optional[str]) -> List[str]:
        return [item.strip() for item in str(value or "").split(",") if item.strip()]

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, str(default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, str(default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _search_results_to_lightweight_dicts(search_results: Sequence[SearchResult]) -> List[Dict[str, Any]]:
        """Serialize SearchResult objects without metadata hydration."""
        return [
            {
                "work_id": result.work_id,
                "paper_id": result.paper_id,
                "source_name": result.source_name,
                "similarity": result.score,
                "text_type": result.text_type,
                "retrieval_debug": result.retrieval_debug,
            }
            for result in search_results
        ]

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

        decision = self._get_insert_vectorization_decision(
            resolved_source_name=resolved_source_name,
            db_result=db_result
        )

        if not decision["should_vectorize"]:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": (
                    "跳过向量化："
                    f"status_code={decision['status_code']}, "
                    f"canonical_changed={decision['canonical_changed']}, "
                    f"is_canonical_source={decision['is_canonical_source']}"
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

    def _handle_insert_sparse_vectorization(
        self,
        resolved_source_name: str,
        db_payload: Dict[str, Any],
        db_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """insert-only BM25 稀疏向量化编排：与 dense 共享 canonical 触发规则。"""
        sparse_enabled = self._is_sparse_vectorization_enabled()
        if not self.enable_vectorization or not self.vector_db:
            return {
                "enabled": sparse_enabled,
                "success": False,
                "skipped": True,
                "message": "稀疏向量化未启用：向量数据库未启用"
            }

        if not sparse_enabled:
            return {
                "enabled": False,
                "success": False,
                "skipped": True,
                "message": "稀疏向量化未启用"
            }

        decision = self._get_insert_vectorization_decision(
            resolved_source_name=resolved_source_name,
            db_result=db_result
        )
        if not decision["should_vectorize"]:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": (
                    "跳过稀疏向量化："
                    f"status_code={decision['status_code']}, "
                    f"canonical_changed={decision['canonical_changed']}, "
                    f"is_canonical_source={decision['is_canonical_source']}"
                )
            }

        paper_id = db_result.get("paper_id")
        work_id = db_result.get("work_id")
        index_text_info = self._build_index_text(db_payload)
        canonical_source_name = db_result.get("canonical_source_name") or resolved_source_name

        if paper_id is None:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": "跳过稀疏向量化：paper_id 为空"
            }

        if not work_id:
            return {
                "enabled": True,
                "success": False,
                "skipped": True,
                "message": "跳过稀疏向量化：work_id 为空"
            }

        if not index_text_info.get("should_vectorize", False):
            return {
                "enabled": True,
                "success": True,
                "skipped": True,
                "message": "跳过稀疏向量化：title 和 abstract 均为空"
            }

        return self._sparse_vectorize_document(
            source_name=canonical_source_name,
            work_id=work_id,
            paper_id=paper_id,
            db_payload=db_payload
        )

    def _get_insert_vectorization_decision(
        self,
        resolved_source_name: str,
        db_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """根据 insert-only 写入结果判断是否需要刷新 canonical 向量索引。"""
        status_code = db_result.get("status_code")
        canonical_changed = bool(db_result.get("canonical_changed", False))
        canonical_source_name = db_result.get("canonical_source_name")
        is_canonical_source = canonical_source_name == resolved_source_name

        should_vectorize = False
        if status_code == "INSERT_NEW_PAPER":
            should_vectorize = True
        elif status_code == "INSERT_APPEND_SOURCE":
            should_vectorize = canonical_changed
        elif status_code == "INSERT_UPDATE_SAME_SOURCE":
            should_vectorize = is_canonical_source

        return {
            "should_vectorize": should_vectorize,
            "status_code": status_code,
            "canonical_changed": canonical_changed,
            "is_canonical_source": is_canonical_source,
        }

    def _is_sparse_vectorization_enabled(self) -> bool:
        """Return whether PaperIndexer should write BM25 sparse documents."""
        vector_db = getattr(self, "vector_db", None)
        sparse_config = getattr(vector_db, "sparse_config", {}) if vector_db else {}
        if not isinstance(sparse_config, dict):
            return False

        enabled = sparse_config.get("enabled", False)
        if isinstance(enabled, str):
            return enabled.strip().lower() in {"1", "true", "yes", "on"}
        return bool(enabled)

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

    def _sparse_vectorize_document(
        self,
        source_name: str,
        work_id: str,
        paper_id: Optional[int],
        db_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成并写入 BM25 sparse_vector 文档。"""
        try:
            index_text_info = self._build_index_text(db_payload)

            if not index_text_info["should_vectorize"]:
                return {
                    "success": True,
                    "enabled": True,
                    "skipped": True,
                    "message": "跳过稀疏向量化：title 和 abstract 均为空"
                }

            result = self.vector_db.add_sparse_document(
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
                "doc_id": result.get("doc_id"),
                "affected_count": result.get("affected_count", 0),
                "message": f"稀疏向量化成功: {result.get('action')}"
            }

        except Exception as e:
            logging.error(f"_sparse_vectorize_document 失败: {str(e)}", exc_info=True)
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
                        "retrieval_debug": result.retrieval_debug,
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
