"""PaperIndexer - 论文索引器

实现文档的索引、搜索、删除等功能
"""

import sys
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import logging
import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from docset_hub.storage.metadata_db import MetadataDB
from docset_hub.storage.vector_db import VectorDB, VectorEntry
from docset_hub.metadata.validator import MetadataValidator
from docset_hub.metadata import generate_work_id
from config.config_loader import (
    init_config, get_db_engine, get_shard_ids_by_routing
)
from sqlalchemy import text

logger = logging.getLogger(__name__)


class PaperIndexer:
    """论文索引器
    
    提供文档的索引、搜索、删除、查询等功能
    """
    
    def __init__(
        self,
        config_path: Path,
        enable_vectorization: bool = True,
        readonly_shard_ids: List[int] = None,
        writable_shard_ids: List[int] = None,
        vector_auto_save: bool = True
    ):
        """初始化索引器
        
        Args:
            enable_vectorization: 是否启用向量化功能（默认True）
            readonly_shard_ids: 只读 shard ID 列表（如果为 None，将从配置读取）
            writable_shard_ids: 可写 shard ID 列表（如果为 None，将从配置读取）
            vector_auto_save: 是否自动保存向量（默认True）
        """
        # 确保配置已初始化
        if config_path is None:
            raise ValueError("未找到配置文件 config.yaml，请指定 config_path")
        init_config(config_path)
        
        self.metadata_db = MetadataDB(config_path=config_path)
        self.validator = MetadataValidator()
        
        # 初始化向量数据库（如果启用）
        self.vector_db = None
        self.enable_vectorization = enable_vectorization
        self.vector_auto_save = vector_auto_save
        
        # 从配置读取 shard_ids（如果未提供）
        if enable_vectorization:
            if readonly_shard_ids is None or writable_shard_ids is None:
                raise ValueError("readonly_shard_ids 和 writable_shard_ids 不能同时为 None")
        
        self.readonly_shard_ids = readonly_shard_ids
        self.writable_shard_ids = writable_shard_ids
        self.search_shard_ids = []
        if readonly_shard_ids:
            self.search_shard_ids.extend(readonly_shard_ids)
        if writable_shard_ids:
            self.search_shard_ids.extend(writable_shard_ids)
        
        
        if enable_vectorization:
            try:
                # 从环境变量获取向量配置
                import os
                vector_db_path = os.getenv('VECTOR_DB_PATH')
                gritlm_model_name = os.getenv('GRITLM_MODEL_NAME', 'GritLM/GritLM-7B')
                gritlm_model_path = os.getenv('GRITLM_MODEL_PATH')
                vector_dim = 4096  # GritLM-7B 的向量维度
                
                if not vector_db_path:
                    raise ValueError("环境变量 VECTOR_DB_PATH 未设置")
                
                self.vector_db = VectorDB(
                    shards_dir=vector_db_path,
                    model_name=gritlm_model_name,
                    model_path=gritlm_model_path,
                    vector_dim=vector_dim,
                    readonly_shard_ids=readonly_shard_ids,
                    writable_shard_ids=writable_shard_ids
                )
                logger.info(f"PaperIndexer initialized with vector database (readonly: {len(readonly_shard_ids)}, writable: {len(writable_shard_ids)})")
            except Exception as e:
                logger.warning(f"向量数据库初始化失败，将禁用向量化功能: {e}")
                self.enable_vectorization = False
                self.vector_db = None
        else:
            logger.info("PaperIndexer initialized (vectorization disabled)")
    
    def add_doc(self, doc_data: Dict[str, Any], include_traceback: bool = False, shard_id: int = None) -> Dict[str, Any]:
        """添加文档到索引
        
        支持两种输入格式：
        1. 完整 DocSet 格式：包含 work_id、default_info 等完整结构
        2. 简化格式：只包含 title、abstract、author（或 authors）字段
        
        Args:
            doc_data: 文档数据字典（完整 DocSet 格式或简化格式）
            include_traceback: 是否在返回结果中包含堆栈跟踪（默认False，适合批量处理）
            shard_id: 指定使用的 shard ID（可选）。如果为 None，将使用第一个可写 shard。
                      如果指定的 shard_id 不在可写 shard 列表中，将使用第一个可写 shard 并记录警告。
            
        Returns:
            Dict: 包含操作结果的字典
                成功时:
                {
                    'success': True,
                    'work_id': str,
                    'paper_id': int,
                    'message': str
                }
                失败时:
                {
                    'success': False,
                    'work_id': str or None,
                    'paper_id': None,
                    'message': str,
                    'error_type': str,  # 错误类型
                    'error_detail': str,  # 详细错误信息（可选）
                    'traceback': str  # 堆栈跟踪（仅当include_traceback=True时）
                }
        """
        try:
            # 检测是否为简化格式（只有 title、abstract、author/authors）
            is_simplified = self._is_simplified_format(doc_data)
            
            if is_simplified:
                # 转换为完整 DocSet 格式
                doc_data = self._convert_simplified_to_docset(doc_data)
                logger.info("检测到简化格式输入，已自动转换为完整 DocSet 格式")
            
            work_id = doc_data.get('work_id')
            
            # 如果没有 work_id，自动生成
            if not work_id:
                work_id = generate_work_id()
                doc_data['work_id'] = work_id
                logger.info(f"自动生成 work_id: {work_id}")
            
            # 验证数据格式（对于简化格式转换后的数据，验证会更宽松，允许空的identifiers）
            is_valid, errors = self.validator.validate(doc_data, allow_empty_identifiers=is_simplified)
            if not is_valid:
                error_msg = f"数据验证失败: {', '.join(errors)}"
                logger.error(f"添加文档失败 (work_id={work_id}): {error_msg}")
                if include_traceback:
                    logger.error(f"验证错误详情: {errors}")
                
                result = {
                    'success': False,
                    'work_id': work_id,
                    'paper_id': None,
                    'message': error_msg,
                    'error_type': 'ValidationError',
                    'error_detail': ', '.join(errors)
                }
                if include_traceback:
                    result['traceback'] = ''.join(traceback.format_exc())
                return result
            
            # 验证 shard_id（如果启用向量化且传入了 shard_id）
            if self.enable_vectorization and shard_id is not None:
                if not self.writable_shard_ids:
                    raise ValueError("没有可用的可写 shard，无法添加向量")
                if shard_id not in self.writable_shard_ids:
                    raise ValueError(
                        f"传入的 shard_id={shard_id} 不在可写 shard 列表中 {self.writable_shard_ids}"
                    )
            
            # 存储到元数据库
            paper_id = self.metadata_db.insert_paper(doc_data)
            logger.info(f"文档已添加到元数据库: work_id={work_id}, paper_id={paper_id}")
            
            # 向量化并存储到向量数据库（如果启用）
            vectorization_success = False
            vectorization_message = ""
            if self.enable_vectorization and self.vector_db:
                try:
                    # 获取摘要文本（支持两种数据格式）
                    # 格式1: 直接在顶层有 abstract 字段
                    # 格式2: 在 default_info 下有 abstract 字段
                    abstract = doc_data.get('abstract', '')
                    if not abstract and 'default_info' in doc_data:
                        abstract = doc_data['default_info'].get('abstract', '')
                    
                    title = doc_data.get('title', '')
                    if not title and 'default_info' in doc_data:
                        title = doc_data['default_info'].get('title', '')
                    
                    # 只有 abstract 无 title：报错
                    if abstract and not title:
                        raise ValueError("仅有 abstract 无 title，无法进行向量化")
                    
                    # 确定待向量化文本
                    if title and abstract:
                        text_to_embed = f"{title} {abstract}".strip()
                    elif title:
                        text_to_embed = title.strip()
                    else:
                        text_to_embed = ""
                    
                    if text_to_embed:
                        # 使用新的 add() 方法，传入 paper_id 和文本
                        vector_success = self.vector_db.add(
                            ids=[paper_id],
                            texts=[text_to_embed],
                            shard_id=shard_id,
                            auto_save=self.vector_auto_save
                        )
                        
                        if vector_success:
                            vectorization_success = True
                            vectorization_message = "向量化成功"
                            logger.info(f"文档摘要已向量化并存储: work_id={work_id}, paper_id={paper_id}, shard_id={shard_id}")
                            
                            # 更新 PostgreSQL 中的 embedding_status 和 shard_id
                            try:
                                status_update_success = self.metadata_db.update_embedding_status_and_shard(
                                    paper_id=paper_id,
                                    embedding_status=2,
                                    shard_id=shard_id
                                )
                                if status_update_success:
                                    logger.info(f"✓ 已更新论文状态: paper_id={paper_id}, embedding_status=2, shard_id={shard_id}")
                                else:
                                    logger.warning(f"更新论文状态失败: paper_id={paper_id}（论文可能不存在）")
                            except Exception as e:
                                # 状态更新失败不影响主流程（向量已成功添加）
                                logger.error(f"更新论文状态异常 (paper_id={paper_id}): {e}", exc_info=True)
                        else:
                            vectorization_message = "向量化失败（可能已存在）"
                            logger.warning(f"文档摘要向量化失败: work_id={work_id}, paper_id={paper_id}")
                    else:
                        vectorization_message = "无标题无摘要，跳过向量化"
                        logger.info(f"文档无标题无摘要，跳过向量化: work_id={work_id}")
                        
                except Exception as e:
                    # 向量化失败不影响主流程
                    vectorization_message = f"向量化异常: {str(e)}"
                    logger.error(f"文档摘要向量化异常 (work_id={work_id}): {e}", exc_info=True)
            
            # 构建返回结果
            result = {
                'success': True,
                'work_id': work_id,
                'paper_id': paper_id,
                'message': f'文档添加成功: {work_id}'
            }
            
            # 添加向量化信息（如果启用）
            if self.enable_vectorization:
                result['vectorization'] = {
                    'enabled': True,
                    'success': vectorization_success,
                    'message': vectorization_message
                }
            else:
                result['vectorization'] = {
                    'enabled': False,
                    'success': False,
                    'message': '向量化功能未启用'
                }
            
            return result
            
        except Exception as e:
            # 记录详细错误信息到日志
            error_type = type(e).__name__
            error_msg = str(e)
            traceback_str = ''.join(traceback.format_exc())
            
            logger.error(f"添加文档失败 (work_id={work_id}): {error_type}: {error_msg}")
            logger.debug(f"详细堆栈跟踪:\n{traceback_str}")
            
            # ValueError（如无效 shard_id）向外抛出，供调用方 assertRaises 等使用
            if isinstance(e, ValueError):
                raise
            
            result = {
                'success': False,
                'work_id': work_id,
                'paper_id': None,
                'message': f'添加文档失败: {error_msg}',
                'error_type': error_type,
                'error_detail': error_msg
            }
            
            if include_traceback:
                result['traceback'] = traceback_str
            
            return result
    
    def _is_simplified_format(self, data: Dict[str, Any]) -> bool:
        """检测是否为简化格式输入
        
        简化格式特征：
        - 没有 default_info 字段
        - 直接包含 title、abstract、author/authors 字段
        
        Args:
            data: 输入数据字典
            
        Returns:
            bool: 是否为简化格式
        """
        # 如果有 default_info，认为是完整格式
        if 'default_info' in data:
            return False
        
        # 检查是否有 title、abstract、author/authors 字段
        has_title = 'title' in data and data.get('title')
        has_abstract = 'abstract' in data
        has_author = 'author' in data or 'authors' in data
        
        # 如果同时有 title 和 (abstract 或 author)，认为是简化格式
        return has_title and (has_abstract or has_author)
    
    def _convert_simplified_to_docset(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """将简化格式转换为完整 DocSet 格式
        
        Args:
            data: 简化格式的数据字典，包含 title、abstract、author/authors
            
        Returns:
            Dict: 完整 DocSet 格式的数据字典
        """
        # 提取字段
        title = data.get('title', '')
        abstract = data.get('abstract', '')
        
        # 处理作者字段（支持 author 或 authors）
        authors = data.get('authors', [])
        if not authors and 'author' in data:
            author = data.get('author')
            # 如果是字符串，转换为列表
            if isinstance(author, str):
                authors = [{'name': author, 'sequence': 1}]
            elif isinstance(author, dict):
                authors = [author]
            elif isinstance(author, list):
                authors = author
        
        # 确保 authors 格式正确
        processed_authors = []
        for idx, author in enumerate(authors, start=1):
            if isinstance(author, str):
                processed_authors.append({
                    'name': author,
                    'sequence': idx
                })
            elif isinstance(author, dict):
                if 'sequence' not in author:
                    author['sequence'] = idx
                processed_authors.append(author)
        
        # 构建完整 DocSet 格式
        docset_data = {
            'work_id': data.get('work_id'),  # 如果有则保留，没有则后续自动生成
            'default_info': {
                'title': title,
                'abstract': abstract,
                'authors': processed_authors,
            },
            'additional_info': {}
        }
        
        # 自动设置年份为当前年份（如果没有提供）
        if 'year' not in data or not data.get('year'):
            docset_data['default_info']['year'] = datetime.datetime.now().year
        
        # 自动设置提交日期和更新日期为当前日期（只包含日期，格式：YYYY-MM-DD）
        today_date = datetime.date.today().isoformat()
        if 'submitted_date' not in data or not data.get('submitted_date'):
            docset_data['default_info']['submitted_date'] = today_date
        if 'updated_date' not in data or not data.get('updated_date'):
            docset_data['default_info']['updated_date'] = today_date
        
        # 保留其他可能存在的字段
        for key in ['year', 'keywords', 'doi', 'arxiv_id', 'pubmed_id']:
            if key in data and data[key]:
                if key in ['year']:
                    docset_data['default_info'][key] = data[key]
                else:
                    if 'identifiers' not in docset_data['default_info']:
                        docset_data['default_info']['identifiers'] = {}
                    if key == 'arxiv_id':
                        docset_data['default_info']['identifiers']['arxiv'] = data[key]
                    elif key == 'doi':
                        docset_data['default_info']['identifiers']['doi'] = data[key]
                    elif key == 'pubmed_id':
                        docset_data['default_info']['identifiers']['pubmed'] = data[key]
        
        return docset_data
    
    def delete_doc(self, identifier: str, by_title: bool = False) -> Dict[str, Any]:
        """从索引删除文档
        
        Args:
            identifier: 文档标识符（work_id、paper_id或title）
            by_title: 如果为True，将identifier作为title查询
            
        Returns:
            Dict: 包含操作结果的字典
                {
                    'success': bool,
                    'identifier': str,
                    'message': str
                }
        """
        try:
            # 获取 paper_id 和 work_id
            paper_id = None
            work_id = None
            
            if by_title:
                # 作为title查询
                paper_info = self._get_paper_by_title(identifier)
                if paper_info:
                    paper_id = paper_info['paper_id']
                    work_id = paper_info['work_id']
                else:
                    raise ValueError(f"未找到标题为'{identifier}'的论文")
            elif identifier.isdigit():
                # 作为paper_id查询
                paper_id = int(identifier)
                paper_info = self._get_paper_by_id(paper_id)
                if paper_info:
                    work_id = paper_info['work_id']
                else:
                    raise ValueError(f"未找到paper_id: {identifier}")
            else:
                # 作为work_id查询
                work_id = identifier
                paper_info = self.metadata_db.get_paper_info_by_work_id(work_id)
                if paper_info:
                    paper_id = paper_info['paper_id']
                else:
                    raise ValueError(f"未找到work_id: {identifier}")
            
            # 删除元数据库记录（级联删除关联表）
            if work_id:
                deleted = self.metadata_db.delete_paper_by_work_id(work_id)
                if not deleted:
                    raise ValueError(f"删除论文失败: work_id={work_id}")
            
            # 从向量数据库删除（如果启用）
            vector_deletion_success = False
            if self.enable_vectorization and self.vector_db and paper_id is not None:
                try:
                    # 尝试从所有可写 shard 中删除（因为不确定数据在哪个 shard）
                    if not self.writable_shard_ids:
                        logger.warning("没有可用的可写 shard，跳过向量删除")
                    else:
                        # 尝试从每个可写 shard 删除（如果不存在会静默失败）
                        for shard_id in self.writable_shard_ids:
                            try:
                                vector_deletion_success = self.vector_db.delete(
                                    ids=[paper_id],
                                    shard_id=shard_id,
                                    auto_save=self.vector_auto_save
                                )
                                if vector_deletion_success:
                                    break  # 成功删除后退出循环
                            except Exception as e:
                                # 继续尝试下一个 shard
                                logger.debug(f"从 shard {shard_id} 删除失败: {e}")
                                continue
                    if vector_deletion_success:
                        logger.info(f"文档向量已从向量数据库删除: work_id={work_id}, paper_id={paper_id}")
                    else:
                        logger.warning(f"文档向量删除失败或不存在: work_id={work_id}, paper_id={paper_id}")
                except Exception as e:
                    # 向量删除失败不影响主流程
                    logger.error(f"文档向量删除异常 (work_id={work_id}): {e}", exc_info=True)
            
            logger.info(f"文档已从索引删除: work_id={work_id}, paper_id={paper_id}")
            
            result = {
                'success': True,
                'identifier': identifier,
                'work_id': work_id,
                'paper_id': paper_id,
                'message': f'文档删除成功: {identifier}'
            }
            
            # 添加向量删除信息（如果启用）
            if self.enable_vectorization:
                result['vector_deletion'] = {
                    'enabled': True,
                    'success': vector_deletion_success,
                    'message': '向量删除成功' if vector_deletion_success else '向量删除失败或不存在'
                }
            
            return result
            
        except Exception as e:
            logger.error(f"删除文档失败: {e}")
            return {
                'success': False,
                'identifier': identifier,
                'message': f'删除文档失败: {str(e)}'
            }
    
    def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
        ) -> List[Dict[str, Any]]:
        """搜索文档（使用向量搜索）
        
        Args:
            query: 搜索查询文本
            filters: 过滤条件字典
                {
                    'year': int,
                    'category': str,
                    'author': str,
                    ...
                }
            limit: 返回结果数量限制
            
        Returns:
            List[Dict]: 搜索结果列表，每个字典包含完整的论文信息：
                - paper_id, work_id, title, abstract, year, primary_category
                - authors: 作者列表
                - pub_info: 发表信息
                - additional_info: 额外信息（包含 mesh_headings 等）
                - categories, citations, fields 等关联信息
                - similarity: 相似度分数
        """
        if not self.enable_vectorization or not self.vector_db:
            raise RuntimeError("向量搜索功能未启用，无法执行搜索")
        
        try:
            search_shard_ids = self.search_shard_ids
            if not search_shard_ids:
                raise ValueError("没有可用的 shard 进行搜索")
            
            # 使用向量搜索，获取更多候选结果（考虑 filters 过滤）
            search_limit = limit * 2 if filters else limit
            vector_results = self.vector_db.search(
                query, 
                top_k=search_limit,
                shard_ids=search_shard_ids
            )
            
            # 将 VectorEntry 结果转换为字典列表
            results = self._vector_results_to_dicts(vector_results)
            
            # 应用 filters（如果提供）
            if filters:
                results = self._apply_filters(results, filters)
            
            # 过滤掉没有 abstract 的结果
            results = [r for r in results if r.get('abstract') and r.get('abstract').strip()]
            
            # 限制结果数量
            results = results[:limit]

            
            logger.info(f"向量搜索完成: query='{query}', 找到 {len(results)} 个结果")
            
            return results
            
        except Exception as e:
            logger.error(f"向量搜索失败: {e}", exc_info=True)
            raise
    
    def _vector_results_to_dicts(
        self,
        vector_results: List[Tuple[VectorEntry, float]]
    ) -> List[Dict[str, Any]]:
        """将向量搜索结果转换为字典列表
        
        Args:
            vector_results: 向量搜索结果列表，格式为 List[Tuple[VectorEntry, float]]
            
        Returns:
            List[Dict]: 包含完整元数据的字典列表（包含 authors、pub_info、additional_info 等）
        """
        if not vector_results:
            return []
        
        results = []
        
        # 对每个结果获取完整的论文信息
        for entry, similarity in vector_results:
            work_id = entry.work_id
            if not work_id:
                logger.warning(f"VectorEntry has no work_id, skipping")
                continue
            
            # 使用 metadata_db.read_paper 获取完整的论文信息
            paper_data = self.metadata_db.read_paper(work_id=work_id)
            
            if paper_data:
                # 添加相似度分数
                paper_data['similarity'] = float(similarity)
                results.append(paper_data)
            else:
                logger.warning(f"Work ID {work_id} not found in database")
        
        return results
    
    def _apply_filters(
        self,
        results: List[Dict[str, Any]],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """应用过滤条件
        
        Args:
            results: 搜索结果列表
            filters: 过滤条件字典
            
        Returns:
            List[Dict]: 过滤后的结果列表
        """
        filtered_results = results
        
        # 年份过滤
        if 'year' in filters:
            year = filters['year']
            filtered_results = [r for r in filtered_results if r.get('year') == year]
        
        # 分类过滤
        if 'category' in filters:
            category = filters['category']
            paper_ids = [r['paper_id'] for r in filtered_results]
            
            if paper_ids:
                engine = get_db_engine()
                with engine.connect() as conn:
                    result = conn.execute(
                        text("""
                            SELECT DISTINCT pc.paper_id
                            FROM paper_categories pc
                            JOIN categories c ON pc.cat_id = c.cat_id
                            WHERE pc.paper_id = ANY(:paper_ids)
                            AND c.subdomain = :category
                        """),
                        {"paper_ids": paper_ids, "category": category}
                    )
                    valid_paper_ids = {row[0] for row in result}
                    filtered_results = [r for r in filtered_results if r['paper_id'] in valid_paper_ids]
        
        # 作者过滤
        if 'author' in filters:
            author = filters['author']
            paper_ids = [r['paper_id'] for r in filtered_results]
            
            if paper_ids:
                engine = get_db_engine()
                with engine.connect() as conn:
                    result = conn.execute(
                        text("""
                            SELECT DISTINCT paper_id
                            FROM paper_author_affiliation
                            WHERE paper_id = ANY(:paper_ids)
                            AND authors::text ILIKE :author_pattern
                        """),
                        {"paper_ids": paper_ids, "author_pattern": f"%{author}%"}
                    )
                    valid_paper_ids = {row[0] for row in result}
                    filtered_results = [r for r in filtered_results if r['paper_id'] in valid_paper_ids]
        
        return filtered_results
    
    def get_doc_by_id_identifier(self, identifier: str, by_title: bool = False) -> Optional[Dict[str, Any]]:
        """通过ID标识符或标题获取文档
        
        Args:
            identifier: 文档标识符（work_id、paper_id或title）
            by_title: 如果为True，将identifier作为title查询
            
        Returns:
            Dict: 文档数据字典，如果不存在则返回None
        """
        try:
            if by_title:
                return self._get_paper_by_title(identifier)
            elif identifier.isdigit():
                return self._get_paper_by_id(int(identifier))
            else:
                # 作为work_id查询
                return self.metadata_db.get_paper_info_by_work_id(identifier)
                
        except Exception as e:
            logger.error(f"获取文档失败: {e}")
            return None
    
    def _get_paper_by_id(self, paper_id: int) -> Optional[Dict[str, Any]]:
        """通过 paper_id 获取论文信息
        
        Args:
            paper_id: 论文ID
            
        Returns:
            Dict: 论文信息字典，如果不存在则返回None
        """
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        p.*,
                        paa.authors,
                        (SELECT array_agg(c.subdomain) 
                         FROM paper_categories pc 
                         JOIN categories c ON pc.cat_id = c.cat_id 
                         WHERE pc.paper_id = p.paper_id) as categories
                    FROM papers p
                    LEFT JOIN paper_author_affiliation paa ON p.paper_id = paa.paper_id
                    WHERE p.paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            )
            row = result.fetchone()
            
            if not row:
                return None
            
            columns = list(result.keys())
            doc_data = dict(zip(columns, row))
            
            logger.info(f"获取文档成功: paper_id={paper_id}")
            return doc_data
    
    def _get_paper_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """通过 title 获取论文信息
        
        Args:
            title: 论文标题
            
        Returns:
            Dict: 论文信息字典，如果不存在则返回None
        """
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        p.*,
                        paa.authors,
                        (SELECT array_agg(c.subdomain) 
                         FROM paper_categories pc 
                         JOIN categories c ON pc.cat_id = c.cat_id 
                         WHERE pc.paper_id = p.paper_id) as categories
                    FROM papers p
                    LEFT JOIN paper_author_affiliation paa ON p.paper_id = paa.paper_id
                    WHERE p.title = :title
                    LIMIT 1
                """),
                {"title": title}
            )
            row = result.fetchone()
            
            if not row:
                return None
            
            columns = list(result.keys())
            doc_data = dict(zip(columns, row))
            
            logger.info(f"获取文档成功: title={title}")
            return doc_data
    
    def read_paper(
        self,
        work_id: Optional[str] = None,
        paper_id: Optional[int] = None,
        title: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """读取论文记录（包含所有关联数据）
        
        Args:
            work_id: 工作ID
            paper_id: 论文ID（数据库内部ID）
            title: 论文标题（精确匹配）
            
        Returns:
            Optional[Dict]: 论文数据字典，包含所有关联信息，如果不存在则返回None
            如果通过title查询且有多篇论文，返回第一个匹配的结果
            
        Raises:
            ValueError: 如果提供了多个参数或没有提供任何参数
        """
        try:
            return self.metadata_db.read_paper(
                work_id=work_id,
                paper_id=paper_id,
                title=title
            )
        except Exception as e:
            logger.error(f"读取论文失败: {e}", exc_info=True)
            return None
    
    def search_by_condition(
        self,
        title: Optional[str] = None,
        author: Optional[str] = None,
        category: Optional[str] = None,
        year: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """基于元数据条件搜索论文（不包含全文检索）
        
        此方法用于基于论文的元数据（标题、作者、分类、年份等）进行搜索。
        与向量检索方法（search）区分开来。
        
        Args:
            title: 标题关键词（支持模糊匹配，ILIKE）
            author: 作者名称（支持模糊匹配，在 authors JSONB 字段中搜索）
            category: 分类子领域（精确匹配 subdomain）
            year: 发表年份（精确匹配）
            limit: 返回结果数量限制，默认 100
            
        Returns:
            List[Dict]: 论文数据列表，每个论文包含完整的关联信息
                - authors: 作者列表
                - categories: 分类列表
                - pub_info: 发表信息
                - citations: 引用信息
                - version_count: 版本数量
                - fields: 领域列表
                
        Note:
            此方法仅基于元数据进行搜索，不涉及全文检索或向量检索。
            如需基于内容进行搜索，请使用 search() 方法。
        """
        try:
            return self.metadata_db.search_by_condition(
                title=title,
                author=author,
                category=category,
                year=year,
                limit=limit
            )
        except Exception as e:
            logger.error(f"条件搜索失败: {e}", exc_info=True)
            return []
    
    def get_daily_updated_papers_detail(self, date: str) -> Dict[str, Any]:
        """获取指定日期的更新论文详情

        Args:
            date: 日期（YYYY-MM-DD）

        Returns:
            Dict: 包含该日期所有更新论文的详情和统计信息
                - update_date: 更新日期
                - paper_count: 论文数量
                - papers: 论文列表（包含 paper_id, work_id, title, created_at, updated_at, imported_at）
        """
        try:
            return self.metadata_db.get_daily_updated_papers_detail(date)
        except Exception as e:
            logger.error(f"获取每日更新论文详情失败: {e}", exc_info=True)
            return {
                "update_date": date,
                "paper_count": 0,
                "papers": []
            }

    def get_daily_updated_papers(self, date: str) -> Dict[str, Any]:
        """获取指定日期的更新论文（兼容性方法）

        此方法为测试用例提供兼容接口，内部调用 get_daily_updated_papers_detail

        Args:
            date: 日期（YYYY-MM-DD）

        Returns:
            Dict: 包含该日期所有更新论文的详情和统计信息
                - update_date: 更新日期
                - paper_count: 论文数量
                - papers: 论文列表
        """
        return self.get_daily_updated_papers_detail(date)

    def batch_import_from_folder(
        self,
        folder_path: str,
        date: Optional[str] = None,
        skip_existing: bool = True,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """批量导入文件夹中的 PubMed JSON 文件
        
        Args:
            folder_path: 文件夹路径
            date: 更新日期（YYYY-MM-DD），默认今天
            skip_existing: 是否跳过已存在的文件（通过 work_id 或 pubmed_id 判断）
            limit: 导入数量限制（None 表示不限制）
            
        Returns:
            Dict: 包含导入统计和 daily_update_id
                {
                    'success': bool,
                    'daily_update_id': int,
                    'total_files': int,
                    'success_count': int,
                    'fail_count': int,
                    'skip_count': int,
                    'message': str,
                    'errors': List[Dict]  # 错误详情列表
                }
        """
        from pathlib import Path
        from docset_hub.input_adapters import JSONAdapter
        
        import_start_time = datetime.datetime.now()
        folder = Path(folder_path)
        
        if not folder.exists() or not folder.is_dir():
            return {
                'success': False,
                'daily_update_id': None,
                'total_files': 0,
                'success_count': 0,
                'fail_count': 0,
                'skip_count': 0,
                'message': f'文件夹不存在或不是目录: {folder_path}',
                'errors': []
            }
        
        # 确定更新日期
        if date is None:
            update_date = datetime.date.today().isoformat()
        else:
            update_date = date
        
        # 获取所有 JSON 文件
        json_files = list(folder.rglob('*.json'))
        if limit:
            json_files = json_files[:limit]
        
        total_files = len(json_files)
        if total_files == 0:
            return {
                'success': True,
                'daily_update_id': None,
                'total_files': 0,
                'success_count': 0,
                'fail_count': 0,
                'skip_count': 0,
                'message': '文件夹中没有找到 JSON 文件',
                'errors': []
            }
        
        # 初始化适配器
        adapter = JSONAdapter()
        
        # 创建初始记录（状态为 pending）
        daily_update_id = self.metadata_db.insert_daily_update(
            update_date=update_date,
            total_files=total_files,
            success_count=0,
            fail_count=0,
            skip_count=0,
            import_start_time=import_start_time,
            import_end_time=None,
            status='pending',
            error_summary=None
        )
        
        # 统计变量
        success_count = 0
        fail_count = 0
        skip_count = 0
        errors = []
        
        # 处理每个 JSON 文件
        for idx, json_file in enumerate(json_files, 1):
            try:
                # 解析 JSON 文件
                doc_data = adapter.parse(str(json_file))
                
                # 检查是否已存在（如果启用跳过）
                if skip_existing:
                    work_id = doc_data.get('work_id')
                    pubmed_id = None
                    
                    # 从 default_info 或顶层获取 pubmed_id
                    if 'default_info' in doc_data and 'identifiers' in doc_data['default_info']:
                        pubmed_id = doc_data['default_info']['identifiers'].get('pubmed')
                    elif 'identifiers' in doc_data:
                        pubmed_id = doc_data['identifiers'].get('pubmed')
                    
                    # 检查是否已存在
                    if work_id:
                        existing = self.metadata_db.get_paper_info_by_work_id(work_id)
                        if existing:
                            skip_count += 1
                            logger.info(f"[{idx}/{total_files}] 跳过已存在文件: {json_file.name} (work_id={work_id})")
                            continue
                    
                    if pubmed_id:
                        # 通过 pubmed_id 查询
                        engine = get_db_engine()
                        with engine.connect() as conn:
                            result = conn.execute(
                                text("SELECT paper_id FROM papers WHERE pubmed_id = :pubmed_id"),
                                {"pubmed_id": pubmed_id}
                            )
                            if result.fetchone():
                                skip_count += 1
                                logger.info(f"[{idx}/{total_files}] 跳过已存在文件: {json_file.name} (pubmed_id={pubmed_id})")
                                continue
                
                # 导入文档
                result = self.add_doc(doc_data, include_traceback=False)
                
                if result['success']:
                    success_count += 1
                    if idx % 100 == 0:
                        logger.info(f"[{idx}/{total_files}] 已处理 {success_count} 个成功, {fail_count} 个失败, {skip_count} 个跳过")
                else:
                    fail_count += 1
                    error_info = {
                        'file': str(json_file),
                        'error': result.get('message', '未知错误'),
                        'error_type': result.get('error_type', 'UnknownError'),
                        'error_detail': result.get('error_detail', '')
                    }
                    errors.append(error_info)
                    logger.warning(f"[{idx}/{total_files}] 导入失败: {json_file.name} - {result.get('message')}")
                    
            except Exception as e:
                fail_count += 1
                error_info = {
                    'file': str(json_file),
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'error_detail': str(e)
                }
                errors.append(error_info)
                logger.error(f"[{idx}/{total_files}] 处理文件异常: {json_file.name} - {e}", exc_info=True)
        
        # 计算导入结束时间
        import_end_time = datetime.datetime.now()
        
        # 确定最终状态
        if fail_count == 0 and success_count > 0:
            final_status = 'completed'
        elif success_count == 0 and fail_count > 0:
            final_status = 'failed'
        else:
            final_status = 'completed'  # 部分成功也算完成
        
        # 构建错误摘要
        error_summary = None
        if errors:
            error_summary = {
                'total_errors': len(errors),
                'error_types': {},
                'sample_errors': errors[:10]  # 只保存前10个错误作为示例
            }
            for error in errors:
                error_type = error.get('error_type', 'UnknownError')
                error_summary['error_types'][error_type] = error_summary['error_types'].get(error_type, 0) + 1
        
        # 更新每日更新记录
        self.metadata_db.insert_daily_update(
            update_date=update_date,
            total_files=total_files,
            success_count=success_count,
            fail_count=fail_count,
            skip_count=skip_count,
            import_start_time=import_start_time,
            import_end_time=import_end_time,
            status=final_status,
            error_summary=error_summary
        )
        
        # 计算耗时
        duration = (import_end_time - import_start_time).total_seconds()
        
        logger.info(
            f"批量导入完成: 日期={update_date}, "
            f"总计={total_files}, 成功={success_count}, 失败={fail_count}, 跳过={skip_count}, "
            f"耗时={duration:.2f}秒"
        )
        
        return {
            'success': True,
            'daily_update_id': daily_update_id,
            'total_files': total_files,
            'success_count': success_count,
            'fail_count': fail_count,
            'skip_count': skip_count,
            'duration_seconds': duration,
            'message': f'批量导入完成: 成功 {success_count}, 失败 {fail_count}, 跳过 {skip_count}',
            'errors': errors[:100]  # 最多返回100个错误详情
        }

