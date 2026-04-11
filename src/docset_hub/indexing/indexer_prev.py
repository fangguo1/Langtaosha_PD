"""PaperIndexer - 论文索引器

实现文档的索引、搜索、删除等功能
"""

import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

from ..storage.metadata_db import MetadataDB
from ..storage.json_storage import JSONStorage
from ..storage.vector_db import VectorDB
from ..input_adapters import JSONAdapter
from ..metadata.validator import MetadataValidator
from config.config_loader import get_db_connection as get_connection
from config.vector_config import VECTOR_DB_PATH, GRITLM_MODEL_NAME, GRITLM_MODEL_PATH, VECTOR_DIM

logger = logging.getLogger(__name__)


class PaperIndexer:
    """论文索引器
    
    提供文档的索引、搜索、删除、查询等功能
    """
    
    def __init__(self, enable_vectorization: bool = True):
        """初始化索引器
        
        Args:
            enable_vectorization: 是否启用向量化功能（默认True）
        """
        self.metadata_db = MetadataDB()
        self.json_storage = JSONStorage()
        self.json_adapter = JSONAdapter()
        self.validator = MetadataValidator()
        
        # 初始化向量数据库（如果启用）
        self.vector_db = None
        self.enable_vectorization = enable_vectorization
        if enable_vectorization:
            try:
                self.vector_db = VectorDB(
                    db_path=VECTOR_DB_PATH,
                    model_name=GRITLM_MODEL_NAME,
                    model_path=GRITLM_MODEL_PATH,
                    vector_dim=VECTOR_DIM
                )
                logger.info("PaperIndexer initialized with vector database")
            except Exception as e:
                logger.warning(f"向量数据库初始化失败，将禁用向量化功能: {e}")
                self.enable_vectorization = False
                self.vector_db = None
        else:
            logger.info("PaperIndexer initialized (vectorization disabled)")
    
    def add_doc(self, doc_data: Dict[str, Any], include_traceback: bool = False) -> Dict[str, Any]:
        """添加文档到索引
        
        Args:
            doc_data: DocSet格式的文档数据字典
            include_traceback: 是否在返回结果中包含堆栈跟踪（默认False，适合批量处理）
            
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
        work_id = doc_data.get('work_id')
        try:
            # 验证数据格式
            is_valid, errors = self.validator.validate(doc_data)
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
            
            if not work_id:
                error_msg = "缺少work_id字段"
                logger.error(f"添加文档失败: {error_msg}")
                result = {
                    'success': False,
                    'work_id': None,
                    'paper_id': None,
                    'message': error_msg,
                    'error_type': 'ValueError',
                    'error_detail': 'work_id字段缺失或为空'
                }
                if include_traceback:
                    result['traceback'] = ''.join(traceback.format_exc())
                return result
            
            # 保存JSON文件（中间步骤）
            json_path = self.json_storage.save(doc_data)
            logger.info(f"JSON文件已保存: {json_path}")
            
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
                    
                    # 如果有摘要，进行向量化
                    if abstract:
                        # 组合标题和摘要（可选：只使用摘要）
                        text_to_embed = abstract
                        # 如果需要包含标题，可以使用：text_to_embed = f"{title}\n\n{abstract}"
                        
                        # 添加到向量数据库
                        vector_success = self.vector_db.add_document(
                            work_id=work_id,
                            text_to_emb=text_to_embed,
                            text_type="abstract"
                        )
                        
                        if vector_success:
                            vectorization_success = True
                            vectorization_message = "向量化成功"
                            logger.info(f"文档摘要已向量化并存储: work_id={work_id}")
                        else:
                            vectorization_message = "向量化失败（可能已存在）"
                            logger.warning(f"文档摘要向量化失败: work_id={work_id}")
                    else:
                        vectorization_message = "无摘要内容，跳过向量化"
                        logger.info(f"文档无摘要，跳过向量化: work_id={work_id}")
                        
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
            conn = get_connection()
            cursor = conn.cursor()
            
            if by_title:
                # 作为title查询
                cursor.execute("SELECT paper_id, work_id FROM papers WHERE title = %s LIMIT 1", (identifier,))
                result = cursor.fetchone()
                if result:
                    paper_id = result[0]
                    work_id = result[1]
                else:
                    raise ValueError(f"未找到标题为'{identifier}'的论文")
            elif identifier.isdigit():
                # 尝试作为paper_id查询
                cursor.execute("SELECT work_id FROM papers WHERE paper_id = %s", (int(identifier),))
                result = cursor.fetchone()
                if result:
                    work_id = result[0]
                    paper_id = int(identifier)
                else:
                    raise ValueError(f"未找到paper_id: {identifier}")
            else:
                # 作为work_id查询
                cursor.execute("SELECT paper_id FROM papers WHERE work_id = %s", (identifier,))
                result = cursor.fetchone()
                if result:
                    work_id = identifier
                    paper_id = result[0]
                else:
                    raise ValueError(f"未找到work_id: {identifier}")
            
            cursor.close()
            conn.close()
            
            # 删除数据库记录（级联删除关联表）
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM papers WHERE paper_id = %s", (paper_id,))
            conn.commit()
            cursor.close()
            conn.close()
            
            # 删除JSON文件
            if work_id:
                self.json_storage.delete(work_id)
            
            # 从向量数据库删除（如果启用）
            vector_deletion_success = False
            if self.enable_vectorization and self.vector_db and work_id:
                try:
                    vector_deletion_success = self.vector_db.delete_document(work_id)
                    if vector_deletion_success:
                        logger.info(f"文档向量已从向量数据库删除: work_id={work_id}")
                    else:
                        logger.warning(f"文档向量删除失败或不存在: work_id={work_id}")
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
        """搜索文档
        
        Args:
            query: 搜索关键词（标题、摘要）
            filters: 过滤条件字典
                {
                    'year': int,
                    'category': str,
                    'author': str,
                    ...
                }
            limit: 返回结果数量限制
            
        Returns:
            List[Dict]: 搜索结果列表
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            conditions = []
            params = []
            
            # 搜索条件（标题和摘要）
            if query:
                conditions.append("""
                    (title ILIKE %s OR abstract ILIKE %s)
                """)
                params.extend([f"%{query}%", f"%{query}%"])
            
            # 过滤条件
            if filters:
                if 'year' in filters:
                    conditions.append("year = %s")
                    params.append(filters['year'])
                
                if 'category' in filters:
                    conditions.append("""
                        EXISTS (
                            SELECT 1 FROM paper_categories pc 
                            JOIN categories c ON pc.cat_id = c.cat_id 
                            WHERE pc.paper_id = papers.paper_id 
                            AND c.subdomain = %s
                        )
                    """)
                    params.append(filters['category'])
                
                if 'author' in filters:
                    conditions.append("""
                        EXISTS (
                            SELECT 1 FROM paper_author_affiliation 
                            WHERE paper_id = papers.paper_id 
                            AND authors::text ILIKE %s
                        )
                    """)
                    params.append(f"%{filters['author']}%")
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query_sql = f"""
                SELECT 
                    paper_id,
                    work_id,
                    title,
                    abstract,
                    year,
                    primary_category
                FROM papers
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
            """
            params.append(limit)
            
            cursor.execute(query_sql, params)
            rows = cursor.fetchall()
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            conn.close()
            
            logger.info(f"搜索完成: query='{query}', 找到 {len(results)} 个结果")
            
            return results
            
        except Exception as e:
            logger.error(f"搜索失败: {e}")
            return []
    
    def get_doc_by_id_identifier(self, identifier: str, by_title: bool = False) -> Optional[Dict[str, Any]]:
        """通过ID标识符或标题获取文档
        
        Args:
            identifier: 文档标识符（work_id、paper_id或title）
            by_title: 如果为True，将identifier作为title查询
            
        Returns:
            Dict: 文档数据字典，如果不存在则返回None
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            if by_title:
                # 作为title查询
                cursor.execute("""
                    SELECT 
                        p.*,
                        paa.authors,
                        (SELECT array_agg(c.subdomain) 
                         FROM paper_categories pc 
                         JOIN categories c ON pc.cat_id = c.cat_id 
                         WHERE pc.paper_id = p.paper_id) as categories
                    FROM papers p
                    LEFT JOIN paper_author_affiliation paa ON p.paper_id = paa.paper_id
                    WHERE p.title = %s
                    LIMIT 1
                """, (identifier,))
            elif identifier.isdigit():
                # 作为paper_id查询
                cursor.execute("""
                    SELECT 
                        p.*,
                        paa.authors,
                        (SELECT array_agg(c.subdomain) 
                         FROM paper_categories pc 
                         JOIN categories c ON pc.cat_id = c.cat_id 
                         WHERE pc.paper_id = p.paper_id) as categories
                    FROM papers p
                    LEFT JOIN paper_author_affiliation paa ON p.paper_id = paa.paper_id
                    WHERE p.paper_id = %s
                """, (int(identifier),))
            else:
                # 作为work_id查询
                cursor.execute("""
                    SELECT 
                        p.*,
                        paa.authors,
                        (SELECT array_agg(c.subdomain) 
                         FROM paper_categories pc 
                         JOIN categories c ON pc.cat_id = c.cat_id 
                         WHERE pc.paper_id = p.paper_id) as categories
                    FROM papers p
                    LEFT JOIN paper_author_affiliation paa ON p.paper_id = paa.paper_id
                    WHERE p.work_id = %s
                """, (identifier,))
            
            row = cursor.fetchone()
            if not row:
                cursor.close()
                conn.close()
                return None
            
            columns = [desc[0] for desc in cursor.description]
            doc_data = dict(zip(columns, row))
            
            cursor.close()
            conn.close()
            
            logger.info(f"获取文档成功: identifier={identifier}")
            
            return doc_data
            
        except Exception as e:
            logger.error(f"获取文档失败: {e}")
            return None

