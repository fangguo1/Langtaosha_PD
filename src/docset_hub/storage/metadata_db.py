"""元数据库操作类"""
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from config.config_loader import init_config, get_db_engine
from ..metadata.transformer import MetadataTransformer

import logging

class MetadataDB:
    """元数据库操作类
    
    负责将DocSet格式的数据存储到PostgreSQL数据库
    支持多数据库配置（PubMed 和 arXiv）
    """
    
    def __init__(self, config_path: Optional[Path] = None, db_key: str = 'metadata_db'):
        """初始化元数据库操作器
        
        Args:
            config_path: 配置文件路径，如果为 None 则自动查找
            db_key: 数据库配置键名，默认为 'metadata_db'
                    支持的值：
                    - 'metadata_db': 统一元数据库（存放 arxiv 和 pubmed 数据）
                    - 'metadata_db_pubmed': PubMed 数据库（可选，用于单独访问）
                    - 'metadata_db_arxiv': arXiv 数据库（可选，用于单独访问）
        """
        # 确保配置已初始化
        if config_path is None:
            raise ValueError("未找到配置文件 config.yaml，请指定 config_path")
        init_config(config_path)
        self.db_key = db_key
        self.transformer = MetadataTransformer()
        self.engine = get_db_engine(db_key=db_key)
        # 在初始化时检查字段存在性（使用独立连接，避免事务问题）
        self._has_new_fields_cache = self._check_new_fields_exist()
    
    def get_or_create_category(self, conn: Connection, domain: str, subdomain: str) -> int:
        """获取或创建分类，返回cat_id
        
        Args:
            conn: SQLAlchemy 连接对象
            domain: 领域
            subdomain: 子领域
            
        Returns:
            int: 分类ID
        """
        result = conn.execute(
            text("SELECT cat_id FROM categories WHERE domain = :domain AND subdomain = :subdomain"),
            {"domain": domain, "subdomain": subdomain}
        )
        row = result.fetchone()
        if row:
            return row[0]
        
        result = conn.execute(
            text("INSERT INTO categories (domain, subdomain) VALUES (:domain, :subdomain) RETURNING cat_id"),
            {"domain": domain, "subdomain": subdomain}
        )
        return result.scalar()
    
    def get_or_create_venue(self, conn: Connection, venue_name: str, venue_type: str) -> int:
        """获取或创建venue，返回venue_id
        
        Args:
            conn: SQLAlchemy 连接对象
            venue_name: 场所名称（不能为None）
            venue_type: 场所类型（如果为None则使用'unknown'）
            
        Returns:
            int: 场所ID
            
        Raises:
            ValueError: 如果venue_name为None或空字符串
        """
        if not venue_name:
            raise ValueError("venue_name不能为空")
        
        # 如果venue_type为None，使用默认值
        if not venue_type:
            venue_type = 'unknown'
        
        result = conn.execute(
            text("SELECT venue_id FROM venues WHERE venue_name = :venue_name"),
            {"venue_name": venue_name}
        )
        row = result.fetchone()
        if row:
            return row[0]
        
        result = conn.execute(
            text("INSERT INTO venues (venue_name, venue_type) VALUES (:venue_name, :venue_type) RETURNING venue_id"),
            {"venue_name": venue_name, "venue_type": venue_type}
        )
        return result.scalar()
    
    def get_or_create_field(self, conn: Connection, field_name: str, field_name_en: Optional[str] = None) -> int:
        """获取或创建field，返回field_id
        
        Args:
            conn: SQLAlchemy 连接对象
            field_name: 领域名称
            field_name_en: 领域英文名称
            
        Returns:
            int: 领域ID
        """
        result = conn.execute(
            text("SELECT field_id FROM fields WHERE field_name = :field_name"),
            {"field_name": field_name}
        )
        row = result.fetchone()
        if row:
            return row[0]
        
        result = conn.execute(
            text("INSERT INTO fields (field_name, field_name_en) VALUES (:field_name, :field_name_en) RETURNING field_id"),
            {"field_name": field_name, "field_name_en": field_name_en}
        )
        return result.scalar()
    
    def check_paper_existence(self, conn: Connection, normalized_ids: Dict[str, Optional[str]]) -> Optional[tuple[str, int]]:
        """检查论文是否存在（根据规范化后的 external IDs）
        
        按照 ingest 流程 Step 3，查询 papers 表检查是否存在已有记录。
        只要命中任意一个 external id，即可复用对应的 work_id。
        
        Args:
            conn: SQLAlchemy 连接对象
            normalized_ids: 规范化后的 external IDs 字典
                {
                    'arxiv_id': '2301.12345',  # base ID，无版本号
                    'doi': '10.1145/xxx',
                    'pubmed_id': '12345678',
                    'semantic_scholar_id': 'abc123'
                }
        
        Returns:
            Optional[tuple]: 若存在返回 (work_id, paper_id)，否则返回 None
        """
        # 动态构建 WHERE 子句，只查询非空的 external IDs
        conditions = []
        params = {}
        
        if normalized_ids.get('arxiv_id'):
            conditions.append("(arxiv_id = :arxiv_id AND arxiv_id IS NOT NULL)")
            params['arxiv_id'] = normalized_ids['arxiv_id']
        
        if normalized_ids.get('doi'):
            conditions.append("(doi = :doi AND doi IS NOT NULL)")
            params['doi'] = normalized_ids['doi']
        
        if normalized_ids.get('pubmed_id'):
            conditions.append("(pubmed_id = :pubmed_id AND pubmed_id IS NOT NULL)")
            params['pubmed_id'] = normalized_ids['pubmed_id']
        
        if normalized_ids.get('semantic_scholar_id'):
            conditions.append("(semantic_scholar_id = :semantic_scholar_id AND semantic_scholar_id IS NOT NULL)")
            params['semantic_scholar_id'] = normalized_ids['semantic_scholar_id']
        
        # 如果没有有效的 external IDs，返回 None
        if not conditions:
            return None
        
        # 构建 SQL 查询（使用 OR 条件）
        where_clause = " OR ".join(conditions)
        sql = f"""
            SELECT work_id, paper_id
            FROM papers
            WHERE {where_clause}
            LIMIT 1
        """
        
        result = conn.execute(text(sql), params)
        row = result.fetchone()
        
        if row:
            return (row[0], row[1])  # (work_id, paper_id)
        
        return None
    
    
    # insert_paper:插入论文数据到数据库, 它的逻辑为，对于paper_data, 先判断arxiv_id、pubmed_id, doi是否存在，如果存在，则更新，否则插入。这些值通过搜索papers表来获得。 插入或者更新，
    def insert_paper(self, data: Dict[str, Any]) -> int:
        """插入论文数据到数据库
        
        按照 ingest 流程实现：
        1. 获取规范化后的 external IDs
        2. 通过 check_paper_existence() 检查是否存在
        3. 若存在：复用查到的 work_id 和 paper_id，更新记录
        4. 若不存在：使用 JSON 中的 work_id，插入新记录
        5. 处理并发冲突（唯一约束冲突）
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            int: 插入或更新的paper_id
        """
        # 转换为数据库格式（包含规范化后的 external IDs）
        db_data = self.transformer.transform_to_db_format(data)
        # 获取规范化后的 external IDs（用于查询）
        normalized_ids = self.transformer.get_normalized_external_ids(data)
        with self.engine.connect() as conn:
            try:
                papers_data = db_data['papers']
                json_work_id = papers_data.get('work_id')
                
                # Step 3 & 4: 检查论文是否存在（通过规范化后的 external IDs）
                existing = self.check_paper_existence(conn, normalized_ids)
                
                if existing:
                    # 命中：复用查到的 work_id 和 paper_id
                    existing_work_id, paper_id = existing
                    logging.info(f"找到已存在的论文: work_id={existing_work_id}, paper_id={paper_id}")
                    
                    # 使用查到的 work_id 更新 db_data（确保使用数据库中的 work_id）
                    papers_data['work_id'] = existing_work_id
                    db_data['papers'] = papers_data
                    
                    # 更新现有记录
                    self._update_paper(conn, paper_id, db_data)
                    logging.info(f"✅更新现有记录: work_id={existing_work_id}, paper_id={paper_id}")
                else:
                    # 未命中：使用 JSON 中的 work_id 插入新记录
                    if not json_work_id:
                        logging.error("没有work_id，跳过该记录")
                        raise ValueError("work_id 不能为空")
                    
                    try:
                        # 插入新记录
                        paper_id = self._insert_new_paper(conn, db_data)
                        logging.info(f"✅插入新记录: work_id={json_work_id}, paper_id={paper_id}")
                    except Exception as insert_error:
                        # 其他类型的异常，直接抛出
                        
                        logging.error(f"插入新记录失败: {str(insert_error)}")   
                        raise insert_error
                    '''    
                    except IntegrityError as insert_error:
                        # 处理并发冲突（唯一约束冲突）
                        # 例如：两个进程同时 ingest 同一个 DOI，都先查未命中，各自尝试插入
                        # 捕获唯一约束冲突，回读已有记录
                        logging.warning(f"插入时发生唯一约束冲突: {insert_error}，尝试回读已有记录")
                        
                        # 重新查询（可能另一个进程已经插入）
                        existing = self.check_paper_existence(conn, normalized_ids)
                        if existing:
                            existing_work_id, paper_id = existing
                            logging.info(f"回读成功: work_id={existing_work_id}, paper_id={paper_id}")
                            
                            # 使用查到的 work_id 更新并重试
                            papers_data['work_id'] = existing_work_id
                            db_data['papers'] = papers_data
                            self._update_paper(conn, paper_id, db_data)
                        else:
                            # 如果回读也失败，抛出原始异常
                            raise insert_error
                    '''
                    
                
                conn.commit()
                return paper_id
                
            except Exception as e:
                conn.rollback()
                logging.error(f"插入论文失败: {str(e)}")
                raise e
    
    def _insert_new_paper(self, conn: Connection, db_data: Dict[str, Any]) -> int:
        """插入新论文
        
        Args:
            conn: SQLAlchemy 连接对象
            db_data: 数据库格式的数据字典
            
        Returns:
            int: paper_id
        """
        papers_data = db_data['papers']
        
        # 检查新字段是否存在
        has_new_fields = self._has_new_fields_cache
        
        # 基础字段列表（始终包含）
        base_columns = [
            'work_id', 'arxiv_id', 'title', 'abstract', 'keywords', 'pdf_url', 'source_url',
            'year', 'is_preprint', 'is_published', 'source', 'platform',
            'primary_category', 'doi', 'semantic_scholar_id', 'pubmed_id', 'comments',
            'contribution_types', 'created_at', 'updated_at', 'imported_at'
        ]
        
        # 新字段列表（条件包含）
        new_columns = [
            'paper_type', 'primary_field', 'target_application_domain', 'is_llm_era', 'short_reasoning'
        ]
        
        # 根据字段存在性动态构建列名和占位符
        all_columns = base_columns + (new_columns if has_new_fields else [])
        all_placeholders = [f':{col}' for col in all_columns]
        # imported_at 使用 CURRENT_TIMESTAMP，不是占位符
        all_placeholders[all_columns.index('imported_at')] = 'CURRENT_TIMESTAMP'
        
        # 构建 SQL 语句
        columns_str = ', '.join(all_columns)
        placeholders_str = ', '.join(all_placeholders)
        sql = f"""
            INSERT INTO papers (
                {columns_str}
            ) VALUES (
                {placeholders_str}
            ) RETURNING paper_id
        """
        
        # 构建基础参数字典
        params = {
            "work_id": papers_data.get('work_id'),
            "arxiv_id": papers_data.get('arxiv_id'),
            "title": papers_data.get('title'),
            "abstract": papers_data.get('abstract'),
            "keywords": papers_data.get('keywords'),
            "pdf_url": papers_data.get('pdf_url'),
            "source_url": papers_data.get('source_url'),
            "year": papers_data.get('year'),
            "is_preprint": papers_data.get('is_preprint'),
            "is_published": papers_data.get('is_published'),
            "source": papers_data.get('source'),
            "platform": papers_data.get('platform'),
            "primary_category": papers_data.get('primary_category'),
            "doi": papers_data.get('doi'),
            "semantic_scholar_id": papers_data.get('semantic_scholar_id'),
            "pubmed_id": papers_data.get('pubmed_id'),
            "comments": papers_data.get('comments'),
            "contribution_types": json.dumps(papers_data.get('contribution_types', [])),
            "created_at": papers_data.get('created_at'),
            "updated_at": papers_data.get('updated_at'),
        }
        
        # 仅在字段存在时添加新字段参数
        if has_new_fields:
            params.update({
                "paper_type": papers_data.get('paper_type'),
                "primary_field": papers_data.get('primary_field'),
                "target_application_domain": papers_data.get('target_application_domain'),
                "is_llm_era": papers_data.get('is_llm_era', False),
                "short_reasoning": papers_data.get('short_reasoning'),
            })
        
        # 插入papers表
        result = conn.execute(text(sql), params)
        
        paper_id = result.scalar()
        
        # 插入关联数据
        self._insert_related_data(conn, paper_id, db_data)
        
        return paper_id
    
    def _update_paper(self, conn: Connection, paper_id: int, db_data: Dict[str, Any]) -> None:
        """更新论文数据
        
        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文ID
            db_data: 数据库格式的数据字典
        """
        papers_data = db_data['papers']
        
        # 检查新字段是否存在
        has_new_fields = self._has_new_fields_cache
        
        # 构建基础字段的 SET 子句
        base_set_clauses = [
            'arxiv_id = :arxiv_id', 'title = :title', 'abstract = :abstract', 'keywords = :keywords',
            'pdf_url = :pdf_url', 'source_url = :source_url', 'year = :year',
            'is_preprint = :is_preprint', 'is_published = :is_published', 'source = :source', 'platform = :platform',
            'primary_category = :primary_category', 'doi = :doi', 'semantic_scholar_id = :semantic_scholar_id',
            'pubmed_id = :pubmed_id', 'comments = :comments', 'contribution_types = :contribution_types',
            'updated_at = :updated_at', 'imported_at = CURRENT_TIMESTAMP'
        ]
        
        # 构建新字段的 SET 子句
        new_set_clauses = [
            'paper_type = :paper_type', 'primary_field = :primary_field',
            'target_application_domain = :target_application_domain', 'is_llm_era = :is_llm_era',
            'short_reasoning = :short_reasoning'
        ]
        
        # 根据字段存在性动态构建 SET 子句
        all_set_clauses = base_set_clauses + (new_set_clauses if has_new_fields else [])
        set_clause_str = ', '.join(all_set_clauses)
        
        # 构建 SQL 语句
        sql = f"""
            UPDATE papers SET
                {set_clause_str}
            WHERE paper_id = :paper_id
        """
        
        # 构建基础参数字典
        params = {
            "arxiv_id": papers_data.get('arxiv_id'),
            "title": papers_data.get('title'),
            "abstract": papers_data.get('abstract'),
            "keywords": papers_data.get('keywords'),
            "pdf_url": papers_data.get('pdf_url'),
            "source_url": papers_data.get('source_url'),
            "year": papers_data.get('year'),
            "is_preprint": papers_data.get('is_preprint'),
            "is_published": papers_data.get('is_published'),
            "source": papers_data.get('source'),
            "platform": papers_data.get('platform'),
            "primary_category": papers_data.get('primary_category'),
            "doi": papers_data.get('doi'),
            "semantic_scholar_id": papers_data.get('semantic_scholar_id'),
            "pubmed_id": papers_data.get('pubmed_id'),
            "comments": papers_data.get('comments'),
            "contribution_types": json.dumps(papers_data.get('contribution_types', [])),
            "updated_at": papers_data.get('updated_at'),
            "paper_id": paper_id,
        }
        
        # 仅在字段存在时添加新字段参数
        if has_new_fields:
            params.update({
                "paper_type": papers_data.get('paper_type'),
                "primary_field": papers_data.get('primary_field'),
                "target_application_domain": papers_data.get('target_application_domain'),
                "is_llm_era": papers_data.get('is_llm_era', False),
                "short_reasoning": papers_data.get('short_reasoning'),
            })
        
        # 更新papers表
        conn.execute(text(sql), params)
        
        # 删除旧的关联数据
        conn.execute(text("DELETE FROM paper_author_affiliation WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_categories WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_publications WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_versions WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_citations WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_fields WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        # 删除keywords（如果表存在）
        try:
            conn.execute(text("DELETE FROM paper_keywords WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        except Exception as e:
            # 如果表不存在，记录调试信息但不抛出异常（兼容性处理）
            error_msg = str(e)
            if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                logging.debug(f"paper_keywords 表不存在，跳过删除操作 (paper_id={paper_id})")
            else:
                logging.warning(f"删除 paper_keywords 失败 (paper_id={paper_id}): {e}")
        
        # 插入新的关联数据
        self._insert_related_data(conn, paper_id, db_data)
    
    def _insert_related_data(self, conn: Connection, paper_id: int, db_data: Dict[str, Any]) -> None:
        """插入关联数据
        
        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文ID
            db_data: 数据库格式的数据字典
        """
        # 插入authors
        authors_data = db_data.get('authors', {})
        if authors_data.get('authors'):
            conn.execute(
                text("""
                    INSERT INTO paper_author_affiliation (paper_id, authors)
                    VALUES (:paper_id, CAST(:authors AS jsonb))
                """),
                {
                    "paper_id": paper_id,
                    "authors": json.dumps(authors_data['authors'])
                }
            )
        
        # 插入categories
        categories_data = db_data.get('categories') or []
        for cat_data in categories_data:
            cat_id = self.get_or_create_category(
                conn, cat_data['domain'], cat_data['subdomain']
            )
            conn.execute(
                text("""
                    INSERT INTO paper_categories (paper_id, cat_id, is_primary)
                    VALUES (:paper_id, :cat_id, :is_primary)
                    ON CONFLICT (paper_id, cat_id) DO UPDATE SET
                        is_primary = EXCLUDED.is_primary
                """),
                {
                    "paper_id": paper_id,
                    "cat_id": cat_id,
                    "is_primary": cat_data['is_primary']
                }
            )
        
        # 插入publication
        publication_data = db_data.get('publication')
        if publication_data and publication_data.get('venue_name'):
            # 只有当venue_name存在且不为None时才创建venue
            venue_name = publication_data.get('venue_name')
            venue_type = publication_data.get('venue_type') or 'unknown'
            venue_id = self.get_or_create_venue(
                conn, venue_name, venue_type
            )
            conn.execute(
                text("""
                    INSERT INTO paper_publications (paper_id, venue_id, publish_time, presentation_type)
                    VALUES (:paper_id, :venue_id, :publish_time, :presentation_type)
                    ON CONFLICT (paper_id, venue_id) DO UPDATE SET
                        publish_time = EXCLUDED.publish_time,
                        presentation_type = EXCLUDED.presentation_type
                """),
                {
                    "paper_id": paper_id,
                    "venue_id": venue_id,
                    "publish_time": publication_data.get('publish_time'),
                    "presentation_type": publication_data.get('presentation_type')
                }
            )
        
        # 插入versions
        versions_data = db_data.get('versions') or []
        for version_data in versions_data:
            conn.execute(
                text("""
                    INSERT INTO paper_versions (paper_id, version_num, version, version_date)
                    VALUES (:paper_id, :version_num, :version, :version_date)
                    ON CONFLICT (paper_id, version_num) DO UPDATE SET
                        version = EXCLUDED.version,
                        version_date = EXCLUDED.version_date
                """),
                {
                    "paper_id": paper_id,
                    "version_num": version_data.get('version_num'),
                    "version": version_data.get('version'),
                    "version_date": version_data.get('version_date')
                }
            )
        
        # 插入citations
        citations_data = db_data.get('citations')
        if citations_data:
            conn.execute(
                text("""
                    INSERT INTO paper_citations (paper_id, cited_by_count, update_time)
                    VALUES (:paper_id, :cited_by_count, :update_time)
                    ON CONFLICT (paper_id) DO UPDATE SET
                        cited_by_count = EXCLUDED.cited_by_count,
                        update_time = EXCLUDED.update_time
                """),
                {
                    "paper_id": paper_id,
                    "cited_by_count": citations_data.get('cited_by_count', 0),
                    "update_time": citations_data.get('update_time')
                }
            )
        
        # 插入fields
        fields_data = db_data.get('fields') or []
        for field_data in fields_data:
            field_id = self.get_or_create_field(
                conn, field_data['field_name'], field_data.get('field_name_en')
            )
            conn.execute(
                text("""
                    INSERT INTO paper_fields (paper_id, field_id, confidence, source)
                    VALUES (:paper_id, :field_id, :confidence, :source)
                    ON CONFLICT (paper_id, field_id) DO UPDATE SET
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source
                """),
                {
                    "paper_id": paper_id,
                    "field_id": field_id,
                    "confidence": field_data.get('confidence', 1.0),
                    "source": field_data.get('source', 'manual')
                }
            )

        # 插入keywords（如果表存在）
        keywords_data = db_data.get('keywords') or []
        if keywords_data:
            try:
                for keyword_data in keywords_data:
                    conn.execute(
                        text("""
                            INSERT INTO paper_keywords (paper_id, keyword_type, keyword, weight, source)
                            VALUES (:paper_id, :keyword_type, :keyword, :weight, :source)
                            ON CONFLICT (paper_id, keyword_type, keyword) DO UPDATE SET
                                weight = EXCLUDED.weight,
                                source = EXCLUDED.source
                        """),
                        {
                            "paper_id": paper_id,
                            "keyword_type": keyword_data.get('keyword_type'),
                            "keyword": keyword_data.get('keyword'),
                            "weight": keyword_data.get('weight', 1.0),
                            "source": keyword_data.get('source')
                        }
                    )
            except Exception as e:
                # 如果表不存在，记录调试信息但不抛出异常（兼容性处理）
                error_msg = str(e)
                if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                    logging.debug(f"paper_keywords 表不存在，跳过 keywords 插入 (paper_id={paper_id})")
                else:
                    logging.warning(f"插入 paper_keywords 失败 (paper_id={paper_id}): {e}")

        # 插入additional_info到pubmed_additional_info表（仅限 source='pubmed' 的论文）
        # 注意：pubmed_additional_info 表只存储 PubMed 论文的额外信息
        # 根据 papers 表的 source 字段判断，而不是 db_key
        additional_info_data = db_data.get('additional_info')
        papers_data = db_data.get('papers', {})
        papers_source = papers_data.get('source')
        if additional_info_data and papers_source == 'pubmed':
            try:
                conn.execute(
                    text("""
                        INSERT INTO pubmed_additional_info (paper_id, additional_info_json)
                        VALUES (:paper_id, CAST(:additional_info_json AS jsonb))
                        ON CONFLICT (paper_id) DO UPDATE SET
                            additional_info_json = EXCLUDED.additional_info_json,
                            updated_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "paper_id": paper_id,
                        "additional_info_json": json.dumps(additional_info_data)
                    }
                )
            except Exception as e:
                # 如果表不存在或其他错误，记录警告但不抛出异常（兼容性处理）
                error_msg = str(e)
                if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                    logging.debug(f"pubmed_additional_info 表不存在，跳过 additional_info 插入 (paper_id={paper_id})")
                else:
                    logging.warning(f"插入 additional_info 失败 (paper_id={paper_id}): {e}")
    
    def get_paper_info_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """根据 work_id 获取论文的全部信息
        
        Args:
            work_id: 论文的业务唯一标识符
            
        Returns:
            Optional[Dict]: 如果找到论文，返回包含 papers 表所有字段的字典；否则返回 None
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        paper_id, work_id, arxiv_id, title, abstract, keywords, 
                        pdf_url, source_url, year, is_preprint, is_published, 
                        source, platform, primary_category, doi, semantic_scholar_id, 
                        pubmed_id, journal_ref, comments, contribution_types, 
                        created_at, updated_at, embedding_status, shard_id
                    FROM papers
                    WHERE work_id = :work_id
                """),
                {"work_id": work_id}
            )
            row = result.fetchone()
            
            if not row:
                return None
            
            # 将结果转换为字典
            paper_info = {
                'paper_id': row[0],
                'work_id': row[1],
                'arxiv_id': row[2],
                'title': row[3],
                'abstract': row[4],
                'keywords': row[5],
                'pdf_url': row[6],
                'source_url': row[7],
                'year': row[8],
                'is_preprint': row[9],
                'is_published': row[10],
                'source': row[11],
                'platform': row[12],
                'primary_category': row[13],
                'doi': row[14],
                'semantic_scholar_id': row[15],
                'pubmed_id': row[16],
                'journal_ref': row[17],
                'comments': row[18],
                'contribution_types': row[19] if row[19] else [],  # JSONB 转 Python 对象
                'created_at': row[20],
                'updated_at': row[21],
                'embedding_status': row[22],  # 向量化状态: 0=raw, 1=exported, 2=ready
                'shard_id': row[23],  # 文献所属的 shard ID
            }
            
            return paper_info
    
    def delete_paper_by_work_id(self, work_id: str) -> bool:
        """根据 work_id 删除论文
        
        注意：由于外键约束设置了 ON DELETE CASCADE，删除 papers 表的记录会
        自动级联删除所有关联表的数据（paper_author_affiliation, paper_categories,
        paper_publications, paper_versions, paper_citations, paper_fields,
        paper_keywords, pubmed_additional_info 等）。
        
        Args:
            work_id: 论文的业务唯一标识符
            
        Returns:
            bool: 如果成功删除返回 True，如果论文不存在返回 False
        """
        with self.engine.connect() as conn:
            try:
                # 先检查论文是否存在
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                    {"work_id": work_id}
                )
                row = result.fetchone()
                
                if not row:
                    logging.warning(f"论文不存在: work_id={work_id}")
                    return False
                
                # 删除论文（级联删除关联数据）
                result = conn.execute(
                    text("DELETE FROM papers WHERE work_id = :work_id"),
                    {"work_id": work_id}
                )
                
                deleted_count = result.rowcount
                conn.commit()
                
                if deleted_count > 0:
                    logging.info(f"成功删除论文: work_id={work_id}")
                    return True
                else:
                    logging.warning(f"删除论文失败: work_id={work_id}")
                    return False
                    
            except Exception as e:
                conn.rollback()
                logging.error(f"删除论文时发生错误: work_id={work_id}, error={str(e)}")
                raise e
    
    def update_embedding_status_and_shard(
        self,
        paper_id: Optional[int] = None,
        work_id: Optional[str] = None,
        embedding_status: int = 2,
        shard_id: Optional[int] = None
    ) -> bool:
        """更新论文的 embedding_status 和 shard_id 字段
        
        Args:
            paper_id: 论文ID（与 work_id 二选一）
            work_id: 论文的业务唯一标识符（与 paper_id 二选一）
            embedding_status: 向量化状态，默认为 2（ready）
            shard_id: 文献所属的 shard ID，如果为 None 则不更新此字段
            
        Returns:
            bool: 如果成功更新返回 True，如果论文不存在返回 False
            
        Raises:
            ValueError: 如果 paper_id 和 work_id 都未提供
        """
        if paper_id is None and work_id is None:
            raise ValueError("必须提供 paper_id 或 work_id 之一")
        
        with self.engine.connect() as conn:
            try:
                with conn.begin():
                    # 如果提供了 work_id，先查询 paper_id
                    if work_id is not None and paper_id is None:
                        result = conn.execute(
                            text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                            {"work_id": work_id}
                        )
                        row = result.fetchone()
                        if not row:
                            logging.warning(f"论文不存在: work_id={work_id}")
                            return False
                        paper_id = row[0]
                    
                    # 构建更新语句
                    if shard_id is not None:
                        # 同时更新 embedding_status 和 shard_id
                        result = conn.execute(
                            text("""
                                UPDATE papers
                                SET embedding_status = :embedding_status,
                                    shard_id = :shard_id
                                WHERE paper_id = :paper_id
                            """),
                            {
                                "paper_id": paper_id,
                                "embedding_status": embedding_status,
                                "shard_id": shard_id
                            }
                        )
                    else:
                        # 只更新 embedding_status
                        result = conn.execute(
                            text("""
                                UPDATE papers
                                SET embedding_status = :embedding_status
                                WHERE paper_id = :paper_id
                            """),
                            {
                                "paper_id": paper_id,
                                "embedding_status": embedding_status
                            }
                        )
                    
                    updated_count = result.rowcount
                    if updated_count > 0:
                        if shard_id is not None:
                            logging.info(
                                f"成功更新论文状态: paper_id={paper_id}, "
                                f"embedding_status={embedding_status}, shard_id={shard_id}"
                            )
                        else:
                            logging.info(
                                f"成功更新论文状态: paper_id={paper_id}, "
                                f"embedding_status={embedding_status}"
                            )
                        return True
                    else:
                        logging.warning(f"更新论文状态失败: paper_id={paper_id}（论文可能不存在）")
                        return False
                        
            except Exception as e:
                logging.error(
                    f"更新论文状态时发生错误: paper_id={paper_id}, "
                    f"work_id={work_id}, error={str(e)}"
                )
                return False
    
    def get_papers_by_embedding_status(
        self,
        embedding_status: Optional[int] = None,
        shard_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """根据 embedding_status 和 shard_id 查询论文列表
        
        Args:
            embedding_status: 向量化状态（可选）。如果为 None，则不按此条件过滤
            shard_id: 文献所属的 shard ID（可选）。如果为 None，则不按此条件过滤
            limit: 返回结果数量限制，默认 100
            
        Returns:
            List[Dict]: 论文数据列表，每个论文包含完整的关联信息
                - authors: 作者列表
                - categories: 分类列表
                - pub_info: 发表信息
                - citations: 引用信息
                - version_count: 版本数量
                - fields: 领域列表
                - additional_info: 额外信息
        """
        with self.engine.connect() as conn:
            conditions = []
            params = {}
            
            if embedding_status is not None:
                conditions.append("p.embedding_status = :embedding_status")
                params['embedding_status'] = embedding_status
            
            if shard_id is not None:
                conditions.append("p.shard_id = :shard_id")
                params['shard_id'] = shard_id
            
            # 构建 WHERE 子句
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # 查询符合条件的 paper_id 列表
            query = f"""
                SELECT p.paper_id 
                FROM papers p
                WHERE {where_clause}
                ORDER BY p.created_at DESC
                LIMIT :limit
            """
            params['limit'] = limit
            
            result = conn.execute(text(query), params)
            paper_ids = [row[0] for row in result.fetchall()]
            
            # 为每个 paper_id 获取完整的论文数据
            results = []
            for paper_id in paper_ids:
                paper_data = self._get_complete_paper_data(conn, paper_id)
                if paper_data:
                    results.append(paper_data)
            
            return results
    
    def get_additional_info_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """根据 work_id 获取 pubmed_additional_info 表的全部信息
        
        先通过 work_id 在 papers 表中查找对应的 paper_id，然后查询 pubmed_additional_info 表。
        
        Args:
            work_id: 论文的业务唯一标识符
            
        Returns:
            Optional[Dict]: 如果找到附加信息，返回包含 pubmed_additional_info 表
                所有字段的字典；否则返回 None
                返回的字典包含：
                - paper_id: 论文ID
                - additional_info_json: 附加信息 JSON 对象（已解析为 Python dict）
                - created_at: 创建时间
                - updated_at: 更新时间
        """
        with self.engine.connect() as conn:
            # 先通过 work_id 查找 paper_id
            paper_result = conn.execute(
                text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                {"work_id": work_id}
            )
            paper_row = paper_result.fetchone()
            
            if not paper_row:
                logging.warning(f"论文不存在: work_id={work_id}")
                return None
            
            paper_id = paper_row[0]
            
            # 先检查论文的 source 字段，判断是否为 PubMed 论文
            source_result = conn.execute(
                text("SELECT source FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            source_row = source_result.fetchone()
            
            # 只有 source='pubmed' 的论文才查询 pubmed_additional_info 表
            if not source_row or source_row[0] != 'pubmed':
                logging.debug(f"非 PubMed 论文不需要查询 pubmed_additional_info (work_id={work_id}, paper_id={paper_id}, source={source_row[0] if source_row else None})")
                return None
            
            try:
                result = conn.execute(
                    text("""
                        SELECT paper_id, additional_info_json, created_at, updated_at
                        FROM pubmed_additional_info
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
                row = result.fetchone()
                
                if not row:
                    logging.info(f"论文 {work_id} (paper_id={paper_id}) 没有附加信息")
                    return None
                
                # 将结果转换为字典
                additional_info = {
                    'paper_id': row[0],
                    'additional_info_json': row[1] if row[1] else {},  # JSONB 转 Python 对象
                    'created_at': row[2],
                    'updated_at': row[3],
                }
                
                return additional_info
            except Exception as e:
                # 如果表不存在或其他错误，返回 None（兼容性处理）
                error_msg = str(e)
                if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                    logging.debug(f"pubmed_additional_info 表不存在，跳过查询 (work_id={work_id}, paper_id={paper_id})")
                else:
                    logging.warning(f"查询 additional_info 失败 (work_id={work_id}, paper_id={paper_id}): {e}")
                return None
    
    def _check_new_fields_exist(self) -> bool:
        """检查新字段是否存在（使用独立连接）
        
        Returns:
            bool: 如果 paper_type 字段存在返回 True，否则返回 False
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'papers' 
                    AND column_name = 'paper_type'
                """))
                return result.fetchone() is not None
        except Exception as e:
            logging.warning(f"检查字段存在性失败: {e}，假设字段不存在")
            return False
    
    def _get_complete_paper_data(self, conn: Connection, paper_id: int) -> Optional[Dict[str, Any]]:
        """获取完整的论文数据（包含所有关联信息）
        
        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文ID
            
        Returns:
            Optional[Dict]: 包含所有关联信息的论文数据字典，如果不存在则返回 None
        """
        # 获取主表数据
        # 使用初始化时缓存的字段存在性检查结果
        has_new_fields = self._has_new_fields_cache
        
        # 根据字段存在性选择查询语句
        try:
            if has_new_fields:
                result = conn.execute(
                    text("""
                        SELECT 
                            paper_id, work_id, arxiv_id, title, abstract, keywords, 
                            pdf_url, source_url, year, is_preprint, is_published, 
                            source, platform, primary_category, doi, semantic_scholar_id, 
                            pubmed_id, journal_ref, comments, contribution_types, 
                            created_at, updated_at, embedding_status, shard_id,
                            paper_type, primary_field, target_application_domain, is_llm_era, short_reasoning
                        FROM papers
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
            else:
                result = conn.execute(
                    text("""
                        SELECT 
                            paper_id, work_id, arxiv_id, title, abstract, keywords, 
                            pdf_url, source_url, year, is_preprint, is_published, 
                            source, platform, primary_category, doi, semantic_scholar_id, 
                            pubmed_id, journal_ref, comments, contribution_types, 
                            created_at, updated_at, embedding_status, shard_id
                        FROM papers
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
            
            row = result.fetchone()
        except Exception as e:
            # 如果事务失败，尝试回滚并记录错误
            error_msg = str(e).lower()
            if "transaction is aborted" in error_msg or "infailed" in error_msg:
                try:
                    conn.rollback()
                    logging.warning(f"事务失败，已回滚 (paper_id={paper_id})，跳过此记录")
                except Exception:
                    pass
                return None
            else:
                # 其他错误，重新抛出
                raise e
        
        if not row:
            return None
        
        # 构建基础数据字典
        paper_data = {
            'paper_id': row[0],
            'work_id': row[1],
            'arxiv_id': row[2],
            'title': row[3],
            'abstract': row[4],
            'keywords': row[5],
            'pdf_url': row[6],
            'source_url': row[7],
            'year': row[8],
            'is_preprint': row[9],
            'is_published': row[10],
            'source': row[11],
            'platform': row[12],
            'primary_category': row[13],
            'doi': row[14],
            'semantic_scholar_id': row[15],
            'pubmed_id': row[16],
            'journal_ref': row[17],
            'comments': row[18],
            'contribution_types': row[19] if row[19] else [],
            'created_at': row[20],
            'updated_at': row[21],
            'embedding_status': row[22],
            'shard_id': row[23],
        }
        
        # 如果查询包含新字段，添加新字段数据
        if has_new_fields and len(row) > 24:
            paper_data['paper_type'] = row[24]
            paper_data['primary_field'] = row[25]
            paper_data['target_application_domain'] = row[26]
            paper_data['is_llm_era'] = row[27] if row[27] is not None else False
            paper_data['short_reasoning'] = row[28]
        else:
            # 新字段不存在，设置为默认值
            paper_data['paper_type'] = None
            paper_data['primary_field'] = None
            paper_data['target_application_domain'] = None
            paper_data['is_llm_era'] = False
            paper_data['short_reasoning'] = None
        
        # 处理JSONB字段（contribution_types）
        if 'contribution_types' in paper_data and paper_data['contribution_types']:
            if isinstance(paper_data['contribution_types'], str):
                try:
                    paper_data['contribution_types'] = json.loads(paper_data['contribution_types'])
                except:
                    pass
        
        # 获取authors
        result = conn.execute(
            text("SELECT authors FROM paper_author_affiliation WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        author_row = result.fetchone()
        if author_row:
            authors_data = author_row[0]
            if isinstance(authors_data, str):
                try:
                    paper_data['authors'] = json.loads(authors_data)
                except:
                    paper_data['authors'] = authors_data if authors_data else []
            else:
                paper_data['authors'] = authors_data if authors_data else []
        else:
            paper_data['authors'] = []
        
        # 获取categories
        result = conn.execute(
            text("""
                SELECT c.domain, c.subdomain, pc.is_primary
                FROM paper_categories pc
                JOIN categories c ON pc.cat_id = c.cat_id
                WHERE pc.paper_id = :paper_id
                ORDER BY pc.is_primary DESC, c.subdomain
            """),
            {"paper_id": paper_id}
        )
        categories = result.fetchall()
        paper_data['categories'] = [
            {'domain': c[0], 'subdomain': c[1], 'is_primary': c[2]} 
            for c in categories
        ]
        
        # 获取publication信息
        result = conn.execute(
            text("""
                SELECT v.venue_name, v.venue_type, pp.publish_time, pp.presentation_type
                FROM paper_publications pp
                JOIN venues v ON pp.venue_id = v.venue_id
                WHERE pp.paper_id = :paper_id
                LIMIT 1
            """),
            {"paper_id": paper_id}
        )
        pub_row = result.fetchone()
        if pub_row:
            paper_data['pub_info'] = {
                'venue_name': pub_row[0],
                'venue_type': pub_row[1],
                'publish_time': str(pub_row[2]) if pub_row[2] else None,
                'presentation_type': pub_row[3]
            }
        else:
            paper_data['pub_info'] = None
        
        # 获取citations
        result = conn.execute(
            text("""
                SELECT cited_by_count, update_time
                FROM paper_citations
                WHERE paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )
        citation_row = result.fetchone()
        if citation_row:
            paper_data['citations'] = {
                'cited_by_count': citation_row[0] or 0,
                'update_time': str(citation_row[1]) if citation_row[1] else None
            }
        else:
            paper_data['citations'] = {'cited_by_count': 0, 'update_time': None}
        
        # 获取版本数
        result = conn.execute(
            text("SELECT COUNT(*) FROM paper_versions WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        version_count = result.scalar()
        paper_data['version_count'] = version_count
        
        # 获取fields
        result = conn.execute(
            text("""
                SELECT f.field_name, f.field_name_en, pf.confidence, pf.source
                FROM paper_fields pf
                JOIN fields f ON pf.field_id = f.field_id
                WHERE pf.paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )
        fields = result.fetchall()
        paper_data['fields'] = [{
            'field_name': f[0],
            'field_name_en': f[1],
            'confidence': f[2],
            'source': f[3]
        } for f in fields]
        
        # 获取keywords（如果表存在）
        try:
            result = conn.execute(
                text("""
                    SELECT keyword_type, keyword, weight, source
                    FROM paper_keywords
                    WHERE paper_id = :paper_id
                    ORDER BY keyword_type, weight DESC
                """),
                {"paper_id": paper_id}
            )
            keywords = result.fetchall()
            paper_data['keywords'] = [{
                'keyword_type': k[0],
                'keyword': k[1],
                'weight': k[2],
                'source': k[3]
            } for k in keywords]
        except Exception as e:
            # 如果表不存在，返回空列表（兼容性处理）
            error_msg = str(e).lower()
            error_type = str(type(e).__name__).lower()
            # 检查多种可能的错误信息格式
            if ("does not exist" in error_msg or 
                "undefinedtable" in error_type or 
                "relation" in error_msg and "paper_keywords" in error_msg):
                logging.debug(f"paper_keywords 表不存在，返回空 keywords 列表 (paper_id={paper_id})")
                paper_data['keywords'] = []
            else:
                logging.warning(f"查询 paper_keywords 失败 (paper_id={paper_id}): {e}")
                paper_data['keywords'] = []
        
        # 获取 additional_info（从 pubmed_additional_info 表）
        # 注意：pubmed_additional_info 表只存储 source='pubmed' 的论文的额外信息
        # 根据 papers 表的 source 字段判断是否需要查询此表
        if paper_data.get('source') == 'pubmed':
            # 只有 source='pubmed' 的论文才查询 pubmed_additional_info 表
            try:
                result = conn.execute(
                    text("""
                        SELECT additional_info_json
                        FROM pubmed_additional_info
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
                additional_info_row = result.fetchone()
                if additional_info_row and additional_info_row[0] is not None:
                    additional_info_value = additional_info_row[0]
                    # 处理 JSONB 字段：SQLAlchemy 可能返回 dict 或 str
                    if isinstance(additional_info_value, dict):
                        # 已经是字典，直接使用
                        paper_data['additional_info'] = additional_info_value
                    elif isinstance(additional_info_value, str):
                        # 是字符串，需要解析
                        try:
                            paper_data['additional_info'] = json.loads(additional_info_value)
                        except (json.JSONDecodeError, ValueError) as e:
                            logging.warning(f"解析 additional_info JSON 失败 (paper_id={paper_id}): {e}")
                            paper_data['additional_info'] = {}
                    else:
                        # 其他类型（如 list），尝试转换或设为空字典
                        logging.warning(f"additional_info 类型异常 (paper_id={paper_id}): {type(additional_info_value)}")
                        paper_data['additional_info'] = {}
                else:
                    # 没有 additional_info 记录，设为空字典
                    paper_data['additional_info'] = {}
            except Exception as e:
                # 如果查询失败，记录警告但设为空字典（不影响其他功能）
                error_msg = str(e)
                if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                    logging.debug(f"pubmed_additional_info 表不存在，跳过 additional_info 查询 (paper_id={paper_id})")
                else:
                    logging.warning(f"查询 additional_info 失败 (paper_id={paper_id}): {e}")
                paper_data['additional_info'] = {}
        else:
            # 非 PubMed 论文，不需要查询 pubmed_additional_info 表
            paper_data['additional_info'] = {}
        
        return paper_data
    
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
        # 参数验证
        param_count = sum([work_id is not None, paper_id is not None, title is not None])
        if param_count == 0:
            raise ValueError("必须提供work_id、paper_id或title中的一个")
        if param_count > 1:
            raise ValueError("只能提供work_id、paper_id或title中的一个参数")
        
        with self.engine.connect() as conn:
            # 先找到paper_id
            if work_id:
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                    {"work_id": work_id}
                )
            elif paper_id:
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
            else:  # title
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE title = :title LIMIT 1"),
                    {"title": title}
                )
            
            row = result.fetchone()
            if not row:
                return None
            
            paper_id = row[0]
            
            # 获取完整的论文数据
            return self._get_complete_paper_data(conn, paper_id)
    
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
        与未来的全文检索或向量检索方法（如 search_by_content）区分开来。
        
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
                - additional_info: 额外信息（包含 mesh_headings 等，从 pubmed_additional_info 表获取）
                
        Note:
            此方法仅基于元数据进行搜索，不涉及全文检索或向量检索。
            如需基于内容进行搜索，请使用其他专门的搜索方法。
        """
        with self.engine.connect() as conn:
            conditions = []
            params = {}
            
            if title:
                conditions.append("p.title ILIKE :title")
                params['title'] = f"%{title}%"
            
            if author:
                # 使用 JSONB 操作符精确搜索作者姓名
                # 要求搜索词作为连续的短语出现，不匹配分散的单词
                author_search = author.strip()
                author_words = [w.strip() for w in author_search.split() if w.strip()]
                
                if len(author_words) == 1:
                    # 单个词：使用单词边界匹配，确保 "ning" 不会匹配 "Kangning"
                    conditions.append(
                        "EXISTS ("
                        "SELECT 1 FROM paper_author_affiliation paa "
                        "CROSS JOIN LATERAL jsonb_array_elements(paa.authors) AS author_elem "
                        "WHERE paa.paper_id = p.paper_id "
                        "AND (author_elem->>'name') ~* :author_pattern"
                        ")"
                    )
                    # 匹配单词边界：\yword\y
                    params['author_pattern'] = f"\\y{author_words[0]}\\y"
                else:
                    # 多个词：要求作为连续短语出现（单词之间可以有空格、连字符等）
                    # 例如 "ning yan" 只匹配 "Ning Yan"、"Ning-Yan" 等，不匹配 "Ning Zhang Yan" 或 "Ning Yan Gu"
                    # 转义特殊字符并构建正则表达式
                    escaped_words = [word.replace('\\', '\\\\').replace('.', '\\.').replace('+', '\\+')
                                   .replace('*', '\\*').replace('?', '\\?').replace('^', '\\^')
                                   .replace('$', '\\$').replace('[', '\\[').replace(']', '\\]')
                                   .replace('(', '\\(').replace(')', '\\)').replace('|', '\\|')
                                   .replace('{', '\\{').replace('}', '\\}') for word in author_words]
                    # 构建模式：单词之间可以有空格、连字符、点号等分隔符，但必须连续
                    # 使用 \s+ 匹配一个或多个空白字符，- 匹配连字符，. 匹配点号
                    pattern_parts = []
                    for i, word in enumerate(escaped_words):
                        if i == 0:
                            pattern_parts.append(f"\\y{word}\\y")
                        else:
                            # 单词之间可以有空格、连字符、点号等，但必须相邻
                            pattern_parts.append(f"[\\s\\-\\.]*\\y{word}\\y")
                    # 在末尾添加 (?=\s|$) 确保后面是空白字符或字符串结尾，避免匹配 "Ning Yan Gu" 中的 "Ning Yan"
                    # 这样 "Ning Yan" 只匹配完整的 "Ning Yan"，不匹配 "Ning Yan Gu"
                    author_pattern = ''.join(pattern_parts) + '(?=\\s|$)'
                    
                    conditions.append(
                        "EXISTS ("
                        "SELECT 1 FROM paper_author_affiliation paa "
                        "CROSS JOIN LATERAL jsonb_array_elements(paa.authors) AS author_elem "
                        "WHERE paa.paper_id = p.paper_id "
                        "AND (author_elem->>'name') ~* :author_pattern"
                        ")"
                    )
                    params['author_pattern'] = author_pattern
            
            if category:
                conditions.append(
                    "EXISTS (SELECT 1 FROM paper_categories pc "
                    "JOIN categories c ON pc.cat_id = c.cat_id "
                    "WHERE pc.paper_id = p.paper_id AND c.subdomain = :category)"
                )
                params['category'] = category
            
            if year:
                conditions.append("p.year = :year")
                params['year'] = year
            
            # 过滤掉没有 abstract 的结果
            conditions.append("p.abstract IS NOT NULL AND p.abstract != ''")
            
            # 构建 WHERE 子句
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # 查询符合条件的 paper_id 列表
            query = f"""
                SELECT p.paper_id 
                FROM papers p
                WHERE {where_clause}
                ORDER BY p.created_at DESC
                LIMIT :limit
            """
            params['limit'] = limit
            
            result = conn.execute(text(query), params)
            paper_ids = [row[0] for row in result.fetchall()]
            
            # 为每个 paper_id 获取完整的论文数据
            results = []
            for paper_id in paper_ids:
                try:
                    paper_data = self._get_complete_paper_data(conn, paper_id)
                    if paper_data:
                        results.append(paper_data)
                except Exception as e:
                    # 如果某个 paper_id 查询失败，记录错误但继续处理下一个
                    logging.warning(f"获取论文数据失败 (paper_id={paper_id}): {e}")
                    # 如果事务失败，尝试回滚
                    error_msg = str(e).lower()
                    if "transaction is aborted" in error_msg or "infailed" in error_msg:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    continue
            
            return results
    
    def get_daily_updated_papers(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """获取每日更新论文统计列表
        基于 GREATEST(created_at, updated_at) 的日期进行分组统计
        
        Args:
            start_date: 开始日期（YYYY-MM-DD，可选）
            end_date: 结束日期（YYYY-MM-DD，可选）
            limit: 返回数量限制（默认30）
            
        Returns:
            List[Dict]: 每日更新统计列表，每个元素包含：
                - update_date: 更新日期
                - paper_count: 该日期的论文数量
        """
        with self.engine.connect() as conn:
            conditions = []
            params = {"limit": limit}
            
            # 构建WHERE条件
            base_condition = "GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp))::DATE"
            
            if start_date:
                conditions.append(f"{base_condition} >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                conditions.append(f"{base_condition} <= :end_date")
                params["end_date"] = end_date
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
                SELECT 
                    GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp))::DATE AS update_date,
                    COUNT(*) AS paper_count
                FROM papers
                WHERE {where_clause}
                  AND (created_at IS NOT NULL OR updated_at IS NOT NULL)
                GROUP BY update_date
                ORDER BY update_date DESC
                LIMIT :limit
            """
            
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            
            updates = []
            for row in rows:
                update_dict = {
                    "update_date": row[0].isoformat() if row[0] else None,
                    "paper_count": row[1]
                }
                updates.append(update_dict)
            
            return updates
    
    def get_daily_updated_papers_detail(
        self,
        date: str
    ) -> Dict[str, Any]:
        """获取指定日期的更新论文详情
        
        Args:
            date: 日期（YYYY-MM-DD）
            
        Returns:
            Dict: 包含该日期所有更新论文的详情和统计信息
                - update_date: 更新日期
                - paper_count: 论文数量
                - papers: 论文列表（包含 paper_id, work_id, title, created_at, updated_at）
        """
        with self.engine.connect() as conn:
            # 先获取统计信息
            stats_result = conn.execute(
                text("""
                    SELECT 
                        GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp))::DATE AS update_date,
                        COUNT(*) AS paper_count
                    FROM papers
                    WHERE GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp))::DATE = :date
                      AND (created_at IS NOT NULL OR updated_at IS NOT NULL)
                    GROUP BY update_date
                """),
                {"date": date}
            )
            stats_row = stats_result.fetchone()
            
            if not stats_row:
                return {
                    "update_date": date,
                    "paper_count": 0,
                    "papers": []
                }
            
            # 获取论文详情列表
            papers_result = conn.execute(
                text("""
                    SELECT 
                        paper_id,
                        work_id,
                        title,
                        created_at,
                        updated_at,
                        imported_at
                    FROM papers
                    WHERE GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp))::DATE = :date
                      AND (created_at IS NOT NULL OR updated_at IS NOT NULL)
                    ORDER BY GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp)) DESC
                """),
                {"date": date}
            )
            papers_rows = papers_result.fetchall()
            
            papers = []
            for row in papers_rows:
                paper_dict = {
                    "paper_id": row[0],
                    "work_id": row[1],
                    "title": row[2],
                    "created_at": row[3].isoformat() if row[3] else None,
                    "updated_at": row[4].isoformat() if row[4] else None,
                    "imported_at": row[5].isoformat() if row[5] else None
                }
                papers.append(paper_dict)
            
            return {
                "update_date": stats_row[0].isoformat() if stats_row[0] else date,
                "paper_count": stats_row[1],
                "papers": papers
            }

