"""数据转换器：将DocSet格式转换为数据库格式"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import re
from .extractor import MetadataExtractor


class MetadataTransformer:
    """数据转换器
    
    将DocSet格式的JSON数据转换为数据库表结构所需的格式
    """
    
    def __init__(self):
        self.extractor = MetadataExtractor()
    
    def parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """解析日期字符串
        
        Args:
            date_str: 日期字符串（格式: YYYY-MM-DD）
            
        Returns:
            datetime对象或None
        """
        if not date_str:
            return None
        
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
    
    def parse_category(self, category: str) -> tuple[str, str]:
        """解析分类
        
        Args:
            category: 分类字符串（如 'cs.CC'）
            
        Returns:
            tuple: (domain, subdomain)
        """
        if '.' in category:
            domain = category.split('.')[0]
            subdomain = category
        else:
            domain = category
            subdomain = category
        
        return domain, subdomain
    
    def extract_version_num(self, version_str: str) -> Optional[int]:
        """从版本字符串中提取版本号
        
        Args:
            version_str: 版本字符串（如 'v1', 'v2'）
            
        Returns:
            int: 版本号，如果无法解析则返回None
        """
        if not version_str:
            return None
        
        # 移除'v'前缀并提取数字
        try:
            num_str = version_str.lstrip('vV')
            return int(num_str)
        except ValueError:
            return None
    
    def normalize_doi(self, doi: Optional[str]) -> Optional[str]:
        """规范化 DOI
        
        规则：
        - 去除 https://doi.org/ 等前缀
        - 转小写
        - 去除空格
        
        Args:
            doi: 原始 DOI 字符串
            
        Returns:
            str: 规范化后的 DOI，如果输入为 None 或空则返回 None
            
        示例:
            'HTTPS://doi.org/10.1145/XXX' -> '10.1145/xxx'
            '10.1145/XXX' -> '10.1145/xxx'
            ' 10.1145/XXX ' -> '10.1145/xxx'
        """
        if not doi:
            return None
        
        # 去除前后空格
        doi = doi.strip()
        if not doi:
            return None
        
        # 去除 DOI URL 前缀（支持多种格式）
        # https://doi.org/10.xxx
        # http://doi.org/10.xxx
        # doi.org/10.xxx
        # doi:10.xxx
        doi = re.sub(r'^https?://(dx\.)?doi\.org/', '', doi, flags=re.IGNORECASE)
        doi = re.sub(r'^doi\.org/', '', doi, flags=re.IGNORECASE)
        doi = re.sub(r'^doi:', '', doi, flags=re.IGNORECASE)
        
        # 转小写并去除所有空格
        doi = doi.lower().replace(' ', '')
        
        return doi if doi else None
    
    def normalize_arxiv_id(self, arxiv_id: Optional[str]) -> Optional[str]:
        """规范化 arXiv ID
        
        规则：
        - 去掉版本号（如 v1, v2, v3），只保留 base ID
        - 去除前后空格
        
        Args:
            arxiv_id: 原始 arXiv ID 字符串
            
        Returns:
            str: 规范化后的 base arXiv ID，如果输入为 None 或空则返回 None
            
        示例:
            '2301.12345v1' -> '2301.12345'
            '2301.12345v2' -> '2301.12345'
            'cs/2301012v3' -> 'cs/2301012'
            '2301.12345' -> '2301.12345'
        """
        if not arxiv_id:
            return None
        
        # 去除前后空格
        arxiv_id = arxiv_id.strip()
        if not arxiv_id:
            return None
        
        # 去掉版本号（v1, v2, v3, V1, V2, V3 等）
        # 匹配模式：v 或 V 后跟数字，在字符串末尾
        arxiv_id = re.sub(r'[vV]\d+$', '', arxiv_id)
        
        return arxiv_id if arxiv_id else None
    
    def normalize_pubmed_id(self, pubmed_id: Optional[str]) -> Optional[str]:
        """规范化 PubMed ID (PMID)
        
        规则：
        - 保留纯数字字符串
        - 去除前导空格和后导空格
        
        Args:
            pubmed_id: 原始 PubMed ID 字符串
            
        Returns:
            str: 规范化后的 PubMed ID，如果输入为 None 或空则返回 None
            
        示例:
            ' 12345678 ' -> '12345678'
            '12345678' -> '12345678'
        """
        if not pubmed_id:
            return None
        
        # 去除前后空格
        pubmed_id = pubmed_id.strip()
        if not pubmed_id:
            return None
        
        # 确保是纯数字（PMID 应该是纯数字）
        if not pubmed_id.isdigit():
            # 如果不是纯数字，尝试提取数字部分
            match = re.search(r'\d+', pubmed_id)
            if match:
                pubmed_id = match.group()
            else:
                return None
        
        return pubmed_id
    
    def normalize_semantic_scholar_id(self, semantic_scholar_id: Optional[str]) -> Optional[str]:
        """规范化 Semantic Scholar ID
        
        规则：
        - 转小写
        - 去除前后空格
        
        Args:
            semantic_scholar_id: 原始 Semantic Scholar ID 字符串
            
        Returns:
            str: 规范化后的 Semantic Scholar ID，如果输入为 None 或空则返回 None
            
        示例:
            ' ABC123 ' -> 'abc123'
            'AbC123' -> 'abc123'
        """
        if not semantic_scholar_id:
            return None
        
        # 去除前后空格并转小写
        semantic_scholar_id = semantic_scholar_id.strip().lower()
        
        return semantic_scholar_id if semantic_scholar_id else None
    
    def get_normalized_external_ids(self, data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """获取规范化后的 external IDs 字典
        
        用于 check_paper_existence 等函数查询时使用
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 规范化后的 external IDs 字典
                {
                    'arxiv_id': '2301.12345',  # base ID，无版本号
                    'doi': '10.1145/xxx',
                    'pubmed_id': '12345678',
                    'semantic_scholar_id': 'abc123'
                }
        """
        identifiers = self.extractor.extract_identifiers(data)
        
        return {
            'arxiv_id': self.normalize_arxiv_id(identifiers.get('arxiv_id')),
            'doi': self.normalize_doi(identifiers.get('doi')),
            'pubmed_id': self.normalize_pubmed_id(identifiers.get('pubmed_id')),
            'semantic_scholar_id': self.normalize_semantic_scholar_id(identifiers.get('semantic_scholar_id')),
        }
    
    def transform_to_db_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """将DocSet格式转换为数据库格式
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 数据库格式的数据字典，包含各个表的数据
        """
        default_info = data.get('default_info', {})
        additional_info = data.get('additional_info', {})
        
        # 提取基本信息
        work_id = self.extractor.extract_work_id(data)
        basic_info = self.extractor.extract_basic_info(data)
        identifiers = self.extractor.extract_identifiers(data)
        authors = self.extractor.extract_authors(data)
        pub_info = self.extractor.extract_publication_info(data)
        additional = self.extractor.extract_additional_info(data)
        
        # 规范化 external IDs（按照 ingest 流程 Step 2 的规范化规则）
        normalized_arxiv_id = self.normalize_arxiv_id(identifiers.get('arxiv_id'))
        normalized_doi = self.normalize_doi(identifiers.get('doi'))
        normalized_pubmed_id = self.normalize_pubmed_id(identifiers.get('pubmed_id'))
        normalized_semantic_scholar_id = self.normalize_semantic_scholar_id(identifiers.get('semantic_scholar_id'))
        
        # 根据 identifiers 判断数据源
        # 如果有 arxiv_id，则 source='arxiv'；如果有 pubmed_id，则 source='pubmed'
        if normalized_arxiv_id:
            source = 'arxiv'
            platform = 'arxiv'
        elif normalized_pubmed_id:
            source = 'pubmed'
            platform = 'pubmed'
        else:
            # 如果都没有，尝试从 additional_info 或其他字段推断
            # 默认使用 'arxiv'（向后兼容）
            source = 'arxiv'
            platform = 'arxiv'
        
        # 构建papers表数据（使用规范化后的 external IDs）
        papers_data = {
            'work_id': work_id,
            'arxiv_id': normalized_arxiv_id,  # 使用规范化后的 base arXiv ID（无版本号）
            'title': basic_info.get('title', ''),
            'abstract': basic_info.get('abstract'),
            'keywords': ', '.join(additional.get('keywords') or []) if additional.get('keywords') else None,
            'pdf_url': basic_info.get('pdf_url'),
            'source_url': basic_info.get('source_url'),
            'year': basic_info.get('year'),
            'is_preprint': basic_info.get('is_preprint', False),
            'is_published': basic_info.get('is_published', False),
            'source': source,  # 根据数据源自动判断：'arxiv' 或 'pubmed'
            'platform': platform,  # 根据数据源自动判断：'arxiv' 或 'pubmed'
            'primary_category': basic_info.get('primary_category'),
            'doi': normalized_doi,  # 使用规范化后的 DOI
            'semantic_scholar_id': normalized_semantic_scholar_id,  # 使用规范化后的 Semantic Scholar ID
            'pubmed_id': normalized_pubmed_id,  # 使用规范化后的 PubMed ID
            'comments': additional.get('comments'),
            'contribution_types': additional.get('contribution_types') or [],
            'created_at': self.parse_date(default_info.get('submitted_date')),
            'updated_at': self.parse_date(default_info.get('updated_date')),
            'paper_type': additional.get('paper_type'),
            'primary_field': additional.get('primary_field'),
            'target_application_domain': additional.get('target_application_domain'),
            'is_llm_era': additional.get('is_llm_era', False),
            'short_reasoning': additional.get('short_reasoning'),
        }
        
        # 构建authors数据（JSONB格式）
        authors_data = {
            'authors': authors
        }
        
        # 构建categories数据
        categories_data = []
        categories = basic_info.get('categories') or []
        primary_category = basic_info.get('primary_category')
        
        for category in categories:
            domain, subdomain = self.parse_category(category)
            is_primary = (subdomain == primary_category) if primary_category else False
            categories_data.append({
                'domain': domain,
                'subdomain': subdomain,
                'is_primary': is_primary,
            })
        
        # 构建publication数据
        publication_data = None
        if pub_info:
            publication_data = {
                'venue_name': pub_info.get('venue_name'),
                'venue_type': pub_info.get('venue_type'),
                'publish_time': self.parse_date(pub_info.get('publish_time')),
                'presentation_type': pub_info.get('presentation_type'),
            }
        
        # 构建versions数据
        versions_data = []
        versions = additional.get('versions') or []
        for version in versions:
            version_str = version.get('version', '')
            version_num = self.extract_version_num(version_str)
            if version_num is not None:
                versions_data.append({
                    'version_num': version_num,
                    'version': version_str,
                    'version_date': self.parse_date(version.get('version_date')),
                })
        
        # 构建citations数据
        citations_data = None
        citations = additional.get('citations') or {}
        if citations:
            citations_data = {
                'cited_by_count': citations.get('cited_by_count', 0),
                'update_time': self.parse_date(citations.get('update_time')),
            }
        
        # 构建fields数据
        fields_data = []
        fields = additional.get('fields') or []
        for field in fields:
            fields_data.append({
                'field_name': field.get('field_name'),
                'field_name_en': field.get('field_name_en'),
                'confidence': field.get('confidence', 1.0),
                'source': field.get('source', 'manual'),
            })
        
        # 构建keywords数据（结构化关键词）
        keywords_data = []
        keywords = additional.get('keywords') or []
        # 如果keywords是列表，检查是否是结构化格式
        if isinstance(keywords, list):
            for keyword in keywords:
                if isinstance(keyword, dict):
                    # 结构化关键词格式：{'keyword_type': 'method', 'keyword': 'transformer', 'weight': 0.9, 'source': 'ai_extract'}
                    keywords_data.append({
                        'keyword_type': keyword.get('keyword_type'),
                        'keyword': keyword.get('keyword'),
                        'weight': keyword.get('weight', 1.0),
                        'source': keyword.get('source'),
                    })
                elif isinstance(keyword, str):
                    # 简单字符串格式，默认作为method类型
                    keywords_data.append({
                        'keyword_type': 'concept',  # 默认类型
                        'keyword': keyword,
                        'weight': 1.0,
                        'source': 'paper_metadata',
                    })
        
        return {
            'papers': papers_data,
            'authors': authors_data,
            'categories': categories_data,
            'publication': publication_data,
            'versions': versions_data,
            'citations': citations_data,
            'fields': fields_data,
            'keywords': keywords_data,
            'additional_info': additional_info,  # 保留原始 additional_info 用于存储到 pubmed_additional_info 表
        }

