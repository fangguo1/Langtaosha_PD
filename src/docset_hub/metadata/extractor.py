"""元数据提取器"""
import time
import uuid
from typing import Dict, Any


def generate_work_id() -> str:
    """生成新的 work_id
    
    使用 UUID v7 格式生成，格式为 W{uuid_v7()}
    
    Returns:
        str: 新生成的 work_id，格式如 W019b73d6-1634-77d3-9574-b6014f85b118
    """
    def uuid_v7():
        """生成 UUID v7 格式的字符串"""
        ts_ms = int(time.time() * 1000)
        rand_a = uuid.uuid4().int & ((1 << 12) - 1)
        rand_b = uuid.uuid4().int & ((1 << 62) - 1)
        uuid_int = (ts_ms & ((1 << 48) - 1)) << 80
        uuid_int |= 0x7 << 76
        uuid_int |= rand_a << 64
        uuid_int |= 0x2 << 62
        uuid_int |= rand_b
        return str(uuid.UUID(int=uuid_int))
    
    return f"W{uuid_v7()}"


class MetadataExtractor:
    """元数据提取器
    
    从DocSet格式的数据中提取元信息
    """
    
    @staticmethod
    def extract_work_id(data: Dict[str, Any]) -> str:
        """提取work_id
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            str: work_id
        """
        return data.get('work_id', '')
    
    @staticmethod
    def extract_basic_info(data: Dict[str, Any]) -> Dict[str, Any]:
        """提取基本信息
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 基本信息字典
        """
        default_info = data.get('default_info', {})
        
        return {
            'title': default_info.get('title', ''),
            'abstract': default_info.get('abstract', ''),
            'year': default_info.get('year'),
            'primary_category': default_info.get('primary_category'),
            'categories': default_info.get('categories') or [],
            'is_preprint': default_info.get('is_preprint', False),
            'is_published': default_info.get('is_published', False),
            'pdf_url': default_info.get('pdf_url'),
            'source_url': default_info.get('source_url'),
        }
    
    @staticmethod
    def extract_identifiers(data: Dict[str, Any]) -> Dict[str, Any]:
        """提取标识符
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 标识符字典
        """
        default_info = data.get('default_info', {})
        identifiers = default_info.get('identifiers', {})
        
        return {
            'arxiv_id': identifiers.get('arxiv'),
            'doi': identifiers.get('doi'),
            'semantic_scholar_id': identifiers.get('semantic_scholar'),
            'pubmed_id': identifiers.get('pubmed'),
        }
    
    @staticmethod
    def extract_authors(data: Dict[str, Any]) -> list[Dict[str, Any]]:
        """提取作者信息
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            List: 作者列表
        """
        default_info = data.get('default_info', {})
        authors = default_info.get('authors') or []
        
        # 转换affiliation为affiliations数组格式
        processed_authors = []
        for author in authors:
            processed_author = {
                'sequence': author.get('sequence'),
                'name': author.get('name'),
                'author_id': author.get('author_id'),
            }
            
            # 将affiliation转换为affiliations数组
            affiliation = author.get('affiliation')
            if affiliation:
                processed_author['affiliations'] = [affiliation]
            else:
                processed_author['affiliations'] = []
            
            processed_authors.append(processed_author)
        
        return processed_authors
    
    @staticmethod
    def extract_publication_info(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """提取发表信息
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 发表信息字典，如果没有则返回None
        """
        default_info = data.get('default_info', {})
        pub_info = default_info.get('pub_info')
        
        if not pub_info:
            return None
        
        return {
            'venue_name': pub_info.get('venue_name'),
            'venue_type': pub_info.get('venue_type'),
            'publish_time': pub_info.get('publish_time'),
            'presentation_type': pub_info.get('presentation_type'),
        }
    
    @staticmethod
    def extract_additional_info(data: Dict[str, Any]) -> Dict[str, Any]:
        """提取附加信息
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 附加信息字典
        """
        additional_info = data.get('additional_info', {})
        
        return {
            'keywords': additional_info.get('keywords') or [],
            'fields': additional_info.get('fields') or [],
            'contribution_types': additional_info.get('contribution_types') or [],
            'comments': additional_info.get('comments'),
            'versions': additional_info.get('versions') or [],
            'citations': additional_info.get('citations') or {},
        }

