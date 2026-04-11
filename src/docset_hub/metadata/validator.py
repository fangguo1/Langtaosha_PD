"""元数据验证器"""
from typing import Dict, Any, List


class MetadataValidator:
    """元数据验证器
    
    验证DocSet格式的数据是否符合要求
    """
    
    @staticmethod
    def validate_work_id(work_id: str) -> bool:
        """验证work_id格式
        
        Args:
            work_id: 工作ID
            
        Returns:
            bool: 是否有效
        """
        if not work_id or not isinstance(work_id, str):
            return False
        
        # work_id格式: W后跟数字，如W0000000001
        if not work_id.startswith('W') or len(work_id) < 2:
            return False
        
        return True
    
    @staticmethod
    def validate_identifiers(identifiers: Dict[str, Any]) -> bool:
        """验证identifiers格式
        
        Args:
            identifiers: 标识符字典
            
        Returns:
            bool: 是否有效
        """
        if not isinstance(identifiers, dict):
            return False
        
        # 至少应该有一个标识符
        valid_identifiers = ['arxiv', 'doi', 'semantic_scholar', 'pubmed']
        has_any = any(
            identifiers.get(key) is not None 
            for key in valid_identifiers
        )
        
        return has_any
    
    @staticmethod
    def validate_authors(authors: List[Dict[str, Any]]) -> bool:
        """验证authors格式
        
        Args:
            authors: 作者列表
            
        Returns:
            bool: 是否有效
        """
        if authors is None:
            return False
        if not isinstance(authors, list) or len(authors) == 0:
            return False
        
        for author in authors:
            if not isinstance(author, dict):
                return False
            if 'name' not in author or not author['name']:
                return False
            if 'sequence' not in author:
                return False
        
        return True
    
    def validate(self, data: Dict[str, Any], allow_empty_identifiers: bool = False) -> tuple[bool, List[str]]:
        """全面验证数据
        
        Args:
            data: DocSet格式的数据字典
            allow_empty_identifiers: 是否允许空的identifiers（用于简化格式，默认False）
            
        Returns:
            tuple: (是否有效, 错误列表)
        """
        errors = []
        
        # 验证work_id
        work_id = data.get('work_id')
        if not self.validate_work_id(work_id):
            errors.append(f"无效的work_id: {work_id}")
        
        # 验证default_info
        default_info = data.get('default_info', {})
        if not isinstance(default_info, dict):
            errors.append("default_info必须是字典")
        else:
            # 验证必需字段
            if 'title' not in default_info or not default_info['title']:
                errors.append("缺少title字段")
            
            # 验证authors
            authors = default_info.get('authors') or []
            if not self.validate_authors(authors):
                errors.append("authors格式不正确或为空")
            
            # 验证identifiers（如果允许为空，则跳过验证）
            if not allow_empty_identifiers:
                identifiers = default_info.get('identifiers') or {}
                if not self.validate_identifiers(identifiers):
                    errors.append("identifiers格式不正确或缺少所有标识符")
        
        return len(errors) == 0, errors

