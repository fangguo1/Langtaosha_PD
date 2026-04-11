"""JSON输入适配器"""
import json
from pathlib import Path
from typing import Dict, Any
from .base_adapter import BaseAdapter

# 尝试导入更快的 JSON 解析库
try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False

try:
    import ujson
    _HAS_UJSON = True
except ImportError:
    _HAS_UJSON = False


class JSONAdapter(BaseAdapter):
    """JSON输入适配器
    
    用于处理用户提供的JSON格式论文数据
    """
    
    def parse(self, input_path: str | Path) -> Dict[str, Any]:
        """解析JSON文件
        
        优化说明：
        - 使用 orjson 或 ujson（如果可用）替代标准库 json，性能提升 2-5 倍
        - 一次性读取文件到内存再解析，减少 I/O 操作
        - 验证由 MetadataValidator 统一处理，避免重复验证
        
        Args:
            input_path: JSON文件路径
            
        Returns:
            Dict: DocSet格式的数据字典
        """
        file_path = Path(input_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON文件不存在: {input_path}")
        
        if not file_path.suffix.lower() == '.json':
            raise ValueError(f"文件不是JSON格式: {input_path}")
        
        try:
            # 优化：一次性读取文件到内存，减少 I/O 操作（特别是网络文件系统）
            # 对于网络挂载的文件系统，减少文件句柄操作次数可以显著提升性能
            with open(file_path, 'rb') as f:  # 使用二进制模式读取
                file_content = f.read()
            
            # 使用更快的 JSON 解析库（如果可用）
            if _HAS_ORJSON:
                # orjson 是最快的 JSON 解析库，比标准库快 2-5 倍
                data = orjson.loads(file_content)
            elif _HAS_UJSON:
                # ujson 也比标准库快，但需要先解码为字符串
                data = ujson.loads(file_content.decode('utf-8'))
            else:
                # 回退到标准库，但使用已读取的内容
                data = json.loads(file_content.decode('utf-8'))
                
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"JSON解析失败: {e}")
        except Exception as e:
            # 处理 orjson/ujson 的异常
            raise ValueError(f"JSON解析失败: {e}")
        
        # 验证由 MetadataValidator 统一处理，避免重复验证
        # self.validate(data)  # 已移除，由 MetadataValidator 处理
        
        return data
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证JSON数据格式
        
        检查必需的字段是否存在
        
        Args:
            data: 数据字典
            
        Returns:
            bool: 数据是否有效
            
        Raises:
            ValueError: 如果数据格式不正确
        """
        # 检查顶层必需字段
        if 'work_id' not in data:
            raise ValueError("缺少必需字段: work_id")
        
        if 'default_info' not in data:
            raise ValueError("缺少必需字段: default_info")
        
        if 'additional_info' not in data:
            raise ValueError("缺少必需字段: additional_info")
        
        default_info = data['default_info']
        required_default_fields = ['title', 'authors']
        for field in required_default_fields:
            if field not in default_info:
                raise ValueError(f"default_info中缺少必需字段: {field}")
        
        # 验证authors格式
        authors = default_info.get('authors') or []
        if not isinstance(authors, list):
            raise ValueError("authors必须是列表")
        
        for i, author in enumerate(authors):
            if not isinstance(author, dict):
                raise ValueError(f"authors[{i}]必须是字典")
            if 'name' not in author:
                raise ValueError(f"authors[{i}]缺少必需字段: name")
        
        return True

