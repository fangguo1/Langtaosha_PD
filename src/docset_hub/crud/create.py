"""创建操作"""
import sys
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from docset_hub.storage.json_storage import JSONStorage
from docset_hub.storage.metadata_db import MetadataDB
from docset_hub.metadata.validator import MetadataValidator


def create_paper(data: Dict[str, Any], save_json: bool = True) -> int:
    """创建论文记录
    
    流程：
    1. 验证数据格式
    2. 保存JSON文件（如果save_json=True）
    3. 存储到元数据库
    
    Args:
        data: DocSet格式的数据字典
        save_json: 是否保存JSON文件
        
    Returns:
        int: paper_id
        
    Raises:
        ValueError: 如果数据格式不正确
    """
    # 验证数据
    validator = MetadataValidator()
    is_valid, errors = validator.validate(data)
    
    if not is_valid:
        raise ValueError(f"数据验证失败: {', '.join(errors)}")
    
    # 保存JSON文件（中间步骤）
    if save_json:
        json_storage = JSONStorage()
        json_storage.save(data)
    
    # 存储到元数据库
    metadata_db = MetadataDB()
    paper_id = metadata_db.insert_paper(data)
    
    return paper_id

