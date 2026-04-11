"""更新操作"""
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from docset_hub.storage.json_storage import JSONStorage
from docset_hub.storage.metadata_db import MetadataDB
from docset_hub.metadata.validator import MetadataValidator


def update_paper(
    identifier: Optional[str] = None,
    work_id: Optional[str] = None,
    title: Optional[str] = None,
    updates: Optional[Dict[str, Any]] = None,
    update_json: bool = True
) -> bool:
    """更新论文记录
    
    流程：
    1. 通过identifier/work_id/title查找论文
    2. 加载现有数据
    3. 合并更新数据
    4. 验证数据格式
    5. 更新JSON文件（如果update_json=True）
    6. 更新元数据库
    
    Args:
        identifier: 文档标识符（work_id或paper_id字符串，向后兼容）
        work_id: 工作ID
        title: 论文标题（精确匹配，如果有多篇则更新第一个）
        updates: 要更新的字段字典（可以是部分字段）
        update_json: 是否更新JSON文件
        
    Returns:
        bool: 是否更新成功
        
    Raises:
        FileNotFoundError: 如果论文不存在
        ValueError: 如果数据格式不正确或参数冲突
    """
    # 兼容旧版本：如果只提供了identifier，当作work_id处理
    if identifier and not work_id and not title:
        work_id = identifier
    
    if not work_id and not title:
        raise ValueError("必须提供work_id或title")
    
    # 通过title查找work_id
    if title and not work_id:
        from docset_hub.crud.read import read_paper
        paper = read_paper(title=title)
        if not paper:
            raise FileNotFoundError(f"未找到标题为'{title}'的论文")
        work_id = paper.get('work_id')
        if not work_id:
            raise ValueError(f"论文存在但没有work_id")
    
    # 加载现有数据
    json_storage = JSONStorage()
    
    if not json_storage.exists(work_id):
        raise FileNotFoundError(f"论文不存在: {work_id}")
    
    existing_data = json_storage.load(work_id)
    
    if not updates:
        raise ValueError("必须提供updates参数")
    
    # 合并更新数据
    # 支持更新default_info和additional_info的子字段
    if 'default_info' in updates:
        if 'default_info' not in existing_data:
            existing_data['default_info'] = {}
        existing_data['default_info'].update(updates['default_info'])
    
    if 'additional_info' in updates:
        if 'additional_info' not in existing_data:
            existing_data['additional_info'] = {}
        existing_data['additional_info'].update(updates['additional_info'])
    
    # 验证数据
    validator = MetadataValidator()
    is_valid, errors = validator.validate(existing_data)
    
    if not is_valid:
        raise ValueError(f"数据验证失败: {', '.join(errors)}")
    
    # 更新JSON文件
    if update_json:
        json_storage.update(existing_data)
    
    # 更新元数据库
    metadata_db = MetadataDB()
    metadata_db.insert_paper(existing_data)  # insert_paper会自动处理更新逻辑
    
    return True

