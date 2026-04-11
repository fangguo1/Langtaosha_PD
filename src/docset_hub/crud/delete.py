"""删除操作"""
import sys
import psycopg2
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.config_loader import init_config, get_db_connection
from docset_hub.storage.json_storage import JSONStorage

# 注意：配置需要在调用这些函数之前通过 init_config(config_path) 初始化

# 向后兼容：提供 get_connection 别名
get_connection = get_db_connection


def delete_paper(
    identifier: Optional[str] = None,
    work_id: Optional[str] = None,
    title: Optional[str] = None,
    delete_json: bool = True
) -> bool:
    """删除论文记录
    
    流程：
    1. 通过identifier/work_id/title查找论文
    2. 从元数据库删除（级联删除相关数据）
    3. 删除JSON文件（如果delete_json=True）
    
    Args:
        identifier: 文档标识符（work_id或paper_id字符串，向后兼容）
        work_id: 工作ID
        title: 论文标题（精确匹配，如果有多篇则删除第一个）
        delete_json: 是否删除JSON文件
        
    Returns:
        bool: 是否删除成功
        
    Raises:
        FileNotFoundError: 如果论文不存在
        ValueError: 如果参数冲突
    """
    # 兼容旧版本：如果只提供了identifier，当作work_id处理
    if identifier and not work_id and not title:
        work_id = identifier
    
    if not work_id and not title:
        raise ValueError("必须提供identifier、work_id或title中的一个")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 通过title查找work_id
        if title and not work_id:
            cursor.execute("SELECT paper_id, work_id FROM papers WHERE title = %s LIMIT 1", (title,))
            result = cursor.fetchone()
            if not result:
                raise FileNotFoundError(f"未找到标题为'{title}'的论文")
            paper_id = result[0]
            work_id = result[1]
        else:
            # 先查找paper_id
            cursor.execute("SELECT paper_id FROM papers WHERE work_id = %s", (work_id,))
            result = cursor.fetchone()
            
            if not result:
                raise FileNotFoundError(f"论文不存在: {work_id}")
            
            paper_id = result[0]
        
        # 删除数据库记录（级联删除会自动处理关联表）
        cursor.execute("DELETE FROM papers WHERE paper_id = %s", (paper_id,))
        
        conn.commit()
        
        # 删除JSON文件
        if delete_json:
            json_storage = JSONStorage()
            json_storage.delete(work_id)
        
        return True
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

