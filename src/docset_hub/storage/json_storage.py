"""JSON文件存储"""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional

from config.config_loader import init_config

class JSONStorage:
    """JSON文件存储类
    
    负责将DocSet格式的数据保存为JSON文件
    文件命名规则: {work_id}.json
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        """初始化JSON存储
        
        Args:
            storage_path: JSON文件存储路径，如果为None则使用环境变量 JSON_STORAGE_PATH
        """
        if storage_path is None:
            storage_path = os.getenv('JSON_STORAGE_PATH')
            if not storage_path:
                raise ValueError("未指定 storage_path 且环境变量 JSON_STORAGE_PATH 未设置")
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def save(self, data: Dict[str, Any]) -> Path:
        """保存数据为JSON文件
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Path: 保存的文件路径
        """
        work_id = data.get('work_id')
        if not work_id:
            raise ValueError("数据中缺少work_id字段")
        
        file_path = self.storage_path / f"{work_id}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return file_path
    
    def load(self, work_id: str) -> Dict[str, Any]:
        """加载JSON文件
        
        Args:
            work_id: 工作ID
            
        Returns:
            Dict: DocSet格式的数据字典
            
        Raises:
            FileNotFoundError: 如果文件不存在
        """
        file_path = self.storage_path / f"{work_id}.json"
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON文件不存在: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def exists(self, work_id: str) -> bool:
        """检查文件是否存在
        
        Args:
            work_id: 工作ID
            
        Returns:
            bool: 文件是否存在
        """
        file_path = self.storage_path / f"{work_id}.json"
        return file_path.exists()
    
    def delete(self, work_id: str) -> bool:
        """删除JSON文件
        
        Args:
            work_id: 工作ID
            
        Returns:
            bool: 是否删除成功
        """
        file_path = self.storage_path / f"{work_id}.json"
        
        if file_path.exists():
            file_path.unlink()
            return True
        
        return False
    
    def update(self, data: Dict[str, Any]) -> Path:
        """更新JSON文件
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Path: 更新的文件路径
        """
        return self.save(data)

