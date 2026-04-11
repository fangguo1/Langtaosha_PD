"""基础适配器抽象类"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path


class BaseAdapter(ABC):
    """输入适配器基类
    
    所有输入适配器（PDF、HTML、JSON）都应该继承这个基类
    并实现parse方法，将输入转换为统一的DocSet格式
    """
    
    @abstractmethod
    def parse(self, input_path: str | Path) -> Dict[str, Any]:
        """解析输入文件并返回DocSet格式的数据
        
        Args:
            input_path: 输入文件路径
            
        Returns:
            Dict: 符合DocSet格式的字典数据
                {
                    "work_id": "W0000000001",
                    "default_info": {...},
                    "additional_info": {...}
                }
                
        Raises:
            ValueError: 如果输入文件格式不正确
            FileNotFoundError: 如果文件不存在
        """
        pass
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证解析后的数据是否符合DocSet格式要求
        
        Args:
            data: 解析后的数据字典
            
        Returns:
            bool: 数据是否有效
            
        Raises:
            ValueError: 如果数据格式不正确
        """
        pass
    
    def read_file(self, file_path: str | Path) -> str:
        """读取文件内容
        
        Args:
            file_path: 文件路径
            
        Returns:
            str: 文件内容
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

