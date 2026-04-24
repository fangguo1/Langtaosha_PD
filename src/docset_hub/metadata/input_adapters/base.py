"""Input Adapter 基类"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path


class BaseInputAdapter(ABC):
    """Input Adapter 基类

    所有 input_adapters 都应该继承这个基类，并实现 parse 方法。
    """

    @abstractmethod
    def parse(self, input_path: str | Path) -> Dict[str, Any]:
        """解析输入文件并返回原始元数据字典

        Args:
            input_path: 输入文件路径

        Returns:
            Dict: 原始元数据字典（来源特异格式）

        Raises:
            FileNotFoundError: 如果文件不存在
            ValueError: 如果文件格式不正确
        """
        pass
