"""JSON Input Adapter"""
import json
from pathlib import Path
from typing import Dict, Any

from .base import BaseInputAdapter


class JSONInputAdapter(BaseInputAdapter):
    """JSON 输入适配器

    解析 JSON 文件，返回原始元数据字典。
    """

    def parse(self, input_path: str | Path) -> Dict[str, Any]:
        """解析 JSON 文件

        Args:
            input_path: JSON 文件路径

        Returns:
            Dict: 原始元数据字典

        Raises:
            FileNotFoundError: 如果文件不存在
            ValueError: 如果文件不是 JSON 格式或解析失败
        """
        file_path = Path(input_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {input_path}")

        if not file_path.suffix.lower() == '.json':
            raise ValueError(f"File is not a JSON file: {input_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        except Exception as e:
            raise ValueError(f"Failed to read file: {e}")

        if not isinstance(data, dict):
            raise ValueError("JSON root must be an object/dict")

        return data
