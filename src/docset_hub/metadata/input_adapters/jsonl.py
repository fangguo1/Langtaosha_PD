"""JSONL Input Adapter"""
import json
from pathlib import Path
from typing import Dict, Any, List

from .base import BaseInputAdapter


class JSONLInputAdapter(BaseInputAdapter):
    """JSONL 输入适配器

    解析 JSONL (JSON Lines) 文件，返回原始元数据字典。

    JSONL 格式：每行一个独立的 JSON 对象
    """

    def parse(self, input_path: str | Path, line_number: int = None) -> Dict[str, Any]:
        """解析 JSONL 文件中的指定行

        Args:
            input_path: JSONL 文件路径
            line_number: 要解析的行号（从 0 开始）。如果为 None，默认解析第一行。

        Returns:
            Dict: 原始元数据字典

        Raises:
            FileNotFoundError: 如果文件不存在
            ValueError: 如果文件格式不正确或解析失败
        """
        file_path = Path(input_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {input_path}")

        if not file_path.suffix.lower() in ['.jsonl', '.jsonl']:
            # 允许 .jsonl 扩展名，但不强制要求
            pass

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if line_number is None:
                    # 默认读取第一行
                    line = f.readline()
                    if not line:
                        raise ValueError("JSONL file is empty")
                else:
                    # 读取指定行
                    for i, line in enumerate(f):
                        if i == line_number:
                            break
                    else:
                        raise ValueError(f"Line {line_number} not found in file")

                # 解析 JSON
                data = json.loads(line.strip())

        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed at line {line_number if line_number is not None else 0}: {e}")
        except Exception as e:
            raise ValueError(f"Failed to read file: {e}")

        if not isinstance(data, dict):
            raise ValueError("JSON line must be an object/dict")

        return data

    def parse_all(self, input_path: str | Path) -> List[Dict[str, Any]]:
        """解析 JSONL 文件中的所有行

        Args:
            input_path: JSONL 文件路径

        Returns:
            List[Dict]: 原始元数据字典列表

        Raises:
            FileNotFoundError: 如果文件不存在
            ValueError: 如果文件格式不正确或解析失败
        """
        file_path = Path(input_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {input_path}")

        results = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        if not isinstance(data, dict):
                            raise ValueError(f"Line {line_num}: JSON must be an object/dict")
                        results.append(data)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"JSON parsing failed at line {line_num}: {e}")

        except Exception as e:
            raise ValueError(f"Failed to read file: {e}")

        return results
