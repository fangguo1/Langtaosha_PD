"""
input_adapters - 按输入介质解析原始 payload

职责：把输入载体（JSON 文件、HTML 页面、API 响应等）解析为可路由的原始 dict。
输出传给 router.py，由 router 决定交给哪个 source_adapter。
"""

from .base import BaseInputAdapter
from .json import JSONInputAdapter
from .jsonl import JSONLInputAdapter

__all__ = [
    "BaseInputAdapter",
    "JSONInputAdapter",
    "JSONLInputAdapter",
]
