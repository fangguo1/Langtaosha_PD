"""
source_adapters - 按来源语义映射字段到统一中间契约

职责：接收 router 路由后的原始 payload，把来源特异字段翻译成 NormalizedRecord 草稿。
不做最终标准化（由 normalizer 负责），不关心数据库表结构（由 db_mapper 负责）。
"""

from .base import BaseSourceAdapter
from .langtaosha import LangtaoshaSourceAdapter
from .biorxiv import BiorxivSourceAdapter

__all__ = [
    "BaseSourceAdapter",
    "LangtaoshaSourceAdapter",
    "BiorxivSourceAdapter",
]
