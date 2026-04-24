"""Metadata 工具函数模块"""
import time
import uuid


def generate_work_id() -> str:
    """生成新的 work_id（UUID v7 格式）

    UUID v7 优势：
    - 时间有序（适合索引和排序）
    - 全局唯一（分布式安全）
    - 包含时间戳（便于调试和追溯）

    Returns:
        str: UUID v7 格式的 work_id，前缀 W
        示例: W019b73d6-1634-77d3-9574-b6014f85b118
    """
    def uuid_v7():
        """生成 UUID v7 格式的字符串"""
        ts_ms = int(time.time() * 1000)
        rand_a = uuid.uuid4().int & ((1 << 12) - 1)
        rand_b = uuid.uuid4().int & ((1 << 62) - 1)
        uuid_int = (ts_ms & ((1 << 48) - 1)) << 80
        uuid_int |= 0x7 << 76
        uuid_int |= rand_a << 64
        uuid_int |= 0x2 << 62
        uuid_int |= rand_b
        return str(uuid.UUID(int=uuid_int))

    return f"W{uuid_v7()}"
