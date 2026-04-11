"""元数据处理模块"""
from .extractor import MetadataExtractor, generate_work_id
from .validator import MetadataValidator
from .transformer import MetadataTransformer

__all__ = ['MetadataExtractor', 'MetadataValidator', 'MetadataTransformer', 'generate_work_id']

