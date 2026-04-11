"""统一配置加载器：从 config.yaml 文件加载配置并设置环境变量

这是项目的唯一配置入口，提供所有配置访问接口。
"""
import os
import yaml
import threading
import logging
import psycopg2
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine

# 配置缓存和初始化状态
_config_cache: Optional[Dict[str, Any]] = None
_initialized: bool = False
_init_lock = threading.Lock()

# 全局 SQLAlchemy Engine（单例模式）
_db_engine: Optional[Engine] = None
_engine_lock = threading.Lock()

# 多数据库引擎缓存（支持同时连接多个数据库）
_db_engines: Dict[str, Engine] = {}
_engines_lock = threading.Lock()



def load_config_from_yaml(config_path: Path) -> Dict[str, Any]:
    """
    从 config.yaml 文件加载配置
    
    Args:
        config_path: config.yaml 文件的路径（必需）
        
    Returns:
        配置字典
        
    Raises:
        FileNotFoundError: 如果配置文件不存在
    """
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
        return config
    except Exception as e:
        raise ValueError(f"无法加载配置文件 {config_path}: {e}")


def flatten_config(config: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, str]:
    """
    将嵌套的配置字典展平为扁平字典，用于设置环境变量
    
    例如:
        {'metadata_db': {'host': '10.0.4.7', 'port': 5432}}
        -> {'DB_HOST': '10.0.4.7', 'DB_PORT': '5432'}
        
        {'storage': {'json': '/path/to/json'}}
        -> {'STORAGE_JSON': '/path/to/json'}
        
        {'vector_db': {'db': '/path/to/db'}}
        -> {'VECTOR_DB_PATH': '/path/to/db'}
    
    Args:
        config: 嵌套的配置字典
        parent_key: 父级键名（用于递归）
        sep: 分隔符，默认使用下划线
        
    Returns:
        扁平化的字典，键名转为大写
    """
    # 特殊键名映射
    key_mapping = {
        'STORAGE_JSON': 'JSON_STORAGE_PATH',
        'STORAGE_PDF': 'PDF_STORAGE_PATH',
        'STORAGE_HTML': 'HTML_STORAGE_PATH',
        'STORAGE_IMAGES': 'IMAGE_STORAGE_PATH',
        'VECTOR_DB_DB': 'VECTOR_DB_PATH',
        'VECTOR_DB_GRITLM_MODEL': 'GRITLM_MODEL_PATH',
    }
    
    items = []
    for key, value in config.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        new_key_upper = new_key.upper()  # 环境变量通常使用大写
        
        # 特殊处理：metadata_db 键下的子键映射为 DB_*
        if new_key_upper == 'METADATA_DB' and isinstance(value, dict):
            # 将 metadata_db.host 映射为 DB_HOST 等
            for sub_key, sub_value in value.items():
                db_env_key = f"DB_{sub_key.upper()}"
                items.append((db_env_key, str(sub_value) if sub_value is not None else ''))
            continue
        
        if isinstance(value, dict):
            items.extend(flatten_config(value, new_key_upper, sep=sep).items())
        else:
            # 应用特殊映射
            final_key = key_mapping.get(new_key_upper, new_key_upper)
            # 将值转换为字符串（环境变量必须是字符串）
            items.append((final_key, str(value) if value is not None else ''))
    
    return dict(items)


def set_env_from_config(override: bool = True, config_path: Optional[Path] = None) -> None:
    """
    从 config.yaml 或 .env 文件加载配置并设置环境变量
    
    优先级：
    1. 已存在的环境变量（如果 override=False）
    2. config.yaml 中的配置
    3. .env 文件中的配置
    
    Args:
        override: 是否覆盖已存在的环境变量
        config_path: config.yaml 文件的路径（必需）
    """
    # 首先加载 .env 文件（如果存在且 override=False，则不会覆盖已存在的环境变量）
    load_dotenv(override=override)
    
    # 然后加载 config.yaml（如果提供了路径）
    if config_path:
        config = load_config_from_yaml(config_path)
        if config:
            # 将嵌套配置展平
            flat_config = flatten_config(config)
            
            # 设置环境变量
            for key, value in flat_config.items():
                if override or key not in os.environ:
                    os.environ[key] = value


def init_config(config_path: Path, override: bool = True) -> None:
    """初始化配置（统一入口）
    
    从 config.yaml 或 .env 文件加载配置并设置环境变量。
    此函数应该在实际使用配置之前调用一次（通常在应用启动时）。
    
    Args:
        config_path: config.yaml 文件的路径（必需）
        override: 是否覆盖已存在的环境变量
    
    Note:
        多次调用是安全的，但只有第一次调用会实际加载配置。
    """
    global _config_cache, _initialized
    
    with _init_lock:
        if _initialized:
            return
        
        # 加载配置到环境变量
        set_env_from_config(override=override, config_path=config_path)
        
        # 加载配置到缓存
        _config_cache = load_config_from_yaml(config_path)
        _initialized = True


def get_db_config(db_key: str = 'metadata_db') -> Dict[str, Any]:
    """获取数据库配置
    
    Args:
        db_key: 数据库配置键名，默认为 'metadata_db'
                支持的值：
                - 'metadata_db': 统一元数据库（存放 arxiv 和 pubmed 数据）
                - 'metadata_db_pubmed': PubMed 数据库（可选，用于单独访问）
                - 'metadata_db_arxiv': arXiv 数据库（可选，用于单独访问）
    
    Returns:
        Dict: 数据库配置字典，包含 host, port, user, password, name
        
    Raises:
        ValueError: 如果配置未初始化或缺少指定的数据库配置
    """
    if not _initialized or not _config_cache:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    # 优先查找指定的数据库配置
    if db_key in _config_cache:
        return _config_cache[db_key].copy()
    
    # 如果指定的配置不存在，抛出错误（不再向后兼容）
    raise ValueError(f"配置文件中未找到 {db_key} 配置")


def get_db_engine(db_key: str = 'metadata_db') -> Engine:
    """获取 SQLAlchemy Engine（支持多数据库）
    
    Args:
        db_key: 数据库配置键名，默认为 'metadata_db'
                支持的值：
                - 'metadata_db': 统一元数据库（存放 arxiv 和 pubmed 数据）
                - 'metadata_db_pubmed': PubMed 数据库（可选，用于单独访问）
                - 'metadata_db_arxiv': arXiv 数据库（可选，用于单独访问）
    
    Returns:
        Engine: SQLAlchemy 引擎对象，包含连接池
        
    Note:
        Engine 会自动管理连接池，无需手动管理连接。
        推荐使用 SQLAlchemy Core 的方式操作数据库。
        每个数据库键名对应一个独立的 Engine 实例（单例模式）。
    """
    global _db_engine, _db_engines
    
    if not _initialized:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    # 如果使用默认的 'metadata_db'，使用旧的单例模式
    if db_key == 'metadata_db':
        if _db_engine is None:
            with _engine_lock:
                if _db_engine is None:
                    db_config = get_db_config(db_key)
                    
                    host = db_config.get('host', 'localhost')
                    port = db_config.get('port', 5432)
                    user = db_config.get('user')
                    password = db_config.get('password')
                    database = db_config.get('name')
                    
                    if not all([host, user, password, database]):
                        raise ValueError("数据库配置不完整，需要 host, user, password, name")
                    
                    # URL 编码密码和用户名中的特殊字符（如 @, :, / 等）
                    encoded_password = quote_plus(str(password))
                    encoded_user = quote_plus(str(user))
                    
                    connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
                    
                    _db_engine = create_engine(
                        connection_string,
                        pool_size=10,
                        max_overflow=20,
                        pool_recycle=3600,
                        pool_pre_ping=True,
                        pool_timeout=30,
                        echo=False
                    )
        return _db_engine
    else:
        # 对于其他数据库键名，使用多数据库引擎缓存
        if db_key not in _db_engines:
            with _engines_lock:
                if db_key not in _db_engines:
                    db_config = get_db_config(db_key)
                    
                    host = db_config.get('host', 'localhost')
                    port = db_config.get('port', 5432)
                    user = db_config.get('user')
                    password = db_config.get('password')
                    database = db_config.get('name')
                    
                    if not all([host, user, password, database]):
                        raise ValueError(f"数据库配置不完整 ({db_key})，需要 host, user, password, name")
                    
                    # URL 编码密码和用户名中的特殊字符（如 @, :, / 等）
                    encoded_password = quote_plus(str(password))
                    encoded_user = quote_plus(str(user))
                    
                    connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
                    
                    _db_engines[db_key] = create_engine(
                        connection_string,
                        pool_size=10,
                        max_overflow=20,
                        pool_recycle=3600,
                        pool_pre_ping=True,
                        pool_timeout=30,
                        echo=False
                    )
        return _db_engines[db_key]


def get_db_connection():
    """获取 psycopg2 数据库连接（用于兼容旧代码）
    
    Returns:
        psycopg2.extensions.connection: 数据库连接对象
        
    Note:
        为了向后兼容保留此函数。推荐使用 get_db_engine() 和 SQLAlchemy Core。
    """
    if not _initialized:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    db_config = get_db_config()
    
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    user = db_config.get('user')
    password = db_config.get('password')
    database = db_config.get('name')
    
    if not all([host, user, password, database]):
        raise ValueError("数据库配置不完整，需要 host, user, password, name")
    
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )


def get_vector_db_path_from_config(config: Dict[str, Any]) -> Optional[Path]:
    """从配置字典中获取向量数据库路径
    
    Args:
        config: 配置字典
        
    Returns:
        Path: 向量数据库路径，如果未找到则返回 None
        
    Note:
        支持以下配置键：
        - vector.db
        - vector_db.db
    """
    # 尝试从 vector.db 获取
    if 'vector' in config and isinstance(config['vector'], dict):
        db_path = config['vector'].get('db')
        if db_path:
            return Path(db_path)
    
    # 尝试从 vector_db.db 获取
    if 'vector_db' in config and isinstance(config['vector_db'], dict):
        db_path = config['vector_db'].get('db')
        if db_path:
            return Path(db_path)
    
    return None


def get_all_db_configs() -> Dict[str, Dict[str, Any]]:
    """获取所有数据库配置
    
    Returns:
        Dict: 包含所有数据库配置的字典，键名为数据库键名（如 'metadata_db', 'metadata_db_pubmed' 等）
        
    Raises:
        ValueError: 如果配置未初始化
    """
    if not _initialized or not _config_cache:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    # 查找所有以 metadata_db 开头的配置
    db_configs = {}
    for key in _config_cache.keys():
        if key.startswith('metadata_db'):
            db_configs[key] = _config_cache[key].copy()
    
    return db_configs


def get_metadata_db_engine_from_config(config: Dict[str, Any], db_key: str = 'metadata_db') -> Engine:
    """从配置字典中创建并返回 metadata_db 的 SQLAlchemy Engine
    
    Args:
        config: 配置字典，必须包含指定的数据库配置
        db_key: 数据库配置键名，默认为 'metadata_db'
        
    Returns:
        Engine: SQLAlchemy 引擎对象
        
    Raises:
        ValueError: 如果配置中缺少指定的数据库配置或配置不完整
    """
    if db_key not in config:
        raise ValueError(f"配置文件中未找到 {db_key} 配置")
    
    db_config = config[db_key]
    
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    user = db_config.get('user')
    password = db_config.get('password')
    database = db_config.get('name')
    
    if not all([host, user, password, database]):
        raise ValueError(f"数据库配置不完整 ({db_key})，需要 host, user, password, name")
    
    # URL 编码密码和用户名中的特殊字符（如 @, :, / 等）
    encoded_password = quote_plus(str(password))
    encoded_user = quote_plus(str(user))
    
    connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{database}"
    
    return create_engine(
        connection_string,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_timeout=30,
        echo=False
    )


def get_vector_db_config() -> Dict[str, Any]:
    """获取向量数据库配置
    
    Returns:
        Dict: 向量数据库配置字典，包含 db, corpora, routing, merge 等
        
    Raises:
        ValueError: 如果配置未初始化或缺少 vector_db 配置
    """
    if not _initialized or not _config_cache:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    if 'vector_db' not in _config_cache:
        raise ValueError("配置文件中未找到 vector_db 配置")
    
    return _config_cache['vector_db'].copy()


def build_routing_to_shard_ids_map() -> Dict[str, Dict[str, List[int]]]:
    """构建 routing 到 shard_ids 的映射字典
    
    根据 vector_db.corpora 和 vector_db.routing 配置，建立从 routing key 到 shard_ids 的映射。
    routing key 的格式为: "domain:{domain}" 或 "source:{source}" 或 "default"
    
    Returns:
        Dict[str, Dict[str, List[int]]]: routing key 到 shard_ids 分类字典的映射
            例如:
            {
                "domain:life_sci": {
                    "writable_shard_ids": [99],
                    "readonly_shard_ids": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                },
                "domain:cs": {
                    "writable_shard_ids": [199],
                    "readonly_shard_ids": [101, 102]
                },
                "source:pubmed": {
                    "writable_shard_ids": [],
                    "readonly_shard_ids": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                },
                "source:arxiv": {
                    "writable_shard_ids": [],
                    "readonly_shard_ids": [101, 102]
                },
                "source:user": {
                    "writable_shard_ids": [99, 199],
                    "readonly_shard_ids": []
                },
                "default": {
                    "writable_shard_ids": [...],
                    "readonly_shard_ids": [...]
                }
            }
        
    Raises:
        ValueError: 如果配置未初始化或缺少必要的配置
    """
    if not _initialized or not _config_cache:
        raise ValueError("配置未初始化，请先调用 init_config(config_path)")
    
    vector_db_config = get_vector_db_config()
    corpora = vector_db_config.get('corpora', [])
    routing = vector_db_config.get('routing', {})
    
    # 建立 corpus 名称到 shard_ids 和属性的映射
    corpus_info: Dict[str, Dict[str, Any]] = {}
    for corpus_config in corpora:
        corpus_name = corpus_config.get('corpus')
        shard_ids = corpus_config.get('shard_ids', [])
        writable = corpus_config.get('writable', False)
        readonly = corpus_config.get('readonly', False)
        if corpus_name and shard_ids:
            corpus_info[corpus_name] = {
                'shard_ids': shard_ids,
                'writable': writable,
                'readonly': readonly
            }
    
    # 构建 routing 到 shard_ids 的映射
    routing_map: Dict[str, Dict[str, List[int]]] = {}
    
    def collect_shard_ids(corpus_list: List[str]) -> Dict[str, List[int]]:
        """收集指定 corpus 列表的 shard_ids，按 writable 和 readonly 分类"""
        writable_shard_ids = []
        readonly_shard_ids = []
        for corpus_name in corpus_list:
            if corpus_name in corpus_info:
                info = corpus_info[corpus_name]
                shard_ids = info['shard_ids']
                if info['writable']:
                    writable_shard_ids.extend(shard_ids)
                if info['readonly']:
                    readonly_shard_ids.extend(shard_ids)
        return {
            'writable_shard_ids': list(set(writable_shard_ids)),  # 去重
            'readonly_shard_ids': list(set(readonly_shard_ids))  # 去重
        }
    
    # 处理 default_corpora
    default_corpora = routing.get('default_corpora', [])
    if default_corpora:
        routing_map['default'] = collect_shard_ids(default_corpora)
    
    # 处理 by_domain
    by_domain = routing.get('by_domain', {})
    for domain, corpus_list in by_domain.items():
        if corpus_list:
            routing_map[f'domain:{domain}'] = collect_shard_ids(corpus_list)
    
    # 处理 by_source
    by_source = routing.get('by_source', {})
    for source, corpus_list in by_source.items():
        if corpus_list:
            routing_map[f'source:{source}'] = collect_shard_ids(corpus_list)
    
    return routing_map


def get_shard_ids_by_routing(
    routing_name: str,
) -> Dict[str, List[int]]:
    """根据 source 和 domain 获取对应的 shard_ids
    
    Args:
        source: 数据源（如 'pubmed', 'arxiv', 'user'）
        domain: 领域（如 'life_sci', 'cs'）
        
    Returns:
        Dict[str, List[int]]: 包含 writable_shard_ids 和 readonly_shard_ids 的字典
            例如:
            {
                "writable_shard_ids": [99],
                "readonly_shard_ids": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            }
        
    Note:
        优先级：domain > source > default
        如果同时提供 domain 和 source，优先使用 domain
        如果找不到匹配的路由键，返回空字典
    """
    routing_map = build_routing_to_shard_ids_map()
    
    # 优先级：domain > source > default
    if routing_name in routing_map:
        return routing_map[routing_name]
    
    # 如果都没有，返回空字典
    return {
        'writable_shard_ids': [],
        'readonly_shard_ids': []
    }
