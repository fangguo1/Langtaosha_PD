from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config.config_loader import load_config_from_yaml


def load_config_mapping(config_path: str | Path) -> Dict[str, Any]:
    """Load a config mapping without initializing global config state."""
    return load_config_from_yaml(Path(config_path))


def build_config_fingerprint(config: Mapping[str, Any]) -> Dict[str, Any]:
    vector_config = dict(config.get("vector_db") or {})
    metadata_config = dict(config.get("metadata_db") or {})
    default_sources = list(config.get("default_sources") or [])
    return {
        "metadata_db_name": str(metadata_config.get("name") or ""),
        "vector_db_database": str(vector_config.get("database") or ""),
        "collection_prefix": str(vector_config.get("collection_prefix") or ""),
        "sparse_collection_prefix": str(vector_config.get("sparse_collection_prefix") or ""),
        "embedding_model": str(vector_config.get("embedding_model") or ""),
        "default_sources": default_sources,
    }


def create_metadata_engine_from_config(config: Mapping[str, Any]) -> Engine:
    metadata_config = dict(config.get("metadata_db") or {})
    host = metadata_config.get("host")
    port = metadata_config.get("port", 5432)
    user = metadata_config.get("user")
    password = metadata_config.get("password")
    database = metadata_config.get("name")
    if not all([host, user, password, database]):
        raise ValueError("metadata_db config requires host, user, password, and name")

    encoded_user = quote_plus(str(user))
    encoded_password = quote_plus(str(password))
    url = (
        f"postgresql+psycopg2://{encoded_user}:{encoded_password}"
        f"@{host}:{port}/{database}"
    )
    return create_engine(url, future=True)
