"""Shared pytest fixtures for repository test data."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import pytest


@lru_cache(maxsize=1)
def get_project_root() -> Path:
    """Return repository root by walking upward from this file."""
    current_path = Path(__file__).resolve()
    for parent in [current_path] + list(current_path.parents):
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists() or (parent / "src").exists():
            return parent
    return current_path.parent.parent


@lru_cache(maxsize=1)
def get_test_data_dir() -> Path:
    """Return the shared directory containing real test JSON payloads."""
    return get_project_root() / "test_data"


@lru_cache(maxsize=1)
def load_test_papers() -> Dict[str, List[Dict[str, Any]]]:
    """Load shared test paper payloads from repository test_data."""
    test_data_dir = get_test_data_dir()
    papers_by_source: Dict[str, List[Dict[str, Any]]] = {}

    for source_name in ("langtaosha", "biorxiv_history", "biorxiv_daily"):
        source_dir = test_data_dir / source_name
        source_files = sorted(source_dir.glob("*.json"))
        papers_by_source[source_name] = []
        for file_path in source_files:
            with open(file_path, "r", encoding="utf-8") as f:
                papers_by_source[source_name].append(json.load(f))

    return papers_by_source


@lru_cache(maxsize=1)
def get_test_paper_files() -> Dict[str, List[Path]]:
    """Return grouped file paths for the shared test paper payloads."""
    test_data_dir = get_test_data_dir()
    return {
        "langtaosha": sorted((test_data_dir / "langtaosha").glob("*.json")),
        "biorxiv_history": sorted((test_data_dir / "biorxiv_history").glob("*.json")),
        "biorxiv_daily": sorted((test_data_dir / "biorxiv_daily").glob("*.json")),
    }


@pytest.fixture(scope="session")
def test_papers() -> Dict[str, List[Dict[str, Any]]]:
    """Shared real-paper payloads, loaded once per pytest session."""
    return load_test_papers()


@pytest.fixture(scope="session")
def test_paper_files() -> Dict[str, List[Path]]:
    """Shared grouped file paths for file-based transformer/indexer tests."""
    return get_test_paper_files()

