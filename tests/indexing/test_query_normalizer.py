"""Tests for QueryNormalizer."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.query_understanding import (
    normalize_author_name,
    normalize_query,
)


def test_normalize_query_strips_and_collapses_spaces():
    result = normalize_query("  Alice   Zhang  ")

    assert result["original_query"] == "  Alice   Zhang  "
    assert result["normalized_query"] == "Alice Zhang"
    assert result["is_valid"] is True


def test_normalize_query_preserves_comma_initial_author_format():
    result = normalize_query(" Zhang,   J. ")

    assert result["normalized_query"] == "Zhang, J."


def test_normalize_query_handles_empty_query():
    result = normalize_query("   ")

    assert result["normalized_query"] == ""
    assert result["is_valid"] is False


def test_normalize_query_preserves_chinese_query():
    result = normalize_query("  单细胞 测序  ")

    assert result["normalized_query"] == "单细胞 测序"


def test_normalize_query_preserves_mixed_language_query():
    result = normalize_query("  CRISPR 基因 editing  ")

    assert result["normalized_query"] == "CRISPR 基因 editing"


def test_normalize_query_trims_edge_punctuation_only():
    result = normalize_query("  (BRCA1/2-mediated repair?)  ")

    assert result["normalized_query"] == "BRCA1/2-mediated repair"


def test_normalize_author_name_equates_common_variants():
    assert normalize_author_name("Alice  Zhang.") == normalize_author_name("alice zhang")
