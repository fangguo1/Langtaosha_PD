"""Tests for scholar search highlight plan construction."""

from src.docset_hub.indexing.search_highlighting import build_search_highlight


def test_author_route_highlights_matched_author_only_in_authors():
    highlight = build_search_highlight(
        query="Alce Zhang",
        search_query="Alice Zhang",
        search_mode="smart",
        understanding={
            "intent": "author_name",
            "route": "metadata_author",
            "matched_author": "Alice Zhang",
        },
    )

    assert highlight == {
        "mode": "author_name",
        "fields": ["authors"],
        "terms": ["Alice Zhang"],
    }


def test_vector_route_uses_phrase_corrections_as_keyword_terms():
    highlight = build_search_highlight(
        query="solvent formtion for cancr cell therpy",
        search_query="solvent formation for cancer cell therapy",
        search_mode="smart",
        understanding={
            "intent": "semantic_search",
            "route": "vector",
            "corrected_query": "solvent formation for cancer cell therapy",
            "corrections": [
                {
                    "original": "solvent formtion",
                    "corrected": "solvent formation",
                    "auto_apply": True,
                },
                {
                    "original": "cancr cell therpy",
                    "corrected": "cancer cell therapy",
                    "auto_apply": True,
                },
            ],
        },
    )

    assert highlight["mode"] == "keyword"
    assert highlight["fields"] == ["title", "abstract"]
    assert highlight["terms"] == [
        "cancer cell therapy",
        "solvent formation",
        "formation",
        "solvent",
        "therapy",
        "cancer",
        "cell",
    ]


def test_vector_route_expands_corrected_phrase_tokens_for_highlighting():
    highlight = build_search_highlight(
        query="cancr cell therpy",
        search_query="cancer cell therapy",
        search_mode="smart",
        understanding={
            "intent": "semantic_search",
            "route": "vector",
            "corrected_query": "cancer cell therapy",
            "corrections": [
                {
                    "original": "cancr cell therpy",
                    "corrected": "cancer cell therapy",
                    "auto_apply": True,
                },
            ],
        },
    )

    assert {"cancer cell therapy", "cancer", "cell", "therapy"}.issubset(
        set(highlight["terms"])
    )


def test_vector_route_falls_back_to_query_tokens_without_stopwords():
    highlight = build_search_highlight(
        query="machine learning algorithms for genomics",
        search_query="machine learning algorithms for genomics",
        search_mode="vector",
        understanding={
            "intent": "semantic_search",
            "route": "vector",
            "corrections": [],
            "candidates": [],
        },
    )

    assert highlight["mode"] == "keyword"
    assert highlight["terms"] == ["algorithms", "genomics", "learning", "machine"]
