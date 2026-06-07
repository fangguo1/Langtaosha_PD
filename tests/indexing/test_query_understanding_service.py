"""Tests for QueryUnderstandingService orchestration."""

from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import sys
import uuid

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, init_config
from src.docset_hub.indexing import PaperIndexer
from src.docset_hub.indexing.keyword_enrichment import DEFAULT_KEYWORD_SOURCES
from src.docset_hub.indexing.query_understanding import QueryUnderstandingService
from src.docset_hub.storage.metadata_db import MetadataDB


class FakeMetadataDB:
    def __init__(self, candidates_by_query, terms_by_query=None):
        self.candidates_by_query = candidates_by_query
        self.terms_by_query = terms_by_query or {}

    def suggest_author_names(self, query, limit=5):
        return list(self.candidates_by_query.get(query, []))

    def suggest_query_terms(self, query, limit=20):
        return list(self.terms_by_query.get(query, []))


def _candidate(name, score, paper_count=1):
    return {
        "name": name,
        "normalized_name": " ".join(name.lower().replace(".", " ").replace(",", " ").split()),
        "score": score,
        "paper_count": paper_count,
    }


def _term(keyword, source="scispacy-en_core_sci_lg-generated", keyword_type="concept", doc_count=2):
    return {
        "keyword": keyword,
        "keyword_type": keyword_type,
        "source": source,
        "doc_count": doc_count,
        "avg_weight": 1.0,
    }


def test_analyze_routes_high_confidence_author_to_metadata_author():
    service = QueryUnderstandingService(
        FakeMetadataDB({"Alice Zhang": [_candidate("Alice Zhang", 1.0, 3)]})
    )

    result = service.analyze(" Alice   Zhang ")

    assert result.intent == "author_name"
    assert result.route == "metadata_author"
    assert result.matched_author == "Alice Zhang"
    assert result.normalized_query == "Alice Zhang"


def test_analyze_routes_semantic_query_to_vector():
    service = QueryUnderstandingService(FakeMetadataDB({"cancer cell therapy": []}))

    result = service.analyze("cancer cell therapy")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.matched_author is None


def test_analyze_author_match_has_priority_over_future_query_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB({"Alce Zhang": [_candidate("Alice Zhang", 0.94, 2)]})
    )

    result = service.analyze("Alce Zhang")

    assert result.intent == "author_name"
    assert result.route == "metadata_author"
    assert result.matched_author == "Alice Zhang"
    assert result.corrected_query is None


def test_analyze_middle_confidence_author_returns_author_suggestion_before_keyword_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"niang yan": [_candidate("Nieng Yan", 0.8888888888888888, 4)]},
            {"niang yan": [_term("anthocyanin")]},
        )
    )

    result = service.analyze("niang yan")

    assert result.intent == "author_name"
    assert result.route == "author_suggestion"
    assert result.suggested_author == "Nieng Yan"
    assert result.matched_author is None
    assert result.corrected_query is None
    assert result.confidence == pytest.approx(0.8888888888888888)
    assert result.reason == "author_candidate_middle_confidence"
    assert result.candidates[0]["name"] == "Nieng Yan"
    assert "keyword" not in result.candidates[0]


def test_analyze_ambiguous_author_returns_candidates_but_vector_route():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {
                "Zhang": [
                    _candidate("Alice Zhang", 0.95, 2),
                    _candidate("Andrew Zhang", 0.94, 1),
                ]
            }
        )
    )

    result = service.analyze("Zhang")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.matched_author is None
    assert [item["name"] for item in result.candidates] == ["Alice Zhang", "Andrew Zhang"]


def test_analyze_empty_query_returns_invalid_result():
    service = QueryUnderstandingService(FakeMetadataDB({}))

    result = service.analyze("   ")

    assert result.intent == "invalid"
    assert result.route == "none"
    assert result.reason == "empty_query"


def test_analyze_result_is_serializable_dict():
    service = QueryUnderstandingService(
        FakeMetadataDB({"Alice Zhang": [_candidate("Alice Zhang", 1.0, 3)]})
    )

    payload = service.analyze("Alice Zhang").to_dict()

    assert payload["intent"] == "author_name"
    json.dumps(payload)


def test_analyze_includes_original_and_normalized_query():
    service = QueryUnderstandingService(FakeMetadataDB({"Alice Zhang": [_candidate("Alice Zhang", 1.0)]}))

    result = service.analyze("  Alice   Zhang  ")

    assert result.original_query == "  Alice   Zhang  "
    assert result.normalized_query == "Alice Zhang"


def test_analyze_exact_keyword_candidate_keeps_vector_route_without_auto_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"solvent formation": []},
            {"solvent formation": [_term("solvent formation")]},
        )
    )

    result = service.analyze("solvent formation")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.corrected_query is None
    assert result.reason == "query_term_exact_match"
    assert result.candidates[0]["keyword"] == "solvent formation"


def test_analyze_typo_uses_generated_keyword_candidate_for_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"solvent formtion": []},
            {"solvent formtion": [_term("solvent formation")]},
        )
    )

    result = service.analyze("solvent formtion")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.corrected_query == "solvent formation"
    assert result.reason == "query_term_high_confidence"


def test_analyze_0875_keyword_match_stays_suggestion_without_auto_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"enzyme prediction": []},
            {
                "enzyme prediction": [
                    _term("Gene Prediction"),
                    _term("Genomic Prediction"),
                    _term("Enzyme Function"),
                ]
            },
        )
    )

    result = service.analyze("enzyme prediction")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.corrected_query is None
    assert result.reason == "query_term_middle_confidence"
    assert result.confidence == 0.875
    assert result.candidates[0]["keyword"] == "Gene Prediction"


def test_analyze_typo_matches_lowercase_query_casing():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"machine learing": []},
            {"machine learing": [_term("Machine Learning")]},
        )
    )

    result = service.analyze("machine learing")

    assert result.corrected_query == "machine learning"


def test_analyze_uses_phrase_corrections_for_sentence_query():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"solvent formtion for cancr cell therpy": []},
            {
                "solvent formtion": [_term("solvent formation")],
                "cancr cell therpy": [_term("cancer cell therapy")],
            },
        )
    )

    result = service.analyze("solvent formtion for cancr cell therpy")

    assert result.intent == "semantic_search"
    assert result.route == "vector"
    assert result.corrected_query == "solvent formation for cancer cell therapy"
    assert result.reason == "phrase_query_terms_high_confidence"
    assert [item["corrected"] for item in result.corrections] == [
        "solvent formation",
        "cancer cell therapy",
    ]


def test_analyze_includes_phrase_corrections_in_payload():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"solvent formtion for cancr cell therpy": []},
            {
                "solvent formtion": [_term("solvent formation")],
                "cancr cell therpy": [_term("cancer cell therapy")],
            },
        )
    )

    payload = service.analyze("solvent formtion for cancr cell therpy").to_dict()

    assert payload["corrections"][0]["original"] == "solvent formtion"
    json.dumps(payload)


def test_analyze_author_match_still_has_priority_over_keyword_correction():
    service = QueryUnderstandingService(
        FakeMetadataDB(
            {"Alce Zhang": [_candidate("Alice Zhang", 0.94, 2)]},
            {"Alce Zhang": [_term("Alice syndrome")]},
        )
    )

    result = service.analyze("Alce Zhang")

    assert result.intent == "author_name"
    assert result.route == "metadata_author"
    assert result.matched_author == "Alice Zhang"
    assert result.corrected_query is None


_global_config_path = None


def get_config_path_from_args() -> Path:
    global _global_config_path
    if _global_config_path:
        return _global_config_path

    config_path = None
    is_pytest = any("pytest" in arg for arg in sys.argv)
    if not is_pytest:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config-path", type=str, default=None)
        args, _ = parser.parse_known_args()
        if args.config_path:
            config_path = Path(args.config_path)

    if config_path is None and os.environ.get("QUERY_UNDERSTANDING_TEST_CONFIG"):
        config_path = Path(os.environ["QUERY_UNDERSTANDING_TEST_CONFIG"])

    if config_path is None:
        config_path = Path(__file__).resolve().parents[2] / "src" / "config" / "config_tecent_backend_server_test.yaml"

    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")

    _global_config_path = config_path
    return config_path


@pytest.fixture(scope="function")
def real_keyword_test_context():
    context = {"paper_ids": []}
    try:
        yield context
    finally:
        metadata_db = MetadataDB(config_path=get_config_path_from_args())
        for paper_id in context["paper_ids"]:
            metadata_db.delete_paper_by_paper_id(paper_id)


def _indexer_with_scispacy_keywords():
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return PaperIndexer(
        config_path=get_config_path_from_args(),
        enable_vectorization=False,
        enable_keyword_enrichment=True,
    )


def _select_biorxiv_payload(test_papers, title_fragment):
    for payload in test_papers["biorxiv_history"]:
        if title_fragment.lower() in payload.get("title", "").lower():
            return payload
    raise AssertionError(f"Missing biorxiv_history test payload with title fragment: {title_fragment}")


def _unique_biorxiv_payload(raw_payload, suffix, index):
    payload = copy.deepcopy(raw_payload)
    doi = f"10.1101/query-understanding-real-keywords.{suffix}.{index}"
    payload["doi"] = doi
    payload["title"] = f"{payload['title']} [query understanding integration {suffix} {index}]"
    payload["jatsxml"] = f"https://www.biorxiv.org/content/early/query-understanding/{suffix}/{index}.source.xml"
    payload["published"] = "NA"
    return payload


def _unique_langtaosha_payload(raw_payload, suffix, index):
    payload = copy.deepcopy(raw_payload)
    numeric_id = str(970000000000 + index)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    doi = f"10.65215/query-understanding-smart-search.{suffix}.{index}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [doi]
    payload["meta"]["citation_title"] = [
        f"{payload['meta']['citation_title'][0]} [query understanding smart search {suffix} {index}]"
    ]
    return payload


def _keyword_rows(metadata_db, paper_ids):
    placeholders = ", ".join(f":paper_id_{idx}" for idx, _ in enumerate(paper_ids))
    params = {f"paper_id_{idx}": paper_id for idx, paper_id in enumerate(paper_ids)}
    with metadata_db.engine.connect() as conn:
        return conn.execute(
            text(
                f"""
                SELECT paper_id, keyword_type, keyword, weight, source
                FROM paper_keywords
                WHERE paper_id IN ({placeholders})
                ORDER BY paper_id, source, keyword
                """
            ),
            params,
        ).mappings().all()


@pytest.mark.integration
def test_real_test_data_scispacy_keywords_feed_query_understanding(
    test_papers,
    real_keyword_test_context,
):
    pytest.importorskip("en_core_sci_lg")
    pytest.importorskip("en_ner_bionlp13cg_md")

    indexer = _indexer_with_scispacy_keywords()
    source_names = set(DEFAULT_KEYWORD_SOURCES.values())
    suffix = uuid.uuid4().hex
    selected_payloads = [
        _select_biorxiv_payload(test_papers, "RNPP-type quorum sensing"),
        _select_biorxiv_payload(test_papers, "Fes tyrosine kinase"),
        _select_biorxiv_payload(test_papers, "Drosophila Ventral Nervous System"),
    ]

    for idx, payload in enumerate(selected_payloads):
        result = indexer.index_dict(
            raw_payload=_unique_biorxiv_payload(payload, suffix, idx),
            source_name="biorxiv_history",
        )
        assert result["success"] is True, result
        assert result["keyword_enrichment"]["success"] is True, result["keyword_enrichment"]
        assert source_names.issubset(set(result["keyword_enrichment"]["sources"]))
        real_keyword_test_context["paper_ids"].append(result["paper_id"])

    metadata_db = MetadataDB(config_path=get_config_path_from_args())
    rows = _keyword_rows(metadata_db, real_keyword_test_context["paper_ids"])
    assert rows

    rows_by_source = {}
    for row in rows:
        rows_by_source.setdefault(row["source"], []).append(row)

    assert source_names.issubset(rows_by_source)
    assert all(rows_by_source[source] for source in source_names)

    lg_keywords = {row["keyword"].lower() for row in rows_by_source["scispacy-en_core_sci_lg-generated"]}
    bionlp_keywords = {row["keyword"].lower() for row in rows_by_source["scispacy-en_ner_bionlp13cg_md-generated"]}
    assert "solvent formation" in lg_keywords
    assert {"butanol", "qspb"}.intersection(bionlp_keywords)

    service = QueryUnderstandingService(metadata_db)

    exact = service.analyze("solvent formation")
    assert exact.intent == "semantic_search"
    assert exact.route == "vector"
    assert exact.corrected_query is None
    assert exact.reason == "query_term_exact_match"
    assert exact.candidates[0]["keyword"].lower() == "solvent formation"
    assert exact.candidates[0]["source"] in source_names

    typo = service.analyze("solvent formtion")
    assert typo.intent == "semantic_search"
    assert typo.route == "vector"
    assert typo.corrected_query == "solvent formation"
    assert typo.reason == "query_term_high_confidence"
    assert typo.confidence >= 0.87


@pytest.mark.integration
def test_smart_search_with_ten_real_test_data_papers_author_and_typo(
    test_papers,
    real_keyword_test_context,
    monkeypatch,
):
    pytest.importorskip("en_core_sci_lg")
    pytest.importorskip("en_ner_bionlp13cg_md")

    indexer = _indexer_with_scispacy_keywords()
    source_names = set(DEFAULT_KEYWORD_SOURCES.values())
    suffix = uuid.uuid4().hex
    biorxiv_payloads = [
        _select_biorxiv_payload(test_papers, "RNPP-type quorum sensing"),
        _select_biorxiv_payload(test_papers, "Fes tyrosine kinase"),
        _select_biorxiv_payload(test_papers, "Drosophila Ventral Nervous System"),
        _select_biorxiv_payload(test_papers, "Sperm chemotaxis"),
        _select_biorxiv_payload(test_papers, "Functional gene categories"),
    ]
    langtaosha_payloads = list(test_papers["langtaosha"][:5])

    for idx, payload in enumerate(biorxiv_payloads):
        result = indexer.index_dict(
            raw_payload=_unique_biorxiv_payload(payload, suffix, idx),
            source_name="biorxiv_history",
        )
        assert result["success"] is True, result
        assert result["keyword_enrichment"]["success"] is True, result["keyword_enrichment"]
        real_keyword_test_context["paper_ids"].append(result["paper_id"])

    for idx, payload in enumerate(langtaosha_payloads, start=len(biorxiv_payloads)):
        result = indexer.index_dict(
            raw_payload=_unique_langtaosha_payload(payload, suffix, idx),
            source_name="langtaosha",
        )
        assert result["success"] is True, result
        assert result["keyword_enrichment"]["success"] is True, result["keyword_enrichment"]
        real_keyword_test_context["paper_ids"].append(result["paper_id"])

    assert len(real_keyword_test_context["paper_ids"]) == 10

    metadata_db = MetadataDB(config_path=get_config_path_from_args())
    rows = _keyword_rows(metadata_db, real_keyword_test_context["paper_ids"])
    rows_by_source = {}
    rows_by_paper = {}
    for row in rows:
        rows_by_source.setdefault(row["source"], []).append(row)
        rows_by_paper.setdefault(row["paper_id"], []).append(row)

    assert source_names.issubset(rows_by_source)
    assert all(rows_by_source[source] for source in source_names)
    assert set(rows_by_paper) == set(real_keyword_test_context["paper_ids"])
    assert all(any(row["source"] in source_names for row in paper_rows) for paper_rows in rows_by_paper.values())

    author_result = indexer.smart_search(
        "Chu Wang",
        source_list=["langtaosha"],
        top_k=5,
        hydrate=True,
    )
    assert author_result["success"] is True
    assert author_result["query_understanding"]["route"] == "metadata_author"
    assert author_result["query_understanding"]["matched_author"] == "Chu Wang"
    assert author_result["results"]
    assert all(
        any(source["source_name"] == "langtaosha" for source in result.get("sources", []))
        for result in author_result["results"]
    )

    vector_calls = []

    def fake_search(query, source_list=None, top_k=10, hydrate=True):
        vector_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
            }
        )
        return [{"paper_id": real_keyword_test_context["paper_ids"][0], "title": "stubbed vector result"}]

    monkeypatch.setattr(indexer, "search", fake_search)

    typo_result = indexer.smart_search(
        "solvent formtion",
        source_list=["biorxiv_history"],
        top_k=3,
        hydrate=False,
    )

    assert typo_result["success"] is True
    assert typo_result["query_understanding"]["route"] == "vector"
    assert typo_result["query_understanding"]["corrected_query"] == "solvent formation"
    assert typo_result["search_query"] == "solvent formation"
    assert vector_calls == [
        {
            "query": "solvent formation",
            "source_list": ["biorxiv_history"],
            "top_k": 3,
            "hydrate": False,
        }
    ]
