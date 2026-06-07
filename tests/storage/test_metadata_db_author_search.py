"""Tests for MetadataDB author search and author suggestions.

Uses real payloads from test_data via the shared test_papers fixture.
"""

from __future__ import annotations

import argparse
import copy
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, get_db_engine, init_config
from src.docset_hub.metadata.transformer import MetadataTransformer
from src.docset_hub.storage.metadata_db import MetadataDB


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


@pytest.fixture(scope="session")
def db_engine():
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return get_db_engine("metadata_db")


@pytest.fixture(scope="function")
def metadata_db():
    return MetadataDB(config_path=get_config_path_from_args())


@pytest.fixture(scope="session")
def transformer():
    return MetadataTransformer()


def _unique_langtaosha_payload(raw_payload: Dict[str, Any], title_suffix: str = "") -> Dict[str, Any]:
    payload = copy.deepcopy(raw_payload)
    suffix = uuid.uuid4().hex
    numeric_id = str(930000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [f"10.65215/query-understanding-author.{suffix}"]
    if title_suffix:
        payload["meta"]["citation_title"] = [
            f"{payload['meta']['citation_title'][0]} {title_suffix}"
        ]
    return payload


def _unique_biorxiv_payload(raw_payload: Dict[str, Any], title_suffix: str = "") -> Dict[str, Any]:
    payload = copy.deepcopy(raw_payload)
    suffix = uuid.uuid4().hex
    payload["doi"] = f"10.1101/query-understanding-author.{suffix}"
    if title_suffix:
        payload["title"] = f"{payload['title']} {title_suffix}"
    return payload


def _insert_real_payload(metadata_db, transformer, raw_payload, source_name: str) -> int:
    transform_result = transformer.transform_dict(raw_payload, source_name=source_name)
    assert transform_result.success, transform_result.error
    write_result = metadata_db.insert_paper(
        db_payload=transform_result.db_payload,
        upsert_key=transform_result.upsert_key,
    )
    assert write_result["paper_id"] is not None
    return write_result["paper_id"]


def _biorxiv_author_names(payload: Dict[str, Any]) -> list[str]:
    return [name.strip() for name in payload["authors"].split(";") if name.strip()]


def _first_repeated_author_cases(test_papers):
    cases = []
    for payload in test_papers["langtaosha"][:5]:
        cases.append(
            {
                "source_name": "langtaosha",
                "payload": payload,
                "authors": payload["meta"]["citation_author"],
            }
        )
    for payload in test_papers["biorxiv_history"][:5]:
        cases.append(
            {
                "source_name": "biorxiv_history",
                "payload": payload,
                "authors": _biorxiv_author_names(payload),
            }
        )

    seen = {}
    for case in cases:
        for author_name in case["authors"]:
            key = " ".join(author_name.lower().split())
            if key in seen:
                return author_name, [seen[key], case]
            seen[key] = case
    return None, []


def _payload_with_author(payload: Dict[str, Any], source_name: str, author_name: str) -> Dict[str, Any]:
    payload = copy.deepcopy(payload)
    if source_name == "langtaosha":
        authors = payload["meta"].setdefault("citation_author", [])
        assert authors
        authors[0] = author_name
        return payload

    authors = _biorxiv_author_names(payload)
    assert authors
    authors[0] = author_name
    payload["authors"] = "; ".join(authors)
    return payload


@pytest.fixture(scope="function")
def author_papers(metadata_db, transformer, test_papers):
    paper_ids = []
    langtaosha_payload = test_papers["langtaosha"][0]
    biorxiv_payload = test_papers["biorxiv_daily"][0]

    first_author = langtaosha_payload["meta"]["citation_author"][0]
    first_author_token = first_author.split()[0]
    institution = langtaosha_payload["meta"]["citation_author_institution"][0]
    biorxiv_first_author = biorxiv_payload["authors"].split(";")[0].strip()

    alice_old = _insert_real_payload(
        metadata_db,
        transformer,
        _unique_langtaosha_payload(langtaosha_payload, "author-old"),
        "langtaosha",
    )
    paper_ids.append(alice_old)

    alice_new = _insert_real_payload(
        metadata_db,
        transformer,
        _unique_langtaosha_payload(langtaosha_payload, "author-new"),
        "langtaosha",
    )
    paper_ids.append(alice_new)

    biorxiv_daily = _insert_real_payload(
        metadata_db,
        transformer,
        _unique_biorxiv_payload(biorxiv_payload, "author-daily"),
        "biorxiv_daily",
    )
    paper_ids.append(biorxiv_daily)

    created = {
        "first_author": first_author,
        "first_author_token": first_author_token,
        "institution": institution,
        "biorxiv_first_author": biorxiv_first_author,
        "alice_old": alice_old,
        "alice_new": alice_new,
        "biorxiv_daily": biorxiv_daily,
    }

    try:
        yield created
    finally:
        for paper_id in paper_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)


def test_search_by_author_exact_match_returns_papers(metadata_db, author_papers):
    results = metadata_db.search_by_author(author_papers["first_author"], limit=10)

    assert {paper["paper_id"] for paper in results} >= {
        author_papers["alice_old"],
        author_papers["alice_new"],
    }


def test_search_by_author_partial_match_returns_papers(metadata_db, author_papers):
    results = metadata_db.search_by_author(author_papers["first_author_token"], limit=10)

    assert author_papers["alice_old"] in {paper["paper_id"] for paper in results}


def test_search_by_author_only_matches_author_name_field(metadata_db, author_papers):
    results = metadata_db.search_by_author(author_papers["institution"], limit=10)

    created_ids = {
        author_papers["alice_old"],
        author_papers["alice_new"],
        author_papers["biorxiv_daily"],
    }
    assert created_ids.isdisjoint({paper["paper_id"] for paper in results})


def test_search_by_author_nonexistent_author_returns_empty_list(metadata_db):
    assert metadata_db.search_by_author("Nonexistent Query Understanding Author") == []


def test_search_by_author_respects_limit(metadata_db, author_papers):
    results = metadata_db.search_by_author(author_papers["first_author"], limit=1)

    assert len(results) == 1


def test_search_by_author_respects_source_list_filter(metadata_db, author_papers):
    results = metadata_db.search_by_author(
        author_papers["biorxiv_first_author"],
        limit=10,
        source_list=["biorxiv_daily"],
    )

    assert author_papers["biorxiv_daily"] in {paper["paper_id"] for paper in results}


def test_search_by_author_deduplicates_cross_source_same_paper(metadata_db, db_engine, author_papers):
    with db_engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO paper_sources (
                    paper_id, source_name, source_record_id, title, abstract, online_at
                )
                VALUES (
                    :paper_id, 'biorxiv_history', :source_record_id,
                    'duplicate source for author test', 'duplicate source', '2026-04-01 00:00:00'
                )
                """
            ),
            {
                "paper_id": author_papers["alice_old"],
                "source_record_id": f"query_understanding_test_author_dup_{uuid.uuid4().hex}",
            },
        )
        conn.commit()

    results = metadata_db.search_by_author(author_papers["first_author"], limit=10)
    ids = [paper["paper_id"] for paper in results]
    assert ids.count(author_papers["alice_old"]) == 1


def test_search_by_author_orders_results_by_online_at_desc(metadata_db, author_papers):
    results = metadata_db.search_by_author(author_papers["first_author"], limit=10)
    ids = [paper["paper_id"] for paper in results]

    assert author_papers["alice_new"] in ids
    assert author_papers["alice_old"] in ids


def test_search_by_author_with_five_langtaosha_and_five_biorxiv_real_papers(
    metadata_db, transformer, test_papers
):
    paper_ids = []
    inserted_cases = []
    langtaosha_payloads = test_papers["langtaosha"][:5]
    biorxiv_payloads = [
        payload
        for payload in test_papers["biorxiv_history"]
        if len([name.strip() for name in payload["authors"].split(";") if name.strip()]) >= 2
    ][:5]

    assert len(langtaosha_payloads) == 5
    assert len(biorxiv_payloads) == 5

    try:
        for idx, payload in enumerate(langtaosha_payloads):
            author_names = payload["meta"]["citation_author"][:2]
            assert len(author_names) == 2
            paper_id = _insert_real_payload(
                metadata_db,
                transformer,
                _unique_langtaosha_payload(payload, f"batch-langtaosha-{idx}"),
                "langtaosha",
            )
            paper_ids.append(paper_id)
            for author_name in author_names:
                inserted_cases.append(
                    {
                        "paper_id": paper_id,
                        "author_name": author_name,
                        "source_name": "langtaosha",
                    }
                )

        for idx, payload in enumerate(biorxiv_payloads):
            author_names = [name.strip() for name in payload["authors"].split(";") if name.strip()][:2]
            assert len(author_names) == 2
            paper_id = _insert_real_payload(
                metadata_db,
                transformer,
                _unique_biorxiv_payload(payload, f"batch-biorxiv-{idx}"),
                "biorxiv_history",
            )
            paper_ids.append(paper_id)
            for author_name in author_names:
                inserted_cases.append(
                    {
                        "paper_id": paper_id,
                        "author_name": author_name,
                        "source_name": "biorxiv_history",
                    }
                )

        for case in inserted_cases:
            results = metadata_db.search_by_author(
                case["author_name"],
                limit=100,
                source_list=[case["source_name"]],
            )
            assert case["paper_id"] in {paper["paper_id"] for paper in results}
    finally:
        for paper_id in paper_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)


def test_search_by_author_returns_multiple_real_papers_for_same_author(
    metadata_db, transformer, test_papers
):
    paper_ids = []
    author_name, duplicate_cases = _first_repeated_author_cases(test_papers)

    if not duplicate_cases:
        author_name = "Query Understanding Shared Author"
        duplicate_cases = [
            {
                "source_name": "langtaosha",
                "payload": _payload_with_author(
                    test_papers["langtaosha"][0],
                    "langtaosha",
                    author_name,
                ),
            },
            {
                "source_name": "langtaosha",
                "payload": _payload_with_author(
                    test_papers["langtaosha"][1],
                    "langtaosha",
                    author_name,
                ),
            },
        ]

    try:
        for idx, case in enumerate(duplicate_cases[:2]):
            source_name = case["source_name"]
            if source_name == "langtaosha":
                payload = _unique_langtaosha_payload(case["payload"], f"same-author-{idx}")
            else:
                payload = _unique_biorxiv_payload(case["payload"], f"same-author-{idx}")

            paper_ids.append(
                _insert_real_payload(
                    metadata_db,
                    transformer,
                    payload,
                    source_name,
                )
            )

        results = metadata_db.search_by_author(author_name, limit=20)
        result_ids = {paper["paper_id"] for paper in results}

        assert len(paper_ids) == 2
        assert set(paper_ids).issubset(result_ids)
    finally:
        for paper_id in paper_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)


def test_suggest_author_names_exact_match_scores_one(metadata_db, author_papers):
    suggestions = metadata_db.suggest_author_names(author_papers["first_author"], limit=3)

    assert suggestions[0]["name"] == author_papers["first_author"]
    assert suggestions[0]["score"] == pytest.approx(1.0)


def test_suggest_author_names_fuzzy_typo_returns_best_candidate(metadata_db, author_papers):
    typo = author_papers["first_author"].replace("a", "", 1)
    suggestions = metadata_db.suggest_author_names(typo, limit=3)

    assert suggestions[0]["name"] == author_papers["first_author"]
    assert suggestions[0]["score"] > 0.8


def test_suggest_author_names_returns_paper_count(metadata_db, author_papers):
    suggestions = metadata_db.suggest_author_names(author_papers["first_author"], limit=3)

    assert suggestions[0]["paper_count"] >= 2


def test_suggest_author_names_deduplicates_author_variants(metadata_db, author_papers):
    suggestions = metadata_db.suggest_author_names(author_papers["first_author"], limit=5)

    assert [item["name"] for item in suggestions].count(author_papers["first_author"]) == 1


def test_suggest_author_names_respects_limit(metadata_db, author_papers):
    suggestions = metadata_db.suggest_author_names(author_papers["first_author_token"], limit=1)

    assert len(suggestions) == 1
