"""Real-service expanded sparse retrieval checks.

This suite is opt-in because it depends on the live PostgreSQL test environment.
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

from src.config import _reset_config, init_config
from src.docset_hub.indexing.expanded_sparse_retrieval import match_papers_by_expanded_sparse_plan
from src.docset_hub.indexing.query_semantic_plan import (
    QuerySemanticPlan,
    SemanticSpanGroup,
    SemanticTerm,
    SemanticTermBucket,
)
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

    if config_path is None and os.environ.get("EXPANDED_SPARSE_TEST_CONFIG"):
        config_path = Path(os.environ["EXPANDED_SPARSE_TEST_CONFIG"])

    if config_path is None:
        worktree_candidate = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "config"
            / "config_tecent_backend_server_test.yaml"
        )
        repo_candidate = Path("/home/wnlab/langtaosha/Langtaosha_PD/src/config/config_tecent_backend_server_test.yaml")
        config_path = worktree_candidate if worktree_candidate.exists() else repo_candidate

    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")

    _global_config_path = config_path
    return config_path


@pytest.fixture(scope="session")
def metadata_db():
    if os.environ.get("RUN_REAL_EXPANDED_SPARSE_INTEGRATION") != "1":
        pytest.skip("set RUN_REAL_EXPANDED_SPARSE_INTEGRATION=1 to run live expanded sparse checks")
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return MetadataDB(config_path=get_config_path_from_args())


@pytest.fixture(scope="session")
def transformer():
    return MetadataTransformer()


def _make_plan() -> QuerySemanticPlan:
    return QuerySemanticPlan(
        original_query="adhesion protein in renal tissue",
        normalized_query="adhesion protein in renal tissue",
        spans=[
            SemanticSpanGroup(
                span_id="s1",
                surface_text="adhesion protein",
                normalized_text="adhesion protein",
            start=0,
            end=16,
            canonical_text="Adhesion protein",
            own_terms=SemanticTermBucket(
                tier1=[SemanticTerm("adhesion protein")],
                tier2=[
                    SemanticTerm("cell adhesion protein"),
                    SemanticTerm("adhesion molecule"),
                    SemanticTerm("cell adhesion molecule"),
                ],
            ),
            evidence=[],
        ),
            SemanticSpanGroup(
                span_id="s2",
                surface_text="renal tissue",
                normalized_text="renal tissue",
            start=20,
            end=32,
            canonical_text="Renal tissue",
            own_terms=SemanticTermBucket(
                tier1=[SemanticTerm("renal tissue")],
                tier2=[
                    SemanticTerm("renal"),
                    SemanticTerm("kidney tissue"),
                    SemanticTerm("kidney"),
                ],
            ),
            evidence=[],
        ),
        ],
    )


def _unique_payload(raw_payload: Dict[str, Any], *, title: str, abstract: str) -> Dict[str, Any]:
    payload = copy.deepcopy(raw_payload)
    suffix = uuid.uuid4().hex
    numeric_id = str(940000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [f"10.65215/expanded-sparse.{suffix}"]
    payload["meta"]["citation_title"] = [title]
    payload["meta"]["citation_abstract"] = [abstract]
    return payload


def _insert_real_payload(metadata_db: MetadataDB, transformer: MetadataTransformer, raw_payload: Dict[str, Any]) -> int:
    transform_result = transformer.transform_dict(raw_payload, source_name="langtaosha")
    assert transform_result.success, transform_result.error
    write_result = metadata_db.insert_paper(
        db_payload=transform_result.db_payload,
        upsert_key=transform_result.upsert_key,
    )
    assert write_result["paper_id"] is not None
    return int(write_result["paper_id"])


def _insert_keywords(metadata_db: MetadataDB, paper_id: int, keywords: list[dict[str, Any]], source: str) -> None:
    result = metadata_db.upsert_generated_keywords(
        paper_id=paper_id,
        keywords=keywords,
        source=source,
    )
    assert not result.get("errors"), result


@pytest.fixture(scope="function")
def expanded_sparse_real_papers(metadata_db, transformer, test_papers):
    created_ids: list[int] = []
    base_payload = test_papers["langtaosha"][0]
    keyword_source = "scispacy-en_core_sci_lg-generated-test"

    try:
        full_hit = _insert_real_payload(
            metadata_db,
            transformer,
            _unique_payload(
                base_payload,
                title="Cell adhesion molecules in renal epithelial injury",
                abstract="This work studies adhesion molecule programs in renal tissue damage.",
            ),
        )
        created_ids.append(full_hit)
        _insert_keywords(
            metadata_db,
            full_hit,
            [
                {"keyword_type": "concept", "keyword": "cell adhesion molecule", "weight": 1.0},
                {"keyword_type": "concept", "keyword": "renal tissue", "weight": 1.0},
            ],
            source=keyword_source,
        )

        kidney_only = _insert_real_payload(
            metadata_db,
            transformer,
            _unique_payload(
                base_payload,
                title="Protein expression in kidney disease",
                abstract="We profile renal dysfunction in chronic tissue injury.",
            ),
        )
        created_ids.append(kidney_only)
        _insert_keywords(
            metadata_db,
            kidney_only,
            [
                {"keyword_type": "concept", "keyword": "kidney", "weight": 1.0},
            ],
            source=keyword_source,
        )

        adhesion_only = _insert_real_payload(
            metadata_db,
            transformer,
            _unique_payload(
                base_payload,
                title="Adhesion protein regulation in melanoma",
                abstract="Adhesion molecule programs were studied outside renal contexts.",
            ),
        )
        created_ids.append(adhesion_only)
        _insert_keywords(
            metadata_db,
            adhesion_only,
            [
                {"keyword_type": "concept", "keyword": "adhesion protein", "weight": 1.0},
            ],
            source=keyword_source,
        )

        yield {
            "paper_ids": {
                "full_hit": full_hit,
                "kidney_only": kidney_only,
                "adhesion_only": adhesion_only,
            },
            "keyword_source": keyword_source,
        }
    finally:
        for paper_id in created_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)


@pytest.mark.integration
def test_live_expanded_sparse_retrieval_reports_expected_span_coverage(
    metadata_db,
    expanded_sparse_real_papers,
):
    plan = _make_plan()
    result_items = match_papers_by_expanded_sparse_plan(
        metadata_db=metadata_db,
        plan=plan,
        source_list=["langtaosha"],
        keyword_sources=[expanded_sparse_real_papers["keyword_source"]],
        top_k=10,
    )

    by_paper_id = {item.paper_id: item for item in result_items}
    ids = expanded_sparse_real_papers["paper_ids"]

    assert ids["full_hit"] in by_paper_id
    assert ids["kidney_only"] in by_paper_id
    assert ids["adhesion_only"] in by_paper_id

    full_hit = by_paper_id[ids["full_hit"]]
    assert full_hit.matched_span_count == 2
    assert full_hit.total_span_count == 2
    assert full_hit.coverage_ratio == pytest.approx(1.0)
    full_terms = {term for span in full_hit.matched_spans for term in span.get("matched_terms", [])}
    full_fields = {field for span in full_hit.matched_spans for field in span.get("matched_fields", [])}
    assert {"cell adhesion molecule", "renal tissue"} & full_terms
    assert {"title", "abstract", "paper_keywords"} & full_fields

    kidney_only = by_paper_id[ids["kidney_only"]]
    assert kidney_only.matched_span_count == 1
    assert kidney_only.total_span_count == 2
    assert kidney_only.coverage_ratio == pytest.approx(0.5)

    adhesion_only = by_paper_id[ids["adhesion_only"]]
    assert adhesion_only.matched_span_count == 1
    assert adhesion_only.total_span_count == 2
    assert adhesion_only.coverage_ratio == pytest.approx(0.5)


@pytest.mark.integration
def test_live_expanded_sparse_retrieval_returns_group_level_match_details(
    metadata_db,
    expanded_sparse_real_papers,
):
    plan = _make_plan()
    result_items = match_papers_by_expanded_sparse_plan(
        metadata_db=metadata_db,
        plan=plan,
        source_list=["langtaosha"],
        keyword_sources=[expanded_sparse_real_papers["keyword_source"]],
        top_k=10,
    )

    full_hit_id = expanded_sparse_real_papers["paper_ids"]["full_hit"]
    full_hit = next(item for item in result_items if item.paper_id == full_hit_id)

    matched_span_ids = {span["span_id"] for span in full_hit.matched_spans}
    assert matched_span_ids == {"s1", "s2"}
    assert all(span.get("matched_terms") for span in full_hit.matched_spans)
    assert all(span.get("matched_fields") for span in full_hit.matched_spans)

    with metadata_db.engine.connect() as conn:
        keyword_rows = conn.execute(
            text(
                """
                SELECT keyword
                FROM paper_keywords
                WHERE paper_id = :paper_id
                ORDER BY keyword
                """
            ),
            {"paper_id": full_hit_id},
        ).mappings().all()
    assert keyword_rows, "expected cleanup fixture to have inserted real paper_keywords rows"
