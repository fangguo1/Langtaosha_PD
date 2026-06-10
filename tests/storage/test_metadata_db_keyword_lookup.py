"""Unit tests for MetadataDB keyword lookup recall."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, init_config
from src.docset_hub.indexing.paper_keyword_lookup import (
    match_paper_keywords_using_span_matcher,
)
from src.docset_hub.indexing.query_phrase_analyzer import (
    MetadataDBPhraseLexicon,
    QueryPhraseAnalyzer,
)
from src.docset_hub.indexing.span_matcher import (
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    SpanMatcherExecutor,
)
from src.docset_hub.storage.metadata_db import MetadataDB


MIMIC_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "config"
    / "config_tecent_backend_server_mimic.yaml"
)


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.engine.last_sql = str(statement)
        self.engine.last_params = dict(params or {})
        return FakeResult(self.engine.lookup_rows(params or {}))


class FakeEngine:
    def __init__(self, papers, keywords, paper_sources):
        self.papers = papers
        self.keywords = keywords
        self.paper_sources = paper_sources
        self.last_sql = ""
        self.last_params = {}

    def connect(self):
        return FakeConnection(self)

    def lookup_rows(self, params):
        query_terms = []
        index = 0
        while f"lookup_term_{index}" in params:
            query_terms.append(
                {
                    "group_id": params.get(f"lookup_group_id_{index}", params.get(f"lookup_concept_idx_{index}")),
                    "item_id": params.get(f"lookup_item_id_{index}", f"item-{index}"),
                    "item_role": params.get(f"lookup_item_role_{index}", "selected_concept"),
                    "item_weight": params.get(f"lookup_item_weight_{index}", 1.0),
                    "concept_idx": params[f"lookup_concept_idx_{index}"],
                    "concept_id": params[f"lookup_concept_id_{index}"],
                    "concept_label": params[f"lookup_concept_label_{index}"],
                    "term": params[f"lookup_term_{index}"],
                    "term_role": params[f"lookup_term_role_{index}"],
                    "term_weight": params[f"lookup_term_weight_{index}"],
                    "route": params.get(f"lookup_route_{index}", "selected_exact"),
                    "route_weight": params.get(f"lookup_route_weight_{index}", 1.0),
                }
            )
            index += 1

        keyword_sources = {
            value
            for key, value in params.items()
            if key.startswith("lookup_keyword_source_")
        }
        paper_source_filter = {
            value
            for key, value in params.items()
            if key.startswith("lookup_paper_source_")
        }
        if "matched_group_count" in self.last_sql:
            return self.lookup_grouped_rows(params, query_terms, keyword_sources, paper_source_filter)

        strict = "HAVING COUNT(DISTINCT cm.concept_idx)" in self.last_sql
        total_concept_count = int(params["total_concept_count"])

        concept_matches = {}
        for term in query_terms:
            for keyword in self.keywords:
                if keyword["keyword"].lower() != term["term"]:
                    continue
                if keyword_sources and keyword["source"] not in keyword_sources:
                    continue
                if paper_source_filter and not (
                    set(self.paper_sources.get(keyword["paper_id"], [])) & paper_source_filter
                ):
                    continue

                key = (
                    keyword["paper_id"],
                    term["concept_idx"],
                    term["concept_id"],
                )
                match_score = term["term_weight"] * keyword.get("weight", 1.0)
                bucket = concept_matches.setdefault(
                    key,
                    {
                        "paper_id": keyword["paper_id"],
                        "concept_idx": term["concept_idx"],
                        "concept_id": term["concept_id"],
                        "concept_label": term["concept_label"],
                        "concept_score": 0.0,
                        "matched_keywords": [],
                    },
                )
                bucket["concept_score"] = max(bucket["concept_score"], match_score)
                bucket["matched_keywords"].append(
                    {
                        "query_term": term["term"],
                        "term_role": term["term_role"],
                        "keyword": keyword["keyword"],
                        "keyword_type": keyword["keyword_type"],
                        "keyword_source": keyword["source"],
                        "keyword_weight": keyword["weight"],
                    }
                )

        by_paper = defaultdict(list)
        for match in concept_matches.values():
            by_paper[match["paper_id"]].append(match)

        rows = []
        for paper_id, matches in by_paper.items():
            matched_concept_count = len({match["concept_idx"] for match in matches})
            if strict and matched_concept_count != total_concept_count:
                continue
            rows.append(
                {
                    "paper_id": paper_id,
                    "work_id": self.papers[paper_id],
                    "matched_concept_count": matched_concept_count,
                    "total_concept_count": total_concept_count,
                    "keyword_lookup_score": sum(match["concept_score"] for match in matches),
                    "matched_concepts": sorted(matches, key=lambda match: match["concept_idx"]),
                }
            )

        rows.sort(
            key=lambda row: (
                -row["matched_concept_count"],
                -row["keyword_lookup_score"],
                -row["paper_id"],
            )
        )
        return rows[: int(params["top_k"])]

    def lookup_grouped_rows(self, params, query_terms, keyword_sources, paper_source_filter):
        raw_matches = []
        for term in query_terms:
            for keyword in self.keywords:
                if keyword["keyword"].lower() != term["term"]:
                    continue
                if keyword_sources and keyword["source"] not in keyword_sources:
                    continue
                if paper_source_filter and not (
                    set(self.paper_sources.get(keyword["paper_id"], [])) & paper_source_filter
                ):
                    continue

                match_score = (
                    float(term["item_weight"])
                    * float(term["term_weight"])
                    * float(term["route_weight"])
                )
                raw_matches.append(
                    {
                        **term,
                        "paper_id": keyword["paper_id"],
                        "keyword": keyword["keyword"],
                        "keyword_type": keyword["keyword_type"],
                        "keyword_source": keyword["source"],
                        "keyword_weight": keyword["weight"],
                        "match_score": match_score,
                    }
                )

        item_scores = {}
        for match in raw_matches:
            key = (match["paper_id"], match["group_id"], match["item_id"])
            existing = item_scores.get(key)
            if existing is None or match["match_score"] > existing["item_score"]:
                item_scores[key] = {
                    "paper_id": match["paper_id"],
                    "group_id": match["group_id"],
                    "item_id": match["item_id"],
                    "item_role": match["item_role"],
                    "concept_idx": match["concept_idx"],
                    "concept_id": match["concept_id"],
                    "concept_label": match["concept_label"],
                    "item_score": match["match_score"],
                }

        keywords_by_group = defaultdict(list)
        for match in raw_matches:
            keywords_by_group[(match["paper_id"], match["group_id"])].append(
                {
                    "query_term": match["term"],
                    "term_role": match["term_role"],
                    "route": match["route"],
                    "route_weight": match["route_weight"],
                    "item_weight": match["item_weight"],
                    "keyword": match["keyword"],
                    "keyword_type": match["keyword_type"],
                    "keyword_source": match["keyword_source"],
                    "keyword_weight": match["keyword_weight"],
                    "match_score": match["match_score"],
                }
            )

        by_group = defaultdict(list)
        for item in item_scores.values():
            by_group[(item["paper_id"], item["group_id"])].append(item)

        group_support_cap = float(params["group_support_cap"])
        by_paper = defaultdict(list)
        for (paper_id, group_id), items in by_group.items():
            primary_score = max(
                [item["item_score"] for item in items if item["item_role"] == "selected_concept"] or [0.0]
            )
            support_score = sum(
                item["item_score"] for item in items if item["item_role"] != "selected_concept"
            )
            group_score = max(primary_score, min(group_support_cap, support_score))
            first = items[0]
            by_paper[paper_id].append(
                {
                    "concept_idx": first["concept_idx"],
                    "group_id": group_id,
                    "concept_id": first["concept_id"],
                    "concept_label": first["concept_label"],
                    "concept_score": group_score,
                    "primary_score": primary_score,
                    "support_score": min(group_support_cap, support_score),
                    "has_primary_match": primary_score > 0,
                    "matched_keywords": keywords_by_group[(paper_id, group_id)],
                }
            )

        rows = []
        total_group_count = int(params["total_group_count"])
        strict = "HAVING COUNT(DISTINCT gm.group_id)" in self.last_sql
        for paper_id, groups in by_paper.items():
            matched_group_count = len({group["group_id"] for group in groups})
            if strict and matched_group_count != total_group_count:
                continue
            matched_primary_group_count = sum(1 for group in groups if group["has_primary_match"])
            rows.append(
                {
                    "paper_id": paper_id,
                    "work_id": self.papers[paper_id],
                    "matched_group_count": matched_group_count,
                    "matched_primary_group_count": matched_primary_group_count,
                    "total_group_count": total_group_count,
                    "keyword_lookup_score": sum(group["concept_score"] for group in groups),
                    "matched_concepts": sorted(groups, key=lambda group: group["group_id"]),
                }
            )

        rows.sort(
            key=lambda row: (
                -row["matched_group_count"],
                -row["matched_primary_group_count"],
                -row["keyword_lookup_score"],
                -row["paper_id"],
            )
        )
        return rows[: int(params["top_k"])]


def _metadata_db_with_fake_engine():
    metadata_db = MetadataDB.__new__(MetadataDB)
    metadata_db.engine = FakeEngine(
        papers={1: "W1", 2: "W2", 3: "W3", 4: "W4"},
        keywords=[
            _keyword(1, "T cell", "generated", 0.9),
            _keyword(1, "Melanoma", "generated", 1.0),
            _keyword(1, "Deep Learning", "generated", 0.8),
            _keyword(2, "T lymphocyte", "generated", 0.9),
            _keyword(2, "Melanoma", "generated", 0.7),
            _keyword(3, "Melanoma", "generated", 1.0),
            _keyword(4, "T cell", "manual", 1.0),
            _keyword(4, "T lymphocyte", "manual", 0.8),
        ],
        paper_sources={
            1: ["langtaosha"],
            2: ["biorxiv_daily"],
            3: ["biorxiv_history"],
            4: ["langtaosha"],
        },
    )
    return metadata_db


def _keyword(paper_id, keyword, source, weight):
    return {
        "paper_id": paper_id,
        "keyword": keyword,
        "keyword_type": "concept",
        "source": source,
        "weight": weight,
    }


def _query_terms():
    return [
        _term(1, "C1", "T cell", "t cell", "primary_canonical", 1.0),
        _term(1, "C1", "T cell", "t lymphocyte", "alias", 0.8),
        _term(2, "C2", "Melanoma", "melanoma", "primary_canonical", 1.0),
        _term(3, "C3", "Deep Learning", "deep learning", "primary_canonical", 1.0),
    ]


def _grouped_query_terms():
    return [
        _plan_term(1, "g1:sub:adhesion", "sub_concept", 0.55, "Adhesion protein", "adhesion"),
        _plan_term(1, "g1:sub:protein", "broad_sub_concept", 0.30, "Adhesion protein", "protein"),
        _plan_term(2, "g2:selected:kidney", "selected_concept", 1.00, "Kidney", "kidney"),
    ]


def _term(concept_idx, concept_id, concept_label, term, term_role, weight):
    return {
        "concept_idx": concept_idx,
        "concept_id": concept_id,
        "concept_label": concept_label,
        "term": term,
        "term_role": term_role,
        "term_weight": weight,
    }


def _plan_term(group_id, item_id, item_role, item_weight, concept_label, term):
    return {
        "group_id": group_id,
        "item_id": item_id,
        "item_role": item_role,
        "item_weight": item_weight,
        "concept_idx": group_id,
        "concept_id": f"keyword:{concept_label.lower()}",
        "concept_label": concept_label,
        "term": term,
        "term_role": "primary_canonical",
        "term_weight": 1.0,
        "route": "sub_concept_exact" if item_role != "selected_concept" else "selected_exact",
        "route_weight": 1.0,
    }


def test_lookup_papers_by_keyword_terms_soft_coverage_ranks_by_matched_concepts():
    metadata_db = _metadata_db_with_fake_engine()

    rows = metadata_db.lookup_papers_by_keyword_terms(_query_terms(), top_k=10)

    assert [row["paper_id"] for row in rows] == [1, 2, 4, 3]
    assert rows[0]["matched_concept_count"] == 3
    assert rows[1]["matched_concept_count"] == 2
    assert rows[2]["matched_concept_count"] == 1
    assert rows[0]["recall_sources"] == ["keyword_lookup"]
    assert rows[0]["retrieval_debug"]["retriever"] == "keyword_lookup"


def test_lookup_papers_by_keyword_terms_aliases_count_as_one_concept():
    metadata_db = _metadata_db_with_fake_engine()

    rows = metadata_db.lookup_papers_by_keyword_terms(
        [_query_terms()[0], _query_terms()[1]],
        keyword_sources=["manual"],
        top_k=10,
    )

    assert rows[0]["paper_id"] == 4
    assert rows[0]["matched_concept_count"] == 1
    assert rows[0]["total_concept_count"] == 1


def test_lookup_papers_by_keyword_terms_strict_all_concepts_filters_partial_hits():
    metadata_db = _metadata_db_with_fake_engine()

    rows = metadata_db.lookup_papers_by_keyword_terms(
        _query_terms(),
        strict_all_concepts=True,
        top_k=10,
    )

    assert [row["paper_id"] for row in rows] == [1]
    assert "HAVING COUNT(DISTINCT cm.concept_idx)" in metadata_db.engine.last_sql


def test_lookup_papers_by_keyword_terms_source_filters_are_applied():
    metadata_db = _metadata_db_with_fake_engine()

    source_rows = metadata_db.lookup_papers_by_keyword_terms(
        _query_terms(),
        source_list=["biorxiv_daily"],
        top_k=10,
    )
    source_sql = metadata_db.engine.last_sql
    keyword_source_rows = metadata_db.lookup_papers_by_keyword_terms(
        _query_terms(),
        keyword_sources=["manual"],
        top_k=10,
    )

    assert [row["paper_id"] for row in source_rows] == [2]
    assert [row["paper_id"] for row in keyword_source_rows] == [4]
    assert "EXISTS (SELECT 1 FROM paper_sources ps" in source_sql


def test_lookup_papers_by_keyword_terms_empty_terms_return_empty_without_query():
    metadata_db = _metadata_db_with_fake_engine()

    assert metadata_db.lookup_papers_by_keyword_terms([]) == []
    assert metadata_db.engine.last_sql == ""


def test_lookup_papers_by_keyword_lookup_terms_groups_support_scores():
    metadata_db = _metadata_db_with_fake_engine()
    metadata_db.engine.papers[5] = "W5"
    metadata_db.engine.paper_sources[5] = ["langtaosha"]
    metadata_db.engine.keywords.extend(
        [
            _keyword(5, "Adhesion", "generated", 1.0),
            _keyword(5, "Protein", "generated", 1.0),
            _keyword(5, "Kidney", "generated", 1.0),
        ]
    )

    rows = metadata_db.lookup_papers_by_keyword_lookup_terms(
        _grouped_query_terms(),
        keyword_sources=["generated"],
        top_k=10,
    )

    assert rows[0]["paper_id"] == 5
    assert rows[0]["matched_group_count"] == 2
    assert rows[0]["matched_primary_group_count"] == 1
    assert rows[0]["total_group_count"] == 2
    assert rows[0]["keyword_lookup_score"] == pytest.approx(1.85)
    group_one = next(group for group in rows[0]["matched_concepts"] if group["group_id"] == 1)
    assert group_one["support_score"] == pytest.approx(0.85)
    assert group_one["has_primary_match"] is False


def test_lookup_papers_by_expanded_sparse_groups_uses_whole_word_exact_matching():
    metadata_db = _metadata_db_with_fake_engine()
    metadata_db.engine.papers[5] = "W5"
    metadata_db.engine.paper_sources[5] = ["langtaosha"]
    metadata_db.engine.keywords.append(_keyword(5, "ren", "generated", 1.0))
    metadata_db.engine.lookup_rows = lambda params: []

    rows = metadata_db.lookup_papers_by_expanded_sparse_groups(
        [
            {
                "group_id": 1,
                "span_id": "s1",
                "canonical_text": "Ren",
                "term": "ren",
                "span_scope": "parent",
                "child_span_id": None,
                "term_tier": "tier1",
                "match_mode": "exact",
            }
        ],
        source_list=["langtaosha"],
        keyword_sources=["generated"],
        top_k=10,
    )

    assert "POSITION(qt.term IN lower(COALESCE(p.canonical_title, ''))) > 0" not in metadata_db.engine.last_sql
    assert "POSITION(qt.term IN lower(COALESCE(p.canonical_abstract, ''))) > 0" not in metadata_db.engine.last_sql
    assert "regexp_replace(qt.term" in metadata_db.engine.last_sql
    assert rows == []


@pytest.fixture(scope="module")
def mimic_metadata_db():
    if not MIMIC_CONFIG_PATH.exists():
        pytest.skip(f"mimic config not found: {MIMIC_CONFIG_PATH}")

    _reset_config()
    init_config(MIMIC_CONFIG_PATH, force_reload=True)
    metadata_db = MetadataDB(config_path=MIMIC_CONFIG_PATH)
    try:
        with metadata_db.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"mimic metadata DB unavailable: {exc}")
    return metadata_db


def _select_mimic_ground_truth_keyword_case(metadata_db):
    """Pick a real paper whose low-doc-count keywords should recall itself."""

    with metadata_db.engine.connect() as conn:
        row = conn.execute(
            text(
                """
                WITH keyword_stats AS (
                    SELECT
                        lower(keyword) AS normalized_keyword,
                        COUNT(DISTINCT paper_id) AS doc_count
                    FROM paper_keywords
                    WHERE length(btrim(keyword)) BETWEEN 4 AND 80
                    GROUP BY lower(keyword)
                ),
                scoped_raw AS (
                    SELECT
                        pk.paper_id,
                        p.work_id,
                        pk.keyword,
                        lower(pk.keyword) AS normalized_keyword,
                        pk.keyword_type,
                        pk.source AS keyword_source,
                        ks.doc_count,
                        POSITION(
                            lower(pk.keyword)
                            IN lower(COALESCE(p.canonical_title, '') || ' ' || COALESCE(p.canonical_abstract, ''))
                        ) = 0 AS absent_from_text
                    FROM paper_keywords pk
                    JOIN papers p ON p.paper_id = pk.paper_id
                    JOIN keyword_stats ks ON ks.normalized_keyword = lower(pk.keyword)
                    WHERE length(btrim(pk.keyword)) BETWEEN 4 AND 80
                      AND ks.doc_count BETWEEN 1 AND 40
                      AND lower(pk.keyword) NOT IN ('neuroscience', 'biology', 'genetics')
                      AND pk.keyword !~ '^[0-9[:punct:][:space:]]+$'
                ),
                scoped AS (
                    SELECT DISTINCT ON (paper_id, normalized_keyword)
                        *
                    FROM scoped_raw
                    ORDER BY
                        paper_id,
                        normalized_keyword,
                        absent_from_text DESC,
                        doc_count ASC,
                        length(keyword) DESC
                ),
                ranked AS (
                    SELECT
                        paper_id,
                        work_id,
                        ARRAY_AGG(keyword ORDER BY absent_from_text DESC, doc_count ASC, length(keyword) DESC) AS keywords,
                        ARRAY_AGG(absent_from_text ORDER BY absent_from_text DESC, doc_count ASC, length(keyword) DESC) AS absent_flags,
                        ARRAY_AGG(doc_count ORDER BY absent_from_text DESC, doc_count ASC, length(keyword) DESC) AS doc_counts,
                        COUNT(*) AS keyword_count,
                        SUM(1.0 / doc_count) AS specificity,
                        BOOL_OR(absent_from_text) AS has_absent
                    FROM scoped
                    GROUP BY paper_id, work_id
                    HAVING COUNT(*) >= 3
                       AND BOOL_OR(absent_from_text)
                )
                SELECT
                    paper_id,
                    work_id,
                    keywords[1:3] AS keywords,
                    absent_flags[1:3] AS absent_flags,
                    doc_counts[1:3] AS doc_counts
                FROM ranked
                ORDER BY specificity DESC, keyword_count DESC, paper_id DESC
                LIMIT 1
                """
            )
        ).mappings().fetchone()

    if row is None:
        pytest.skip("no suitable mimic paper with low-doc-count paper_keywords was found")
    return dict(row)


def _keyword_lookup_results_for_query(metadata_db, query):
    lexicon = MetadataDBPhraseLexicon(metadata_db=metadata_db)
    analyzer = QueryPhraseAnalyzer(lexicon=lexicon)
    normalized = analyzer.normalizer.normalize_query(query).normalized_query
    candidates = analyzer.extractor.extract(normalized)
    executor = SpanMatcherExecutor(KeywordSurfaceSpanMatcher(lexicon))
    span_results = executor.match_candidates(candidates)
    selected_concepts = MaximalConceptSelector().select(span_results)
    lookup_results = match_paper_keywords_using_span_matcher(
        metadata_db=metadata_db,
        selected_concepts=selected_concepts,
        top_k=20,
    )
    return selected_concepts, lookup_results


@pytest.mark.integration
def test_mimic_keyword_lookup_recalls_ground_truth_paper_from_its_keywords(mimic_metadata_db):
    ground_truth = _select_mimic_ground_truth_keyword_case(mimic_metadata_db)
    query = " and ".join(ground_truth["keywords"])

    selected_concepts, lookup_results = _keyword_lookup_results_for_query(
        metadata_db=mimic_metadata_db,
        query=query,
    )

    assert any(ground_truth["absent_flags"]), (
        "test should include at least one paper_keyword that is not a title/abstract substring"
    )
    assert len(selected_concepts) >= 2

    target_result = next(
        (result for result in lookup_results if result.paper_id == ground_truth["paper_id"]),
        None,
    )
    assert target_result is not None, {
        "paper_id": ground_truth["paper_id"],
        "work_id": ground_truth["work_id"],
        "query": query,
        "selected": [concept.primary_evidence.canonical for concept in selected_concepts],
        "returned_paper_ids": [result.paper_id for result in lookup_results],
    }
    assert target_result.matched_concept_count >= 2
    assert target_result.recall_sources == ["keyword_lookup"]
