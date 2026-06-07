#!/usr/bin/env python3
"""Read-only smoke test for span-selected keyword lookup recall."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import _reset_config, init_config  # noqa: E402
from src.docset_hub.indexing.paper_keyword_lookup import (  # noqa: E402
    match_paper_keywords_using_span_matcher,
)
from src.docset_hub.indexing.query_phrase_analyzer import (  # noqa: E402
    MetadataDBPhraseLexicon,
    QueryPhraseAnalyzer,
)
from src.docset_hub.indexing.span_matcher import (  # noqa: E402
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    SpanMatcherExecutor,
)
from src.docset_hub.storage.metadata_db import MetadataDB  # noqa: E402


DEFAULT_CONFIG = (
    PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"
)


def parse_csv(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--query",
        default="melanoma and deep learning",
        help="Connector-separated query for keyword-only span matching.",
    )
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--source-list", default=None)
    parser.add_argument("--keyword-sources", default=None)
    args = parser.parse_args()

    _reset_config()
    init_config(args.config, force_reload=True)
    metadata_db = MetadataDB(config_path=args.config)

    source_list = parse_csv(args.source_list)
    keyword_sources = parse_csv(args.keyword_sources)
    lexicon = MetadataDBPhraseLexicon(
        metadata_db=metadata_db,
        paper_source_names=source_list,
        keyword_sources=keyword_sources,
    )
    analyzer = QueryPhraseAnalyzer(lexicon=lexicon)
    normalized = analyzer.normalizer.normalize_query(args.query).normalized_query
    candidates = analyzer.extractor.extract(normalized)
    executor = SpanMatcherExecutor(KeywordSurfaceSpanMatcher(lexicon))
    span_results = executor.match_candidates(candidates)
    selected_concepts = MaximalConceptSelector().select(span_results)
    lookup_results = match_paper_keywords_using_span_matcher(
        metadata_db=metadata_db,
        selected_concepts=selected_concepts,
        source_list=source_list,
        keyword_sources=keyword_sources,
        top_k=args.top_k,
    )

    payload = {
        "config": str(args.config),
        "query": args.query,
        "selected_concepts": [
            {
                "text": concept.candidate.text,
                "canonical": concept.primary_evidence.canonical,
                "concept_id": concept.primary_evidence.concept_id,
            }
            for concept in selected_concepts
        ],
        "result_count": len(lookup_results),
        "results": [result.to_dict() for result in lookup_results[: args.top_k]],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))

    if not selected_concepts:
        print("No selected concepts were produced.", file=sys.stderr)
        return 2
    if not lookup_results:
        print("No keyword lookup results were produced.", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
