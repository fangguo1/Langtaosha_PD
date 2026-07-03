from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.config import _reset_config, init_config
from src.docset_hub.indexing import SpanMatcherPipeline, SpanMatcherProfile
from src.docset_hub.storage.metadata_db import MetadataDB


MIMIC_CONFIG_PATH = Path("src/config/config_tecent_backend_server_mimic.yaml")


@pytest.mark.integration
def test_ontology_plus_keyword_pipeline_selects_adhesion_protein_and_kidney():
    if os.environ.get("RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION") != "1":
        pytest.skip("set RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION=1 to run live span matcher pipeline checks")

    _reset_config()
    init_config(MIMIC_CONFIG_PATH, force_reload=True)
    metadata_db = MetadataDB(config_path=MIMIC_CONFIG_PATH)
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.ontology_plus_keyword(
            enable_scispacy=False,
            paper_sources=("langtaosha", "biorxiv_history", "biorxiv_daily"),
        ),
        metadata_db=metadata_db,
    )

    result = pipeline.run("adhesion protein in kidney")

    surfaces = [concept.candidate.text for concept in result.selected_concepts]
    assert "kidney" in surfaces
    assert any(surface in {"adhesion protein", "adhesion"} for surface in surfaces)
