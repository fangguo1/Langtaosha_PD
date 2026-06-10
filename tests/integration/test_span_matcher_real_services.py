"""SpanMatcher ontology filtering checks.

Most tests here are deterministic and do not require live services. The live
ontology linker check remains opt-in via RUN_REAL_SPAN_MATCHER_INTEGRATION=1.
"""

from __future__ import annotations

import os

import pytest
import requests

from src.docset_hub.indexing.entity_filter_policy import (
    classify_ontology_evidence_for_retrieval,
    filter_ontology_evidence_items,
)
from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import (
    RemoteOntologySpanMatcher,
)


def _candidate(text: str, kind: str = "probe") -> PhraseCandidate:
    return PhraseCandidate(
        text=text,
        normalized_text=text.lower(),
        kind=kind,
        start=0,
        end=len(text),
    )


class FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self.response = FakeResponse(payload)
        self.requests = []

    def post(self, url, json, timeout):
        self.requests.append({"url": url, "json": json, "timeout": timeout})
        return self.response


POSITIVE_FILTER_CASES = [
    ("adhesion protein in kidney", "umls", "adhesion protein", ["T116"], "umls_group:CHEM"),
    ("adhesion protein in kidney", "umls", "kidney", ["T023"], "umls_group:ANAT"),
    ("adhesion protein in kidney", "mesh", "Proteins", ["T116"], "mesh_tui_group:CHEM"),
    ("adhesion protein in kidney", "mesh", "Kidney", ["T023"], "mesh_tui_group:ANAT"),
    ("EGFR expression in lung cancer", "umls", "EGFR", ["T028"], "umls_group:GENE"),
    ("EGFR expression in lung cancer", "umls", "lung cancer", ["T191"], "umls_group:DISO"),
    ("EGFR expression in lung cancer", "mesh", "Receptor, Epidermal Growth Factor", ["T116"], "mesh_tui_group:CHEM"),
    ("EGFR expression in lung cancer", "mesh", "Lung Neoplasms", ["T191"], "mesh_tui_group:DISO"),
    ("single-cell RNA sequencing of mouse kidney", "umls", "RNA sequencing", ["T063"], "umls_group:PROC"),
    ("single-cell RNA sequencing of mouse kidney", "umls", "mouse", ["T015"], "umls_group:LIVB"),
    ("single-cell RNA sequencing of mouse kidney", "mesh", "Sequence Analysis, RNA", ["T063"], "mesh_tui_group:PROC"),
    ("single-cell RNA sequencing of mouse kidney", "mesh", "Mice", ["T015"], "mesh_tui_group:LIVB"),
    ("CRISPR screening in T cells", "umls", "CRISPR", ["T063"], "umls_group:PROC"),
    ("CRISPR screening in T cells", "umls", "T cells", ["T025"], "umls_group:ANAT"),
    ("CRISPR screening in T cells", "mesh", "CRISPR-Cas Systems", ["T063"], "mesh_tui_group:PROC"),
    ("CRISPR screening in T cells", "mesh", "T-Lymphocytes", ["T025"], "mesh_tui_group:ANAT"),
    ("insulin signaling in beta cells", "umls", "insulin", ["T116"], "umls_group:CHEM"),
    ("insulin signaling in beta cells", "umls", "signaling", ["T038"], "umls_group:PHEN"),
    ("insulin signaling in beta cells", "mesh", "Insulin", ["T116"], "mesh_tui_group:CHEM"),
    ("insulin signaling in beta cells", "mesh", "Signal Transduction", ["T038"], "mesh_tui_group:PHEN"),
    ("macrophage activation in liver fibrosis", "umls", "macrophage", ["T025"], "umls_group:ANAT"),
    ("macrophage activation in liver fibrosis", "umls", "liver fibrosis", ["T047"], "umls_group:DISO"),
    ("macrophage activation in liver fibrosis", "mesh", "Macrophages", ["T025"], "mesh_tui_group:ANAT"),
    ("macrophage activation in liver fibrosis", "mesh", "Liver Fibrosis", ["T047"], "mesh_tui_group:DISO"),
    ("mitochondrial dysfunction in Parkinson disease", "umls", "mitochondrial dysfunction", ["T046"], "umls_group:DISO"),
    ("mitochondrial dysfunction in Parkinson disease", "umls", "Parkinson disease", ["T047"], "umls_group:DISO"),
    ("mitochondrial dysfunction in Parkinson disease", "mesh", "Mitochondria", ["T025"], "mesh_tui_group:ANAT"),
    ("mitochondrial dysfunction in Parkinson disease", "mesh", "Parkinson Disease", ["T047"], "mesh_tui_group:DISO"),
    ("renal tubular epithelial cells under hypoxia", "umls", "renal tubular epithelial cells", ["T025"], "umls_group:ANAT"),
    ("renal tubular epithelial cells under hypoxia", "umls", "hypoxia", ["T046"], "umls_group:DISO"),
    ("renal tubular epithelial cells under hypoxia", "mesh", "Epithelial Cells", ["T025"], "mesh_tui_group:ANAT"),
    ("renal tubular epithelial cells under hypoxia", "mesh", "Hypoxia", ["T046"], "mesh_tui_group:DISO"),
    ("clinical trial of EGFR inhibitor", "umls", "clinical trial", ["T062"], "umls_group:PROC"),
    ("clinical trial of EGFR inhibitor", "umls", "EGFR inhibitor", ["T121"], "umls_group:CHEM"),
    ("clinical trial of EGFR inhibitor", "mesh", "Clinical Trials as Topic", ["T062"], "mesh_tui_group:PROC"),
    ("clinical trial of EGFR inhibitor", "mesh", "Protein Kinase Inhibitors", ["T121"], "mesh_tui_group:CHEM"),
    ("mouse model of kidney injury", "umls", "mouse", ["T015"], "umls_group:LIVB"),
    ("mouse model of kidney injury", "umls", "kidney injury", ["T047"], "umls_group:DISO"),
    ("mouse model of kidney injury", "mesh", "Mice", ["T015"], "mesh_tui_group:LIVB"),
    ("mouse model of kidney injury", "mesh", "Acute Kidney Injury", ["T047"], "mesh_tui_group:DISO"),
    ("mouse model of kidney injury", "mesh", "Disease Models, Animal", ["T062"], "mesh_tui_group:PROC"),
]


@pytest.mark.parametrize(
    ("query", "source", "canonical", "semantic_types", "expected_reason"),
    POSITIVE_FILTER_CASES,
)
def test_entity_filter_allows_biomedical_umls_and_mesh_categories(
    query,
    source,
    canonical,
    semantic_types,
    expected_reason,
):
    filtered = filter_ontology_evidence_items(
        [
            {
                "source": source,
                "concept_id": f"{source}:{canonical}",
                "canonical": canonical,
                "semantic_types": semantic_types,
            }
        ]
    )

    assert filtered, f"expected retained evidence for {query}: {source} {canonical}"
    assert filtered[0]["filter_status"] == "allow"
    assert filtered[0]["filter_reason"] == expected_reason


NEGATIVE_FILTER_CASES = [
    ("novel method for biological study", "umls", "method", ["T170"], "umls_group:CONC"),
    ("novel method for biological study", "umls", "study", ["T171"], "umls_group:CONC"),
    ("novel method for biological study", "mesh", "Research Design", ["T170"], "mesh_tui_group:CONC"),
    ("role of protein in disease", "umls", "role", ["T170"], "umls_group:CONC"),
    ("data analysis approach", "umls", "approach", ["T170"], "umls_group:CONC"),
    ("data analysis approach", "mesh", "Data Analysis", ["T170"], "mesh_tui_group:CONC"),
    ("effect of treatment on patients", "umls", "effect", ["T170"], "umls_group:CONC"),
]


@pytest.mark.parametrize(
    ("query", "source", "canonical", "semantic_types", "expected_reason"),
    NEGATIVE_FILTER_CASES,
)
def test_entity_filter_drops_generic_umls_and_mesh_concepts(
    query,
    source,
    canonical,
    semantic_types,
    expected_reason,
):
    item = {
        "source": source,
        "concept_id": f"{source}:{canonical}",
        "canonical": canonical,
        "semantic_types": semantic_types,
    }
    filtered = filter_ontology_evidence_items(
        [item]
    )

    assert filtered == [], f"expected dropped generic evidence for {query}: {source} {canonical}"
    assert classify_ontology_evidence_for_retrieval(item) == ("drop", expected_reason)
    decision = filter_ontology_evidence_items(
        [
            {
                "source": source,
                "concept_id": f"{source}:{canonical}",
                "canonical": canonical,
                "semantic_types": semantic_types,
            },
            {
                "source": source,
                "concept_id": f"{source}:kept",
                "canonical": "Kidney",
                "semantic_types": ["T023"],
            },
        ]
    )
    assert len(decision) == 1
    assert decision[0]["filter_reason"] != expected_reason


def test_remote_ontology_span_matcher_applies_mesh_semantic_type_filtering():
    matcher = RemoteOntologySpanMatcher(
        "http://127.0.0.1:8765/",
        sources=("umls", "mesh"),
        session=FakeSession(
            {
                "results": [
                    {
                        "candidate_id": "c0",
                        "evidence": [
                            {
                                "source": "mesh",
                                "concept_id": "C1700001",
                                "canonical": "Research Design",
                                "confidence": 0.97,
                                "semantic_types": ["T170"],
                            },
                            {
                                "source": "mesh",
                                "concept_id": "C0025202",
                                "canonical": "Lung Neoplasms",
                                "confidence": 0.96,
                                "semantic_types": ["T191"],
                            },
                        ],
                    }
                ]
            }
        ),
    )

    evidence = matcher.match(_candidate("lung cancer"))

    assert [(item.source, item.concept_id) for item in evidence] == [("mesh", "C0025202")]
    assert evidence[0].payload["filter_status"] == "allow"
    assert evidence[0].payload["filter_reason"] == "mesh_tui_group:DISO"


@pytest.mark.integration
def test_live_ontology_api_returns_filterable_umls_and_mesh_evidence():
    if os.environ.get("RUN_REAL_SPAN_MATCHER_INTEGRATION") != "1":
        pytest.skip("set RUN_REAL_SPAN_MATCHER_INTEGRATION=1 to run live ontology linker checks")

    ontology_url = os.environ.get("ONTOLOGY_LINKER_URL", "http://127.0.0.1:8765").rstrip("/")
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(f"{ontology_url}/readyz", timeout=10)
    except requests.RequestException as exc:
        pytest.skip(f"ontology linker service is unavailable: {exc}")
    if response.status_code != 200:
        pytest.skip(f"ontology linker service is not ready: HTTP {response.status_code}")

    matcher = RemoteOntologySpanMatcher(
        ontology_url,
        sources=("umls", "mesh"),
        top_k=5,
        threshold=0.7,
        timeout=60,
        session=session,
    )
    evidence = matcher.match(_candidate("lung cancer"))
    sources = {item.source for item in evidence}

    assert {"umls", "mesh"} <= sources
    assert all(item.payload.get("filter_status") == "allow" for item in evidence)
    assert any(item.payload.get("filter_reason", "").startswith("mesh_") for item in evidence)
