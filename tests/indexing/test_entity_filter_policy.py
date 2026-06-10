import pytest

from src.docset_hub.indexing.entity_filter_policy import (
    classify_ontology_evidence_for_retrieval,
    filter_ontology_evidence_items,
)


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ({"source": "umls", "semantic_types": ["T023"]}, ("allow", "umls_group:ANAT")),
        ({"source": "umls", "semantic_types": ["T116"]}, ("allow", "umls_group:CHEM")),
        ({"source": "umls", "semantic_types": ["T191"]}, ("allow", "umls_group:DISO")),
        ({"source": "umls", "semantic_types": ["T028"]}, ("allow", "umls_group:GENE")),
        ({"source": "umls", "semantic_types": ["T060"]}, ("allow", "umls_group:PROC")),
        ({"source": "umls", "semantic_types": ["T038"]}, ("allow", "umls_group:PHEN")),
        ({"source": "umls", "semantic_types": ["T008"]}, ("allow", "umls_group:LIVB")),
        ({"source": "umls", "semantic_types": ["T074"]}, ("allow", "umls_tui_allowlist:T074")),
        ({"source": "umls", "semantic_types": ["T075"]}, ("allow", "umls_tui_allowlist:T075")),
        ({"source": "umls", "semantic_types": ["T091"]}, ("allow", "umls_tui_allowlist:T091")),
        ({"source": "umls", "semantic_types": ["T093"]}, ("allow", "umls_tui_allowlist:T093")),
        ({"source": "umls", "semantic_types": ["T203"]}, ("allow", "umls_tui_allowlist:T203")),
        ({"source": "umls", "semantic_types": ["T170"]}, ("drop", "umls_group:CONC")),
        ({"source": "umls", "semantic_types": ["T171"]}, ("drop", "umls_group:CONC")),
        ({"source": "umls"}, ("unknown_keep", "missing_tui")),
        ({"source": "mesh", "tree_numbers": ["A11.284"]}, ("allow", "mesh_prefix:A")),
        ({"source": "mesh", "tree_numbers": ["B01.050.150"]}, ("allow", "mesh_prefix:B")),
        ({"source": "mesh", "tree_numbers": ["C04.557.465.625.650"]}, ("allow", "mesh_prefix:C")),
        ({"source": "mesh", "tree_numbers": ["D12.776"]}, ("allow", "mesh_prefix:D")),
        ({"source": "mesh", "tree_numbers": ["E05.318"]}, ("allow", "mesh_prefix:E")),
        ({"source": "mesh", "tree_numbers": ["G05.360"]}, ("allow", "mesh_prefix:G")),
        ({"source": "mesh", "tree_numbers": ["L01.224.050.375"]}, ("drop", "mesh_prefix:L")),
        ({"source": "mesh", "semantic_types": ["T023"]}, ("allow", "mesh_tui_group:ANAT")),
        ({"source": "mesh", "semantic_types": ["T116"]}, ("allow", "mesh_tui_group:CHEM")),
        ({"source": "mesh", "semantic_types": ["T191"]}, ("allow", "mesh_tui_group:DISO")),
        ({"source": "mesh", "semantic_types": ["T028"]}, ("allow", "mesh_tui_group:GENE")),
        ({"source": "mesh", "semantic_types": ["T063"]}, ("allow", "mesh_tui_group:PROC")),
        ({"source": "mesh", "semantic_types": ["T038"]}, ("allow", "mesh_tui_group:PHEN")),
        ({"source": "mesh", "semantic_types": ["T015"]}, ("allow", "mesh_tui_group:LIVB")),
        ({"source": "mesh", "semantic_types": ["T074"]}, ("allow", "mesh_tui_allowlist:T074")),
        ({"source": "mesh", "semantic_types": ["T075"]}, ("allow", "mesh_tui_allowlist:T075")),
        ({"source": "mesh", "semantic_types": ["T091"]}, ("allow", "mesh_tui_allowlist:T091")),
        ({"source": "mesh", "semantic_types": ["T093"]}, ("allow", "mesh_tui_allowlist:T093")),
        ({"source": "mesh", "semantic_types": ["T203"]}, ("allow", "mesh_tui_allowlist:T203")),
        ({"source": "mesh", "semantic_types": ["T170"]}, ("drop", "mesh_tui_group:CONC")),
        ({"source": "mesh", "semantic_types": ["T171"]}, ("drop", "mesh_tui_group:CONC")),
        ({"source": "mesh", "semantic_types": ["T090"]}, ("drop", "mesh_tui_group:OCCU")),
        ({"source": "mesh", "semantic_types": ["T092"]}, ("drop", "mesh_tui_group:ORGA")),
        ({"source": "mesh"}, ("unknown_keep", "missing_tree_number")),
    ],
)
def test_classify_ontology_evidence_for_retrieval(item, expected):
    assert classify_ontology_evidence_for_retrieval(item) == expected


def test_filter_ontology_evidence_items_keeps_allowed_and_unknown_but_drops_concepts():
    filtered = filter_ontology_evidence_items(
        [
            {
                "source": "umls",
                "concept_id": "C1",
                "canonical": "Method",
                "semantic_types": ["T170"],
            },
            {
                "source": "umls",
                "concept_id": "C2",
                "canonical": "Melanoma",
                "semantic_types": ["T191"],
            },
            {
                "source": "mesh",
                "concept_id": "C3",
                "canonical": "Databases as Topic",
                "tree_numbers": ["L01.224.050.375"],
            },
            {
                "source": "mesh",
                "concept_id": "C4",
                "canonical": "Melanoma",
                "semantic_types": ["T191"],
            },
            {
                "source": "umls",
                "concept_id": "C5",
                "canonical": "Genome",
            },
        ]
    )

    assert [(item["source"], item["concept_id"]) for item in filtered] == [
        ("umls", "C2"),
        ("mesh", "C4"),
        ("umls", "C5"),
    ]
    assert filtered[0]["filter_status"] == "allow"
    assert filtered[0]["filter_reason"] == "umls_group:DISO"
    assert filtered[1]["filter_reason"] == "mesh_tui_group:DISO"
    assert filtered[2]["filter_status"] == "unknown_keep"


def test_filter_ontology_evidence_items_retains_only_explicit_allowlist_tuis_from_blocked_groups():
    filtered = filter_ontology_evidence_items(
        [
            {
                "source": "umls",
                "concept_id": "U074",
                "canonical": "Medical Device",
                "semantic_types": ["T074"],
            },
            {
                "source": "umls",
                "concept_id": "U090",
                "canonical": "Professional Role",
                "semantic_types": ["T090"],
            },
            {
                "source": "umls",
                "concept_id": "U203",
                "canonical": "Drug Delivery Device",
                "semantic_types": ["T203"],
            },
            {
                "source": "umls",
                "concept_id": "U092",
                "canonical": "Organization",
                "semantic_types": ["T092"],
            },
            {
                "source": "mesh",
                "concept_id": "M091",
                "canonical": "Biomedical Occupation",
                "semantic_types": ["T091"],
            },
            {
                "source": "mesh",
                "concept_id": "M094",
                "canonical": "Regulation",
                "semantic_types": ["T094"],
            },
            {
                "source": "mesh",
                "concept_id": "M093",
                "canonical": "Health Organization",
                "semantic_types": ["T093"],
            },
            {
                "source": "mesh",
                "concept_id": "M095",
                "canonical": "Self-help Group",
                "semantic_types": ["T095"],
            },
        ]
    )

    assert [(item["source"], item["concept_id"]) for item in filtered] == [
        ("umls", "U074"),
        ("umls", "U203"),
        ("mesh", "M091"),
        ("mesh", "M093"),
    ]
    assert [item["filter_reason"] for item in filtered] == [
        "umls_tui_allowlist:T074",
        "umls_tui_allowlist:T203",
        "mesh_tui_allowlist:T091",
        "mesh_tui_allowlist:T093",
    ]
