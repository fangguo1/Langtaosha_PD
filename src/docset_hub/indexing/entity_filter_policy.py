"""Retrieval-oriented filtering for ontology linker evidence."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


RETRIEVAL_UMLS_GROUPS = {
    "ANAT",
    "CHEM",
    "DISO",
    "GENE",
    "PHEN",
    "PROC",
    "PHYS",
    "LIVB",
}

RETRIEVAL_MESH_PREFIXES = {
    "A",
    "B",
    "C",
    "D",
    "E",
    "G",
}

RETRIEVAL_EXPLICIT_TUIS = {
    "T203",
    "T074",
    "T075",
    "T091",
    "T093",
}

UMLS_TUI_TO_GROUP = {
    "T052": "ACTI",
    "T053": "ACTI",
    "T056": "ACTI",
    "T051": "ACTI",
    "T064": "ACTI",
    "T055": "ACTI",
    "T066": "ACTI",
    "T057": "ACTI",
    "T054": "ACTI",
    "T017": "ANAT",
    "T029": "ANAT",
    "T023": "ANAT",
    "T030": "ANAT",
    "T031": "ANAT",
    "T022": "ANAT",
    "T025": "ANAT",
    "T026": "ANAT",
    "T018": "ANAT",
    "T021": "ANAT",
    "T024": "ANAT",
    "T116": "CHEM",
    "T195": "CHEM",
    "T123": "CHEM",
    "T122": "CHEM",
    "T103": "CHEM",
    "T120": "CHEM",
    "T104": "CHEM",
    "T200": "CHEM",
    "T196": "CHEM",
    "T126": "CHEM",
    "T131": "CHEM",
    "T125": "CHEM",
    "T129": "CHEM",
    "T130": "CHEM",
    "T197": "CHEM",
    "T114": "CHEM",
    "T109": "CHEM",
    "T121": "CHEM",
    "T192": "CHEM",
    "T127": "CHEM",
    "T185": "CONC",
    "T077": "CONC",
    "T169": "CONC",
    "T102": "CONC",
    "T078": "CONC",
    "T170": "CONC",
    "T171": "CONC",
    "T080": "CONC",
    "T081": "CONC",
    "T089": "CONC",
    "T082": "CONC",
    "T079": "CONC",
    "T203": "DEVI",
    "T074": "DEVI",
    "T075": "DEVI",
    "T020": "DISO",
    "T190": "DISO",
    "T049": "DISO",
    "T019": "DISO",
    "T047": "DISO",
    "T050": "DISO",
    "T033": "DISO",
    "T037": "DISO",
    "T048": "DISO",
    "T191": "DISO",
    "T046": "DISO",
    "T184": "DISO",
    "T087": "GENE",
    "T088": "GENE",
    "T028": "GENE",
    "T085": "GENE",
    "T086": "GENE",
    "T083": "GEOG",
    "T100": "LIVB",
    "T011": "LIVB",
    "T001": "LIVB",
    "T007": "LIVB",
    "T008": "LIVB",
    "T194": "LIVB",
    "T012": "LIVB",
    "T204": "LIVB",
    "T099": "LIVB",
    "T013": "LIVB",
    "T004": "LIVB",
    "T096": "LIVB",
    "T016": "LIVB",
    "T015": "LIVB",
    "T101": "LIVB",
    "T002": "LIVB",
    "T098": "LIVB",
    "T097": "LIVB",
    "T014": "LIVB",
    "T010": "LIVB",
    "T005": "LIVB",
    "T071": "OBJC",
    "T168": "OBJC",
    "T073": "OBJC",
    "T072": "OBJC",
    "T167": "OBJC",
    "T091": "OCCU",
    "T090": "OCCU",
    "T093": "ORGA",
    "T092": "ORGA",
    "T094": "ORGA",
    "T095": "ORGA",
    "T038": "PHEN",
    "T069": "PHEN",
    "T068": "PHEN",
    "T034": "PHEN",
    "T070": "PHEN",
    "T067": "PHEN",
    "T043": "PHYS",
    "T201": "PHYS",
    "T045": "PHYS",
    "T041": "PHYS",
    "T044": "PHYS",
    "T032": "PHYS",
    "T040": "PHYS",
    "T042": "PHYS",
    "T039": "PHYS",
    "T058": "PROC",
    "T059": "PROC",
    "T060": "PROC",
    "T061": "PROC",
    "T062": "PROC",
    "T063": "PROC",
    "T065": "PROC",
}


def filter_ontology_evidence_items(evidence_items: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """Return ontology evidence retained for retrieval with debug annotations."""

    retained: List[Mapping[str, Any]] = []
    for item in evidence_items:
        decision, reason = classify_ontology_evidence_for_retrieval(item)
        if decision == "drop":
            continue
        annotated = dict(item)
        annotated["filter_status"] = decision
        annotated["filter_reason"] = reason
        retained.append(annotated)
    return retained


def classify_ontology_evidence_for_retrieval(item: Mapping[str, Any]) -> Tuple[str, str]:
    source = str(item.get("source") or "").strip().lower()
    if source == "umls":
        return _classify_umls(item)
    if source == "mesh":
        return _classify_mesh(item)
    return "unknown_keep", f"unsupported_source:{source or 'unknown'}"


def _classify_umls(item: Mapping[str, Any]) -> Tuple[str, str]:
    tuis = _extract_string_list(item, "semantic_types", "types")
    if not tuis:
        return "unknown_keep", "missing_tui"
    allowed_tui = _first_allowed_tui(tuis)
    if allowed_tui:
        return "allow", f"umls_tui_allowlist:{allowed_tui}"
    groups = {UMLS_TUI_TO_GROUP[tui] for tui in tuis if tui in UMLS_TUI_TO_GROUP}
    if not groups:
        return "unknown_keep", f"unmapped_tui:{tuis[0]}"
    allowed_groups = sorted(group for group in groups if group in RETRIEVAL_UMLS_GROUPS)
    if allowed_groups:
        return "allow", f"umls_group:{allowed_groups[0]}"
    return "drop", f"umls_group:{sorted(groups)[0]}"


def _classify_mesh(item: Mapping[str, Any]) -> Tuple[str, str]:
    tree_numbers = _extract_string_list(item, "tree_numbers", "tree_number")
    if tree_numbers:
        prefixes = sorted({_mesh_prefix(value) for value in tree_numbers if _mesh_prefix(value)})
        if prefixes:
            allowed_prefixes = [prefix for prefix in prefixes if prefix in RETRIEVAL_MESH_PREFIXES]
            if allowed_prefixes:
                return "allow", f"mesh_prefix:{allowed_prefixes[0]}"
            return "drop", f"mesh_prefix:{prefixes[0]}"

    tuis = _extract_string_list(item, "semantic_types", "types")
    if not tuis:
        return "unknown_keep", "missing_tree_number"
    allowed_tui = _first_allowed_tui(tuis)
    if allowed_tui:
        return "allow", f"mesh_tui_allowlist:{allowed_tui}"

    groups = {UMLS_TUI_TO_GROUP[tui] for tui in tuis if tui in UMLS_TUI_TO_GROUP}
    if not groups:
        return "unknown_keep", f"unmapped_mesh_tui:{tuis[0]}"
    allowed_groups = sorted(group for group in groups if group in RETRIEVAL_UMLS_GROUPS)
    if allowed_groups:
        return "allow", f"mesh_tui_group:{allowed_groups[0]}"
    return "drop", f"mesh_tui_group:{sorted(groups)[0]}"


def _extract_string_list(item: Mapping[str, Any], *keys: str) -> List[str]:
    for key in keys:
        if key in item and item.get(key) is not None:
            return _coerce_string_list(item.get(key))
    nested_payload = item.get("payload")
    if isinstance(nested_payload, Mapping):
        for key in keys:
            if key in nested_payload and nested_payload.get(key) is not None:
                return _coerce_string_list(nested_payload.get(key))
    return []


def _coerce_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _first_allowed_tui(tuis: Sequence[str]) -> str:
    for tui in tuis:
        if tui in RETRIEVAL_EXPLICIT_TUIS:
            return tui
    return ""


def _mesh_prefix(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    return text.split(".", 1)[0][:1]
