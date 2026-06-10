# Ontology TUI Allowlist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow only `T203`, `T074`, `T075`, `T091`, and `T093` to pass retrieval ontology filtering for both `umls` and `mesh` semantic-type evidence without broadening whole ontology groups.

**Architecture:** Keep the existing group-based retrieval filter as the default path, and add a small explicit TUI allowlist checked before group-based rejection in both `_classify_umls()` and `_classify_mesh()`. Verify the change with focused unit tests plus matcher-level regression coverage so we preserve current drops for non-whitelisted `DEVI`, `OCCU`, and `ORGA` TUIs.

**Tech Stack:** Python, pytest, existing ontology filter policy helpers in `src/docset_hub/indexing/entity_filter_policy.py`

---

### Task 1: Lock Down Desired Filter Behavior in Tests

**Files:**
- Modify: `tests/indexing/test_entity_filter_policy.py:9-89`
- Modify: `tests/integration/test_span_matcher_real_services.py:58-219`

- [ ] **Step 1: Add explicit allowlist cases to the filter policy parametrized test**

```python
@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ({"source": "umls", "semantic_types": ["T074"]}, ("allow", "umls_tui_allowlist:T074")),
        ({"source": "umls", "semantic_types": ["T075"]}, ("allow", "umls_tui_allowlist:T075")),
        ({"source": "umls", "semantic_types": ["T091"]}, ("allow", "umls_tui_allowlist:T091")),
        ({"source": "umls", "semantic_types": ["T093"]}, ("allow", "umls_tui_allowlist:T093")),
        ({"source": "umls", "semantic_types": ["T203"]}, ("allow", "umls_tui_allowlist:T203")),
        ({"source": "mesh", "semantic_types": ["T074"]}, ("allow", "mesh_tui_allowlist:T074")),
        ({"source": "mesh", "semantic_types": ["T075"]}, ("allow", "mesh_tui_allowlist:T075")),
        ({"source": "mesh", "semantic_types": ["T091"]}, ("allow", "mesh_tui_allowlist:T091")),
        ({"source": "mesh", "semantic_types": ["T093"]}, ("allow", "mesh_tui_allowlist:T093")),
        ({"source": "mesh", "semantic_types": ["T203"]}, ("allow", "mesh_tui_allowlist:T203")),
    ],
)
def test_classify_ontology_evidence_for_retrieval(item, expected):
    assert classify_ontology_evidence_for_retrieval(item) == expected
```

- [ ] **Step 2: Add a regression test proving only the named TUIs are opened, not the full groups**

```python
def test_filter_ontology_evidence_items_keeps_only_explicitly_allowed_tuis_from_blocked_groups():
    filtered = filter_ontology_evidence_items(
        [
            {"source": "umls", "concept_id": "C1", "canonical": "Brain-Computer Interfaces", "semantic_types": ["T074"]},
            {"source": "mesh", "concept_id": "C2", "canonical": "Drug Delivery Device", "semantic_types": ["T203"]},
            {"source": "umls", "concept_id": "C3", "canonical": "Occupation", "semantic_types": ["T090"]},
            {"source": "mesh", "concept_id": "C4", "canonical": "Organization", "semantic_types": ["T092"]},
        ]
    )

    assert [(item["source"], item["concept_id"]) for item in filtered] == [
        ("umls", "C1"),
        ("mesh", "C2"),
    ]
    assert filtered[0]["filter_reason"] == "umls_tui_allowlist:T074"
    assert filtered[1]["filter_reason"] == "mesh_tui_allowlist:T203"
```

- [ ] **Step 3: Extend matcher-level positive cases so both sources accept the new TUIs**

```python
POSITIVE_FILTER_CASES = [
    ("brain computer interface", "umls", "Brain-Computer Interfaces", ["T074"], "umls_tui_allowlist:T074"),
    ("brain computer interface", "mesh", "Brain-Computer Interfaces", ["T074"], "mesh_tui_allowlist:T074"),
    ("drug delivery device", "umls", "Drug Delivery Device", ["T203"], "umls_tui_allowlist:T203"),
    ("drug delivery device", "mesh", "Drug Delivery Device", ["T203"], "mesh_tui_allowlist:T203"),
    ("biomedical occupation", "umls", "Biomedical Occupation or Discipline", ["T091"], "umls_tui_allowlist:T091"),
    ("biomedical occupation", "mesh", "Biomedical Occupation or Discipline", ["T091"], "mesh_tui_allowlist:T091"),
    ("health care organization", "umls", "Health Care Related Organization", ["T093"], "umls_tui_allowlist:T093"),
    ("health care organization", "mesh", "Health Care Related Organization", ["T093"], "mesh_tui_allowlist:T093"),
]
```

- [ ] **Step 4: Add a matcher-level negative regression for nearby blocked TUIs**

```python
NEGATIVE_FILTER_CASES = [
    ("occupation taxonomy", "umls", "Occupation", ["T090"], "umls_group:OCCU"),
    ("organization taxonomy", "mesh", "Organization", ["T092"], "mesh_tui_group:ORGA"),
    ("organization taxonomy", "umls", "Organization", ["T094"], "umls_group:ORGA"),
    ("research instrument taxonomy", "mesh", "Research Device Family", ["T095"], "mesh_tui_group:ORGA"),
]
```

Then keep the new negative set focused on nearby but non-whitelisted TUIs such as `T090`, `T092`, `T094`, and `T095`, so the suite demonstrates that only the named allowlisted TUIs were opened.

- [ ] **Step 5: Run the focused tests and confirm they fail before implementation**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
pytest tests/indexing/test_entity_filter_policy.py tests/integration/test_span_matcher_real_services.py -q
```

Expected: FAIL on the new allowlist assertions because the current code still returns `drop` for `T074`, `T075`, `T091`, `T093`, and `T203`.

- [ ] **Step 6: Commit the red test changes**

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
git add tests/indexing/test_entity_filter_policy.py tests/integration/test_span_matcher_real_services.py
git commit -m "test: cover ontology tui allowlist behavior"
```

### Task 2: Implement Explicit TUI Allowlist in the Filter Policy

**Files:**
- Modify: `src/docset_hub/indexing/entity_filter_policy.py:8-216`
- Test: `tests/indexing/test_entity_filter_policy.py`
- Test: `tests/integration/test_span_matcher_real_services.py`

- [ ] **Step 1: Add a dedicated explicit-TUI allowlist constant near the existing filter constants**

```python
RETRIEVAL_EXPLICIT_TUIS = {
    "T203",  # Drug Delivery Device
    "T074",  # Medical Device
    "T075",  # Research Device
    "T091",  # Biomedical Occupation or Discipline
    "T093",  # Health Care Related Organization
}
```

- [ ] **Step 2: Add a small helper that returns the first matching allowlisted TUI in stable order**

```python
def _allowed_tui(tuis: Sequence[str]) -> str:
    for tui in tuis:
        if tui in RETRIEVAL_EXPLICIT_TUIS:
            return tui
    return ""
```

- [ ] **Step 3: Update `_classify_umls()` to short-circuit on the allowlist before group rejection**

```python
def _classify_umls(item: Mapping[str, Any]) -> Tuple[str, str]:
    tuis = _extract_string_list(item, "semantic_types", "types")
    if not tuis:
        return "unknown_keep", "missing_tui"

    allowed_tui = _allowed_tui(tuis)
    if allowed_tui:
        return "allow", f"umls_tui_allowlist:{allowed_tui}"

    groups = {UMLS_TUI_TO_GROUP[tui] for tui in tuis if tui in UMLS_TUI_TO_GROUP}
    if not groups:
        return "unknown_keep", f"unmapped_tui:{tuis[0]}"
    allowed_groups = sorted(group for group in groups if group in RETRIEVAL_UMLS_GROUPS)
    if allowed_groups:
        return "allow", f"umls_group:{allowed_groups[0]}"
    return "drop", f"umls_group:{sorted(groups)[0]}"
```

- [ ] **Step 4: Update `_classify_mesh()` to apply the same explicit allowlist on semantic-type fallback**

```python
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

    allowed_tui = _allowed_tui(tuis)
    if allowed_tui:
        return "allow", f"mesh_tui_allowlist:{allowed_tui}"

    groups = {UMLS_TUI_TO_GROUP[tui] for tui in tuis if tui in UMLS_TUI_TO_GROUP}
    if not groups:
        return "unknown_keep", f"unmapped_mesh_tui:{tuis[0]}"
    allowed_groups = sorted(group for group in groups if group in RETRIEVAL_UMLS_GROUPS)
    if allowed_groups:
        return "allow", f"mesh_tui_group:{allowed_groups[0]}"
    return "drop", f"mesh_tui_group:{sorted(groups)[0]}"
```

- [ ] **Step 5: Run the focused tests and verify the new expectations pass**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
pytest tests/indexing/test_entity_filter_policy.py tests/integration/test_span_matcher_real_services.py -q
```

Expected: PASS, including the new `*_tui_allowlist:*` reasons and the existing negative checks for `T090` and `T092`.

- [ ] **Step 6: Run a narrower smoke test for the exact motivating case**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
pytest tests/integration/test_span_matcher_real_services.py -q -k "brain or filter"
```

Expected: PASS for deterministic filter-policy and matcher-path tests; live-service tests may skip unless `RUN_REAL_SPAN_MATCHER_INTEGRATION=1` is set.

- [ ] **Step 7: Commit the implementation**

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
git add src/docset_hub/indexing/entity_filter_policy.py tests/indexing/test_entity_filter_policy.py tests/integration/test_span_matcher_real_services.py
git commit -m "feat: allow selected ontology tuis for retrieval"
```

### Task 3: Final Verification and Manual Trace Check

**Files:**
- Modify: none
- Verify: `src/docset_hub/indexing/entity_filter_policy.py`
- Verify: `scripts/run_span_matcher_trace.py`

- [ ] **Step 1: Re-run the full targeted verification set**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
pytest tests/indexing/test_entity_filter_policy.py tests/indexing/test_span_matcher.py tests/integration/test_span_matcher_real_services.py -q
```

Expected: PASS for deterministic tests; live integration tests either PASS or SKIP cleanly depending on service availability and env flags.

- [ ] **Step 2: Manually inspect the motivating query with the trace script**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
python3 scripts/run_span_matcher_trace.py --query "brain computer interface"
```

Expected: `Raw Ontology Evidence` still includes `C3494288`, and `Filtered Ontology Evidence` now retains at least the `T074`-backed ontology evidence instead of printing `none`.

- [ ] **Step 3: Check for accidental broadening by reviewing reasons in test output**

Run:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
pytest tests/indexing/test_entity_filter_policy.py -q -k "allowlist or drops"
```

Expected: PASS, with no failures indicating `T090`, `T092`, `T094`, or other nearby TUIs were unintentionally retained.

- [ ] **Step 4: Commit any final test-only adjustments if needed**

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
git add tests/indexing/test_entity_filter_policy.py tests/indexing/test_span_matcher.py tests/integration/test_span_matcher_real_services.py
git commit -m "test: verify ontology allowlist regression coverage"
```
