# Span Matcher Modification Design

**Date:** 2026-06-10
**Scope:** Query Semantic Plan generation for Expanded Sparse Retrieval and Coverage Engine

---

## 1. Background

Current Span Matcher can:

- extract query spans
- attach ontology and keyword evidence
- select final non-overlapping concepts

That is enough for concept selection, but not enough for downstream retrieval planning.

For expanded sparse retrieval and coverage analysis, we need a richer output that preserves:

1. top-level semantic spans
2. each span's own terms
3. each span's child spans
4. each child span's own terms
5. exact vs prefix match behavior
6. ontology and keyword evidence trace

Span Matcher still does not perform retrieval or ranking. It only produces structured query understanding.

---

## 2. Core Design Change

Span Matcher should produce a new object:

```text
Query Semantic Plan
```

Instead of exposing only:

```text
SelectedConcept[]
```

it should expose a semantic span forest with one level of children:

```text
Top-level semantic spans
  -> own terms
  -> child spans
      -> child own terms
```

This structure is shared by:

- Expanded Sparse Retrieval
- Coverage Engine

---

## 3. Semantic Span Definition

A semantic span is:

> The largest query span that can independently express a retrieval intent.

Example:

```text
brain computer interface
```

should remain one semantic span:

```text
S1 = brain computer interface
```

It should not be decomposed into:

```text
brain
computer
interface
```

For a multi-concept query:

```text
adhesion protein in kidney
```

the semantic spans should be:

```text
S1 = adhesion protein
S2 = kidney
```

because `in` separates two retrieval intents.

---

## 4. Tree Depth Rule

Each top-level semantic span may contain child spans, but child expansion is limited to one level.

Rules:

1. top-level spans come from final selected non-overlapping concepts
2. children come only from existing `subphrase candidate` matches
3. children are attached only when they are fully contained by the parent span
4. children do not create grandchildren
5. ontology aliases must not generate new child spans

This keeps the structure simple and predictable.

---

## 5. Query Semantic Plan Structure

Each top-level semantic span should contain:

1. span identity and offsets
2. canonical text
3. own Tier 1 / Tier 2 terms
4. child spans
5. child own Tier 1 / Tier 2 terms
6. evidence trace

Recommended conceptual structure:

```yaml
query: string
normalized_query: string
semantic_spans:
  - id: s1
    text: string
    canonical: string
    span: start:end
    own_terms:
      tier1:
        - text: string
          match_mode: exact
      tier2:
        - text: string
          match_mode: exact | prefix
    children:
      - id: s1.1
        text: string
        canonical: string
        span: start:end
        own_terms:
          tier1:
            - text: string
              match_mode: exact
          tier2:
            - text: string
              match_mode: exact | prefix
        evidence:
          - source
          - concept_id
          - canonical
          - confidence
    evidence:
      - source
      - concept_id
      - canonical
      - confidence
```

---

## 6. Term Model

Terms should no longer be represented as plain strings only. Each term should preserve match behavior.

Recommended conceptual term structure:

```yaml
text: string
match_mode: exact | prefix
```

Supported match modes in this phase:

1. `exact`
2. `prefix`

No other match modes are in scope for this phase.

---

## 7. Prefix Rule

We keep prefix handling intentionally simple.

Only one prefix form is supported:

```text
alias ending with "-"
```

Examples:

```text
renal-
reno-
nephro-
```

Interpretation:

- `renal-` means prefix match on `renal`
- `reno-` means prefix match on `reno`
- `nephro-` means prefix match on `nephro`

Normalization rule:

- remove the trailing `-`
- store the base term text
- mark the term as `match_mode: prefix`

Examples:

```text
renal-   -> text="renal",  match_mode=prefix
reno-    -> text="reno",   match_mode=prefix
nephro-  -> text="nephro", match_mode=prefix
```

Matching rule:

- prefix terms match only at token start
- they do not behave as arbitrary substring matches

Examples:

```text
renal- matches: renal, renals, renalac
renal- does not match: adrenal
```

This is strict prefix matching only.

---

## 8. Own Terms vs Child Terms

The distinction between parent own terms and child terms is required.

For:

```text
adhesion protein
```

the parent own terms represent the complete concept:

```text
adhesion protein
adhesion protein substance
```

the children represent internal components:

```text
adhesion
protein
```

These must remain separate in the semantic plan.

We should not flatten parent and child terms into one undifferentiated bag at plan-construction time.

---

## 9. Tier Definitions

### Tier 1

Tier 1 terms are direct expressions of the span itself.

They may include:

- original query phrase
- normalized surface form
- canonical surface form when it is still a direct expression
- hyphen and space variants when appropriate

Examples:

```text
brain computer interface
brain-computer interface
```

### Tier 2

Tier 2 terms are controlled expansions from ontology or keyword evidence.

They may include:

- ontology aliases
- keyword-derived variants
- cleaner alternate surface forms
- prefix aliases

Examples:

```text
kidney -> renal
kidney -> kidneys
kidney -> renal-
adhesion protein -> adhesion protein substance
```

Tier 2 is used for controlled expansion, not for arbitrary free-form rewriting.

---

## 10. Child Construction Rule

Children are created only from subphrase candidates already produced by Span Matcher.

Child construction rules:

1. start from the top-level selected concept span
2. find subphrase candidates fully contained inside that span
3. keep only child candidates that have usable evidence
4. build one child node per retained subphrase candidate
5. do not recursively expand inside child nodes

This gives us:

- stable parent spans
- lightweight internal structure
- no ontology-driven over-expansion

---

## 11. Example 1: Single Semantic Span Query

Query:

```text
brain computer interface
```

Expected Query Semantic Plan:

```yaml
query: brain computer interface
normalized_query: brain computer interface

semantic_spans:
  - id: s1
    text: brain computer interface
    canonical: Brain-Computer Interface
    span: 0:24
    own_terms:
      tier1:
        - text: brain computer interface
          match_mode: exact
        - text: brain-computer interface
          match_mode: exact
      tier2: []
    children: []
```

Explanation:

- the whole query already expresses one complete concept
- no child span is needed

---

## 12. Example 2: Multi-Concept Query

Query:

```text
adhesion protein in kidney
```

Expected Query Semantic Plan:

```yaml
query: adhesion protein in kidney
normalized_query: adhesion protein in kidney

semantic_spans:
  - id: s1
    text: adhesion protein
    canonical: Adhesion protein
    span: 0:16
    own_terms:
      tier1:
        - text: adhesion protein
          match_mode: exact
      tier2:
        - text: adhesion protein substance
          match_mode: exact
    children:
      - id: s1.1
        text: adhesion
        canonical: adhesion
        span: 0:8
        own_terms:
          tier1:
            - text: adhesion
              match_mode: exact
          tier2:
            - text: process of adhesion
              match_mode: exact
            - text: tissue adhesions
              match_mode: exact
      - id: s1.2
        text: protein
        canonical: protein
        span: 9:16
        own_terms:
          tier1:
            - text: protein
              match_mode: exact
          tier2:
            - text: proteins
              match_mode: exact

  - id: s2
    text: kidney
    canonical: Kidney
    span: 20:26
    own_terms:
      tier1:
        - text: kidney
          match_mode: exact
      tier2:
        - text: renal
          match_mode: exact
        - text: renal
          match_mode: prefix
        - text: renal tissue
          match_mode: exact
        - text: kidney tissue
          match_mode: exact
        - text: kidneys
          match_mode: exact
    children: []
```

Notes:

- `s1` is the top-level concept
- `adhesion` and `protein` are child spans because they come from subphrase candidates
- `s2` has no children in this example
- the same surface text may appear with different match modes when the evidence supports both exact and prefix behavior

---

## 13. Canonical Selection Rule

Some ontology results may provide awkward canonicals.

Example:

```text
kidney -> Both kidneys
```

The semantic plan should prefer a cleaner retrieval-facing canonical:

```text
Kidney
```

Recommended preference order:

1. exact or near-exact surface match to the query span
2. keyword-supported canonical when it is cleaner
3. shorter ontology preferred name
4. MeSH heading when it is cleaner than the UMLS variant
5. avoid plural or body-structure-heavy variants as primary canonical when a simpler form exists

For:

```text
adhesion protein in kidney
```

preferred canonicals are:

```text
S1 canonical = Adhesion protein
S2 canonical = Kidney
```

not:

```text
S2 canonical = Both kidneys
```

---

## 14. Output for Expanded Sparse Retrieval

Expanded Sparse Retrieval should consume the semantic plan in a structured way.

It should receive, for each top-level group:

1. parent own terms
2. child own terms
3. match mode for every term

Recommended conceptual retrieval payload:

```yaml
expanded_sparse_groups:
  - group_id: s1
    canonical: Adhesion protein
    own_terms:
      tier1:
        - text: adhesion protein
          match_mode: exact
      tier2:
        - text: adhesion protein substance
          match_mode: exact
    children:
      - child_id: s1.1
        own_terms:
          tier1:
            - text: adhesion
              match_mode: exact
          tier2:
            - text: process of adhesion
              match_mode: exact
      - child_id: s1.2
        own_terms:
          tier1:
            - text: protein
              match_mode: exact

  - group_id: s2
    canonical: Kidney
    own_terms:
      tier1:
        - text: kidney
          match_mode: exact
      tier2:
        - text: renal
          match_mode: exact
        - text: renal
          match_mode: prefix
        - text: kidney tissue
          match_mode: exact
    children: []
```

Expanded Sparse Retrieval can later distinguish:

- parent-own exact matches
- parent-own prefix matches
- child exact matches
- child prefix matches

This phase does not define final scoring yet. It only preserves the structure needed for later matching.

---

## 15. Output for Coverage Engine

Coverage Engine should consume the full semantic span forest.

It should have access to:

- top-level spans
- own terms
- child spans
- child own terms
- exact vs prefix match mode
- evidence trace

This allows coverage reporting such as:

- matched semantic spans
- missing semantic spans
- matched parent own terms
- matched child terms
- matched exact terms
- matched prefix terms
- coverage ratio

Coverage Engine remains independent from Expanded Sparse Retrieval.

---

## 16. Module Boundaries

Span Matcher modification should only produce structured query understanding.

It must not:

- perform PostgreSQL retrieval
- perform sparse ranking
- perform BM25 ranking
- perform dense retrieval
- perform RRF fusion
- perform final ranking

It should only output:

- Query Semantic Plan
- Semantic Span Forest
- Expanded Sparse Retrieval input structure
- Coverage Engine input structure

---

## 17. Final Design Summary

This design fixes two core limitations in the current plan:

1. each top-level group now preserves both `own` terms and one-level `children`
2. aliases can now carry simple prefix behavior through the trailing `-` rule

Final rules to implement:

1. top-level groups come from final selected concepts
2. children come only from subphrase candidates
3. child expansion is one level only
4. every term has `text` and `match_mode`
5. supported match modes are only `exact` and `prefix`
6. `alias-` means strict token-prefix matching on `alias`
7. parent own terms and child terms remain structurally separate

This gives downstream retrieval and coverage modules a stable and sufficiently expressive query representation without making Span Matcher responsible for retrieval behavior.
