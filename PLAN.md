# Semantic DSL and Grain-Agnostic Metrics – Implementation Plan

This plan replaces the prior refactor goals and establishes the path to a grain-agnostic, relational-algebra DSL. The engine remains fully in-memory; all breaking changes are acceptable.

## Phase 1 – Schema & Semantic Model Foundation
- **Define schema primitives**: formalize fact relations, dimension relations, and the join graph (foreign keys, reachability, cardinalities).
- **Logical attributes**: map logical names to concrete relation attributes plus default base fact. Ensure every column is an attribute (no special measure type).
- **Metadata normalization**: validate attribute uniqueness, join graph consistency, and reachability of dimensions from each fact. Seed with representative fixtures to exercise joins.

## Phase 2 – Grain-Agnostic Metric Model
- **Metric contract**: encode metrics as `MetricEval` functions receiving `(rows, groupKey, evalMetric)` where `rows` are the fact rows for the current group.
- **Metric body DSL**: support aggregators (`sum`, `avg`, `count`, `countDistinct`, `min`, `max`), row-level expressions, metric references, and higher-order transforms; forbid any baked-in grain.
- **Dependency handling**: allow metrics to invoke other metrics at the same grain via `evalMetric`, with cycle detection.
- **Catalog updates**: re-author existing metrics to the new contract; measures become plain attributes/columns.

## Phase 3 – Query DSL & Relational Algebra Execution
- **DSL shape**: implement `query { dimensions …; metrics …; where …; having … }` syntax (or equivalent structured form).
- **Base fact selection**: choose a fact compatible with all requested metrics and dimensions; surface errors when incompatible.
- **Join planning**: derive required joins to resolve dimensions and `where` attributes via the semantic model.
- **Execution pipeline**:
  1. Build base relation `R` (fact + necessary dimensions).
  2. Apply `where` as selection.
  3. Group by explicit grain (dimensions list).
  4. For each group, evaluate metrics using group rows.
  5. Apply `having` filters over computed metric values.
- **Result shaping**: emit grouped dimensions plus metric scalars; no implicit grain.

## Phase 4 – Transforms (Rowset + Metric)
- **Rowset transforms**: implement selection-style filters (e.g., time windows) as rowset modifiers before aggregation.
- **MicroStrategy-style transforms**: support table-driven shifts (e.g., last-year week) via joins to transform tables; expose helpers to map current grain keys to transformed rowsets.
- **Metric transforms**: add higher-order functions (e.g., `last_year(m, date_attr)`, YoY) that evaluate a base metric against transformed rowsets and return scalars at the current grain.
- **Validation**: ensure transforms preserve grain-agnostic invariants and operate on arbitrary groupings.

## Phase 5 – Runtime, Tests, and Demo
- **Runtime updates**: rework the in-memory engine to follow the RA semantics, replacing LINQ-specific shortcuts where needed.
- **Testing**: extend Mocha/Chai coverage for base metrics, nested metric references, rowset transforms, MicroStrategy-style transforms, and query/having behavior across multiple grains.
- **Demo refresh**: update the in-memory demo to showcase the new DSL (base + last-year metrics, YoY) and query execution over sample data.
- **Documentation**: align README/SPEC with the new DSL, semantics, and invariants (metrics always recomputed at query grain).

## Phase 6 – Migration/Compatibility Cleanup
- **Remove legacy paths**: delete or deprecate LINQ-only metric builders and measure-centric code paths superseded by the new model.
- **API cleanliness**: rename types and helpers to match the DSL terminology (facts, dimensions, metrics, transforms); strip unused compatibility shims.

This phased plan delivers a DSL-driven, grain-agnostic metric engine with explicit relational-algebra semantics, ready to extend to SQL backends later while remaining in-memory for now.
