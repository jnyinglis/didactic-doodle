# Implementation Plan for FilterRange and Measure/Metric Improvements

This document outlines the concrete steps required to refactor the filter range typing and decouple measure aggregation from metric definitions. The goal is to increase type safety, reduce ambiguity, and enable richer metric reuse.

## 1. Adopt a Tagged Union for `FilterRange`

1. **Introduce discriminated union types**
   - Replace the current loose `FilterRange` object with a tagged union that enumerates the supported variants (e.g., `between`, `gte`, `gt`, `lte`, `lt`).
   - Centralize the new type definition near the existing filter utilities so it can be shared across runtime and modeling layers.
2. **Update construction and parsing logic**
   - Refactor helper functions that build or consume `FilterRange` instances to switch on the discriminant instead of probing optional properties.
   - Ensure serialization/deserialization routines emit and expect the `kind` field, preventing ambiguous payloads.
3. **Expand automated tests**
   - Cover each valid union variant through unit tests.
   - Add regression tests for invalid combinations to guarantee the runtime rejects ambiguous filters going forward.

## 2. Decouple Measure Aggregation into Metrics

1. **Simplify `MeasureDefinition`**
   - Remove the `aggregation` attribute so measures only describe the base fact (table, column, type, grain).
   - Audit serialization schemas and documentation to reflect the new contract.
2. **Extend metric definitions with aggregation**
   - Update `SimpleMetricDefinition` (and other metric types) to include a required or defaulted aggregation operator.
   - Ensure derived metrics can override aggregation behavior while reusing the same measure.
3. **Adjust runtime evaluation flow**
   - Modify `evaluateMeasureDefinition` / `evaluateMetric` so aggregation is applied when the metric requests it, leaving measures as raw fact fetchers.
   - Confirm that metrics can pass aggregation hints, enabling scenarios like “total budget” vs. “average budget” on the same measure.
4. **Migrate existing catalog entries**
   - Update current measure and metric declarations to conform to the new separation of concerns.
   - Create multiple metrics (e.g., total and average budget) referencing the same measure to demonstrate reuse.
5. **Add targeted tests**
   - Verify that a single measure supports multiple metric aggregations with consistent results.
   - Ensure derived metrics and downstream consumers behave correctly when aggregation is specified at the metric layer.

---

Following this plan will improve the semantic layer’s type safety and flexibility, allowing richer metric definitions without duplicating underlying facts.
