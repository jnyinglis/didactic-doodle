PRD 2 — Data-Based Transforms (Table Transforms)

1. Overview

Current transforms are expression-based only. We need support for lookup-table-based transforms, where a transform applies a mapping derived from a two-column table.

This matches how MicroStrategy handles time shifts and map-based transforms (e.g., “previous year”, “previous quarter”, “custom grouping”, “bucketization”).

⸻

2. Goals
	•	Allow transforms to be defined using an arbitrary 2-column dataset.
	•	Support one-to-one, many-to-one, one-to-many, and non-surjective mappings.
	•	Plug into the existing transform DSL with minimal syntax expansion.
	•	Integrate with dimensionality tracking and join graph.
	•	Enable key use cases:
	•	calendar shifts (year-ago, week-ago, period-ago)
	•	territory remapping
	•	hierarchical rollups
	•	custom buckets

⸻

3. Non-Goals
	•	Automatic generation of time intelligence transforms (future feature).
	•	Automatic cardinality inference beyond the mapping table.

⸻

4. Requirements

4.1 Transform Definition API

Proposed API:

const prevYear = engine.defineTableTransform({
  name: "previousYear",
  table: [
    { key: "202301", mappedKey: "202201" },
    { key: "202302", mappedKey: "202202" },
    ...
  ],
  inputAttr: "calendar.year_month",
  outputAttr: "calendar.year_month"
});

or using a relation:

engine.defineTableTransformFromRelation({
  name: "previousYear",
  relation: "calendar_shift_table",
  keyColumn: "year_month",
  mappedColumn: "year_month_last_year"
});


⸻

4.2 Execution Behavior

When a metric with a transform is executed:
	1.	Input rows are projected into the transform’s key column.
	2.	The transform mapping is applied:
	•	exact match → emit mappedKey
	•	no match → return null or fallback (configurable)
	3.	Output rows are rejoined by the mapped attribute (or used as filter).

⸻

4.3 AST Integration

Transform nodes in AST must represent:
	•	transform type (expression vs table)
	•	mapping table reference
	•	input attribute
	•	output attribute
	•	cardinality (inferred or user-specified)

Example AST node:

{
  type: "Transform",
  kind: "table",
  name: "previousYear",
  inputAttr: "calendar.year_month",
  outputAttr: "calendar.year_month",
  mappingRef: "previousYearMapping"
}


⸻

4.4 Dimensionality Rules
	•	A table transform may shift grain (e.g., wk → wk-1).
	•	The resulting dimensionality must replace inputAttr’s dimension with the dimension implied by outputAttr.
	•	Many-to-many transforms must be flagged for validation**.

⸻

4.5 Error Handling

Errors to detect:
	•	missing input attribute
	•	transform table missing rows
	•	mapped keys not belonging to the same domain
	•	transform creates higher cardinality (unless explicit)
	•	transform output dimension is not reachable from fact

⸻

4.6 Integration with Filtering

Using transform inside filters:

metric.applyTransform("previousYear").filter({ year_month: "202502" })

Resulting SQL-like behavior:

WHERE year_month = previousYear(year_month_input)


⸻

5. Deliverables
	•	Transform registry with separate namespace:
engine.defineTableTransform()
	•	New AST node type
	•	Changes to context execution
	•	Changes to validator (PRD #1)
	•	Unit tests for:
	•	weekly shifts
	•	custom buckets
	•	missing map entries
	•	many-to-one transforms
	•	non-injective transforms

⸻
