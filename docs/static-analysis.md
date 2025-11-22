PRD 1 — Static Analysis & Metric Validation

1. Overview

The semantic engine now has an AST representation for metric definitions, expressions, and transforms. To make the engine stable for alpha, we must leverage this AST to perform static validation before execution. This prevents runtime errors and ensures that all metric definitions are semantically valid.

This is not a runtime optimizer; this is about correctness and safety.

⸻

2. Goals
	•	Detect and report invalid metric definitions before execution.
	•	Prevent circular dependencies in derived metrics.
	•	Ensure all attributes referenced by metrics are reachable from the base fact.
	•	Prevent invalid transforms (e.g., applying a calendar transform to a non-date attribute).
	•	Validate that a metric’s dimensionality is coherent.
	•	Validate that row/aggregate scope usage is legal.

This creates a much safer developer experience.

⸻

3. Non-Goals
	•	No full SQL planning.
	•	No performance optimization.
	•	No caching or code generation.

⸻

4. Requirements

4.1 AST Traversal

Implement a generic walker capable of:
	•	scanning expressions
	•	scanning argument lists
	•	scanning nested derived metrics
	•	returning a structured list of referenced:
	•	attributes
	•	literals
	•	metric dependencies
	•	transform dependencies

4.2 Attribute Reachability Validation

For each attribute referenced:
	•	Determine its base fact association (using the semantic model)
	•	Determine if it can be reached from the metric’s declared base fact through the join graph
	•	If unreachable → error

4.3 Circular Dependency Detection

Example:

metric A = derived(B + 1)
metric B = derived(A + 1)

Must fail before execution.

Mechanism:
	•	Build dependency graph (metricName → dependencyList)
	•	Perform DFS with cycle detection
	•	Report cycle path

4.4 Dimensionality Validation

Track:
	•	source dimensionality
	•	result dimensionality
	•	transform effects on dimensionality

Rules to enforce:
	•	Aggregates cannot output higher grain than input
	•	A metric cannot mix attributes from incompatible grains without an explicit join path
	•	Transforms must not violate dimensional constraints
	•	Derived metrics must not widen dimensionality unless explicitly allowed

4.5 Transform Validation

Expression-based transforms:
	•	Validate referenced attributes match expected types (date, number, text, etc.).

Table transforms:
	•	Validate that transform input attribute belongs to base fact (or reachable dimension).
	•	Validate transform outputs preserve or shift grain correctly.

4.6 Error Reporting

All validation errors must include:
	•	metric name
	•	full dependency path
	•	AST snippet or pointer
	•	suggestion for how to fix it

Example error:

Metric "sales_ytd" references attribute "calendar.fiscal_year"
which is not reachable from base fact "fact_sales".


⸻

5. Deliverables
	•	validator.ts (or integrated into semanticEngine.ts)
	•	API:

engine.validateMetric(name: string): ValidationResult
engine.validateAll(): ValidationResult[]


	•	Tests covering:
	•	invalid attributes
	•	invalid transforms
	•	circular metrics
	•	unreachable dimensions
	•	invalid window definitions (if implemented)
	•	cross-fact misuse

⸻
