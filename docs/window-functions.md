PRD 3 — Window Functions (windowBy)

1. Overview

The engine currently supports basic LINQ aggregate operations, but lacks a native notion of analytic window functions. These are essential for rolling periods, running totals, and time intelligence patterns.

The goal for alpha is minimal but functional window support, not a full SQL-like implementation.

⸻

2. Goals

Provide a windowBy API that allows:
	•	partitioning by dimension keys
	•	ordering by an attribute
	•	defining a window frame (rolling N periods, cumulative, point-in-time)
	•	applying an aggregate (sum, avg, min, max, count)
	•	creating composable semantic metrics using the window result

This should plug directly into the DSL and AST.

⸻

3. Non-Goals
	•	No full SQL OVER clause.
	•	No support for frame exclusions.
	•	No need for peer group semantics or UNBOUNDED FOLLOWING.
	•	No complex null-handling rules.

⸻

4. Requirements

4.1 API Syntax

Base version:

metric.windowBy({
  partitionBy: ["region", "product"],
  orderBy: "calendar.date",
  frame: { kind: "rolling", count: 3 },
  aggregate: "sum"
});

Possible DSL shortcuts:

window.sumRolling(3).by("date").partition("region")
window.runningTotal().by("date")


⸻

4.2 Supported Frames

4.2.1 Rolling Window

ROWS N PRECEDING to CURRENT ROW

4.2.2 Cumulative Window

UNBOUNDED PRECEDING to CURRENT ROW

4.2.3 Exact Offset Window (lag/lead)

frame: { kind: "offset", offset: -1 } // previous period

This is useful for YoY, MoM, WoW before table transforms land.

⸻

4.3 Internal Computation

Leverage LINQ or implement directly:
	1.	Partition rows
	2.	Sort by orderBy
	3.	For each row, compute aggregate over frame
	4.	Emit new sequence with windowed value attached (new column)

⸻

4.4 AST Representation

Example:

{
  type: "Window",
  partitionBy: ["region", "product"],
  orderBy: "calendar.date",
  frame: { kind: "rolling", count: 3 },
  aggregate: "sum"
}

Validator integration:
	•	orderBy must belong to same partition’s dimension or reachable dimension
	•	cannot use attribute that raises grain incorrectly
	•	frame count must be integer

⸻

4.5 Output Dimensionality
	•	Result dimensionality = input dimensionality (window does not change grain)
	•	Window produces a derived column on the same row
	•	If window is applied to an aggregated metric, enforce that metric yields a row-level sequence before windowing.

⸻

4.6 Error Handling
	•	orderBy attribute not reachable
	•	partitionBy attributes not reachable
	•	mixing columns from incompatible dimensions
	•	invalid frame count
	•	aggregate type mismatch
	•	applying window to a metric that is already aggregated at higher grain

⸻

5. Deliverables
	•	windowBy implementation
	•	New AST Window node
	•	Validation rules (PRD #1)
	•	Unit tests for:
	•	cumulative
	•	rolling
	•	offset/lags
	•	partitioned windows
	•	multi-key partitions
	•	error conditions

⸻
