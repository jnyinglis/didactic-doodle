Below is a clear, implementation-ready set of instructions for your developer describing exactly what needs to be built next to support the “budget-style” scenario (metrics defined at a coarser grain than the query grain).

These instructions assume the new multi-fact frame logic is already in place and working.

⸻

✅ Instructions for Implementing Coarse-Grain Metrics (Budget-Style Metrics)

This feature must allow metrics whose native grain (the attributes that metric depends on) is coarser than the query grain.

Example:
Query grain = {store, week}
Budget metric grain = {week}
→ Budget must broadcast to all stores.

This requires changes in three areas:

⸻

1. Determine the “Frame Facts” — Only Facts that Support All Query Dimensions

When building the unified frame (the set of dimension combinations), only include facts that contain all the query dimensions.

Why?
	•	The frame must represent the finest grain requested by the query.
	•	A fact like fact_budget(week) is not capable of producing {store, week} rows — it cannot define the frame.
	•	Facts lacking any dimension(s) are skipped at frame-building time.

Developer Implementation Tasks
	•	When iterating through metricsByFact, identify which baseFact supports all query dimensions.
	•	You already know how to determine this from requiredAttrs. You now must check:

factSupportsAllDimensions = dimensions.every(dim => 
  model.attributes[dim].relation is reachable from this fact
)

	•	Only include facts that return true in the frame-building stage.
	•	Facts that do NOT support all dimensions will be evaluated after the frame is built.

This produces a consistent, fully-detailed frame.

⸻

2. During Join-in of Coarse-Grain Metrics: Join Only on the Metric’s Native Grain

After the frame is created:
	•	Each metric has its own native grain metric.attributes (already in your V2 model).
	•	When joining metric results into the final frame:
	•	Match only on those attributes, not all query dimensions.

Example:

metric BUDGET
  grain = {week}
  baseFact = fact_budget

Join pattern:

frame: {store, week}
budget rows: {week}
join on: week

➡ Automatically broadcasts budget across stores.

Developer Implementation Tasks

Modify the join-in phase so that:
	•	Each fact-metric result set is keyed on its own metric grain, not on the full query grain.
	•	When merging results into the frame, match on:

metricKey = JSON.stringify(pick(row, metricGrain))
frameKey = JSON.stringify(pick(row, metricGrain))

Specifically:
	•	During the loop where you attach metric values to the frame rows, change:

keyStr = keyFromRow(row, dimensions)

to:

keyStr = keyFromRow(row, metricGrain)

Use the metric’s own attributes field.

⸻

3. Do Not Discard Frame Rows When Metric Grain Is Coarser

Current code implicitly assumes:

Facts define all the dimensions that appear in the frame.

This is now not true.

You must ensure:
	•	Frame rows always remain, even if a metric cannot match a row (because its grain is coarser).

This produces:

store	week	budget
A	202501	100
B	202501	100
C	202501	100

Even though the budget fact returns only one row:

{week: 202501, budget: 100}

Developer Implementation Tasks

Ensure the merge uses outer-join semantics, not inner-join.

Specifically:

In the merge loop:

const metricRow = factMetricMap.get(keyMatch) // using metric grain
if (!metricRow) {
    // leave metric undefined/null
}
Object.assign(frameRow, metricRow)

Never prune or skip the frame row.

⸻

4. Metric Definition Must Explicitly Declare Its Grain

Your V2 metric definitions already support attributes?: string[].

This is now required for coarse-grain metrics.

Example:

metricsV2: {
  budget: {
    name: "budget",
    baseFact: "fact_budget",
    attributes: ["week"],  // native grain
    eval: (...) => ...
  }
}

Developer should:
	•	Ensure every metric definition includes attributes.
	•	If a metric depends on another metric (derived), the grain must be the grain of the derived metric’s dependencies.

⸻

5. Order of Operations Summary (Developer Cheat Sheet)

Step 1 — Partition metrics by baseFact

Already implemented.

Step 2 — For each fact, determine whether it supports all query dims

→ If yes, run fact query → contributes to frame
→ If no, run fact query separately, to be joined later

Step 3 — Build frame from union of dimension keys from all full-grain facts

(If none, produce empty frame or dimension fallback.)

Step 4 — For each fact-metric set:
	•	Compute grain-local rows
	•	Group by metric grain, not query grain

Step 5 — For each frame row:
	•	Locate matching metric row using metric-grain key
	•	Attach metric values
	•	If no match: leave metric null (broadcast happens automatically)

Step 6 — Apply HAVING at the very end

⸻

6. Edge Case Notes For Developer
	•	If no fact supports all dimensions → fallback to dimension-only logic.
	•	If all metrics have coarse grain → frame may be determined from dimensions only.
	•	If metric grain is empty (global metric), join on no attributes → broadcast everywhere.
	•	Avoid double-counting joins by separating metric-grain and frame-grain concepts.
