Gotcha — thanks for clarifying what you meant. You *do* want fact-anchored, but with the **frame built from the union of all fact-specific dimension sets**, not from a single primary fact.

So the intended behaviour is:

> For each fact that participates in the query, run a fact-anchored query that returns rows at the requested grain (dimensions + that fact’s metrics). Then take the **outer union** of all those dimension keys and stitch the metrics together.

Here’s how I’d explain to your dev what needs to change / be added.

---

## 1. Keep the fact-anchored design

Don’t throw away the current logic that:

* chooses a `baseFact`
* calls `buildFactBaseRelation(...)` or `buildDimensionBaseRelation(...)`
* groups by the query `dimensions`
* runs metrics as `{ rows, groupKey }` over that base relation.

Instead, **factor that into a reusable helper** that is explicitly “run a query for this one base fact”.

Roughly:

```ts
interface SingleFactResult {
  rows: Row[];
  fact: string | null;   // null for pure dimension-based
}

function runSemanticQueryForBase(
  env: MetricRuntimeEnv,
  model: SemanticModel,
  spec: QuerySpec,
  baseFact: string | null,           // null => dimension-only case
  metricsForThisBase: string[],      // subset of spec.metrics
): SingleFactResult {
  // today’s logic lives here:
  // - determine requiredAttributes
  // - buildFactBaseRelation or buildDimensionBaseRelation
  // - apply where filter
  // - group by dimensions
  // - evaluate metrics using MetricRuntime { relation, baseFact, ... }
  // - return [{ ...groupKey, ...metricValues }]
}
```

Then the top-level `runSemanticQuery` becomes just an orchestrator over multiple calls to `runSemanticQueryForBase`.

---

## 2. Partition metrics by base fact

Right now you effectively pick **one** `baseFact` using the metrics. To support the behaviour you described, you want to **partition** by `baseFact` instead:

```ts
// metricName -> MetricDefinition
const metricDefs = model.metrics;

const metricsByBase: Map<string | null, string[]> = new Map();

// null key = dimension-only metrics (no baseFact)
for (const mName of spec.metrics) {
  const def = metricDefs[mName];
  if (!def) throw new Error(`Unknown metric: ${mName}`);
  const key = def.baseFact ?? null;
  if (!metricsByBase.has(key)) metricsByBase.set(key, []);
  metricsByBase.get(key)!.push(mName);
}
```

Now you know which facts are involved:

* keys like `"fact_sales"`, `"fact_visits"`, etc.
* a `null` key for metrics that are dimension-based only (e.g. count of stores from `dim_store`).

---

## 3. Run one fact-anchored query per base fact

For each entry in `metricsByBase`:

```ts
const perFactResults: SingleFactResult[] = [];

for (const [baseFact, metricNames] of metricsByBase.entries()) {
  const childSpec: QuerySpec = {
    dimensions: spec.dimensions,
    metrics: metricNames,
    where: spec.where,
    having: undefined, // apply HAVING at the outer level only
  };

  perFactResults.push(
    runSemanticQueryForBase(env, model, childSpec, baseFact, metricNames)
  );
}
```

At this stage, each `SingleFactResult.rows` is already:

```ts
[
  { dim1: ..., dim2: ..., metricA: ..., metricB: ... }, // fact F1
  ...
]
```

all **grouped by the same logical dimensions** (because you passed `spec.dimensions` through).

---

## 4. Build the frame from the **union** of dimension keys

This is the key change you were asking for.

Instead of letting a single base fact define the frame, you:

1. Collect all distinct dimension keys across **all facts**.
2. Outer-join each per-fact result set onto that frame by the dimension key.

Something like:

```ts
type DimKey = string; // JSON of the dimensions

function dimKeyFromRow(row: Row, dims: string[]): DimKey {
  const obj: Row = {};
  dims.forEach(d => { obj[d] = row[d]; });
  return JSON.stringify(obj);
}

export function runSemanticQuery(
  env: MetricRuntimeEnv,
  model: SemanticModel,
  spec: QuerySpec
): Row[] {
  // 1. Partition metrics by baseFact (step 2 above).
  const metricDefs = model.metrics;
  const metricsByBase = new Map<string | null, string[]>();

  for (const mName of spec.metrics) {
    const def = metricDefs[mName];
    if (!def) throw new Error(`Unknown metric: ${mName}`);
    const key = def.baseFact ?? null;
    if (!metricsByBase.has(key)) metricsByBase.set(key, []);
    metricsByBase.get(key)!.push(mName);
  }

  // 2. Run a fact-anchored query for each baseFact.
  const perFactResults: SingleFactResult[] = [];
  for (const [baseFact, metricNames] of metricsByBase.entries()) {
    const childSpec: QuerySpec = {
      dimensions: spec.dimensions,
      metrics: metricNames,
      where: spec.where,
      having: undefined,
    };
    perFactResults.push(
      runSemanticQueryForBase(env, model, childSpec, baseFact, metricNames)
    );
  }

  // 3. Build union frame of dimension keys.
  const frame = new Map<DimKey, Row>();

  for (const { rows } of perFactResults) {
    for (const row of rows) {
      const key = dimKeyFromRow(row, spec.dimensions);
      const existing = frame.get(key) ?? {};
      frame.set(key, { ...existing, ...pickDims(row, spec.dimensions) });
    }
  }

  // 4. Outer-join each fact’s metrics onto the frame.
  for (const { rows } of perFactResults) {
    for (const row of rows) {
      const key = dimKeyFromRow(row, spec.dimensions);
      const target = frame.get(key)!;
      for (const [k, v] of Object.entries(row)) {
        if (!spec.dimensions.includes(k)) {
          // metric column
          target[k] = v;
        }
      }
    }
  }

  // 5. Apply HAVING at the outer level (if needed).
  let finalRows = Array.from(frame.values());

  if (spec.having) {
    finalRows = finalRows.filter((r) => {
      const metricValues: Record<string, number | undefined> = {};
      spec.metrics.forEach((m) => {
        metricValues[m] = r[m] as number | undefined;
      });
      return spec.having!(metricValues);
    });
  }

  return finalRows;
}

function pickDims(row: Row, dims: string[]): Row {
  const out: Row = {};
  dims.forEach((d) => { out[d] = row[d]; });
  return out;
}
```

This gives you exactly what you described:

* **Each fact contributes its own view of the world at the requested grain.**
* The **frame** is the **union of all dimension combinations** that appear in any fact.
* If a given dimension combination only exists in `fact_A`, metrics from `fact_B` will be `undefined` for that row (outer-join semantics), not dropped.

---

## 5. Dimension-only metrics

Because we’ve removed `defaultFact` from `LogicalAttribute` and treat dimension tables as “facts of one”, dimension-only metrics (e.g. “Store Count” on `dim_store`) can be handled as:

* `MetricDefinition.baseFact` set to the **dimension relation name** (e.g. `"dim_store"`), and
* `runSemanticQueryForBase` calling `buildDimensionBaseRelation` instead of `buildFactBaseRelation` when `baseFact` is not in `model.facts` (or when `baseFact === null` and you find a single common dimension relation, depending on how you wired it earlier).

From the orchestrator’s point of view, that’s just **one more entry in `metricsByBase`**, so it seamlessly participates in the union-of-dimensions logic.

---

### TL;DR for your dev

* Don’t try to make `MetricRuntime` span multiple facts at once.
* Instead, **reuse the existing single-fact pipeline** and layer a **multi-fact orchestrator** on top:

  * Partition metrics by `baseFact`.
  * For each `baseFact`, run the existing logic (fact-anchored query) to get rows at the requested grain.
  * Build the frame from the **union of all dimension keys** across those results.
  * Outer-join each fact’s metric columns onto that frame.
* Treat dimension tables as valid `baseFact`s for dimension-only metrics.

That matches your “each query against a fact returns dimensions, then do a union of the returned dimensions” idea, and it keeps the internals conceptually clean.
