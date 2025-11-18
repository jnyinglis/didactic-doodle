# Relational Semantic Engine POC

This repository hosts a TypeScript proof-of-concept for a **LINQ-only, relational-algebra semantic engine**. The runtime embraces three pillars:

1. **Frame-first execution** – Queries build a cross-join frame from attribute domains and treat every metric as a LEFT JOIN off that frame.
2. **Measure-only metrics** – Metrics operate exclusively on measure value streams so aggregations and expressions are deterministic and reusable.
3. **LINQ everywhere** – Filtering, projection, grouping, joining, and aggregation all lean on the bundled [`linq.js`](src/linq.js) operators via `rowsToEnumerable`.

Key capabilities include:

- Unified table metadata covering grains, relationships, and column descriptions.
- Filter contexts expressed through a concise DSL (`f.eq`, `f.and`, range helpers) that normalize down to LINQ predicates.
- **Relational metrics** that return `Enumerable<Row>` instances for the `runRelationalQuery` flow, while the legacy scalar builder API remains available through `buildEngine(env)` for backwards compatibility.
- Relational aggregate + expression metric builders that consume `Enumerable<number>` sequences, ensuring consistent behavior regardless of grain.

Everything is implemented inside [`src/semanticEngine.ts`](src/semanticEngine.ts) with a standalone demo harness in [`src/semanticEngineDemo.ts`](src/semanticEngineDemo.ts) that wires up sample data and relational metrics.

## Repository layout

| Path | Description |
| --- | --- |
| [`src/semanticEngine.ts`](src/semanticEngine.ts) | Core engine, builders (`attr`, `measure`, `relAggregateMetric`, etc.), scalar + relational query execution, and helper utilities. |
| [`src/semanticEngineDemo.ts`](src/semanticEngineDemo.ts) | Small runnable environment that seeds a store/product dataset and showcases relational query execution. |
| [`src/linq.js`](src/linq.js) / [`src/linq.d.ts`](src/linq.d.ts) | Bundled LINQ runtime plus the TypeScript declaration file used by the engine and tests. |
| [`src/operators.md`](src/operators.md) | Operator reference for anyone authoring custom LINQ-powered metrics or transforms. |
| [`test/semanticEngine.test.ts`](test/semanticEngine.test.ts) | Mocha + Chai coverage for filter normalization, measure projection, relational metrics, and the LINQ-only relational executor. |
| [`SPEC.md`](SPEC.md) | Design notes motivating the LINQ-first refactor and documenting the architectural goals. |

## Getting started

1. **Install dependencies** (TypeScript toolchain + Mocha/Chai):
   ```bash
   npm install
   ```
2. **Run the automated tests** to exercise the relational engine end-to-end:
   ```bash
   npm test
   ```
   The suite validates `applyContextToTable`, `projectMeasureValues`, relational metric builders, and the `runRelationalQuery` pipeline.
3. **Try the CLI demo** (optional) to see the relational execution path in action:
   ```bash
   npx ts-node src/semanticEngineDemo.ts
   ```
   The script prints two relational frames (store × product × year and year × month) so you can observe how metrics join back onto the frame.

## Using the relational engine

Relational queries are described through `QuerySpec` objects that list the requested attributes, metrics, and filters. Each relational metric returns a relation keyed by its grain and the engine LEFT JOINs them onto the frame:

```ts
import {
  MetricEvaluationEnvironment,
  relAggregateMetric,
  relExpressionMetric,
  runRelationalQuery,
  attr,
  measure,
  f,
} from "./src/semanticEngine";

// Build a SemanticModel with attributes/measures omitted for brevity.
const env: MetricEvaluationEnvironment = { model, db };

model.relationalMetrics = {
  totalSalesAmount: relAggregateMetric({ name: "totalSalesAmount", measure: "amount", agg: "sum" }),
  totalBudget: relAggregateMetric({ name: "totalBudget", measure: "budgetAmount", agg: "sum", grain: ["year"] }),
  salesDelta: relExpressionMetric({
    name: "salesDelta",
    measure: "amount",
    grain: ["storeId", "productId", "year"],
    expr: (values) => values.sum() * -1,
  }),
};

const spec = {
  table: "sales", // only used by the scalar path; relational queries derive the frame from attributes
  attributes: ["storeId", "productId", "year"],
  metrics: ["totalSalesAmount", "totalBudget", "salesDelta"],
  filters: f.eq("year", 2025),
};

const rows = runRelationalQuery(env, spec);
console.table(rows);
```

This produces the classic relational flow:

```
Frame (store × product × year)
  LEFT JOIN totalSalesAmount
  LEFT JOIN totalBudget
  LEFT JOIN salesDelta
```

## Extending the POC

1. **Model tables and attributes** via `TableDefinitionRegistry` and `AttributeRegistry`. Attribute definitions map semantic names to the columns that appear in your frame tables.
2. **Declare measures** with `measure.fact(...)` to describe which table + column supply numeric values and what grain they naturally live at.
3. **Author relational metrics** using `relAggregateMetric` or `relExpressionMetric`. You can also keep scalar metrics (simple, expression, derived, context transforms) for parity with the legacy executor.
4. **Compose query specs** by hand or through `buildEngine(env).query(table)` if you still need the scalar builder API.
5. **Experiment**: because every operation is LINQ-backed, it is straightforward to add more complex joins, windows, or label enrichment steps directly inside metrics or helper functions.

Everything runs fully in-memory, making this repository a safe playground for prototyping relational-semantic ideas before porting them to a SQL-backed engine.
