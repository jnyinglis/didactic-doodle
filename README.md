# Semantic Engine

This repository contains a **grain-agnostic semantic metrics engine** built entirely in TypeScript. Metrics see only the rows and group key for the current query grain, while the runtime selects the base fact table from metric declarations or falls back to a single related dimension when no facts are needed. LINQ primitives from [`src/linq.js`](src/linq.js) power projection, filtering, grouping, and joining so everything stays in-memory and deterministic.

## Core ideas

- **Metric functions, not grain-bound measures** – `MetricDefinition` exposes an `eval` function that receives grouped rows and the current group key; grains are provided by the query, not by the metric definition. 【F:src/semanticEngine.ts†L288-L314】
- **Explicit base fact selection with graceful dimension fallback** – When metrics declare `baseFact`, the engine builds a fact-backed frame and stitches in additional fact groups as needed; when no metrics or `baseFact` values are present, it runs against a single dimension relation instead of requiring a fact table. 【F:src/semanticEngine.ts†L637-L799】【F:src/semanticEngine.ts†L801-L939】
- **Rowset transforms for time intelligence** – Metrics can invoke `helpers.applyRowsetTransform` to swap the rowset they aggregate over (e.g., last-year alignment) while still reporting at the current grain. 【F:src/semanticEngine.ts†L308-L314】【F:src/semanticEngine.ts†L525-L591】
- **Attribute-first filtering** – The `f` helpers normalize objects or filter nodes into expressions that are pruned to available attributes before evaluation, keeping `where` clauses composable across facts and dimensions. 【F:src/semanticEngine.ts†L32-L93】【F:src/semanticEngine.ts†L95-L216】

## Repository layout

| Path | Description |
| --- | --- |
| [`src/semanticEngine.ts`](src/semanticEngine.ts) | Core engine with filter helpers, metric definitions, rowset transforms, and `runSemanticQuery`. |
| [`src/semanticEngineDemo.ts`](src/semanticEngineDemo.ts) | Minimal demo wiring a store dataset, two fact tables, and aggregate metrics. |
| [`src/linq.js`](src/linq.js) / [`src/linq.d.ts`](src/linq.d.ts) | Bundled LINQ runtime plus TypeScript declarations used by the engine and tests. |
| [`src/operators.md`](src/operators.md) | Operator reference for custom LINQ-powered transforms. |
| [`test/semanticEngine.test.ts`](test/semanticEngine.test.ts) | Mocha + Chai coverage for base fact selection, filters, and rowset transforms. |
| [`SPEC.md`](SPEC.md) | Design notes motivating the grain-agnostic flow. |

## Getting started

1. **Install dependencies**:
   ```bash
   npm install
   ```
2. **Run the automated tests** to exercise the engine end-to-end:
   ```bash
   npm test
   ```
3. **Try the demo** to see a multi-fact query at work:
   ```bash
   npx ts-node src/semanticEngineDemo.ts
   ```
4. **Explore the DSL demo** for parsing metric and query definitions from text:
   ```bash
   npx ts-node src/dslDemo.ts
   ```

## Using the engine

Relational queries are described with `QuerySpec` objects that list the output dimensions, metrics, and filters. Metrics are plain functions declared via helpers such as `aggregateMetric` and are free to call other metrics through `evalMetric` or swap rowsets through `applyRowsetTransform`.

```ts
import {
  aggregateMetric,
  runSemanticQuery,
  QuerySpec,
  SemanticModel,
} from "./src/semanticEngine";

const model: SemanticModel = {
  facts: {
    fact_orders: { name: "fact_orders" },
    fact_returns: { name: "fact_returns" },
  },
  dimensions: { dim_store: { name: "dim_store" } },
  attributes: {
    storeId: { name: "storeId", relation: "fact_orders", column: "storeId" },
    storeName: { name: "storeName", relation: "dim_store", column: "storeName" },
    amount: { name: "amount", relation: "fact_orders", column: "amount" },
    refund: { name: "refund", relation: "fact_returns", column: "refund" },
  },
  joins: [
    { fact: "fact_orders", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    { fact: "fact_returns", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
  ],
  metrics: {
    totalSales: aggregateMetric("totalSales", "fact_orders", "amount", "sum"),
    totalRefunds: aggregateMetric("totalRefunds", "fact_returns", "refund", "sum"),
  },
};

const spec: QuerySpec = {
  dimensions: ["storeId", "storeName"],
  metrics: ["totalSales", "totalRefunds"],
};

const rows = runSemanticQuery({ db, model }, spec);
console.log(rows);
```

The engine builds a fact-backed frame for the primary fact, runs metric evaluators within each grouped rowset, and merges results across additional facts or dimension-scoped metrics before applying optional `having` filters. 【F:src/semanticEngine.ts†L801-L976】
