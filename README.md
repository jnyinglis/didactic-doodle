# Semantic Metrics Engine POC

This repository contains a TypeScript proof-of-concept for a semantic metrics engine inspired by tools such as MicroStrategy, MetricFlow, and other semantic layers. The project demonstrates how to:

- Declare **table metadata** (grain, relationships, formatting hints) alongside the in-memory data they describe.
- Register **attributes, measures, and metrics** with consistent naming and documentation.
- Express **filter contexts** using a small DSL (`f.eq`, `f.and`, range helpers, etc.) that can be merged, normalized, and reused across queries.
- Compose **metric types**—fact measures, arbitrary expressions, derived metrics, and context transforms (YTD, prior year, YTD last year).
- Run **LINQ-style queries** over in-memory rows (via the bundled [`linq.js`](src/linq.js)) to return formatted, label-enriched result sets.

Everything lives in [`src/semanticEngine.ts`](src/semanticEngine.ts), including the demo data/model, query builder, and CLI showcase guarded by `require.main === module`.

## Repository layout

| Path | Description |
| --- | --- |
| [`src/semanticEngine.ts`](src/semanticEngine.ts) | Engine implementation, builders (`attr`, `measure`, `metric`, `contextTransform`), query execution, demo data/model, and CLI demo. |
| [`src/linq.js`](src/linq.js) / [`src/linq.d.ts`](src/linq.d.ts) | Bundled LINQ implementation plus TypeScript definitions that power `rowsToEnumerable` and query execution. |
| [`src/operators.md`](src/operators.md) | Quick reference of every available LINQ helper for authoring custom metrics or transforms. |
| [`test/semanticEngine.test.ts`](test/semanticEngine.test.ts) | Mocha + Chai suite covering context filtering, metric evaluation, and formatted query output. |
| [`SPEC.md`](SPEC.md) | Design notes outlining the motivation behind the refactor to a LINQ-first execution model. |

## Getting started

1. **Install dependencies** (TypeScript toolchain + Mocha/Chai):
   ```bash
   npm install
   ```
2. **Run the automated tests**:
   ```bash
   npm test
   ```
   The suite executes directly against the demo database to validate `applyContextToTable`, metric evaluation (including transforms), and the query builder.
3. **Try the CLI demo** (optional):
   ```bash
   npx ts-node src/semanticEngine.ts
   ```
   The script prints several queries for 2025 sales, showcasing dimension grains, reused builders, metric formatting, and the built-in time-intelligence transforms.

## Using the engine

```ts
import {
  createEngine,
  demoDb,
  demoModel,
} from "./src/semanticEngine";

const engine = createEngine(demoDb, demoModel);

const rows = engine
  .query("sales")
  .where({ year: 2025, month: 2 })
  .addAttributes("regionId", "productId")
  .addMetrics("totalSalesAmount", "salesAmountYTD", "salesVsBudgetPct")
  .run();

console.table(rows);
```

Under the hood the engine will:

1. Apply the provided filter context, respecting the table grain and any metric-level overrides (e.g., `salesAmountYearRegion`).
2. Evaluate each metric via the registry, caching repeated calculations and running dependency chains for derived metrics.
3. Format values (currency, integer, percent, etc.) and enrich attributes with their label columns (`regionName`, `productName`).

You can also call `engine.evaluateMetric("salesAmountYTD", { filter: { year: 2025, month: 2 } })` for ad-hoc metric checks, or reuse partially-built queries (`const base = engine.query("sales").where({ year: 2025 });`) to reduce boilerplate across slices.

## Extending the POC

1. **Model your tables** by inserting raw rows into `db.tables` and describing them inside a `TableDefinitionRegistry` (grain, column roles, label relationships, formats).
2. **Declare attributes and measures** using the helper builders exported from `src/semanticEngine.ts`. This keeps documentation, column mappings, and default aggregations centralized.
3. **Register metrics** in a `MetricRegistry` using:
   - `simpleMetric` (fact measures)
   - `expressionMetric` (custom LINQ aggregates)
   - `derivedMetric` (metric arithmetic / dependency graphs)
   - `contextTransformMetric` (time-intelligence / scenario filters)
4. **Add context transforms** that accept and return `MetricContext` objects. The demo includes `ytd`, `lastYear`, and `ytdLastYear`, and they can be composed via `composeTransforms`.
5. **Query the data** through `engine.query(table)` to configure attributes, metrics, and filters, or call `runQuery` directly if you prefer to build `QuerySpec` objects yourself.

Because everything is in-memory, the POC is a convenient playground for experimenting with new metric types, richer labeling strategies, or eventually swapping the execution layer for SQL push-down.
