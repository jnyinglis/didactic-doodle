# Semantic Metrics Engine POC

This repository hosts a TypeScript proof-of-concept for a semantic metrics engine inspired by MicroStrategy, MetricFlow, and other semantic layers. The core engine lives in [`src/semanticEngine.ts`](src/semanticEngine.ts) and exposes a declarative way to model tables, metrics, and time-aware context transforms before materializing formatted result sets.

## Key capabilities

- **Unified table metadata** – All physical data lives in an in-memory [`InMemoryDb`](src/semanticEngine.ts) and is described through `TableDefinition` objects. Columns capture their role (attribute/measure), data type, default aggregation, and optional label relationships so lookup captions (e.g., `regionName`) are enriched automatically.
- **Metric registry** – Define metrics once and re-use them everywhere. The engine ships with four metric types: `factMeasure`, `expression`, `derived`, and `contextTransform` for time intelligence scenarios.
- **Filter contexts & transforms** – Any metric can be evaluated under a filter context (`year`, `regionId`, etc.). Context-transform metrics (YTD, last year, YTD last year, and more) mutate those filters on the fly before deferring to the underlying base metric.
- **LINQ-style execution** – Row operations use the bundled [`linq.js`](src/linq.js) implementation and the helper [`rowsToEnumerable`](src/semanticEngine.ts) so queries, metrics, and transforms can fluently compose `Enumerable` operators (`where`, `groupBy`, `sum`, `select`, …).
- **Formatted query results** – `runQuery` builds dimensioned result sets, applies metric formatting hints (currency, integer, percent), and injects label columns for human-friendly tables or chart inputs.
- **Documented operators** – [`src/operators.md`](src/operators.md) is a quick-start for every LINQ helper provided by the bundled library, making it easy to author richer metrics.

## Repository layout

| Path | Description |
| --- | --- |
| `src/semanticEngine.ts` | Engine implementation plus demo data, metric registry, context transforms, and a CLI demo. |
| `src/linq.js` / `src/linq.d.ts` | Bundled LINQ implementation and accompanying TypeScript declarations used by the engine. |
| `src/operators.md` | Reference for every LINQ operator available when writing metrics or transforms. |
| `test/semanticEngine.test.ts` | Mocha/Chai suite covering context application, metric evaluation, time intelligence, and `runQuery` formatting. |
| `SPEC.md` | Design notes that describe the refactor toward LINQ-first execution and unified table metadata. |

## Getting started

1. **Install dependencies** (for TypeScript + testing):
   ```bash
   npm install
   ```
2. **Run the automated tests**:
   ```bash
   npm test
   ```
   The suite executes directly against the demo data/metrics to validate the public helpers (`applyContextToTable`, `evaluateMetric`, and `runQuery`).
3. **Try the demo script** – Because `src/semanticEngine.ts` includes a `require.main === module` guard, you can run the sample queries via `ts-node`:
   ```bash
   npx ts-node src/semanticEngine.ts
   ```
   The script prints several dimensioned result sets for 2025 sales, showcasing filters, different row grains, and metric formatting.

## Usage example

```ts
import {
  runQuery,
  demoDb,
  demoTableDefinitions,
  demoMetrics,
  demoTransforms,
} from "./src/semanticEngine";

const rows = runQuery(
  demoDb,
  demoTableDefinitions,
  demoMetrics,
  demoTransforms,
  {
    rows: ["regionId", "productId"],
    filters: { year: 2025, month: 2 },
    metrics: ["totalSalesAmount", "salesAmountYTD", "salesVsBudgetPct"],
    tableForRows: "sales",
  }
);

console.table(rows);
```

The engine will:
1. Filter the `sales` table to the requested context and respect metric-level grain overrides (e.g., metrics defined at `year + regionId`).
2. Evaluate each metric through its registered definition and cache repeated calls inside the query loop.
3. Format numeric results (currency, integer, percent, etc.) and enrich attributes with their label columns (`regionName`, `productName`).

## Extending the engine

1. **Model your tables** – Populate `db.tables` with raw rows and describe each dataset inside `tableDefinitions`. Use the `labelFor` metadata whenever a lookup caption should accompany a key.
2. **Register metrics** – Add entries to a `MetricRegistry` using the type that matches your needs:
   - `factMeasure` for direct aggregations over a column.
   - `expression` for custom LINQ aggregations.
   - `derived` when a metric depends on other metrics.
   - `contextTransform` to wrap a base metric with reusable filter mutations (YTD, prior year, rolling windows, etc.).
3. **Add context transforms** – Implement functions that accept a `FilterContext` and return a new one (see `demoTransforms` for YTD, last year, and YTD last year examples).
4. **Query the data** – Call `runQuery` with the row dimensions, filters, metric IDs, and the table that determines the row grain. The returned rows are ready for tables, charts, or API responses.

Because the engine is entirely in-memory and powered by fluent `Enumerable` operators, it is straightforward to experiment with additional scenarios such as new time-intelligence patterns, richer label joins, or future SQL push-down prototypes.
