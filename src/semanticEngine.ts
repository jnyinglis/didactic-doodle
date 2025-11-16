// semanticEngine.ts
// POC semantic metrics engine with factMeasure, expression, derived, contextTransform
//
// The original version of this file used the `linq` package for fluent,
// chainable array helpers. Shipping that dependency caused the GH Pages build
// to fail because the bundler could not resolve modules outside the Vite app
// root. To keep the ergonomics without the dependency, we ship a tiny
// RowSequence helper below that implements just the operators we need.

export class RowSequence {
  private readonly rows: Row[];

  private constructor(rows: Row[]) {
    this.rows = rows;
  }

  static from(rows: Row[]): RowSequence {
    return new RowSequence([...rows]);
  }

  where(predicate: (row: Row) => boolean): RowSequence {
    return new RowSequence(this.rows.filter(predicate));
  }

  sum(selector: (row: Row) => number): number {
    return this.rows.reduce((total, row) => {
      const value = Number(selector(row));
      if (Number.isNaN(value)) return total;
      return total + value;
    }, 0);
  }

  average(selector: (row: Row) => number): number | null {
    if (this.rows.length === 0) return null;
    return this.sum(selector) / this.rows.length;
  }

  count(): number {
    return this.rows.length;
  }

  groupBy<TKey extends string, TValue>(
    keySelector: (row: Row) => TKey,
    valueSelector: (row: Row) => TValue
  ) {
    const groups = new Map<TKey, TValue[]>();
    this.rows.forEach((row) => {
      const key = keySelector(row);
      const existing = groups.get(key);
      const value = valueSelector(row);
      if (existing) {
        existing.push(value);
      } else {
        groups.set(key, [value]);
      }
    });

    const groupArray = Array.from(groups.entries()).map(([key, values]) => ({
      key: () => key,
      toArray: () => [...values],
    }));

    return {
      toArray: () => groupArray,
    };
  }

  toArray(): Row[] {
    return [...this.rows];
  }
}

/**
 * Basic row type for facts/dimensions.
 */
export type Row = Record<string, any>;

/**
 * Filter context types:
 * - primitive equality (year = 2025, regionId = 'NA')
 * - range/comparison for numeric-like fields (month <= 6, etc.)
 */
export type FilterPrimitive = string | number | boolean;
export interface FilterRange {
  from?: number;
  to?: number;
  gte?: number;
  lte?: number;
  gt?: number;
  lt?: number;
}
export type FilterValue = FilterPrimitive | FilterRange;
export type FilterContext = Record<string, FilterValue>;

/**
 * In-memory DB: dimensions + facts.
 * This is a POC dataset; in a real app you’d wire your own.
 */
export interface InMemoryDb {
  dimensions: Record<string, Row[]>;
  facts: Record<string, Row[]>;
}

/**
 * Fact-table metadata: describes the grain + numeric fact columns (measures).
 */
export interface FactMeasureDefinition {
  /** Column name in the raw fact rows */
  column: string;
  /** Default aggregation (e.g., "sum", "avg", "count") */
  defaultAgg: "sum" | "avg" | "count";
  /** Optional default format (currency, integer, percent, etc.) */
  format?: string;
  /** Optional description */
  description?: string;
}

export interface FactTableDefinition {
  /** Native grain of the fact table (dimension keys present in rows) */
  grain: string[];
  /** Measure definitions (fact columns) */
  measures: Record<string, FactMeasureDefinition>;
}

export type FactTableRegistry = Record<string, FactTableDefinition>;

/**
 * Dimension config for label enrichment.
 * Maps a dimension key (e.g., "regionId") to its lookup table and label.
 */
export interface DimensionConfigEntry {
  table: string;      // "regions"
  key: string;        // "regionId"
  labelProp: string;  // "name"
  labelAlias: string; // "regionName"
}

export type DimensionConfig = Record<string, DimensionConfigEntry>;

/**
 * Metric definitions
 */

// Common base for all metric definitions
interface MetricBase {
  /** Unique ID / registry key (not required on type, but used by registry) */
  name: string;
  /** Human description */
  description?: string;
  /** Suggested format (currency, integer, percent, etc.) */
  format?: string;
}

/**
 * Metric evaluated directly from a single fact column with a simple aggregation.
 */
export interface FactMeasureMetric extends MetricBase {
  kind: "factMeasure";
  factTable: string;
  factColumn: string; // key into FactTableDefinition.measures
  agg?: "sum" | "avg" | "count"; // default from fact column if omitted
  /**
   * Metric grain: which dimensions from the filter context affect this metric.
   * If omitted, defaults to the fact table's grain.
   */
  grain?: string[];
}

/**
 * Metric evaluated with a custom expression over the filtered fact rows.
 */
export interface ExpressionMetric extends MetricBase {
  kind: "expression";
  factTable: string;
  /**
   * Metric grain; controls which filters are respected/ignored.
   * If omitted, defaults to the fact table's grain.
   */
  grain?: string[];
  /**
   * Custom aggregator: receives a LINQ sequence over filtered fact rows.
   * Returns a numeric value or null.
   */
  expression: (q: RowSequence, db: InMemoryDb, context: FilterContext) => number | null;
}

/**
 * Metric that depends on other metrics.
 * The engine evaluates its dependencies first.
 */
export interface DerivedMetric extends MetricBase {
  kind: "derived";
  dependencies: string[]; // metric IDs
  evalFromDeps: (
    depValues: Record<string, number | null>,
    db: InMemoryDb,
    context: FilterContext
  ) => number | null;
}

/**
 * Metric that wraps another metric and applies a context transform
 * (e.g., YTD, LastYear, YTDLastYear).
 */
export interface ContextTransformMetric extends MetricBase {
  kind: "contextTransform";
  baseMeasure: string;   // metric ID
  transform: string;     // key into ContextTransformsRegistry
}

/**
 * Union of all metric definitions.
 */
export type MetricDefinition =
  | FactMeasureMetric
  | ExpressionMetric
  | DerivedMetric
  | ContextTransformMetric;

/**
 * Metric registry: id -> metric definition.
 */
export type MetricRegistry = Record<string, MetricDefinition>;

/**
 * Context-transform functions (time intelligence, etc.).
 * Input: current filter context
 * Output: transformed filter context
 */
export type ContextTransformFn = (ctx: FilterContext) => FilterContext;
export type ContextTransformsRegistry = Record<string, ContextTransformFn>;

/**
 * Formatting helper: interpret a numeric value using metric format.
 */
export function formatValue(value: number | null | undefined, format?: string): string | null {
  if (value == null || Number.isNaN(value)) return null;
  const n = Number(value);
  switch (format) {
    case "currency":
      return `$${n.toFixed(2)}`;
    case "integer":
      return n.toFixed(0);
    case "percent":
      return `${n.toFixed(2)}%`;
    default:
      return String(n);
  }
}

/**
 * Helpers for filter application to fact rows.
 */

// Match a value to a filter (primitive or range/comparison)
export function matchesFilter(value: any, filter: FilterValue): boolean {
  if (
    filter != null &&
    typeof filter === "object" &&
    !Array.isArray(filter)
  ) {
    const f = filter as FilterRange;

    if ("from" in f || "to" in f) {
      if (f.from != null && value < f.from) return false;
      if (f.to != null && value > f.to) return false;
      return true;
    }

    if (f.gte != null && value < f.gte) return false;
    if (f.lte != null && value > f.lte) return false;
    if (f.gt != null && value <= f.gt) return false;
    if (f.lt != null && value >= f.lt) return false;
    return true;
  }

  // Primitive equality
  return value === filter;
}

/**
 * Apply a filter context to a fact table,
 * respecting only the dimensions in `grain`.
 */
export function applyContextToFact(
  rows: Row[],
  context: FilterContext,
  grain: string[]
): RowSequence {
  let q = RowSequence.from(rows);

  Object.entries(context || {}).forEach(([key, filter]) => {
    if (filter === undefined || filter === null) return;
    if (!grain.includes(key)) return; // ignore filters this metric doesn't care about

    q = q.where((r: Row) => matchesFilter(r[key], filter));
  });

  return q;
}

/**
 * Pick a subset of keys from an object.
 */
export function pick(obj: Row, keys: string[]): Row {
  const out: Row = {};
  keys.forEach((k) => {
    if (obj[k] !== undefined) out[k] = obj[k];
  });
  return out;
}

/**
 * Enrich dimension keys with their label fields, using dimensionConfig.
 */
export function enrichDimensions(
  keyObj: Row,
  db: InMemoryDb,
  dimensionConfig: DimensionConfig
): Row {
  const result: Row = { ...keyObj };
  for (const [dimKey, cfg] of Object.entries(dimensionConfig)) {
    if (keyObj[dimKey] == null) continue;

    const dimTable = db.dimensions[cfg.table];
    if (!dimTable) continue;

    const match = dimTable.find((d) => d[cfg.key] === keyObj[dimKey]);
    if (match) {
      result[cfg.labelAlias] = match[cfg.labelProp];
    }
  }
  return result;
}

/**
 * Metric evaluation engine
 */

function cacheKey(metricName: string, context: FilterContext): string {
  return `${metricName}::${JSON.stringify(context || {})}`;
}

/**
 * Evaluate a single metric with context and cache.
 */
export function evaluateMetric(
  metricName: string,
  db: InMemoryDb,
  factTables: FactTableRegistry,
  metricRegistry: MetricRegistry,
  context: FilterContext,
  transforms: ContextTransformsRegistry,
  cache: Map<string, number | null> = new Map()
): number | null {
  const key = cacheKey(metricName, context);
  if (cache.has(key)) {
    return cache.get(key) ?? null;
  }

  const def = metricRegistry[metricName];
  if (!def) {
    throw new Error(`Unknown metric: ${metricName}`);
  }

  let value: number | null;

  if (def.kind === "factMeasure") {
    const factDef = factTables[def.factTable];
    if (!factDef) throw new Error(`Unknown fact table: ${def.factTable}`);

    const rows = db.facts[def.factTable];
    if (!rows) throw new Error(`Missing rows for fact table: ${def.factTable}`);

    const grain = def.grain ?? factDef.grain;
    const q = applyContextToFact(rows, context, grain);

    const factMeasureDef = factDef.measures[def.factColumn];
    if (!factMeasureDef) {
      throw new Error(`Unknown fact column '${def.factColumn}' for table '${def.factTable}'`);
    }
    const col = factMeasureDef.column;
    const agg = def.agg ?? factMeasureDef.defaultAgg;

    switch (agg) {
      case "sum":
        value = q.sum((r: Row) => Number(r[col] ?? 0));
        break;
      case "avg":
        value = q.average((r: Row) => Number(r[col] ?? 0));
        break;
      case "count":
        value = q.count();
        break;
      default:
        throw new Error(`Unsupported aggregation: ${agg}`);
    }
  } else if (def.kind === "expression") {
    const factDef = factTables[def.factTable];
    if (!factDef) throw new Error(`Unknown fact table: ${def.factTable}`);

    const rows = db.facts[def.factTable];
    if (!rows) throw new Error(`Missing rows for fact table: ${def.factTable}`);

    const grain = def.grain ?? factDef.grain;
    const q = applyContextToFact(rows, context, grain);
    value = def.expression(q, db, context);

  } else if (def.kind === "derived") {
    const depValues: Record<string, number | null> = {};
    for (const dep of def.dependencies) {
      depValues[dep] = evaluateMetric(
        dep,
        db,
        factTables,
        metricRegistry,
        context,
        transforms,
        cache
      );
    }
    value = def.evalFromDeps(depValues, db, context);

  } else if (def.kind === "contextTransform") {
    const transformFn = transforms[def.transform];
    if (!transformFn) {
      throw new Error(`Unknown context transform: ${def.transform}`);
    }
    const transformedContext = transformFn(context || {});
    value = evaluateMetric(
      def.baseMeasure,
      db,
      factTables,
      metricRegistry,
      transformedContext,
      transforms,
      cache
    );
  } else {
    const exhaustiveCheck: never = def;
    throw new Error(`Unknown metric kind: ${(exhaustiveCheck as any).kind}`);
  }

  cache.set(key, value);
  return value;
}

/**
 * Evaluate multiple metrics together, sharing a cache.
 */
export function evaluateMetrics(
  metricNames: string[],
  db: InMemoryDb,
  factTables: FactTableRegistry,
  metricRegistry: MetricRegistry,
  context: FilterContext,
  transforms: ContextTransformsRegistry
): Record<string, number | null> {
  const cache = new Map<string, number | null>();
  const results: Record<string, number | null> = {};
  for (const m of metricNames) {
    results[m] = evaluateMetric(
      m,
      db,
      factTables,
      metricRegistry,
      context,
      transforms,
      cache
    );
  }
  return results;
}

/**
 * Build a dimensioned result set: rows by dimension keys + metrics.
 */
export interface RunQueryOptions {
  rows: string[];         // dimension keys for row axis, e.g. ["regionId", "productId"]
  filters?: FilterContext;
  metrics: string[];      // metric IDs
  factForRows: string;    // fact table used to find distinct row combinations
}

export function runQuery(
  db: InMemoryDb,
  factTables: FactTableRegistry,
  metricRegistry: MetricRegistry,
  transforms: ContextTransformsRegistry,
  dimensionConfig: DimensionConfig,
  options: RunQueryOptions
): Row[] {
  const { rows: rowDims, filters = {}, metrics, factForRows } = options;

  const factRows = db.facts[factForRows];
  if (!factRows) throw new Error(`Unknown fact table: ${factForRows}`);

  const factGrain = Object.keys(factRows[0] || {});
  const filtered = applyContextToFact(factRows, filters, factGrain);

  const groups = filtered
    .groupBy(
      (r: Row) => JSON.stringify(pick(r, rowDims)),
      (r: Row) => r
    )
    .toArray();

  const cache = new Map<string, number | null>();
  const result: Row[] = [];

  for (const g of groups) {
    const keyObj: Row = JSON.parse(g.key());
    const rowContext: FilterContext = {
      ...filters,
      ...keyObj,
    };

    const metricValues: Row = {};
    for (const m of metrics) {
      const numericValue = evaluateMetric(
        m,
        db,
        factTables,
        metricRegistry,
        rowContext,
        transforms,
        cache
      );
      const def = metricRegistry[m];
      metricValues[m] = formatValue(numericValue, def.format);
    }

    const dimPart = enrichDimensions(keyObj, db, dimensionConfig);
    result.push({
      ...dimPart,
      ...metricValues,
    });
  }

  return result;
}

/* --------------------------------------------------------------------------
 * BELOW: POC DATA + METRIC REGISTRY + DEMO USAGE
 * You can move this into a separate file in a real project.
 * -------------------------------------------------------------------------- */

/**
 * Example in-memory DB for the POC.
 */
export const demoDb: InMemoryDb = {
  dimensions: {
    products: [
      { productId: 1, name: "Widget A" },
      { productId: 2, name: "Widget B" },
    ],
    regions: [
      { regionId: "NA", name: "North America" },
      { regionId: "EU", name: "Europe" },
    ],
  },
  facts: {
    sales: [
      // 2024
      { year: 2024, month: 1, regionId: "NA", productId: 1, quantity: 7, amount: 700 },
      { year: 2024, month: 1, regionId: "NA", productId: 2, quantity: 4, amount: 480 },
      { year: 2024, month: 2, regionId: "NA", productId: 1, quantity: 5, amount: 650 },
      { year: 2024, month: 2, regionId: "EU", productId: 1, quantity: 3, amount: 420 },

      // 2025
      { year: 2025, month: 1, regionId: "NA", productId: 1, quantity: 10, amount: 1000 },
      { year: 2025, month: 1, regionId: "NA", productId: 2, quantity: 5, amount: 600 },
      { year: 2025, month: 1, regionId: "EU", productId: 1, quantity: 4, amount: 500 },
      { year: 2025, month: 2, regionId: "NA", productId: 1, quantity: 8, amount: 950 },
      { year: 2025, month: 2, regionId: "EU", productId: 2, quantity: 3, amount: 450 },
    ],
    budget: [
      { year: 2024, regionId: "NA", budgetAmount: 1500 },
      { year: 2024, regionId: "EU", budgetAmount: 1000 },
      { year: 2025, regionId: "NA", budgetAmount: 2200 },
      { year: 2025, regionId: "EU", budgetAmount: 1600 },
    ],
  },
};

/**
 * Example fact-table metadata.
 */
export const demoFactTables: FactTableRegistry = {
  sales: {
    grain: ["year", "month", "regionId", "productId"],
    measures: {
      amount: {
        column: "amount",
        defaultAgg: "sum",
        format: "currency",
      },
      quantity: {
        column: "quantity",
        defaultAgg: "sum",
        format: "integer",
      },
    },
  },
  budget: {
    grain: ["year", "regionId"],
    measures: {
      budgetAmount: {
        column: "budgetAmount",
        defaultAgg: "sum",
        format: "currency",
      },
    },
  },
};

/**
 * Example dimension config for label enrichment.
 */
export const demoDimensionConfig: DimensionConfig = {
  regionId: {
    table: "regions",
    key: "regionId",
    labelProp: "name",
    labelAlias: "regionName",
  },
  productId: {
    table: "products",
    key: "productId",
    labelProp: "name",
    labelAlias: "productName",
  },
};

/**
 * Example context transforms (time intelligence).
 */
export const demoTransforms: ContextTransformsRegistry = {
  ytd(ctx) {
    if (ctx.year == null || ctx.month == null) return ctx;
    return { ...ctx, month: { lte: Number(ctx.month) } };
  },
  lastYear(ctx) {
    if (ctx.year == null) return ctx;
    return { ...ctx, year: Number(ctx.year) - 1 };
  },
  ytdLastYear(ctx) {
    if (ctx.year == null || ctx.month == null) return ctx;
    return {
      ...ctx,
      year: Number(ctx.year) - 1,
      month: { lte: Number(ctx.month) },
    };
  },
};

/**
 * Example metric registry implementing factMeasure, expression, derived, contextTransform.
 */
export const demoMetrics: MetricRegistry = {};

/**
 * Helper to register a context-transform metric into a registry.
 */
export function addContextTransformMetric(
  registry: MetricRegistry,
  def: Omit<ContextTransformMetric, "kind">
): void {
  registry[def.name] = {
    kind: "contextTransform",
    ...def,
  };
}

/**
 * Build demo metrics (you can mirror this pattern in your own project).
 */
function buildDemoMetrics() {
  // Simple fact measures
  demoMetrics.totalSalesAmount = {
    kind: "factMeasure",
    name: "totalSalesAmount",
    description: "Sum of sales amount over the current context.",
    factTable: "sales",
    factColumn: "amount",
    format: "currency",
    // grain omitted → defaults to factTables.sales.grain
  };

  demoMetrics.totalSalesQuantity = {
    kind: "factMeasure",
    name: "totalSalesQuantity",
    description: "Sum of sales quantity.",
    factTable: "sales",
    factColumn: "quantity",
    format: "integer",
  };

  demoMetrics.totalBudget = {
    kind: "factMeasure",
    name: "totalBudget",
    description: "Total budget at (year, region) grain; ignores product/month filters.",
    factTable: "budget",
    factColumn: "budgetAmount",
    format: "currency",
    // grain omitted → defaults to factTables.budget.grain
  };

  // Fact measure with coarser metric grain (like MicroStrategy level metric)
  demoMetrics.salesAmountYearRegion = {
    kind: "factMeasure",
    name: "salesAmountYearRegion",
    description: "Sales aggregated at (year, region) level; ignores month and product filters.",
    factTable: "sales",
    factColumn: "amount",
    format: "currency",
    grain: ["year", "regionId"],
  };

  // Expression metric: price per unit
  demoMetrics.pricePerUnit = {
    kind: "expression",
    name: "pricePerUnit",
    description: "Sales amount / quantity over the current context.",
    factTable: "sales",
    format: "currency",
    expression: (q: any) => {
      const amount = q.sum((r: Row) => Number(r.amount ?? 0));
      const qty = q.sum((r: Row) => Number(r.quantity ?? 0));
      return qty ? amount / qty : null;
    },
  };

  // Derived metric: Sales vs Budget %
  demoMetrics.salesVsBudgetPct = {
    kind: "derived",
    name: "salesVsBudgetPct",
    description: "Total sales / total budget.",
    dependencies: ["totalSalesAmount", "totalBudget"],
    format: "percent",
    evalFromDeps: ({ totalSalesAmount, totalBudget }) => {
      const s = totalSalesAmount ?? 0;
      const b = totalBudget ?? 0;
      if (!b) return null;
      return (s / b) * 100;
    },
  };

  // Time-int metrics (context-transform)
  addContextTransformMetric(demoMetrics, {
    name: "salesAmountYTD",
    baseMeasure: "totalSalesAmount",
    transform: "ytd",
    description: "YTD of total sales amount.",
    format: "currency",
  });

  addContextTransformMetric(demoMetrics, {
    name: "salesAmountLastYear",
    baseMeasure: "totalSalesAmount",
    transform: "lastYear",
    description: "Total sales amount for previous year.",
    format: "currency",
  });

  addContextTransformMetric(demoMetrics, {
    name: "salesAmountYTDLastYear",
    baseMeasure: "totalSalesAmount",
    transform: "ytdLastYear",
    description: "YTD of total sales amount in previous year.",
    format: "currency",
  });

  addContextTransformMetric(demoMetrics, {
    name: "budgetYTD",
    baseMeasure: "totalBudget",
    transform: "ytd",
    description: "YTD of total budget (may match full year if budget is annual).",
    format: "currency",
  });

  addContextTransformMetric(demoMetrics, {
    name: "budgetLastYear",
    baseMeasure: "totalBudget",
    transform: "lastYear",
    description: "Total budget in previous year.",
    format: "currency",
  });

  // Derived YTD comparison metric
  demoMetrics.salesVsBudgetPctYTD = {
    kind: "derived",
    name: "salesVsBudgetPctYTD",
    description: "YTD sales / YTD budget.",
    dependencies: ["salesAmountYTD", "budgetYTD"],
    format: "percent",
    evalFromDeps: ({ salesAmountYTD, budgetYTD }) => {
      const s = salesAmountYTD ?? 0;
      const b = budgetYTD ?? 0;
      if (!b) return null;
      return (s / b) * 100;
    },
  };
}

// Build demo metrics immediately
buildDemoMetrics();

/**
 * DEMO USAGE
 *
 * This is just to illustrate. In a real application you'd likely:
 * - import the library parts
 * - define your own db, factTables, dimensionConfig, metrics
 * - call runQuery() from your UI / API layer
 */

if (typeof require !== "undefined" && typeof module !== "undefined" && require.main === module) {
  const metricBundle = [
    "totalSalesAmount",
    "totalSalesQuantity",
    "totalBudget",
    "salesAmountYearRegion",
    "pricePerUnit",
    "salesVsBudgetPct",
    "salesAmountYTD",
    "salesAmountLastYear",
    "salesAmountYTDLastYear",
    "budgetYTD",
    "budgetLastYear",
    "salesVsBudgetPctYTD",
  ];

  console.log("\n=== Demo: 2025-02, Region x Product ===");
  const result1 = runQuery(
    demoDb,
    demoFactTables,
    demoMetrics,
    demoTransforms,
    demoDimensionConfig,
    {
      rows: ["regionId", "productId"],
      filters: { year: 2025, month: 2 },
      metrics: metricBundle,
      factForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result1);

  console.log("\n=== Demo: 2025-02, Region only ===");
  const result2 = runQuery(
    demoDb,
    demoFactTables,
    demoMetrics,
    demoTransforms,
    demoDimensionConfig,
    {
      rows: ["regionId"],
      filters: { year: 2025, month: 2 },
      metrics: metricBundle,
      factForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result2);

  console.log("\n=== Demo: 2025-02, Region=NA, by Product ===");
  const result3 = runQuery(
    demoDb,
    demoFactTables,
    demoMetrics,
    demoTransforms,
    demoDimensionConfig,
    {
      rows: ["productId"],
      filters: { year: 2025, month: 2, regionId: "NA" },
      metrics: metricBundle,
      factForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result3);
}
