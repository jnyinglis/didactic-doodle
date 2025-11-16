// semanticEngine.ts
// POC semantic metrics engine with factMeasure, expression, derived, contextTransform
//
// This file now relies on the bundled linq.js implementation for fluent row
// operations so callers can chain `Enumerable` helpers end-to-end.

import Enumerable from "./linq";

/**
 * Basic row type for tables.
 */
export type Row = Record<string, any>;

export type RowSequence = Enumerable.IEnumerable<Row>;

export function rowsToEnumerable(rows: Row[] = []): RowSequence {
  return Enumerable.from(rows ?? []);
}

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
 * In-memory DB storing raw table rows.
 */
export interface InMemoryDb {
  tables: Record<string, Row[]>;
}

export interface TableColumn {
  role: "attribute" | "measure";
  dataType?: "string" | "number" | "date";
  defaultAgg?: "sum" | "avg" | "count" | "min" | "max";
  format?: string;
  labelFor?: string;
  labelAlias?: string;
}

export interface TableDefinition {
  name: string;
  grain: string[];
  columns: Record<string, TableColumn>;
  relationships?: Record<string, { references: string; column: string }>;
}

export type TableDefinitionRegistry = Record<string, TableDefinition>;

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
  table: string;
  column: string; // key into TableDefinition.columns
  agg?: "sum" | "avg" | "count" | "min" | "max"; // default from column if omitted
  /**
   * Metric grain: which dimensions from the filter context affect this metric.
   * If omitted, defaults to the table's grain.
   */
  grain?: string[];
}

/**
 * Metric evaluated with a custom expression over the filtered fact rows.
 */
export interface ExpressionMetric extends MetricBase {
  kind: "expression";
  table: string;
  /**
   * Metric grain; controls which filters are respected/ignored.
   * If omitted, defaults to the table's grain.
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
 * Apply a filter context to a table,
 * respecting only the dimensions in `grain`.
 */
export function applyContextToTable(
  rows: Row[],
  context: FilterContext,
  grain: string[]
): RowSequence {
  let sequence = rowsToEnumerable(rows);

  Object.entries(context || {}).forEach(([key, filter]) => {
    if (filter === undefined || filter === null) return;
    if (!grain.includes(key)) return;

    sequence = sequence.where((r: Row) => matchesFilter(r[key], filter));
  });

  return sequence;
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
 * Enrich dimension keys with their label fields defined in table metadata.
 */
interface LabelColumnInfo {
  tableName: string;
  columnName: string;
  matchColumn: string;
  alias?: string;
}

function buildLabelColumnIndex(
  tableDefinitions: TableDefinitionRegistry
): Map<string, LabelColumnInfo[]> {
  const map = new Map<string, LabelColumnInfo[]>();

  for (const [tableName, tableDef] of Object.entries(tableDefinitions)) {
    for (const [columnName, columnDef] of Object.entries(tableDef.columns)) {
      if (!columnDef.labelFor) continue;
      const entry: LabelColumnInfo = {
        tableName,
        columnName,
        matchColumn: columnDef.labelFor,
        alias: columnDef.labelAlias ?? columnName,
      };
      const existing = map.get(columnDef.labelFor) ?? [];
      existing.push(entry);
      map.set(columnDef.labelFor, existing);
    }
  }

  return map;
}

const labelIndexCache = new WeakMap<
  TableDefinitionRegistry,
  Map<string, LabelColumnInfo[]>
>();

function getLabelColumnIndex(
  tableDefinitions: TableDefinitionRegistry
): Map<string, LabelColumnInfo[]> {
  let cached = labelIndexCache.get(tableDefinitions);
  if (!cached) {
    cached = buildLabelColumnIndex(tableDefinitions);
    labelIndexCache.set(tableDefinitions, cached);
  }
  return cached;
}

export function enrichDimensions(
  keyObj: Row,
  db: InMemoryDb,
  tableDefinitions: TableDefinitionRegistry
): Row {
  const result: Row = { ...keyObj };
  const labelIndex = getLabelColumnIndex(tableDefinitions);

  for (const [dimKey, value] of Object.entries(keyObj)) {
    if (value == null) continue;
    const labelColumns = labelIndex.get(dimKey);
    if (!labelColumns) continue;

    for (const columnInfo of labelColumns) {
      const lookupRows = db.tables[columnInfo.tableName];
      if (!lookupRows) continue;
      const match = lookupRows.find(
        (row) => row[columnInfo.matchColumn] === value
      );
      if (match && match[columnInfo.columnName] !== undefined) {
        result[columnInfo.alias ?? columnInfo.columnName] =
          match[columnInfo.columnName];
      }
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
  tableDefinitions: TableDefinitionRegistry,
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
    const tableDef = tableDefinitions[def.table];
    if (!tableDef) throw new Error(`Unknown table: ${def.table}`);

    const rows = db.tables[def.table];
    if (!rows) throw new Error(`Missing rows for table: ${def.table}`);

    const grain = def.grain ?? tableDef.grain;
    const sequence = applyContextToTable(rows, context, grain);

    const columnDef = tableDef.columns[def.column];
    if (!columnDef) {
      throw new Error(`Unknown column '${def.column}' for table '${def.table}'`);
    }
    if (columnDef.role !== "measure") {
      throw new Error(
        `Column '${def.column}' on table '${def.table}' is not a measure`
      );
    }

    const agg = def.agg ?? columnDef.defaultAgg ?? "sum";
    const pickValue = (row: Row): number | null => {
      const raw = Number(row[def.column]);
      return Number.isNaN(raw) ? null : raw;
    };

    switch (agg) {
      case "sum":
        value = sequence.sum((r: Row) => pickValue(r) ?? 0);
        break;
      case "avg": {
        const count = sequence.count();
        const total = sequence.sum((r: Row) => pickValue(r) ?? 0);
        value = count === 0 ? null : total / count;
        break;
      }
      case "count":
        value = sequence.count();
        break;
      case "min": {
        const values = sequence
          .select((r: Row) => pickValue(r))
          .where((num): num is number => typeof num === "number")
          .toArray();
        value = values.length === 0 ? null : Math.min(...values);
        break;
      }
      case "max": {
        const values = sequence
          .select((r: Row) => pickValue(r))
          .where((num): num is number => typeof num === "number")
          .toArray();
        value = values.length === 0 ? null : Math.max(...values);
        break;
      }
      default:
        throw new Error(`Unsupported aggregation: ${agg}`);
    }
  } else if (def.kind === "expression") {
    const tableDef = tableDefinitions[def.table];
    if (!tableDef) throw new Error(`Unknown table: ${def.table}`);

    const rows = db.tables[def.table];
    if (!rows) throw new Error(`Missing rows for table: ${def.table}`);

    const grain = def.grain ?? tableDef.grain;
    const q = applyContextToTable(rows, context, grain);
    value = def.expression(q, db, context);

  } else if (def.kind === "derived") {
    const depValues: Record<string, number | null> = {};
    for (const dep of def.dependencies) {
      depValues[dep] = evaluateMetric(
        dep,
        db,
        tableDefinitions,
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
      tableDefinitions,
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
  tableDefinitions: TableDefinitionRegistry,
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
      tableDefinitions,
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
  rows: string[]; // dimension keys for row axis
  filters?: FilterContext;
  metrics: string[]; // metric IDs
  tableForRows: string; // table used to find distinct row combinations
}

export function runQuery(
  db: InMemoryDb,
  tableDefinitions: TableDefinitionRegistry,
  metricRegistry: MetricRegistry,
  transforms: ContextTransformsRegistry,
  options: RunQueryOptions
): Row[] {
  const { rows: rowDims, filters = {}, metrics, tableForRows } = options;

  const tableDef = tableDefinitions[tableForRows];
  if (!tableDef) throw new Error(`Unknown table: ${tableForRows}`);

  const tableRows = db.tables[tableForRows];
  if (!tableRows) throw new Error(`Missing rows for table: ${tableForRows}`);

  const filtered = applyContextToTable(tableRows, filters, tableDef.grain);

  const groups = filtered
    .groupBy((r: Row) => JSON.stringify(pick(r, rowDims)))
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
        tableDefinitions,
        metricRegistry,
        rowContext,
        transforms,
        cache
      );
      const def = metricRegistry[m];
      metricValues[m] = formatValue(numericValue, def.format);
    }

    const dimPart = enrichDimensions(keyObj, db, tableDefinitions);
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
  tables: {
    products: [
      { productId: 1, productName: "Widget A" },
      { productId: 2, productName: "Widget B" },
    ],
    regions: [
      { regionId: "NA", regionName: "North America" },
      { regionId: "EU", regionName: "Europe" },
    ],
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
export const demoTableDefinitions: TableDefinitionRegistry = {
  sales: {
    name: "sales",
    grain: ["year", "month", "regionId", "productId"],
    columns: {
      year: { role: "attribute", dataType: "number" },
      month: { role: "attribute", dataType: "number" },
      regionId: { role: "attribute", dataType: "string" },
      productId: { role: "attribute", dataType: "number" },
      quantity: {
        role: "measure",
        dataType: "number",
        defaultAgg: "sum",
        format: "integer",
      },
      amount: {
        role: "measure",
        dataType: "number",
        defaultAgg: "sum",
        format: "currency",
      },
    },
    relationships: {
      region: { references: "regions", column: "regionId" },
      product: { references: "products", column: "productId" },
    },
  },
  budget: {
    name: "budget",
    grain: ["year", "regionId"],
    columns: {
      year: { role: "attribute", dataType: "number" },
      regionId: { role: "attribute", dataType: "string" },
      budgetAmount: {
        role: "measure",
        dataType: "number",
        defaultAgg: "sum",
        format: "currency",
      },
    },
    relationships: {
      region: { references: "regions", column: "regionId" },
    },
  },
  regions: {
    name: "regions",
    grain: ["regionId"],
    columns: {
      regionId: { role: "attribute", dataType: "string" },
      regionName: {
        role: "attribute",
        dataType: "string",
        labelFor: "regionId",
      },
    },
  },
  products: {
    name: "products",
    grain: ["productId"],
    columns: {
      productId: { role: "attribute", dataType: "number" },
      productName: {
        role: "attribute",
        dataType: "string",
        labelFor: "productId",
      },
    },
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
    table: "sales",
    column: "amount",
    format: "currency",
    // grain omitted → defaults to tableDefinitions.sales.grain
  };

  demoMetrics.totalSalesQuantity = {
    kind: "factMeasure",
    name: "totalSalesQuantity",
    description: "Sum of sales quantity.",
    table: "sales",
    column: "quantity",
    format: "integer",
  };

  demoMetrics.totalBudget = {
    kind: "factMeasure",
    name: "totalBudget",
    description: "Total budget at (year, region) grain; ignores product/month filters.",
    table: "budget",
    column: "budgetAmount",
    format: "currency",
    // grain omitted → defaults to tableDefinitions.budget.grain
  };

  // Fact measure with coarser metric grain (like MicroStrategy level metric)
  demoMetrics.salesAmountYearRegion = {
    kind: "factMeasure",
    name: "salesAmountYearRegion",
    description: "Sales aggregated at (year, region) level; ignores month and product filters.",
    table: "sales",
    column: "amount",
    format: "currency",
    grain: ["year", "regionId"],
  };

  // Expression metric: price per unit
  demoMetrics.pricePerUnit = {
    kind: "expression",
    name: "pricePerUnit",
    description: "Sales amount / quantity over the current context.",
    table: "sales",
    format: "currency",
    expression: (q: RowSequence) => {
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
 * - define your own db, tableDefinitions, metrics
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
    demoTableDefinitions,
    demoMetrics,
    demoTransforms,
    {
      rows: ["regionId", "productId"],
      filters: { year: 2025, month: 2 },
      metrics: metricBundle,
      tableForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result1);

  console.log("\n=== Demo: 2025-02, Region only ===");
  const result2 = runQuery(
    demoDb,
    demoTableDefinitions,
    demoMetrics,
    demoTransforms,
    {
      rows: ["regionId"],
      filters: { year: 2025, month: 2 },
      metrics: metricBundle,
      tableForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result2);

  console.log("\n=== Demo: 2025-02, Region=NA, by Product ===");
  const result3 = runQuery(
    demoDb,
    demoTableDefinitions,
    demoMetrics,
    demoTransforms,
    {
      rows: ["productId"],
      filters: { year: 2025, month: 2, regionId: "NA" },
      metrics: metricBundle,
      tableForRows: "sales",
    }
  );
  // eslint-disable-next-line no-console
  console.table(result3);
}
