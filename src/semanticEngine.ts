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

export function rowsToEnumerable(rows: Row[] = []) {
  return Enumerable.from(rows ?? []);
}

export type RowSequence = ReturnType<typeof rowsToEnumerable>;

/**
 * Filter context types:
 * - primitive equality (year = 2025, regionId = 'NA')
 * - range/comparison for numeric-like fields (month <= 6, etc.)
 */
export type FilterPrimitive = string | number | boolean | Date;
export interface FilterRange {
  from?: number;
  to?: number;
  gte?: number;
  lte?: number;
  gt?: number;
  lt?: number;
}
export type FilterValue = FilterPrimitive | FilterRange | FilterPrimitive[];

export type ScalarOp = "eq" | "lt" | "lte" | "gt" | "gte" | "between" | "in";

export interface FilterExpression {
  kind: "expression";
  field: string;
  op: ScalarOp;
  value: any;
  value2?: any;
}

export interface FilterConjunction {
  kind: "and" | "or";
  filters: FilterNode[];
}

export type FilterNode = FilterExpression | FilterConjunction;

export type FilterObject = Record<string, FilterValue>;
export type FilterContext = FilterNode | FilterObject | undefined;

export const f = {
  eq(field: string, value: FilterPrimitive): FilterExpression {
    return { kind: "expression", field, op: "eq", value };
  },
  lt(field: string, value: number): FilterExpression {
    return { kind: "expression", field, op: "lt", value };
  },
  lte(field: string, value: number): FilterExpression {
    return { kind: "expression", field, op: "lte", value };
  },
  gt(field: string, value: number): FilterExpression {
    return { kind: "expression", field, op: "gt", value };
  },
  gte(field: string, value: number): FilterExpression {
    return { kind: "expression", field, op: "gte", value };
  },
  between(field: string, from: number, to: number): FilterExpression {
    return { kind: "expression", field, op: "between", value: from, value2: to };
  },
  in(field: string, values: FilterPrimitive[]): FilterExpression {
    return { kind: "expression", field, op: "in", value: values };
  },
  and(...filters: FilterNode[]): FilterConjunction {
    return { kind: "and", filters };
  },
  or(...filters: FilterNode[]): FilterConjunction {
    return { kind: "or", filters };
  },
};

function isFilterNode(node: FilterContext): node is FilterNode {
  if (!node) return false;
  return (node as FilterExpression | FilterConjunction).kind !== undefined;
}

function normalizeFilterContext(ctx: FilterContext): FilterNode | undefined {
  if (!ctx) return undefined;
  if (isFilterNode(ctx)) return ctx;
  const entries = Object.entries(ctx ?? {});
  if (!entries.length) return undefined;
  const filters = entries
    .map(([field, value]) => convertFilterValueToExpression(field, value))
    .filter((expr): expr is FilterExpression => Boolean(expr));
  if (!filters.length) return undefined;
  if (filters.length === 1) return filters[0];
  return f.and(...filters);
}

function convertFilterValueToExpression(
  field: string,
  value: FilterValue
): FilterExpression | undefined {
  if (value == null) return undefined;
  if (Array.isArray(value)) {
    return f.in(field, value);
  }
  if (typeof value === "object") {
    const range = value as FilterRange;
    if (range.from != null && range.to != null) {
      return f.between(field, Number(range.from), Number(range.to));
    }
    if (range.gte != null) {
      return f.gte(field, Number(range.gte));
    }
    if (range.lte != null) {
      return f.lte(field, Number(range.lte));
    }
    if (range.gt != null) {
      return f.gt(field, Number(range.gt));
    }
    if (range.lt != null) {
      return f.lt(field, Number(range.lt));
    }
    if (range.from != null) {
      return f.gte(field, Number(range.from));
    }
    if (range.to != null) {
      return f.lte(field, Number(range.to));
    }
    return undefined;
  }
  return f.eq(field, value);
}

export function mergeFilters(
  ...contexts: (FilterContext | undefined)[]
): FilterContext | undefined {
  const nodes = contexts
    .map((ctx) => normalizeFilterContext(ctx))
    .filter((node): node is FilterNode => Boolean(node));
  if (!nodes.length) return undefined;
  if (nodes.length === 1) return nodes[0];
  const merged: FilterConjunction = { kind: "and", filters: [] };
  nodes.forEach((node) => {
    if (node.kind === "and") {
      merged.filters.push(...node.filters);
    } else {
      merged.filters.push(node);
    }
  });
  return merged;
}

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

export interface AttributeDefinition {
  name: string;
  table: string;
  column?: string;
  description?: string;
}

export type AttributeRegistry = Record<string, AttributeDefinition>;

export interface MeasureDefinition {
  name: string;
  table: string;
  column?: string;
  aggregation?: "sum" | "avg" | "count" | "min" | "max";
  format?: string;
  description?: string;
  grain?: string[];
}

export type MeasureRegistry = Record<string, MeasureDefinition>;

export const attr = {
  id(opts: AttributeDefinition): AttributeDefinition {
    return {
      ...opts,
      column: opts.column ?? opts.name,
    };
  },
};

export const measure = {
  sum(opts: MeasureDefinition): MeasureDefinition {
    return {
      ...opts,
      column: opts.column ?? opts.name,
      aggregation: opts.aggregation ?? "sum",
    };
  },
  count(opts: MeasureDefinition): MeasureDefinition {
    return {
      ...opts,
      aggregation: "count",
    };
  },
};

export interface SemanticModel {
  tables: TableDefinitionRegistry;
  attributes: AttributeRegistry;
  measures: MeasureRegistry;
  metrics: MetricRegistry;
  transforms: ContextTransformsRegistry;
}

/**
 * Metric definitions
 */

interface MetricBase {
  name: string;
  description?: string;
  format?: string;
  grain?: string[];
}

export interface MetricContext {
  filter?: FilterContext;
  grain?: string[];
}

export type MetricEval = (
  ctx: MetricContext,
  runtime: MetricRuntime
) => number | null;

export interface MetricDefinition extends MetricBase {
  eval: MetricEval;
  deps?: string[];
}

export type MetricRegistry = Record<string, MetricDefinition>;

export type ContextTransform = (ctx: MetricContext) => MetricContext;
export type ContextTransformsRegistry = Record<string, ContextTransform>;

export function composeTransforms(
  ...transforms: ContextTransform[]
): ContextTransform {
  return (ctx: MetricContext) =>
    transforms.reduce((acc, transform) => transform(acc), ctx);
}

export function simpleMetric(opts: {
  name: string;
  measure: string;
  description?: string;
  format?: string;
  grain?: string[];
}): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    grain: opts.grain,
    eval: (ctx, runtime) => runtime.evaluateMeasure(opts.measure, ctx),
  };
}

export function derivedMetric(opts: {
  name: string;
  deps: string[];
  combine: (values: Record<string, number | null>) => number | null;
  description?: string;
  format?: string;
  grain?: string[];
}): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    grain: opts.grain,
    deps: opts.deps,
    eval: (ctx, runtime) => {
      const values: Record<string, number | null> = {};
      opts.deps.forEach((dep) => {
        values[dep] = runtime.evaluate(dep, ctx);
      });
      return opts.combine(values);
    },
  };
}

export function contextTransformMetric(opts: {
  name: string;
  baseMetric: string;
  transform: ContextTransform;
  description?: string;
  format?: string;
}): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    deps: [opts.baseMetric],
    eval: (ctx, runtime) => runtime.evaluate(opts.baseMetric, opts.transform(ctx)),
  };
}

interface MetricEvaluationEnvironment {
  db: InMemoryDb;
  model: SemanticModel;
}

export interface MetricRuntime {
  evaluate(metricName: string, ctx: MetricContext): number | null;
  evaluateMeasure(measureName: string, ctx: MetricContext): number | null;
  env: MetricEvaluationEnvironment;
}

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
function matchesExpression(value: any, expr: FilterExpression): boolean {
  switch (expr.op) {
    case "eq":
      return value === expr.value;
    case "lt":
      return value < expr.value;
    case "lte":
      return value <= expr.value;
    case "gt":
      return value > expr.value;
    case "gte":
      return value >= expr.value;
    case "between":
      return value >= expr.value && value <= expr.value2;
    case "in":
      if (!Array.isArray(expr.value)) return false;
      return expr.value.includes(value);
    default:
      return false;
  }
}

function pruneFilterNode(
  node: FilterNode,
  allowed: Set<string>
): FilterNode | undefined {
  if (node.kind === "expression") {
    return allowed.has(node.field) ? node : undefined;
  }
  const children = node.filters
    .map((child) => pruneFilterNode(child, allowed))
    .filter((child): child is FilterNode => Boolean(child));
  if (!children.length) return undefined;
  if (children.length === 1) return children[0];
  return { ...node, filters: children };
}

function evaluateFilterNode(node: FilterNode, row: Row): boolean {
  if (node.kind === "expression") {
    return matchesExpression(row[node.field], node);
  }
  if (node.kind === "and") {
    return node.filters.every((child) => evaluateFilterNode(child, row));
  }
  return node.filters.some((child) => evaluateFilterNode(child, row));
}

function omitFieldsFromNode(
  node: FilterNode,
  fields: Set<string>
): FilterNode | undefined {
  if (node.kind === "expression") {
    return fields.has(node.field) ? undefined : node;
  }
  const children = node.filters
    .map((child) => omitFieldsFromNode(child, fields))
    .filter((child): child is FilterNode => Boolean(child));
  if (!children.length) return undefined;
  if (children.length === 1) return children[0];
  return { ...node, filters: children };
}

export function omitFilterFields(
  context: FilterContext | undefined,
  ...fields: string[]
): FilterContext | undefined {
  const node = normalizeFilterContext(context);
  if (!node) return undefined;
  const result = omitFieldsFromNode(node, new Set(fields));
  return result;
}

function expressionToFilterValue(
  expr: FilterExpression
): FilterValue | undefined {
  switch (expr.op) {
    case "eq":
      return expr.value;
    case "between":
      return { from: expr.value, to: expr.value2 };
    case "lt":
      return { lt: expr.value };
    case "lte":
      return { lte: expr.value };
    case "gt":
      return { gt: expr.value };
    case "gte":
      return { gte: expr.value };
    case "in":
      return Array.isArray(expr.value) ? expr.value : [expr.value];
    default:
      return undefined;
  }
}

function findFilterValueInNode(
  node: FilterNode,
  field: string
): FilterValue | undefined {
  if (node.kind === "expression") {
    if (node.field === field) {
      return expressionToFilterValue(node);
    }
    return undefined;
  }
  for (const child of node.filters) {
    const found = findFilterValueInNode(child, field);
    if (found !== undefined) return found;
  }
  return undefined;
}

export function getFilterValue(
  context: FilterContext | MetricContext | undefined,
  field: string
): FilterValue | undefined {
  if (!context) return undefined;
  const filterContext = (context as MetricContext).filter ?? context;
  const node = normalizeFilterContext(filterContext as FilterContext);
  if (!node) return undefined;
  return findFilterValueInNode(node, field);
}

function asNumber(value: FilterValue | undefined): number | undefined {
  if (value == null) return undefined;
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isNaN(parsed) ? undefined : parsed;
  }
  return undefined;
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
  const node = normalizeFilterContext(context);
  if (!node) return rowsToEnumerable(rows);
  const allowed = new Set(grain);
  const pruned = pruneFilterNode(node, allowed);
  if (!pruned) return rowsToEnumerable(rows);
  return rowsToEnumerable(rows).where((r: Row) => evaluateFilterNode(pruned, r));
}

function serializeFilter(filter?: FilterContext): any {
  if (!filter) return null;
  const node = normalizeFilterContext(filter);
  if (!node) return null;
  return node;
}

function mergeMetricContexts(
  base: MetricContext,
  override?: MetricContext
): MetricContext {
  if (!override) return base;
  return {
    filter: override.filter ?? base.filter,
    grain: override.grain ?? base.grain,
  };
}

function evaluateMeasureDefinition(
  measureName: string,
  ctx: MetricContext,
  env: MetricEvaluationEnvironment
): number | null {
  const def = env.model.measures[measureName];
  if (!def) {
    throw new Error(`Unknown measure: ${measureName}`);
  }
  const tableDef = env.model.tables[def.table];
  if (!tableDef) {
    throw new Error(`Unknown table: ${def.table}`);
  }
  const rows = env.db.tables[def.table];
  if (!rows) {
    throw new Error(`Missing rows for table: ${def.table}`);
  }

  const grain = ctx.grain ?? def.grain ?? tableDef.grain;
  const filtered = applyContextToTable(rows, ctx.filter, grain);

  const aggregation = def.aggregation ?? "sum";
  const column = def.column ?? def.name;
  const pickValue = (row: Row): number | null => {
    const raw = Number(row[column]);
    return Number.isNaN(raw) ? null : raw;
  };

  switch (aggregation) {
    case "sum":
      return filtered.sum((r: Row) => pickValue(r) ?? 0);
    case "avg": {
      const count = filtered.count();
      const total = filtered.sum((r: Row) => pickValue(r) ?? 0);
      return count === 0 ? null : total / count;
    }
    case "count":
      return filtered.count();
    case "min": {
      const values = filtered
        .select((r: Row) => pickValue(r))
        .where((num: number | null): num is number => typeof num === "number")
        .toArray();
      return values.length === 0 ? null : Math.min(...values);
    }
    case "max": {
      const values = filtered
        .select((r: Row) => pickValue(r))
        .where((num: number | null): num is number => typeof num === "number")
        .toArray();
      return values.length === 0 ? null : Math.max(...values);
    }
    default:
      throw new Error(`Unsupported aggregation: ${aggregation}`);
  }
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

function metricCacheKey(metricName: string, context: MetricContext): string {
  return `${metricName}::${JSON.stringify({
    filter: serializeFilter(context.filter),
    grain: context.grain ?? null,
  })}`;
}

/**
 * Evaluate a single metric with context and cache.
 */
export function evaluateMetric(
  metricName: string,
  env: MetricEvaluationEnvironment,
  context: MetricContext = {},
  cache: Map<string, number | null> = new Map()
): number | null {
  const metric = env.model.metrics[metricName];
  if (!metric) {
    throw new Error(`Unknown metric: ${metricName}`);
  }
  const effectiveContext: MetricContext = {
    filter: context.filter,
    grain: context.grain ?? metric.grain,
  };
  const key = metricCacheKey(metricName, effectiveContext);
  if (cache.has(key)) {
    return cache.get(key) ?? null;
  }

  const runtime: MetricRuntime = {
    env,
    evaluate: (name: string, ctx: MetricContext = {}) =>
      evaluateMetric(
        name,
        env,
        mergeMetricContexts(effectiveContext, ctx),
        cache
      ),
    evaluateMeasure: (measureName: string, ctx: MetricContext = {}) =>
      evaluateMeasureDefinition(
        measureName,
        mergeMetricContexts(effectiveContext, ctx),
        env
      ),
  };

  const value = metric.eval(effectiveContext, runtime);
  cache.set(key, value);
  return value;
}

/**
 * Evaluate multiple metrics together, sharing a cache.
 */
export function evaluateMetrics(
  metricNames: string[],
  env: MetricEvaluationEnvironment,
  context: MetricContext = {}
): Record<string, number | null> {
  const cache = new Map<string, number | null>();
  const results: Record<string, number | null> = {};
  for (const m of metricNames) {
    results[m] = evaluateMetric(m, env, context, cache);
  }
  return results;
}

export interface QuerySpec {
  table: string;
  attributes: string[];
  metrics: string[];
  filters?: FilterContext;
}

export interface QueryBuilder {
  addAttributes(...attrs: string[]): QueryBuilder;
  addMetrics(...metricNames: string[]): QueryBuilder;
  where(filter: FilterContext): QueryBuilder;
  build(): QuerySpec;
  run(): Row[];
}

export interface Engine {
  query(table: string): QueryBuilder;
  run(spec: QuerySpec): Row[];
  evaluateMetric(name: string, ctx?: MetricContext): number | null;
  listMetrics(): MetricDefinition[];
  getMetric(name: string): MetricDefinition | undefined;
  readonly model: SemanticModel;
}

class EngineQueryBuilder implements QueryBuilder {
  constructor(private engine: EngineImpl, private spec: QuerySpec) {}

  private withSpec(partial: Partial<QuerySpec>): EngineQueryBuilder {
    return new EngineQueryBuilder(this.engine, {
      ...this.spec,
      ...partial,
    });
  }

  addAttributes(...attrs: string[]): QueryBuilder {
    const merged = Array.from(new Set([...this.spec.attributes, ...attrs]));
    return this.withSpec({ attributes: merged });
  }

  addMetrics(...metricNames: string[]): QueryBuilder {
    const merged = Array.from(new Set([...this.spec.metrics, ...metricNames]));
    return this.withSpec({ metrics: merged });
  }

  where(filter: FilterContext): QueryBuilder {
    const mergedFilter = mergeFilters(this.spec.filters, filter);
    return this.withSpec({ filters: mergedFilter });
  }

  build(): QuerySpec {
    return { ...this.spec };
  }

  run(): Row[] {
    return this.engine.run(this.build());
  }
}

class EngineImpl implements Engine {
  public readonly model: SemanticModel;
  private env: MetricEvaluationEnvironment;

  constructor(db: InMemoryDb, model: SemanticModel) {
    this.model = model;
    this.env = { db, model };
  }

  query(table: string): QueryBuilder {
    return new EngineQueryBuilder(this, {
      table,
      attributes: [],
      metrics: [],
    });
  }

  run(spec: QuerySpec): Row[] {
    return executeQuery(this.env, spec);
  }

  evaluateMetric(name: string, ctx: MetricContext = {}): number | null {
    return evaluateMetric(name, this.env, ctx);
  }

  listMetrics(): MetricDefinition[] {
    return Object.values(this.model.metrics);
  }

  getMetric(name: string): MetricDefinition | undefined {
    return this.model.metrics[name];
  }
}

export function createEngine(db: InMemoryDb, model: SemanticModel): Engine {
  return new EngineImpl(db, model);
}

function executeQuery(
  env: MetricEvaluationEnvironment,
  spec: QuerySpec
): Row[] {
  const tableDef = env.model.tables[spec.table];
  if (!tableDef) throw new Error(`Unknown table: ${spec.table}`);

  const tableRows = env.db.tables[spec.table];
  if (!tableRows) throw new Error(`Missing rows for table: ${spec.table}`);

  const filtered = applyContextToTable(tableRows, spec.filters, tableDef.grain);
  const groupKeyFn = spec.attributes.length
    ? (r: Row) => JSON.stringify(pick(r, spec.attributes))
    : () => "{}";
  const groups = filtered.groupBy(groupKeyFn).toArray();
  const cache = new Map<string, number | null>();
  const result: Row[] = [];

  for (const g of groups) {
    const keyObj: Row = JSON.parse(g.key());
    const dimensionFilters = spec.attributes
      .map((attr) =>
        keyObj[attr] === undefined ? undefined : f.eq(attr, keyObj[attr])
      )
      .filter((node): node is FilterExpression => Boolean(node));
    const dimFilterNode: FilterContext = dimensionFilters.length
      ? dimensionFilters.length === 1
        ? dimensionFilters[0]
        : f.and(...dimensionFilters)
      : undefined;
    const rowFilter = mergeFilters(spec.filters, dimFilterNode);
    const metricValues: Row = {};

    for (const metricName of spec.metrics) {
      const numericValue = evaluateMetric(
        metricName,
        env,
        { filter: rowFilter },
        cache
      );
      const def = env.model.metrics[metricName];
      metricValues[metricName] = formatValue(numericValue, def?.format);
    }

    const dimPart = enrichDimensions(keyObj, env.db, env.model.tables);
    result.push({
      ...dimPart,
      ...metricValues,
    });
  }

  return result;
}

export function runQuery(env: MetricEvaluationEnvironment, spec: QuerySpec): Row[] {
  return executeQuery(env, spec);
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

export const demoAttributes: AttributeRegistry = {
  year: attr.id({ name: "year", table: "sales", description: "Calendar year" }),
  month: attr.id({ name: "month", table: "sales", description: "Calendar month" }),
  regionId: attr.id({ name: "regionId", table: "sales", description: "Region identifier" }),
  productId: attr.id({ name: "productId", table: "sales", description: "Product identifier" }),
};

export const demoMeasures: MeasureRegistry = {
  salesAmount: measure.sum({
    name: "salesAmount",
    table: "sales",
    column: "amount",
    format: "currency",
  }),
  salesQuantity: measure.sum({
    name: "salesQuantity",
    table: "sales",
    column: "quantity",
    format: "integer",
  }),
  budgetAmount: measure.sum({
    name: "budgetAmount",
    table: "budget",
    column: "budgetAmount",
    format: "currency",
  }),
};

/**
 * Example context transforms (time intelligence).
 */
const ytdTransform: ContextTransform = (ctx) => {
  const year = asNumber(getFilterValue(ctx, "year"));
  const month = asNumber(getFilterValue(ctx, "month"));
  if (year == null || month == null) return ctx;
  return {
    ...ctx,
    filter: mergeFilters(
      omitFilterFields(ctx.filter, "year", "month"),
      f.eq("year", year),
      f.lte("month", month)
    ),
  };
};

const lastYearTransform: ContextTransform = (ctx) => {
  const year = asNumber(getFilterValue(ctx, "year"));
  if (year == null) return ctx;
  return {
    ...ctx,
    filter: mergeFilters(
      omitFilterFields(ctx.filter, "year"),
      f.eq("year", year - 1)
    ),
  };
};

export const demoTransforms: ContextTransformsRegistry = {
  ytd: ytdTransform,
  lastYear: lastYearTransform,
  ytdLastYear: composeTransforms(ytdTransform, lastYearTransform),
};

export const demoMetrics: MetricRegistry = {
  totalSalesAmount: simpleMetric({
    name: "totalSalesAmount",
    measure: "salesAmount",
    description: "Sum of sales amount over the current context.",
    format: "currency",
  }),
  totalSalesQuantity: simpleMetric({
    name: "totalSalesQuantity",
    measure: "salesQuantity",
    description: "Sum of sales quantity.",
    format: "integer",
  }),
  totalBudget: simpleMetric({
    name: "totalBudget",
    measure: "budgetAmount",
    description: "Total budget at (year, region) grain.",
    format: "currency",
  }),
  salesAmountYearRegion: simpleMetric({
    name: "salesAmountYearRegion",
    measure: "salesAmount",
    description: "Sales aggregated at (year, region) level; ignores month and product filters.",
    format: "currency",
    grain: ["year", "regionId"],
  }),
  pricePerUnit: derivedMetric({
    name: "pricePerUnit",
    deps: ["totalSalesAmount", "totalSalesQuantity"],
    description: "Sales amount / quantity over the current context.",
    format: "currency",
    combine: ({ totalSalesAmount, totalSalesQuantity }) => {
      const amount = totalSalesAmount ?? 0;
      const qty = totalSalesQuantity ?? 0;
      if (!qty) return null;
      return amount / qty;
    },
  }),
  salesVsBudgetPct: derivedMetric({
    name: "salesVsBudgetPct",
    deps: ["totalSalesAmount", "totalBudget"],
    description: "Total sales / total budget.",
    format: "percent",
    combine: ({ totalSalesAmount, totalBudget }) => {
      const s = totalSalesAmount ?? 0;
      const b = totalBudget ?? 0;
      if (!b) return null;
      return (s / b) * 100;
    },
  }),
  salesAmountYTD: contextTransformMetric({
    name: "salesAmountYTD",
    baseMetric: "totalSalesAmount",
    transform: demoTransforms.ytd,
    description: "YTD of total sales amount.",
    format: "currency",
  }),
  salesAmountLastYear: contextTransformMetric({
    name: "salesAmountLastYear",
    baseMetric: "totalSalesAmount",
    transform: demoTransforms.lastYear,
    description: "Total sales amount for previous year.",
    format: "currency",
  }),
  salesAmountYTDLastYear: contextTransformMetric({
    name: "salesAmountYTDLastYear",
    baseMetric: "totalSalesAmount",
    transform: demoTransforms.ytdLastYear,
    description: "YTD of total sales amount in previous year.",
    format: "currency",
  }),
  budgetYTD: contextTransformMetric({
    name: "budgetYTD",
    baseMetric: "totalBudget",
    transform: demoTransforms.ytd,
    description: "YTD of total budget.",
    format: "currency",
  }),
  budgetLastYear: contextTransformMetric({
    name: "budgetLastYear",
    baseMetric: "totalBudget",
    transform: demoTransforms.lastYear,
    description: "Total budget in previous year.",
    format: "currency",
  }),
  salesVsBudgetPctYTD: derivedMetric({
    name: "salesVsBudgetPctYTD",
    deps: ["salesAmountYTD", "budgetYTD"],
    description: "YTD sales / YTD budget.",
    format: "percent",
    combine: ({ salesAmountYTD, budgetYTD }) => {
      const s = salesAmountYTD ?? 0;
      const b = budgetYTD ?? 0;
      if (!b) return null;
      return (s / b) * 100;
    },
  }),
};

export const demoModel: SemanticModel = {
  tables: demoTableDefinitions,
  attributes: demoAttributes,
  measures: demoMeasures,
  metrics: demoMetrics,
  transforms: demoTransforms,
};

export const demoEngine = createEngine(demoDb, demoModel);

/**
 * DEMO USAGE
 *
 * This is just to illustrate. In a real application you'd likely:
 * - import the library parts
 * - define your own db, tableDefinitions, metrics
 * - call runQuery() from your UI / API layer
 */

if (typeof require !== "undefined" && typeof module !== "undefined" && require.main === module) {
  const engine = demoEngine;
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

  const base2025 = engine.query("sales").where({ year: 2025 });

  console.log("\n=== Demo: 2025-02, Region x Product ===");
  const result1 = base2025
    .addAttributes("regionId", "productId")
    .addMetrics(...metricBundle)
    .where({ month: 2 })
    .run();
  // eslint-disable-next-line no-console
  console.table(result1);

  console.log("\n=== Demo: 2025-02, Region only ===");
  const result2 = base2025
    .addAttributes("regionId")
    .addMetrics(...metricBundle)
    .where({ month: 2 })
    .run();
  // eslint-disable-next-line no-console
  console.table(result2);

  console.log("\n=== Demo: 2025-02, Region=NA, by Product ===");
  const result3 = base2025
    .addAttributes("productId")
    .addMetrics(...metricBundle)
    .where({ month: 2, regionId: "NA" })
    .run();
  // eslint-disable-next-line no-console
  console.table(result3);
}
