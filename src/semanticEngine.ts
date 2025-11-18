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

export type RowSequence = Enumerable.IEnumerable<Row>;

export type NumberSequence = Enumerable.IEnumerable<number>;

/**
 * Declarative specification for table-based transforms.
 */
export type TableTransform = {
  id: string;
  relation: () => RowSequence;
  anchorAttr: string;
  fromColumn: string;
  toColumn: string;
  factKey: string;
};

/* --------------------------------------------------------------------------
 * TABLE + ATTRIBUTE + MEASURE DEFINITIONS
 * -------------------------------------------------------------------------- */

export type AggregationOperator = "sum" | "avg" | "count" | "min" | "max";

interface TableColumn {
  name: string;
  type: "number" | "string" | "date" | "boolean";
  defaultAgg?: AggregationOperator;
  description?: string;
}

interface TableDefinition {
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
  description?: string;
  aggregation?: AggregationOperator;
  grain?: string[];
}

export type MeasureRegistry = Record<string, MeasureDefinition>;

export interface InMemoryDb {
  tables: Record<string, Row[]>;
}

export interface SemanticModel {
  tables: TableDefinitionRegistry;
  attributes: AttributeRegistry;
  measures: MeasureRegistry;
  metrics: MetricRegistry;
  relationalMetrics?: RelationalMetricRegistry;
  transforms: ContextTransformsRegistry;
}

/* --------------------------------------------------------------------------
 * METRIC DEFINITIONS (SCALAR)
 * -------------------------------------------------------------------------- */

interface MetricBase {
  name: string;
  description?: string;
  format?: string;
  grain?: string[];
  aggregation?: AggregationOperator;
}

export interface MetricContext {
  filter?: FilterContext;
  grain?: string[];
}

export interface MetricEvaluationEnvironment {
  model: SemanticModel;
  db: InMemoryDb;
}

export interface TransformHelpers {
  env: MetricEvaluationEnvironment;
  rows(tableName: string): RowSequence;
  first(tableName: string, predicate: (r: Row) => boolean): Row | undefined;
  filterRows(tableName: string, predicate: (r: Row) => boolean): RowSequence;

  // Measure / metric helpers
  evaluateMeasure(
    measureName: string,
    ctx: MetricContext,
    options?: MeasureEvaluationOptions
  ): number | null;
  evaluateMetric(
    metricName: string,
    ctx: MetricContext
  ): number | null;

  // Context helpers
  getFilterValue(field: string, ctx: MetricContext): FilterValue | undefined;
  mergeFilters: typeof mergeFilters;
  f: typeof f;
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

/**
 * Relational metrics: metrics whose evaluation returns a RowSequence (relation)
 * keyed by a grain of attributes, instead of a single scalar.
 */

export type GrainSpec = string[] | "global";

export interface RelationalMetricDefinition {
  name: string;
  measure: string; // measure name; implies the underlying fact table
  grain?: GrainSpec;
  format?: string;
  description?: string;
  evalEnumerable: (
    ctx: MetricContext,
    env: MetricEvaluationEnvironment
  ) => RowSequence;
}

export type RelationalMetricRegistry = Record<string, RelationalMetricDefinition>;

export type ContextTransform = (
  ctx: MetricContext,
  helpers: TransformHelpers
) => MetricContext;
export type ContextTransformsRegistry = Record<string, ContextTransform>;

export function composeTransforms(
  ...transforms: ContextTransform[]
): ContextTransform {
  return (ctx: MetricContext, helpers: TransformHelpers) =>
    transforms.reduce((acc, transform) => transform(acc, helpers), ctx);
}

export const attr = {
  id(opts: AttributeDefinition): AttributeDefinition {
    return {
      ...opts,
      column: opts.column ?? opts.name,
    };
  },
};

export const measure = {
  fact(opts: MeasureDefinition): MeasureDefinition {
    return {
      ...opts,
      column: opts.column ?? opts.name,
    };
  },
};

/* --------------------------------------------------------------------------
 * FILTER TYPES + HELPERS
 * -------------------------------------------------------------------------- */

export type FilterPrimitive = string | number | boolean | Date;
export type FilterRange =
  | { kind: "between"; from: number; to: number }
  | { kind: "gte"; value: number }
  | { kind: "gt"; value: number }
  | { kind: "lte"; value: number }
  | { kind: "lt"; value: number };

export type FilterValue = FilterPrimitive | FilterRange;

export interface FilterExpression {
  kind: "expression";
  field: string;
  op:
    | "eq"
    | "lt"
    | "lte"
    | "gt"
    | "gte"
    | "between"
    | "in";
  value: FilterValue | FilterPrimitive[] | null;
  value2?: FilterPrimitive | null;
}

export interface FilterConjunction {
  kind: "and" | "or";
  filters: FilterNode[];
}

export type FilterNode = FilterExpression | FilterConjunction;

export type FilterContext =
  | FilterNode
  | Record<string, FilterValue | FilterValue[] | undefined>
  | undefined;

export const f = {
  eq(field: string, value: FilterPrimitive | FilterPrimitive[]): FilterExpression {
    if (Array.isArray(value)) {
      return { kind: "expression", field, op: "in", value };
    }
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

function normalizeFilterContext(context?: FilterContext): FilterNode | null {
  if (!context) return null;
  if ("kind" in (context as any)) {
    return context as FilterNode;
  }
  const expressions: FilterNode[] = [];
  for (const [field, value] of Object.entries(context)) {
    if (value == null) continue;
    if (Array.isArray(value)) {
      expressions.push(f.in(field, value as FilterPrimitive[]));
    } else if (typeof value === "object" && "kind" in value) {
      const v = value as FilterRange;
      switch (v.kind) {
        case "between":
          expressions.push(f.between(field, v.from, v.to));
          break;
        case "gte":
          expressions.push(f.gte(field, v.value));
          break;
        case "gt":
          expressions.push(f.gt(field, v.value));
          break;
        case "lte":
          expressions.push(f.lte(field, v.value));
          break;
        case "lt":
          expressions.push(f.lt(field, v.value));
          break;
        default:
          break;
      }
    } else {
      expressions.push(f.eq(field, value as FilterPrimitive));
    }
  }
  if (expressions.length === 0) return null;
  if (expressions.length === 1) return expressions[0];
  return f.and(...expressions);
}

export function mergeFilters(
  a?: FilterContext,
  b?: FilterContext
): FilterContext {
  const na = normalizeFilterContext(a);
  const nb = normalizeFilterContext(b);
  if (!na && !nb) return undefined;
  if (!na) return nb!;
  if (!nb) return na;
  return f.and(na, nb);
}

/* --------------------------------------------------------------------------
 * MEASURE EVALUATION (SCALAR)
 * -------------------------------------------------------------------------- */

export interface MeasureEvaluationOptions {
  aggregation?: AggregationOperator;
}

function getFilterValue(
  field: string,
  ctx: MetricContext
): FilterValue | undefined {
  const node = normalizeFilterContext(ctx.filter);
  if (!node) return undefined;

  function search(node: FilterNode): FilterValue | undefined {
    if (node.kind === "expression") {
      if (node.field === field) {
        return node.value as FilterValue;
      }
      return undefined;
    } else {
      for (const child of node.filters) {
        const v = search(child);
        if (v !== undefined) return v;
      }
      return undefined;
    }
  }

  return search(node);
}

export function applyContextToTable(
  rows: Row[] | RowSequence,
  context: FilterContext,
  grain: string[]
): RowSequence {
  const node = normalizeFilterContext(context);
  const sequence = Array.isArray(rows) ? rowsToEnumerable(rows) : rows;
  if (!node) return sequence;
  const allowed = new Set(grain);
  const pruned = pruneFilterNode(node, allowed);
  if (!pruned) return sequence;
  return sequence.where((r: Row) => evaluateFilterNode(pruned, r));
}

/**
 * Join the current context rows through the transform relation to determine
 * the fact keys that should be included, then filter the fact rows.
 */
export function applyTableTransform(
  contextRows: RowSequence,
  factRows: RowSequence,
  transform: TableTransform
): RowSequence {
  const transformRows = transform.relation();

  const targetKeys = contextRows
    .join(
      transformRows,
      (ctx) => ctx[transform.anchorAttr],
      (tr) => tr[transform.fromColumn],
      (_ctx, tr) => tr[transform.toColumn]
    )
    .distinct()
    .toArray();

  if (!targetKeys.length) {
    return Enumerable.empty<Row>();
  }

  const keySet = new Set(targetKeys);
  return factRows.where((row) => keySet.has(row[transform.factKey]));
}

function buildContextRowsForTransform(
  ctx: MetricContext,
  transform: TableTransform
): RowSequence {
  const anchorValue = getFilterValue(transform.anchorAttr, ctx);

  if (Array.isArray(anchorValue)) {
    const rows = (anchorValue as FilterPrimitive[]).map((value) => ({
      [transform.anchorAttr]: value,
    }));
    return rowsToEnumerable(rows);
  }

  if (anchorValue === undefined) {
    const anchors = transform
      .relation()
      .select((row: Row) => row[transform.fromColumn])
      .distinct((value: any) => JSON.stringify(value))
      .toArray();

    return rowsToEnumerable(
      anchors.map((value) => ({ [transform.anchorAttr]: value }))
    );
  }

  if (
    anchorValue !== null &&
    typeof anchorValue === "object" &&
    "kind" in anchorValue
  ) {
    throw new Error(
      `Table transform ${transform.id} requires equality filters for ${transform.anchorAttr}`
    );
  }

  return rowsToEnumerable([
    {
      [transform.anchorAttr]: anchorValue,
    },
  ]);
}

function serializeFilter(filter?: FilterContext): any {
  if (!filter) return null;
  if ("kind" in (filter as any)) return filter;
  return filter;
}

function metricCacheKey(metricName: string, ctx: MetricContext): string {
  return JSON.stringify({
    metricName,
    grain: ctx.grain ?? [],
    filter: serializeFilter(ctx.filter),
  });
}

export interface MetricRuntime {
  evaluate(metricName: string, ctx: MetricContext): number | null;
  evaluateMeasure(
    measureName: string,
    ctx: MetricContext,
    options?: MeasureEvaluationOptions
  ): number | null;
  env: MetricEvaluationEnvironment;
}

export function buildTransformHelpers(
  env: MetricEvaluationEnvironment
): TransformHelpers {
  const rawGetFilterValue = getFilterValue;
  return {
    env,
    rows(tableName: string): RowSequence {
      const tableRows = env.db.tables[tableName] ?? [];
      return rowsToEnumerable(tableRows);
    },
    first(tableName: string, predicate: (r: Row) => boolean): Row | undefined {
      const tableRows = env.db.tables[tableName] ?? [];
      return tableRows.find(predicate);
    },
    filterRows(
      tableName: string,
      predicate: (r: Row) => boolean
    ): RowSequence {
      const tableRows = env.db.tables[tableName] ?? [];
      return rowsToEnumerable(tableRows.filter(predicate));
    },
    evaluateMeasure(measureName, ctx, options) {
      return evaluateMeasureDefinition(measureName, ctx, env, options);
    },
    evaluateMetric(metricName, ctx) {
      return evaluateMetric(metricName, env, ctx);
    },
    getFilterValue(field, ctx) {
      return rawGetFilterValue(field, ctx);
    },
    mergeFilters,
    f,
  };
}

function evaluateMeasureDefinition(
  measureName: string,
  ctx: MetricContext,
  env: MetricEvaluationEnvironment,
  options?: MeasureEvaluationOptions
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
  const column = def.column ?? def.name;
  const columnDef = tableDef.columns[column];
  const aggregation =
    options?.aggregation ?? columnDef?.defaultAgg ?? "sum";
  const pickValue = (row: Row): number | null => {
    const raw = Number(row[column]);
    return Number.isNaN(raw) ? null : raw;
  };

  const numericValues = filtered
    .select((r: Row) => pickValue(r))
    .where((num: number | null): num is number => typeof num === "number")
    .memoize();

  switch (aggregation) {
    case "sum":
      return numericValues.sum();
    case "avg":
      return numericValues.isEmpty() ? null : numericValues.average();
    case "count":
      return filtered.count();
    case "min":
      return numericValues.isEmpty() ? null : numericValues.min();
    case "max":
      return numericValues.isEmpty() ? null : numericValues.max();
    default:
      throw new Error(`Unsupported aggregation: ${aggregation}`);
  }
}

/**
 * Project a measure to an Enumerable< number > for a given metric context.
 * This is similar to evaluateMeasureDefinition, but returns the raw value
 * sequence instead of aggregating it.
 */
export function projectMeasureValues(
  measureName: string,
  ctx: MetricContext,
  env: MetricEvaluationEnvironment
): NumberSequence {
  const def = env.model.measures[measureName];
  if (!def) {
    throw new Error(`Unknown measure: ${measureName}`);
  }
  const tableDef = env.model.tables[def.table];
  if (!tableDef) {
    throw new Error(`Unknown table: ${def.table}`);
  }
  const rows = rowsToEnumerable(env.db.tables[def.table] ?? []);
  const grain =
    ctx.grain && ctx.grain.length > 0
      ? ctx.grain
      : def.grain ?? tableDef.grain;
  const filtered = applyContextToTable(rows, ctx.filter, grain);
  const column = def.column ?? def.name;

  return filtered
    .select((r: Row) => {
      const raw = Number((r as any)[column]);
      return Number.isNaN(raw) ? null : raw;
    })
    .where((num: number | null): num is number => typeof num === "number");
}

/**
 * Pick a subset of keys from an object.
 */
export function pick(obj: Row, keys: string[]): Row {
  const out: Row = {};
  for (const k of keys) {
    out[k] = obj[k];
  }
  return out;
}

/* --------------------------------------------------------------------------
 * METRIC BUILDERS (SCALAR)
 * -------------------------------------------------------------------------- */

interface SimpleMetricOptions {
  name: string;
  measure: string;
  aggregation?: AggregationOperator;
  description?: string;
  format?: string;
  grain?: string[];
}

export interface FactMeasureConfig {
  name: string;
  factTable: string;
  column: string;
  aggregate?: AggregationOperator;
  description?: string;
  format?: string;
  grain?: string[];
  filters?: FilterContext;
  transform?: TableTransform;
}

export function simpleMetric(opts: SimpleMetricOptions): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    grain: opts.grain,
    aggregation: opts.aggregation,
    eval: (ctx, runtime) =>
      runtime.evaluateMeasure(opts.measure, ctx, {
        aggregation: opts.aggregation,
      }),
  };
}

export function factMeasure(opts: FactMeasureConfig): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    grain: opts.grain,
    aggregation: opts.aggregate,
    eval: (ctx, runtime) => {
      const env = runtime.env;
      const tableDef = env.model.tables[opts.factTable];
      if (!tableDef) {
        throw new Error(`Unknown table for fact measure: ${opts.factTable}`);
      }

      const factRowsRaw = env.db.tables[opts.factTable] ?? [];
      const factGrain = tableDef.grain;
      let factRows = rowsToEnumerable(factRowsRaw);

      const filterGrain = (() => {
        if (!opts.transform) return factGrain;
        const excluded = new Set<string>([
          opts.transform.anchorAttr,
          opts.transform.factKey,
        ]);
        return factGrain.filter((attr) => !excluded.has(attr));
      })();

      factRows = applyContextToTable(factRows, ctx.filter, filterGrain);

      if (opts.transform) {
        const contextRows = buildContextRowsForTransform(ctx, opts.transform);
        factRows = applyTableTransform(contextRows, factRows, opts.transform);
      }

      if (opts.filters) {
        factRows = applyContextToTable(factRows, opts.filters, factGrain);
      }

      const numericValues = factRows
        .select((row: Row) => {
          const raw = Number((row as any)[opts.column]);
          return Number.isNaN(raw) ? null : raw;
        })
        .where(
          (num: number | null): num is number => typeof num === "number"
        )
        .memoize();

      const aggregate = opts.aggregate ?? "sum";
      switch (aggregate) {
        case "sum":
          return numericValues.sum();
        case "avg":
          return numericValues.isEmpty() ? null : numericValues.average();
        case "count":
          return numericValues.count();
        case "min":
          return numericValues.isEmpty() ? null : numericValues.min();
        case "max":
          return numericValues.isEmpty() ? null : numericValues.max();
        default:
          throw new Error(`Unknown aggregate: ${aggregate}`);
      }
    },
  };
}

export function expressionMetric(opts: {
  name: string;
  description?: string;
  format?: string;
  eval: MetricEval;
  grain?: string[];
}): MetricDefinition {
  return {
    name: opts.name,
    description: opts.description,
    format: opts.format,
    eval: opts.eval,
    grain: opts.grain,
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
    eval: (ctx, runtime) => {
      const helpers = buildTransformHelpers(runtime.env);
      const transformedCtx = opts.transform(ctx, helpers);
      return runtime.evaluate(opts.baseMetric, transformedCtx);
    },
  };
}

/**
 * Relational metric helpers: build metrics that operate over a measure's
 * value sequence and produce a relation keyed by a grain of attributes.
 */

export type RelAggregateKind = "sum" | "avg" | "min" | "max" | "count";

export type RelAggregateFn = (values: NumberSequence) => number | null;

export interface RelAggregateMetricOptions {
  name: string;
  measure: string;
  agg?: RelAggregateKind | RelAggregateFn;
  grain?: GrainSpec;
  description?: string;
  format?: string;
}

export interface RelExpressionMetricOptions {
  name: string;
  measure: string;
  expr: (values: NumberSequence) => number;
  grain?: GrainSpec;
  description?: string;
  format?: string;
}

function buildRelAggregateFn(agg: RelAggregateKind | RelAggregateFn): RelAggregateFn {
  if (typeof agg === "function") return agg;
  return (values: NumberSequence) => {
    switch (agg) {
      case "sum":
        return values.sum();
      case "avg":
        return values.average();
      case "min":
        return values.min();
      case "max":
        return values.max();
      case "count":
        return values.count();
      default:
        throw new Error(`Unsupported relational aggregate kind: ${agg}`);
    }
  };
}

export function relAggregateMetric(opts: RelAggregateMetricOptions): RelationalMetricDefinition {
  const agg = opts.agg ?? "sum";
  const aggFn = buildRelAggregateFn(agg);

  return {
    name: opts.name,
    measure: opts.measure,
    grain: opts.grain,
    format: opts.format,
    description: opts.description,
    evalEnumerable: (ctx, env) => {
      const effectiveGrain: GrainSpec =
        opts.grain ?? (ctx.grain ?? []);

      // Global metric: compute once, no grouping keys
      if (effectiveGrain === "global" || effectiveGrain.length === 0) {
        const values = projectMeasureValues(opts.measure, ctx, env);
        const value = aggFn(values);
        return rowsToEnumerable([{ [opts.name]: value }]);
      }

      const def = env.model.measures[opts.measure]!;
      const factRows = rowsToEnumerable(env.db.tables[def.table] ?? []);
      const grainAttrs = effectiveGrain as string[];

      const filtered = applyContextToTable(factRows, ctx.filter, grainAttrs);
      const column = def.column ?? def.name;

      return filtered.groupBy(
        (r: Row) => JSON.stringify(pick(r, grainAttrs)),
        (r: Row) => r,
        (key: string, group: any) => {
          const keyObj = JSON.parse(key) as Row;
          const values = group
            .select((r: Row) => {
              const raw = Number((r as any)[column]);
              return Number.isNaN(raw) ? null : raw;
            })
            .where((num: number | null): num is number => typeof num === "number");

          const value = aggFn(values);
          return {
            ...keyObj,
            [opts.name]: value,
          };
        }
      );
    },
  };
}

export function relExpressionMetric(opts: RelExpressionMetricOptions): RelationalMetricDefinition {
  return {
    name: opts.name,
    measure: opts.measure,
    grain: opts.grain,
    format: opts.format,
    description: opts.description,
    evalEnumerable: (ctx, env) => {
      const effectiveGrain: GrainSpec =
        opts.grain ?? (ctx.grain ?? []);

      if (effectiveGrain === "global" || effectiveGrain.length === 0) {
        const values = projectMeasureValues(opts.measure, ctx, env);
        const value = opts.expr(values);
        return rowsToEnumerable([{ [opts.name]: value }]);
      }

      const def = env.model.measures[opts.measure]!;
      const factRows = rowsToEnumerable(env.db.tables[def.table] ?? []);
      const grainAttrs = effectiveGrain as string[];
      const filtered = applyContextToTable(factRows, ctx.filter, grainAttrs);
      const column = def.column ?? def.name;

      return filtered.groupBy(
        (r: Row) => JSON.stringify(pick(r, grainAttrs)),
        (r: Row) => r,
        (key: string, group: any) => {
          const keyObj = JSON.parse(key) as Row;
          const values = group
            .select((r: Row) => {
              const raw = Number((r as any)[column]);
              return Number.isNaN(raw) ? null : raw;
            })
            .where((num: number | null): num is number => typeof num === "number");
          const value = opts.expr(values);
          return {
            ...keyObj,
            [opts.name]: value,
          };
        }
      );
    },
  };
}

/* --------------------------------------------------------------------------
 * METRIC RUNTIME + EVALUATION (SCALAR PATH)
 * -------------------------------------------------------------------------- */

export function buildMetricRuntime(
  env: MetricEvaluationEnvironment
): MetricRuntime {
  return {
    env,
    evaluate(metricName: string, ctx: MetricContext = {}): number | null {
      return evaluateMetric(metricName, env, ctx);
    },
    evaluateMeasure(
      measureName: string,
      ctx: MetricContext,
      options?: MeasureEvaluationOptions
    ): number | null {
      return evaluateMeasureDefinition(measureName, ctx, env, options);
    },
  };
}

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
    return cache.get(key)!;
  }

  const runtime = buildMetricRuntime(env);
  const value = metric.eval(effectiveContext, runtime);
  cache.set(key, value);
  return value;
}

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

/* --------------------------------------------------------------------------
 * FILTER APPLICATION HELPERS
 * -------------------------------------------------------------------------- */

function pruneFilterNode(node: FilterNode, allowed: Set<string>): FilterNode | null {
  if (node.kind === "expression") {
    return allowed.has(node.field) ? node : null;
  } else {
    const children = node.filters
      .map((child) => pruneFilterNode(child, allowed))
      .filter((c): c is FilterNode => c != null);
    if (children.length === 0) return null;
    if (children.length === 1) return children[0];
    return { kind: node.kind, filters: children };
  }
}

function evaluateFilterNode(node: FilterNode, row: Row): boolean {
  if (node.kind === "expression") {
    const value = (row as any)[node.field];
    return matchesExpression(value, node);
  } else if (node.kind === "and") {
    return node.filters.every((child) => evaluateFilterNode(child, row));
  } else {
    return node.filters.some((child) => evaluateFilterNode(child, row));
  }
}

function matchesExpression(value: any, expr: FilterExpression): boolean {
  switch (expr.op) {
    case "eq":
      if (Array.isArray(expr.value)) {
        return (expr.value as FilterPrimitive[]).includes(value);
      }
      return value === expr.value;
    case "lt":
      return value < Number(expr.value);
    case "lte":
      return value <= Number(expr.value);
    case "gt":
      return value > Number(expr.value);
    case "gte":
      return value >= Number(expr.value);
    case "between": {
      const from = ensureNumericRangeValue(expr.value as number, expr.field, "between-from");
      const to = ensureNumericRangeValue(expr.value2 as number, expr.field, "between-to");
      return value >= from && value <= to;
    }
    case "in":
      return Array.isArray(expr.value)
        ? (expr.value as FilterPrimitive[]).includes(value)
        : false;
    default:
      return false;
  }
}

function ensureNumericRangeValue(
  raw: number,
  field: string,
  kind: string
): number {
  const coerced = Number(raw);
  if (Number.isNaN(coerced)) {
    throw new Error(`Invalid numeric filter value for ${field} (${kind})`);
  }
  return coerced;
}

/* --------------------------------------------------------------------------
 * RELATIONAL QUERY EXECUTION
 * -------------------------------------------------------------------------- */

interface QuerySpec {
  table?: string;
  attributes: string[];
  metrics: string[];
  filters?: FilterContext;
}

function evaluateMetricWithRuntime(
  metricName: string,
  ctx: MetricContext,
  runtime: MetricRuntime,
  env: MetricEvaluationEnvironment,
  cache: Map<string, number | null>
): number | null {
  const metric = env.model.metrics[metricName];
  if (!metric) {
    throw new Error(`Unknown metric: ${metricName}`);
  }
  const effectiveContext: MetricContext = {
    filter: ctx.filter,
    grain: ctx.grain ?? metric.grain,
  };
  const key = metricCacheKey(metricName, effectiveContext);
  if (cache.has(key)) {
    return cache.get(key)!;
  }
  const value = metric.eval(effectiveContext, runtime);
  cache.set(key, value);
  return value;
}

/**
 * Relational-algebra query execution:
 * - Build a frame as the cross-join of attribute domains.
 * - Evaluate each relational metric as a relation keyed by its grain.
 * - LEFT JOIN each metric relation onto the frame using LINQ.
 */

interface AttributeDomain {
  table: string;
  attributeNames: string[];
}

function inferAttributeDomains(
  spec: QuerySpec,
  model: SemanticModel
): AttributeDomain[] {
  const byTable = new Map<string, string[]>();

  for (const attrName of spec.attributes) {
    const attrDef = model.attributes[attrName];
    if (!attrDef) {
      throw new Error(`Unknown attribute: ${attrName}`);
    }
    const list = byTable.get(attrDef.table) ?? [];
    list.push(attrName);
    byTable.set(attrDef.table, list);
  }

  return Array.from(byTable.entries()).map(([table, attributeNames]) => ({
    table,
    attributeNames,
  }));
}

export function buildFrameEnumerable(
  spec: QuerySpec,
  env: MetricEvaluationEnvironment
): RowSequence {
  const domains = inferAttributeDomains(spec, env.model);
  if (domains.length === 0) {
    return rowsToEnumerable([{}]);
  }

  const domainEnumerables = domains.map((domain) => {
    const tableRows: Row[] = env.db.tables[domain.table] ?? [];
    const attrs = domain.attributeNames;

    return rowsToEnumerable(tableRows)
      .select((r: Row) => {
        const out: Row = {};
        for (const a of attrs) {
          const attrDef = env.model.attributes[a];
          const col = attrDef.column ?? attrDef.name;
          out[a] = (r as any)[col];
        }
        return out;
      })
      .distinct((row: Row) => JSON.stringify(pick(row, attrs)));
  });

  let frame: RowSequence = domainEnumerables[0];

  for (let i = 1; i < domainEnumerables.length; i++) {
    const next = domainEnumerables[i];
    frame = frame.selectMany((left: Row) =>
      next.select((right: Row) => ({ ...left, ...right }))
    );
  }

  return frame;
}

function keyFromRow(row: Row, attrs: string[]): string {
  const obj: Row = {};
  for (const a of attrs) {
    obj[a] = row[a];
  }
  return JSON.stringify(obj);
}

export function runRelationalQuery(
  env: MetricEvaluationEnvironment,
  spec: QuerySpec
): Row[] {
  const frame = buildFrameEnumerable(spec, env);
  const model = env.model;
  const relMetrics: RelationalMetricRegistry =
    (model as any).relationalMetrics ?? {};

  let result: RowSequence = frame;

  const baseContext: MetricContext = {
    filter: spec.filters,
    grain: spec.attributes,
  };

  for (const metricName of spec.metrics) {
    const metric = relMetrics[metricName];
    if (!metric) {
      throw new Error(`Unknown relational metric: ${metricName}`);
    }

    const metricCtx: MetricContext = {
      filter: baseContext.filter,
      grain:
        metric.grain === "global"
          ? []
          : Array.isArray(metric.grain)
          ? metric.grain
          : baseContext.grain,
    };

    const metricRel = metric.evalEnumerable(metricCtx, env);
    const joinAttrs = Array.isArray(metricCtx.grain) ? metricCtx.grain : [];

    if (joinAttrs.length === 0) {
      // Global metric: repeat scalar across all frame rows
      const sample = metricRel.firstOrDefault();
      const val = sample ? (sample as any)[metric.name] : null;
      result = result.select((r: Row) => ({
        ...r,
        [metric.name]: val,
      }));
    } else {
      // Use LINQ left join so the frame always wins
      result = (result as any).leftJoin(
        metricRel,
        (l: Row) => keyFromRow(l, joinAttrs),
        (r: Row) => keyFromRow(r, joinAttrs),
        (l: Row, r: Row | null) => ({
          ...l,
          [metric.name]: r ? (r as any)[metric.name] : null,
        })
      );
    }
  }

  const enriched = result.select((r: Row) =>
    enrichDimensions(r, env.db, env.model.tables)
  );

  return enriched.toArray();
}

/* --------------------------------------------------------------------------
 * DIMENSION ENRICHMENT
 * -------------------------------------------------------------------------- */

function enrichDimensions(
  row: Row,
  db: InMemoryDb,
  tables: TableDefinitionRegistry
): Row {
  // Placeholder: hook where you can look up human-readable labels
  // from dimension tables based on keys in `row`.
  // Currently returns the row unchanged.
  return row;
}
