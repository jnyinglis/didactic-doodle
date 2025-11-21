// semanticEngineV2.ts
// Minimal V2-only semantic metrics engine
//
// - Metrics are grain-agnostic: they only see { rows, groupKey }.
// - Grain is defined by the query's dimensions.
// - Engine chooses a base fact, joins dimensions, applies filters,
//   groups by dimensions, then evaluates metrics per group.
// - Rowset transforms support time-intelligence (e.g. last year/week).

import Enumerable from "./linq";

/* --------------------------------------------------------------------------
 * BASIC TYPES
 * -------------------------------------------------------------------------- */

export type Row = Record<string, any>;

export function rowsToEnumerable(rows: Row[] = []) {
  return Enumerable.from(rows ?? []);
}

export type RowSequence = Enumerable.IEnumerable<Row>;

/* --------------------------------------------------------------------------
 * FILTER TYPES + HELPERS (ATTRIBUTE-LEVEL WHERE)
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
  op: "eq" | "lt" | "lte" | "gt" | "gte" | "between" | "in";
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
      }
    } else {
      expressions.push(f.eq(field, value as FilterPrimitive));
    }
  }
  if (expressions.length === 0) return null;
  if (expressions.length === 1) return expressions[0];
  return f.and(...expressions);
}

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
      const from = Number(expr.value);
      const to = Number(expr.value2);
      if (Number.isNaN(from) || Number.isNaN(to)) {
        throw new Error(`Invalid numeric filter for ${expr.field}`);
      }
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

function applyWhereFilter(
  relation: RowSequence,
  where?: FilterContext,
  availableAttrs: string[] = []
): RowSequence {
  if (!where) return relation;
  const node = normalizeFilterContext(where);
  if (!node) return relation;
  const allowed = availableAttrs.length ? new Set(availableAttrs) : null;
  const pruned = allowed ? pruneFilterNode(node, allowed) : node;
  if (!pruned) return relation;
  return relation.where((row: Row) => evaluateFilterNode(pruned, row));
}

function collectFilterFields(ctx?: FilterContext): Set<string> {
  const node = normalizeFilterContext(ctx);
  const fields = new Set<string>();
  if (!node) return fields;
  const walk = (n: FilterNode) => {
    if (n.kind === "expression") {
      fields.add(n.field);
    } else {
      n.filters.forEach(walk);
    }
  };
  walk(node);
  return fields;
}

/* --------------------------------------------------------------------------
 * IN-MEMORY DB
 * -------------------------------------------------------------------------- */

export interface InMemoryDb {
  tables: Record<string, Row[]>;
}

/* --------------------------------------------------------------------------
 * SEMANTIC MODEL V2 (FACTS, DIMENSIONS, ATTRIBUTES, JOINS)
 * -------------------------------------------------------------------------- */

/**
 * Logical attribute definition that maps a semantic name to a concrete
 * relation/column and declares which fact it is reachable from by default.
 */
export interface LogicalAttribute {
  name: string;
  relation: string; // table name where the column lives
  column: string;
  defaultFact: string; // base fact that can reach this attribute
}

export interface FactRelation {
  name: string;
}

export interface JoinEdge {
  fact: string;
  dimension: string;
  factKey: string;       // physical column name on fact
  dimensionKey: string;  // physical column name on dimension
}

export interface RowsetTransformDefinition {
  id: string;
  table: string;       // transform lookup table
  anchorAttr: string;  // logical attribute used as "current period"
  fromColumn: string;  // column on transform table that matches current period
  toColumn: string;    // column on transform table that matches factKey (e.g., last-year period)
  factKey: string;     // logical attribute on fact rows to filter by (should be in attributes)
}

export interface SemanticModelV2 {
  facts: Record<string, FactRelation>;
  dimensions: Record<string, { name: string }>;
  attributes: Record<string, LogicalAttribute>;
  joins: JoinEdge[];
  metricsV2: MetricRegistryV2;
  rowsetTransforms?: Record<string, RowsetTransformDefinition>;
}

/* --------------------------------------------------------------------------
 * METRIC DEFINITIONS (V2, GRAIN-AGNOSTIC)
 * -------------------------------------------------------------------------- */

export type AggregationOperator = "sum" | "avg" | "count" | "min" | "max";

export interface MetricRuntimeV2 {
  model: SemanticModelV2;
  db: InMemoryDb;
  baseFact: string;
  relation: RowSequence;        // joined + where-filtered relation
  whereFilter: FilterNode | null;
  groupDimensions: string[];    // logical dimension names for this query
}

/**
 * Metric computation context: metrics only know about the rows of their group,
 * the groupKey, and a way to evaluate dependent metrics or apply rowset transforms.
 */
export interface MetricComputationContext {
  rows: RowSequence;                             // grouped rows for this output row
  groupKey: Record<string, any>;                // dimension values for this group
  evalMetric: (name: string) => number | undefined;
  helpers: MetricComputationHelpers;
}

export type MetricEvalV2 = (
  ctx: MetricComputationContext
) => number | undefined;

export interface MetricDefinitionV2 {
  name: string;
  description?: string;
  baseFact?: string;      // optional: preferred base fact
  attributes?: string[];  // logical attributes this metric needs
  deps?: string[];        // dependent metrics
  eval: MetricEvalV2;
}

export type MetricRegistryV2 = Record<string, MetricDefinitionV2>;

export interface MetricComputationHelpers {
  runtime: MetricRuntimeV2;
  applyRowsetTransform(
    transformId: string,
    groupKey: Record<string, any>
  ): RowSequence;
}

/* --------------------------------------------------------------------------
 * QUERY SPEC (V2)
 * -------------------------------------------------------------------------- */

export interface QuerySpecV2 {
  dimensions: string[];                          // logical attributes for grain
  metrics: string[];                             // metric names
  where?: FilterContext;                         // attribute-level filter
  having?: (values: Record<string, number | undefined>) => boolean;
  baseFact?: string;                             // optional override
}

/* --------------------------------------------------------------------------
 * SEMANTIC HELPERS
 * -------------------------------------------------------------------------- */

function normalizeAttribute(def: LogicalAttribute): LogicalAttribute {
  return {
    ...def,
    column: def.column ?? def.name,
  };
}

function attributesForRelation(
  relation: string,
  attributes: Record<string, LogicalAttribute>
): LogicalAttribute[] {
  return Object.values(attributes)
    .map(normalizeAttribute)
    .filter((a) => a.relation === relation);
}

function buildJoinLookup(edges: JoinEdge[]): Map<string, JoinEdge> {
  const map = new Map<string, JoinEdge>();
  edges.forEach((edge) => {
    map.set(`${edge.fact}|${edge.dimension}`, edge);
  });
  return map;
}

function mapLogicalAttributes(
  row: Row,
  relation: string,
  attrRegistry: Record<string, LogicalAttribute>,
  needed: Set<string>
): Row {
  const attrs = attributesForRelation(relation, attrRegistry);
  const mapped: Row = {};
  for (const attr of attrs) {
    const logical = attr.name;
    if (needed.size && !needed.has(logical)) continue;
    mapped[logical] = (row as any)[attr.column];
  }
  return mapped;
}

/**
 * Build a base relation starting from the base fact:
 * - factProjected: fact rows projected to logical attributes on the fact.
 * - joined: factProjected joined with any needed dimensions, projected to logical attrs.
 *
 * NOTE: for simplicity, joins are always from fact -> dimension using JoinEdge,
 * and only one edge per dim is supported.
 */
function buildBaseRelation(
  model: SemanticModelV2,
  db: InMemoryDb,
  baseFact: string,
  requiredAttributes: Set<string>
): { joined: RowSequence } {
  const joinLookup = buildJoinLookup(model.joins);
  const factRows = rowsToEnumerable(db.tables[baseFact] ?? []);

  const factProjected = factRows.select((row: Row) => ({
    ...mapLogicalAttributes(row, baseFact, model.attributes, requiredAttributes),
  }));

  const neededRelations = new Set<string>();
  for (const attr of requiredAttributes) {
    const def = model.attributes[attr];
    if (!def) continue;
    if (def.relation !== baseFact) {
      neededRelations.add(def.relation);
    }
  }

  let joined: RowSequence = factProjected;

  neededRelations.forEach((relation) => {
    const join = joinLookup.get(`${baseFact}|${relation}`);
    if (!join) {
      throw new Error(
        `No join defined from fact ${baseFact} to dimension ${relation}`
      );
    }
    const dimensionRows = rowsToEnumerable(db.tables[relation] ?? []);

    // project logical attrs from dimension, plus a __joinKey to match on
    const mappedDimension = dimensionRows.select((row: Row) => ({
      __joinKey: (row as any)[join.dimensionKey],
      ...mapLogicalAttributes(row, relation, model.attributes, requiredAttributes),
    }));

    joined = joined.selectMany((factRow: Row) => {
      const joinKey = (factRow as any)[join.factKey];
      const matches = mappedDimension
        .where((d: Row) => d.__joinKey === joinKey)
        .toArray();
      if (!matches.length) return [factRow];
      return matches.map((d) => {
        const { __joinKey, ...rest } = d;
        return { ...factRow, ...rest };
      });
    });
  });

  return { joined };
}

function keyFromGroup(groupKey: Record<string, any>): string {
  return JSON.stringify(groupKey);
}

function keyFromRow(row: Row, attrs: string[]): string {
  const obj: Row = {};
  for (const a of attrs) obj[a] = row[a];
  return JSON.stringify(obj);
}

/* --------------------------------------------------------------------------
 * SIMPLE AGGREGATE METRICS (SUM/AVG/etc OVER AN ATTRIBUTE)
 * -------------------------------------------------------------------------- */

function aggregate(
  rows: RowSequence,
  attr: string,
  op: AggregationOperator
): number | undefined {
  const values = rows
    .select((r: Row) => Number((r as any)[attr]))
    .where((num: number) => !Number.isNaN(num))
    .toArray();

  if (values.length === 0) return undefined;

  switch (op) {
    case "sum":
      return values.reduce((a, b) => a + b, 0);
    case "avg":
      return values.reduce((a, b) => a + b, 0) / values.length;
    case "min":
      return Math.min(...values);
    case "max":
      return Math.max(...values);
    case "count":
      return values.length;
    default:
      return undefined;
  }
}

/**
 * Aggregate metric over a single logical attribute.
 * Grain is entirely determined by the query; metric just sums/avgs over group rows.
 */
export function aggregateMetric(
  name: string,
  attr: string,
  op: AggregationOperator = "sum"
): MetricDefinitionV2 {
  return {
    name,
    attributes: [attr],
    eval: ({ rows }) => aggregate(rows, attr, op),
  };
}

/* --------------------------------------------------------------------------
 * ROWSET TRANSFORMS (E.G. LAST-YEAR)
 * -------------------------------------------------------------------------- */

/**
 * Rowset-transform metric: wraps a base metric and evaluates it on a transformed
 * rowset (e.g., last-year rows) while still reporting at the current grain.
 *
 * Example usage:
 *
 *   const sumSales = aggregateMetric("sum_sales", "sales", "sum");
 *   const sumSalesLastYear = rowsetTransformMetric({
 *     name: "sum_sales_last_year",
 *     baseMetric: "sum_sales",
 *     transformId: "lastYearWeek",
 *   });
 */
export function rowsetTransformMetric(opts: {
  name: string;
  baseMetric: string;
  transformId: string;
  description?: string;
}): MetricDefinitionV2 {
  return {
    name: opts.name,
    deps: [opts.baseMetric],
    description: opts.description,
    eval: (ctx) => {
      const transformedRows = ctx.helpers.applyRowsetTransform(
        opts.transformId,
        ctx.groupKey
      );
      const cacheLabel = `${opts.name}:transform`;
      const value = evaluateMetricV2(
        opts.baseMetric,
        ctx.helpers.runtime,
        ctx.groupKey,
        transformedRows,
        cacheLabel
      );
      return value;
    },
  };
}

function applyRowsetTransform(
  runtime: MetricRuntimeV2,
  transformId: string,
  groupKey: Record<string, any>
): RowSequence {
  const transform = runtime.model.rowsetTransforms?.[transformId];
  if (!transform) {
    throw new Error(`Unknown rowset transform: ${transformId}`);
  }

  const anchorValue = groupKey[transform.anchorAttr];
  const transformRows = rowsToEnumerable(
    runtime.db.tables[transform.table] ?? []
  );

  // 1. Find target anchor values (e.g., last-year codes) for this group's anchor.
  const targetAnchors = transformRows
    .where((r: Row) => (r as any)[transform.fromColumn] === anchorValue)
    .select((r: Row) => (r as any)[transform.toColumn])
    .distinct()
    .toArray();

  if (!targetAnchors.length) return rowsToEnumerable([]);

  const allowedAnchor = new Set(targetAnchors);

  // 2. Filter the main joined+where-filtered relation to:
  //    - rows whose factKey is in targetAnchors
  //    - rows that match all groupDimensions EXCEPT the anchorAttr (which is shifted)
  const relation = runtime.relation;
  return relation.where((row: Row) => {
    if (!allowedAnchor.has((row as any)[transform.factKey])) return false;

    return runtime.groupDimensions.every((dim) => {
      if (dim === transform.anchorAttr) return true; // we are shifting this
      return (row as any)[dim] === groupKey[dim];
    });
  });
}

/* --------------------------------------------------------------------------
 * METRIC EVALUATION V2
 * -------------------------------------------------------------------------- */

function evaluateMetricV2(
  metricName: string,
  runtime: MetricRuntimeV2,
  groupKey: Record<string, any>,
  rows: RowSequence,
  cacheLabel?: string,
  cache?: Map<string, number | undefined>
): number | undefined {
  const registry = runtime.model.metricsV2;
  const metric = registry[metricName];
  if (!metric) {
    throw new Error(`Unknown metric: ${metricName}`);
  }

  const cacheKey = `${metricName}|${cacheLabel ?? "base"}|${keyFromGroup(
    groupKey
  )}`;
  const workingCache = cache ?? new Map<string, number | undefined>();
  if (workingCache.has(cacheKey)) {
    return workingCache.get(cacheKey);
  }

  const evalMetric = (dep: string) =>
    evaluateMetricV2(dep, runtime, groupKey, rows, undefined, workingCache);

  const helpers: MetricComputationHelpers = {
    runtime,
    applyRowsetTransform: (transformId, gk) =>
      applyRowsetTransform(runtime, transformId, gk),
  };

  const value = metric.eval({ rows, groupKey, evalMetric, helpers });
  workingCache.set(cacheKey, value);
  return value;
}

/* --------------------------------------------------------------------------
 * MAIN QUERY EXECUTION (V2)
 * -------------------------------------------------------------------------- */

export function runSemanticQuery(
  env: { db: InMemoryDb; model: SemanticModelV2 },
  spec: QuerySpecV2
): Row[] {
  const { db, model } = env;
  const dimensions = spec.dimensions;

  // 1. Figure out which logical attributes are required:
  const whereFields = collectFilterFields(spec.where);
  const metricAttrFields = new Set<string>();

  spec.metrics.forEach((m) => {
    const def = model.metricsV2[m];
    def?.attributes?.forEach((a) => metricAttrFields.add(a));
  });

  const requiredAttrs = new Set<string>([...dimensions]);
  whereFields.forEach((f) => requiredAttrs.add(f));
  metricAttrFields.forEach((f) => requiredAttrs.add(f));

  // 2. Choose base fact:
  const baseFact =
    spec.baseFact ??
    spec.metrics
      .map((m) => model.metricsV2[m]?.baseFact)
      .find((f) => !!f) ??
    [...requiredAttrs]
      .map((attr) => model.attributes[attr]?.defaultFact)
      .find((f) => !!f);

  if (!baseFact) {
    throw new Error("Unable to determine a base fact for the query");
  }

  // 3. Build joined relation from base fact + needed dimensions:
  const { joined } = buildBaseRelation(model, db, baseFact, requiredAttrs);

  // 4. Apply where filter:
  const availableAttrs = Array.from(requiredAttrs);
  const whereNode = normalizeFilterContext(spec.where);
  const filtered = applyWhereFilter(joined, spec.where, availableAttrs);

  // 5. Build runtime for metric evaluation:
  const runtime: MetricRuntimeV2 = {
    model,
    db,
    baseFact,
    relation: filtered,
    whereFilter: whereNode,
    groupDimensions: dimensions,
  };

  // 6. Group by dimensions:
  const grouped = filtered.groupBy(
    (row: Row) => keyFromRow(row, dimensions),
    (row: Row) => row
  );

  const results: Row[] = [];

  grouped.forEach((group) => {
    const sample = group.first();
    const groupKey: Record<string, any> = {};
    if (sample) {
      dimensions.forEach((d) => (groupKey[d] = (sample as any)[d]));
    }

    const metricCache = new Map<string, number | undefined>();
    const metricValues: Record<string, number | undefined> = {};

    spec.metrics.forEach((m) => {
      metricValues[m] = evaluateMetricV2(
        m,
        runtime,
        groupKey,
        group,
        undefined,
        metricCache
      );
    });

    if (spec.having && !spec.having(metricValues)) {
      return;
    }

    results.push({ ...groupKey, ...metricValues });
  });

  return results;
}
