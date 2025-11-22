// semanticEngine.ts
// Minimal semantic metrics engine
//
// Key ideas:
//
// - Metrics are grain-agnostic: they only see { rows, groupKey }.
// - Grain is defined by the query's dimensions.
// - If metrics exist, their baseFact determines which fact table to use.
// - If there are *no metrics*, and all attributes come from a single relation
//   (e.g. dim_store), we treat that dimension table itself as the base.
// - Rowset transforms support time-intelligence (e.g. last-year) by shifting
//   which rows a metric sees, but still reporting at the current grain.

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
 * SEMANTIC MODEL
 * -------------------------------------------------------------------------- */

/**
 * Logical attribute definition that maps a semantic name to a concrete
 * relation/column. There is no defaultFact; if metrics exist, their baseFact
 * drives the fact choice. If there are no metrics, and all attributes come
 * from a single relation, that relation becomes the base (dimension fallback).
 */
export interface LogicalAttribute {
  name: string;
  relation: string; // table name where the column lives (fact or dimension)
  column: string;
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
  table: string;       // transform lookup table (e.g. tradyrwk_transform)
  anchorAttr: string;  // logical attribute used as "current period" (e.g. tradyrwkcode)
  fromColumn: string;  // column on transform table that matches current period
  toColumn: string;    // column on transform table that matches factKey (e.g. tradyrwkcode_lastyear)
  factKey: string;     // logical attribute on fact rows to filter by (e.g. tradyrwkcode)
}

export interface SemanticModel {
  facts: Record<string, FactRelation>;
  dimensions: Record<string, { name: string }>;
  attributes: Record<string, LogicalAttribute>;
  joins: JoinEdge[];
  metrics: MetricRegistry;
  rowsetTransforms?: Record<string, RowsetTransformDefinition>;
}

/* --------------------------------------------------------------------------
 * METRICS (GRAIN-AGNOSTIC)
 * -------------------------------------------------------------------------- */

export type AggregationOperator = "sum" | "avg" | "count" | "min" | "max";

export interface MetricRuntime {
  model: SemanticModel;
  db: InMemoryDb;
  relation: RowSequence;        // joined + where-filtered relation
  whereFilter: FilterNode | null;
  groupDimensions: string[];    // logical dimension names for this query
  baseFact?: string;            // present when metrics use a fact table
}

export interface MetricComputationContext {
  rows: RowSequence;                             // grouped rows for this output row
  groupKey: Record<string, any>;                // dimension values for this group
  evalMetric: (name: string) => number | undefined;
  helpers: MetricComputationHelpers;
}

export type MetricEval = (
  ctx: MetricComputationContext
) => number | undefined;

// V2 compatibility aliases
export type MetricEvalV2 = MetricEval;

export interface MetricDefinition {
  name: string;
  description?: string;
  baseFact?: string;      // which fact this metric is tied to (for fact-based metrics)
  attributes?: string[];  // logical attributes this metric depends on
  deps?: string[];        // dependent metrics
  eval: MetricEval;
}

export interface MetricDefinitionV2 extends MetricDefinition {
  exprAst?: MetricExpr;
}

export type MetricRegistry = Record<string, MetricDefinition>;

export interface MetricComputationHelpers {
  runtime: MetricRuntime;
  applyRowsetTransform(
    transformId: string,
    groupKey: Record<string, any>
  ): RowSequence;
}

// ---------------------------------------------------------------------------
// METRIC EXPRESSION AST
// ---------------------------------------------------------------------------

export type MetricExpr =
  | { kind: "Literal"; value: number }
  | { kind: "AttrRef"; name: string }
  | { kind: "MetricRef"; name: string }
  | { kind: "Call"; fn: string; args: MetricExpr[] }
  | {
      kind: "BinaryOp";
      op: "+" | "-" | "*" | "/";
      left: MetricExpr;
      right: MetricExpr;
    };

export const Expr = {
  lit(value: number): MetricExpr {
    return { kind: "Literal", value };
  },

  attr(name: string): MetricExpr {
    return { kind: "AttrRef", name };
  },

  metric(name: string): MetricExpr {
    return { kind: "MetricRef", name };
  },

  call(fn: string, ...args: MetricExpr[]): MetricExpr {
    return { kind: "Call", fn, args };
  },

  add(left: MetricExpr, right: MetricExpr): MetricExpr {
    return { kind: "BinaryOp", op: "+", left, right };
  },

  sub(left: MetricExpr, right: MetricExpr): MetricExpr {
    return { kind: "BinaryOp", op: "-", left, right };
  },

  mul(left: MetricExpr, right: MetricExpr): MetricExpr {
    return { kind: "BinaryOp", op: "*", left, right };
  },

  div(left: MetricExpr, right: MetricExpr): MetricExpr {
    return { kind: "BinaryOp", op: "/", left, right };
  },

  sum(attrName: string): MetricExpr {
    return this.call("sum", this.attr(attrName));
  },
  avg(attrName: string): MetricExpr {
    return this.call("avg", this.attr(attrName));
  },
  min(attrName: string): MetricExpr {
    return this.call("min", this.attr(attrName));
  },
  max(attrName: string): MetricExpr {
    return this.call("max", this.attr(attrName));
  },
  count(attrName: string | "*"): MetricExpr {
    return attrName === "*"
      ? this.call("count", this.attr("*"))
      : this.call("count", this.attr(attrName));
  },

  lastYear(metricName: string, anchorAttr: string): MetricExpr {
    return this.call("last_year", this.metric(metricName), this.attr(anchorAttr));
  },
};

export function collectAttrsAndDeps(expr: MetricExpr): {
  attrs: Set<string>;
  deps: Set<string>;
} {
  const attrs = new Set<string>();
  const deps = new Set<string>();

  function walk(e: MetricExpr) {
    switch (e.kind) {
      case "Literal":
        return;
      case "AttrRef":
        attrs.add(e.name);
        return;
      case "MetricRef":
        deps.add(e.name);
        return;
      case "Call":
        e.args.forEach(walk);
        return;
      case "BinaryOp":
        walk(e.left);
        walk(e.right);
        return;
    }
  }

  walk(expr);
  return { attrs, deps };
}

/* --------------------------------------------------------------------------
 * QUERY SPEC
 * -------------------------------------------------------------------------- */

export interface QuerySpec {
  dimensions: string[];                          // logical attributes for grain
  metrics: string[];                             // metric names
  where?: FilterContext;                         // attribute-level filter
  having?: (values: Record<string, number | undefined>) => boolean;
  // NOTE: no baseFact here; base fact is determined by metrics if present,
  // or falls back to a dimension relation when there are no metrics.
}

export type QuerySpecV2 = QuerySpec;

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
 * Build a base relation starting from a fact:
 * - factProjected: fact rows projected to logical attributes on the fact.
 * - joined: factProjected joined with any needed dimensions, projected to logical attrs.
 *
 * Assumes joins are from baseFact -> dimension using JoinEdge.
 */
function buildFactBaseRelation(
  model: SemanticModel,
  db: InMemoryDb,
  baseFact: string,
  requiredAttributes: Set<string>
): RowSequence {
  const joinLookup = buildJoinLookup(model.joins);
  const factRows = rowsToEnumerable(db.tables[baseFact] ?? []);

  const factJoinKeys = model.joins
    .filter((edge) => edge.fact === baseFact)
    .map((edge) => edge.factKey);

  const factProjected = factRows.select((row: Row) => ({
    ...mapLogicalAttributes(row, baseFact, model.attributes, requiredAttributes),
    ...factJoinKeys.reduce((acc, key) => {
      acc[key] = (row as any)[key];
      return acc;
    }, {} as Row),
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

  return joined;
}

/**
 * Build a base relation from a single relation (typically a dimension) when
 * there are no metrics and all attributes live on that one table.
 */
function buildDimensionBaseRelation(
  model: SemanticModel,
  db: InMemoryDb,
  relationName: string,
  requiredAttributes: Set<string>
): RowSequence {
  const rows = rowsToEnumerable(db.tables[relationName] ?? []);
  return rows.select((row: Row) =>
    mapLogicalAttributes(row, relationName, model.attributes, requiredAttributes)
  );
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
 * SIMPLE AGGREGATE METRICS
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

function aggregateRows(
  rows: RowSequence,
  attr: string | null,
  op: AggregationOperator
): number | undefined {
  const values = rows
    .select((r: Row) => (attr ? Number((r as any)[attr]) : 1))
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

function evalBinary(
  op: "+" | "-" | "*" | "/",
  left?: number,
  right?: number
): number | undefined {
  if (left == null || right == null) return undefined;
  switch (op) {
    case "+":
      return left + right;
    case "-":
      return left - right;
    case "*":
      return left * right;
    case "/":
      return right === 0 ? undefined : left / right;
    default:
      return undefined;
  }
}

export function compileMetricExpr(expr: MetricExpr): MetricEvalV2 {
  function validate(node: MetricExpr): void {
    if (node.kind === "Call") {
      const fn = node.fn.toLowerCase();
      if (["sum", "avg", "min", "max", "count"].includes(fn)) {
        if (node.args.length !== 1 || node.args[0].kind !== "AttrRef") {
          throw new Error(`${fn}() expects a single attribute reference argument`);
        }
      } else if (fn === "last_year") {
        const [metricArg, anchorArg] = node.args;
        if (node.args.length !== 2) {
          throw new Error("last_year() expects a metric reference and anchor attribute");
        }
        if (!metricArg || metricArg.kind !== "MetricRef") {
          throw new Error("last_year() first argument must be a MetricRef");
        }
        if (!anchorArg || anchorArg.kind !== "AttrRef") {
          throw new Error("last_year() second argument must be an AttrRef");
        }
      }
    }

    if (node.kind === "BinaryOp") {
      validate(node.left);
      validate(node.right);
    } else if (node.kind === "Call") {
      node.args.forEach(validate);
    }
  }

  validate(expr);

  const evaluator = (node: MetricExpr, ctx: MetricComputationContext): number | undefined => {
    switch (node.kind) {
      case "Literal":
        return node.value;
      case "AttrRef":
        return Number(ctx.groupKey[node.name]);
      case "MetricRef":
        return ctx.evalMetric(node.name);
      case "BinaryOp":
        return evalBinary(
          node.op,
          evaluator(node.left, ctx),
          evaluator(node.right, ctx)
        );
      case "Call": {
        const fn = node.fn.toLowerCase();
        if (["sum", "avg", "min", "max", "count"].includes(fn)) {
          const [arg] = node.args;
          const attr = (arg as any).name as string;
          if (fn === "count" && attr === "*") {
            return aggregateRows(ctx.rows, null, "count");
          }
          return aggregate(ctx.rows, attr, fn as AggregationOperator);
        }
        if (fn === "last_year") {
          const [metricArg, anchorArg] = node.args;
          const transformId = `last_year:${(anchorArg as any).name}`;
          const cacheLabel = `last_year(${(metricArg as any).name})`;
          const transformed = ctx.helpers.applyRowsetTransform(transformId, ctx.groupKey);
          return evaluateMetricRuntime(
            (metricArg as any).name,
            ctx.helpers.runtime,
            ctx.groupKey,
            transformed,
            cacheLabel
          );
        }
        throw new Error(`Unknown function: ${node.fn}`);
      }
    }
  };

  return (ctx) => evaluator(expr, ctx);
}

export function buildMetricFromExpr(opts: {
  name: string;
  baseFact?: string;
  expr: MetricExpr;
  description?: string;
}): MetricDefinitionV2 {
  const { attrs, deps } = collectAttrsAndDeps(opts.expr);
  return {
    name: opts.name,
    baseFact: opts.baseFact,
    description: opts.description,
    attributes: Array.from(attrs),
    deps: Array.from(deps),
    exprAst: opts.expr,
    eval: compileMetricExpr(opts.expr),
  };
}

/**
 * Aggregate metric over a single logical attribute.
 * Grain is entirely determined by the query; metric just aggregates over group rows.
 *
 * NOTE: baseFact is optional. For fact-based metrics you should set it so
 * the engine knows which fact table to use when metrics are present.
 */
export function aggregateMetric(
  name: string,
  baseFact: string,
  attr: string,
  op: AggregationOperator = "sum"
): MetricDefinitionV2 {
  let expr: MetricExpr;
  switch (op) {
    case "sum":
      expr = Expr.sum(attr);
      break;
    case "avg":
      expr = Expr.avg(attr);
      break;
    case "min":
      expr = Expr.min(attr);
      break;
    case "max":
      expr = Expr.max(attr);
      break;
    case "count":
      expr = Expr.count(attr);
      break;
    default:
      throw new Error(`Unsupported aggregation operator: ${op}`);
  }

  return buildMetricFromExpr({
    name,
    baseFact,
    expr,
  });
}

export function lastYearMetric(
  name: string,
  baseFact: string,
  baseMetricName: string,
  anchorAttr: string
): MetricDefinitionV2 {
  const expr = Expr.lastYear(baseMetricName, anchorAttr);

  return buildMetricFromExpr({
    name,
    baseFact,
    expr,
  });
}

/* --------------------------------------------------------------------------
 * ROWSET TRANSFORMS (E.G. LAST-YEAR)
 * -------------------------------------------------------------------------- */

export function rowsetTransformMetric(opts: {
  name: string;
  baseMetric: string;
  transformId: string;
  description?: string;
}): MetricDefinition {
  // NOTE: Opaque metric – not backed by MetricExpr. Not visible to DSL tools.
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
      const value = evaluateMetric(
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
  runtime: MetricRuntime,
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

  // 1. Find target anchor values (e.g., last-year week codes) for this group's anchor.
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
      if (dim === transform.anchorAttr) return true; // this is the shifted dimension
      return (row as any)[dim] === groupKey[dim];
    });
  });
}

/* --------------------------------------------------------------------------
 * METRIC EVALUATION
 * -------------------------------------------------------------------------- */

function evaluateMetric(
  metricName: string,
  runtime: MetricRuntime,
  groupKey: Record<string, any>,
  rows: RowSequence,
  cacheLabel?: string,
  cache?: Map<string, number | undefined>
): number | undefined {
  const registry = runtime.model.metrics;
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
    evaluateMetric(dep, runtime, groupKey, rows, undefined, workingCache);

  const helpers: MetricComputationHelpers = {
    runtime,
    applyRowsetTransform: (transformId, gk) =>
      applyRowsetTransform(runtime, transformId, gk),
  };

  const value = metric.eval({ rows, groupKey, evalMetric, helpers });
  workingCache.set(cacheKey, value);
  return value;
}

export function evaluateMetricRuntime(
  metricName: string,
  runtime: MetricRuntime,
  groupKey: Record<string, any>,
  rows: RowSequence,
  cacheLabel?: string,
  cache?: Map<string, number | undefined>
): number | undefined {
  return evaluateMetric(metricName, runtime, groupKey, rows, cacheLabel, cache);
}

/* --------------------------------------------------------------------------
 * MAIN QUERY EXECUTION
 * -------------------------------------------------------------------------- */

export function runSemanticQuery(
  env: { db: InMemoryDb; model: SemanticModel },
  spec: QuerySpec
): Row[] {
  const { db, model } = env;
  const dimensions = spec.dimensions;

  const whereFields = collectFilterFields(spec.where);

  const metricAttributesByFact = new Map<string | undefined, Set<string>>();
  const metricsByFact = new Map<string | undefined, string[]>();

  spec.metrics.forEach((m) => {
    const def = model.metrics[m];
    const fact = def?.baseFact;
    const list = metricsByFact.get(fact) ?? [];
    list.push(m);
    metricsByFact.set(fact, list);

    const attrSet = metricAttributesByFact.get(fact) ?? new Set<string>();
    def?.attributes?.forEach((a) => attrSet.add(a));
    metricAttributesByFact.set(fact, attrSet);
  });

  const factNames = Array.from(metricsByFact.keys()).filter(
    (f): f is string => !!f
  );

  // Dimension-only query (no metrics)
  if (spec.metrics.length === 0) {
    const relationsUsed = new Set<string>();
    const requiredAttrs = new Set<string>([...dimensions]);
    whereFields.forEach((f) => requiredAttrs.add(f));

    for (const attr of requiredAttrs) {
      const def = model.attributes[attr];
      if (!def) continue;
      relationsUsed.add(def.relation);
    }

    if (relationsUsed.size === 0) {
      return [];
    }

    if (relationsUsed.size > 1) {
      throw new Error(
        `Dimension-only query references attributes from multiple relations ` +
          `(${Array.from(relationsUsed).join(", ")}); engine cannot relate them without a fact table`
      );
    }

    const singleRelation = Array.from(relationsUsed)[0];
    const relation = buildDimensionBaseRelation(
      model,
      db,
      singleRelation,
      requiredAttrs
    );

    const filtered = applyWhereFilter(
      relation,
      spec.where,
      Array.from(requiredAttrs)
    );

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
      results.push(groupKey);
    });

    return results;
  }

  // Metrics provided but none declared baseFact -> treat as dimension metrics
  if (factNames.length === 0) {
    const requiredAttrs = new Set<string>([...dimensions]);
    whereFields.forEach((f) => requiredAttrs.add(f));
    (metricAttributesByFact.get(undefined) ?? new Set()).forEach((a) =>
      requiredAttrs.add(a)
    );

    const relationsUsed = new Set<string>();
    for (const attr of requiredAttrs) {
      const def = model.attributes[attr];
      if (!def) continue;
      relationsUsed.add(def.relation);
    }

    if (relationsUsed.size !== 1) {
      throw new Error(
        `Metrics provided but none declares a baseFact; engine cannot choose a fact table. ` +
          `Please set baseFact on fact metrics or constrain attributes to one relation.`
      );
    }

    const singleRelation = Array.from(relationsUsed)[0];
    const relation = buildDimensionBaseRelation(
      model,
      db,
      singleRelation,
      requiredAttrs
    );
    const filtered = applyWhereFilter(
      relation,
      spec.where,
      Array.from(requiredAttrs)
    );
    const whereNode = normalizeFilterContext(spec.where);

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

      const runtime: MetricRuntime = {
        model,
        db,
        baseFact: undefined,
        relation: relation,
        whereFilter: whereNode,
        groupDimensions: dimensions,
      };

      const metricValues: Record<string, number | undefined> = {};
      const metricCache = new Map<string, number | undefined>();
      (metricsByFact.get(undefined) ?? []).forEach((m) => {
        metricValues[m] = evaluateMetric(
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

  const primaryFact = factNames[0];
  const frameRequiredAttrs = new Set<string>([...dimensions]);
  whereFields.forEach((f) => frameRequiredAttrs.add(f));
  (metricAttributesByFact.get(primaryFact) ?? new Set()).forEach((a) =>
    frameRequiredAttrs.add(a)
  );
  (metricAttributesByFact.get(undefined) ?? new Set()).forEach((a) =>
    frameRequiredAttrs.add(a)
  );

  const frameRelation = buildFactBaseRelation(
    model,
    db,
    primaryFact,
    frameRequiredAttrs
  );

  const whereNode = normalizeFilterContext(spec.where);
  const frameFiltered = applyWhereFilter(
    frameRelation,
    spec.where,
    Array.from(frameRequiredAttrs)
  );

  const frameGroups = frameFiltered.groupBy(
    (row: Row) => keyFromRow(row, dimensions),
    (row: Row) => row
  );

  const results: Row[] = [];
  const frameGroupLookup = new Map<string, RowSequence>();

  frameGroups.forEach((group) => {
    const sample = group.first();
    const groupKey: Record<string, any> = {};
    if (sample) {
      dimensions.forEach((d) => (groupKey[d] = (sample as any)[d]));
    }

    const keyStr = keyFromGroup(groupKey);
    frameGroupLookup.set(keyStr, group);

    const runtime: MetricRuntime = {
      model,
      db,
      baseFact: primaryFact,
      relation: frameRelation,
      whereFilter: whereNode,
      groupDimensions: dimensions,
    };

    const metricValues: Record<string, number | undefined> = {};
    const metricCache = new Map<string, number | undefined>();

    (metricsByFact.get(primaryFact) ?? []).forEach((m) => {
      metricValues[m] = evaluateMetric(
        m,
        runtime,
        groupKey,
        group,
        undefined,
        metricCache
      );
    });

    results.push({ ...groupKey, ...metricValues });
  });

  for (const [fact, metricNames] of metricsByFact.entries()) {
    if (!fact || fact === primaryFact) continue;

    const attrsForFact = new Set<string>([...dimensions]);
    whereFields.forEach((f) => attrsForFact.add(f));
    (metricAttributesByFact.get(fact) ?? new Set()).forEach((a) =>
      attrsForFact.add(a)
    );

    const factRelation = buildFactBaseRelation(
      model,
      db,
      fact,
      attrsForFact
    );

    const factFiltered = applyWhereFilter(
      factRelation,
      spec.where,
      Array.from(attrsForFact)
    );

    const factGroups = factFiltered.groupBy(
      (row: Row) => keyFromRow(row, dimensions),
      (row: Row) => row
    );

    const factMetricMap = new Map<string, Record<string, number | undefined>>();

    factGroups.forEach((group) => {
      const sample = group.first();
      if (!sample) return;

      const groupKey: Record<string, any> = {};
      dimensions.forEach((d) => (groupKey[d] = (sample as any)[d]));
      const keyStr = keyFromGroup(groupKey);

      const runtime: MetricRuntime = {
        model,
        db,
        baseFact: fact,
        relation: factRelation,
        whereFilter: whereNode,
        groupDimensions: dimensions,
      };

      const metricCache = new Map<string, number | undefined>();
      const values: Record<string, number | undefined> = {};

      metricNames.forEach((m) => {
        values[m] = evaluateMetric(
          m,
          runtime,
          groupKey,
          group,
          undefined,
          metricCache
        );
      });

      factMetricMap.set(keyStr, values);
    });

    for (const row of results) {
      const groupKey: Record<string, any> = {};
      dimensions.forEach((d) => (groupKey[d] = row[d]));
      const keyStr = keyFromGroup(groupKey);
      const factValues = factMetricMap.get(keyStr) ?? {};
      Object.assign(row, factValues);
    }
  }

  const dimensionMetrics = metricsByFact.get(undefined) ?? [];
  if (dimensionMetrics.length) {
    for (const row of results) {
      const groupKey: Record<string, any> = {};
      dimensions.forEach((d) => (groupKey[d] = row[d]));
      const keyStr = keyFromGroup(groupKey);
      const group = frameGroupLookup.get(keyStr) ?? rowsToEnumerable([]);

      const runtime: MetricRuntime = {
        model,
        db,
        baseFact: undefined,
        relation: frameRelation,
        whereFilter: whereNode,
        groupDimensions: dimensions,
      };

      const metricCache = new Map<string, number | undefined>();
      dimensionMetrics.forEach((m) => {
        row[m] = evaluateMetric(
          m,
          runtime,
          groupKey,
          group,
          undefined,
          metricCache
        );
      });
    }
  }

  if (spec.having) {
    return results.filter((r) => spec.having?.(r as any));
  }

  return results;
}

