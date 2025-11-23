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
import { compileDslToModel } from "./dsl";

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

function normalizeFilterContext(
  context?: FilterContext | null
): FilterNode | null {
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

export function resolveBindingsInFilter(
  filterNode: FilterNode | null,
  bindings: Record<string, any>
): FilterNode | null {
  if (!filterNode) return null;

  const resolveValue = (value: any): any => {
    if (typeof value === "string" && value.startsWith(":")) {
      const key = value.slice(1);
      if (!(key in bindings)) {
        throw new Error(`Missing binding '${value}'`);
      }
      return bindings[key];
    }

    if (Array.isArray(value)) {
      return value.map((v) => resolveValue(v));
    }

    if (value && typeof value === "object") {
      const clone: Record<string, any> = { ...value };
      if ("value" in clone) clone.value = resolveValue(clone.value);
      if ("from" in clone) clone.from = resolveValue(clone.from);
      if ("to" in clone) clone.to = resolveValue(clone.to);
      return clone;
    }

    return value;
  };

  if (filterNode.kind === "expression") {
    return {
      ...filterNode,
      value: resolveValue(filterNode.value),
      value2: resolveValue(filterNode.value2),
    } as FilterExpression;
  }

  return {
    kind: filterNode.kind,
    filters: filterNode.filters.map((child) => resolveBindingsInFilter(child, bindings)!) as FilterNode[],
  };
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
  where?: FilterContext | null,
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

function collectFilterFields(ctx?: FilterContext | null): Set<string> {
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
  /** Physical table where this attribute lives (fact or dimension). */
  table: string;

  /** Physical column name; defaults to the attribute ID. */
  column?: string;
}

export interface FactDefinition {
  /** Physical table name for this fact. Defaults to the fact ID. */
  table?: string;
}

export interface DimensionDefinition {
  /** Physical table name for this dimension. Defaults to the dimension ID. */
  table?: string;
}

export interface JoinEdge {
  fact: string; // fact ID (registry key)
  dimension: string; // dimension ID (registry key)
  factKey: string; // physical column name on fact
  dimensionKey: string; // physical column name on dimension
}

export interface RowsetTransformDefinition {
  /** Physical transform lookup table (e.g. tradyrwk_transform). */
  table: string;

  /** Attribute ID used as "current period" (e.g. tradyrwkcode). */
  anchorAttr: string;

  /** Column on transform.table that matches current period. */
  fromColumn: string;

  /** Column on transform.table that matches factAttr (e.g. tradyrwkcode_lastyear). */
  toColumn: string;

  /** Attribute ID on fact rows to filter by (e.g. tradyrwkcode). */
  factAttr: string;
}

export interface TableTransformMappingRow {
  key: any;
  mappedKey: any;
}

export interface TableTransformDefinition {
  /** Inline two-column mapping table. */
  table?: TableTransformMappingRow[];

  /** Physical relation name containing the transform mapping. */
  relation?: string;

  /** Column name for the input key when using a relation. Defaults to "key". */
  keyColumn?: string;

  /** Column name for the mapped key when using a relation. Defaults to "mappedKey". */
  mappedColumn?: string;

  /** Attribute ID used as the input anchor (e.g. week). */
  inputAttr: string;

  /** Attribute ID used to filter fact rows after mapping. */
  outputAttr: string;

  /** Optional fallback mapped key when no match is found. */
  fallbackMappedKey?: any;
}

export interface Schema {
  facts: Record<string, FactDefinition>;
  dimensions: Record<string, DimensionDefinition>;
  attributes: Record<string, LogicalAttribute>;
  joins: JoinEdge[];
}

export interface SemanticModel extends Schema {
  metrics: MetricRegistry;
  rowsetTransforms?: Record<string, RowsetTransformDefinition>;
  tableTransforms?: Record<string, TableTransformDefinition>;
}

/* --------------------------------------------------------------------------
 * METRICS (GRAIN-AGNOSTIC)
 * -------------------------------------------------------------------------- */

export type AggregationOperator = "sum" | "avg" | "count" | "min" | "max";

export type WindowFrameSpec =
  | { kind: "rolling"; count: number }
  | { kind: "cumulative" }
  | { kind: "offset"; offset: number };

export interface MetricRuntime {
  model: SemanticModel;
  db: InMemoryDb;
  relation: RowSequence;        // joined + where-filtered relation
  whereFilter: FilterNode | null;
  groupDimensions: string[];    // logical dimension names for this query
  bindings: Record<string, any>;
  baseFact?: string;            // present when metrics use a fact table
}

export type MetricRuntimeV2 = MetricRuntime;

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
  applyTableTransform(
    transformId: string,
    groupKey: Record<string, any>
  ): RowSequence;
  bind(name: string): any;
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
      kind: "Window";
      base: MetricExpr;
      partitionBy: string[];
      orderBy: string;
      frame: WindowFrameSpec;
      aggregate: AggregationOperator;
    }
  | {
      kind: "BinaryOp";
      op: "+" | "-" | "*" | "/";
      left: MetricExpr;
      right: MetricExpr;
    }
  | {
      kind: "Transform";
      transformId: string;
      transformKind: "table";
      base: MetricExpr;
      inputAttr?: string;
      outputAttr?: string;
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

  tableTransform(
    metricName: string,
    transformId: string,
    inputAttr?: string,
    outputAttr?: string
  ): MetricExpr {
    return {
      kind: "Transform",
      transformId,
      transformKind: "table",
      base: this.metric(metricName),
      inputAttr,
      outputAttr,
    } as MetricExpr;
  },

  window(
    base: MetricExpr,
    opts: {
      partitionBy?: string[];
      orderBy: string;
      frame: WindowFrameSpec;
      aggregate: AggregationOperator;
    }
  ): MetricExpr {
    return {
      kind: "Window",
      base,
      partitionBy: opts.partitionBy ?? [],
      orderBy: opts.orderBy,
      frame: opts.frame,
      aggregate: opts.aggregate,
    } as MetricExpr;
  },
};

export interface MetricReferenceSummary {
  attrs: Set<string>;
  deps: Set<string>;
  literals: number[];
  transforms: Set<string>;
  tableTransforms: Set<string>;
}

export function collectMetricReferences(expr: MetricExpr): MetricReferenceSummary {
  const attrs = new Set<string>();
  const deps = new Set<string>();
  const transforms = new Set<string>();
  const tableTransforms = new Set<string>();
  const literals: number[] = [];

  function walk(e: MetricExpr) {
    switch (e.kind) {
      case "Literal":
        literals.push(e.value);
        return;
      case "AttrRef":
        attrs.add(e.name);
        return;
      case "MetricRef":
        deps.add(e.name);
        return;
      case "Transform":
        tableTransforms.add(e.transformId);
        if (e.inputAttr) attrs.add(e.inputAttr);
        if (e.outputAttr) attrs.add(e.outputAttr);
        walk(e.base);
        return;
      case "Call": {
        const fn = e.fn.toLowerCase();
        if (fn === "last_year" && e.args[1]?.kind === "AttrRef") {
          transforms.add(`last_year:${(e.args[1] as any).name}`);
        }
        e.args.forEach(walk);
        return;
      }
      case "Window":
        e.partitionBy.forEach((attr) => attrs.add(attr));
        attrs.add(e.orderBy);
        walk(e.base);
        return;
      case "BinaryOp":
        walk(e.left);
        walk(e.right);
        return;
    }
  }

  walk(expr);
  return { attrs, deps, literals, transforms, tableTransforms };
}

export function collectAttrsAndDeps(expr: MetricExpr): {
  attrs: Set<string>;
  deps: Set<string>;
} {
  const { attrs, deps } = collectMetricReferences(expr);
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

export interface ExecutionOptions {
  bindings?: Record<string, any>;
}

export class SemanticEngine {
  private schema: Schema;
  private model: SemanticModel;
  private queries: Record<string, QuerySpecV2> = {};
  private db: InMemoryDb;

  private constructor(schema: Schema, db: InMemoryDb) {
    this.schema = schema;
    this.model = { ...schema, metrics: {} };
    this.db = db;
  }

  static fromSchema(schema: Schema, db: InMemoryDb): SemanticEngine {
    return new SemanticEngine(schema, db);
  }

  useDslFile(text: string): this {
    const { model, queries } = compileDslToModel(text, this.model);
    this.model = model;
    this.queries = { ...this.queries, ...queries };
    return this;
  }

  registerMetric(def: MetricDefinition): this {
    this.model.metrics[def.name] = def;
    return this;
  }

  registerQuery(name: string, spec: QuerySpecV2): this {
    this.queries[name] = spec;
    return this;
  }

  getModel(): SemanticModel {
    return this.model;
  }

  getQuery(name: string): QuerySpecV2 {
    const q = this.queries[name];
    if (!q) {
      throw new Error(`Unknown query: ${name}`);
    }
    return q;
  }

  runQuery(nameOrSpec: string | QuerySpecV2, bindings?: FilterContext) {
    const spec =
      typeof nameOrSpec === "string" ? this.getQuery(nameOrSpec) : nameOrSpec;
    return runSemanticQuery({ db: this.db, model: this.model }, spec);
  }
}

/* --------------------------------------------------------------------------
 * SEMANTIC HELPERS
 * -------------------------------------------------------------------------- */

interface NormalizedAttribute {
  id: string;
  table: string;
  column: string;
}

interface NormalizedFact {
  id: string;
  table: string;
}

interface NormalizedDimension {
  id: string;
  table: string;
}

function normalizeAttribute(id: string, def: LogicalAttribute): NormalizedAttribute {
  return {
    id,
    table: def.table,
    column: def.column ?? id,
  };
}

function normalizeFact(id: string, def: FactDefinition): NormalizedFact {
  return { id, table: def.table ?? id };
}

function normalizeDimension(
  id: string,
  def: DimensionDefinition
): NormalizedDimension {
  return { id, table: def.table ?? id };
}

function attributesForTable(
  table: string,
  attributes: Record<string, LogicalAttribute>
): NormalizedAttribute[] {
  return Object.entries(attributes)
    .map(([id, def]) => normalizeAttribute(id, def))
    .filter((a) => a.table === table);
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
  table: string,
  attrRegistry: Record<string, LogicalAttribute>,
  needed: Set<string>
): Row {
  const attrs = attributesForTable(table, attrRegistry);
  const mapped: Row = {};
  for (const attr of attrs) {
    const logical = attr.id;
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
  const normalizedFact = normalizeFact(baseFact, model.facts[baseFact]);
  const factRows = rowsToEnumerable(db.tables[normalizedFact.table] ?? []);

  const factJoinKeys = model.joins
    .filter((edge) => edge.fact === baseFact)
    .map((edge) => edge.factKey);

  const factProjected = factRows.select((row: Row) => ({
    ...mapLogicalAttributes(
      row,
      normalizedFact.table,
      model.attributes,
      requiredAttributes
    ),
    ...factJoinKeys.reduce((acc, key) => {
      acc[key] = (row as any)[key];
      return acc;
    }, {} as Row),
  }));

  const neededRelations = new Set<string>();
  for (const attr of requiredAttributes) {
    const def = model.attributes[attr];
    if (!def) continue;
    if (def.table !== normalizedFact.table) {
      neededRelations.add(def.table);
    }
  }

  let joined: RowSequence = factProjected;

  neededRelations.forEach((table) => {
    const dimensionId = Object.entries(model.dimensions).find(
      ([_, def]) => normalizeDimension(_, def).table === table
    )?.[0];
    const join = joinLookup.get(`${baseFact}|${dimensionId}`);
    if (!join) {
      throw new Error(
        `No join defined from fact ${baseFact} to dimension ${table}`
      );
    }
    const normalizedDimension = normalizeDimension(
      join.dimension,
      model.dimensions[join.dimension]
    );
    const dimensionRows = rowsToEnumerable(
      db.tables[normalizedDimension.table] ?? []
    );

    const mappedDimension = dimensionRows.select((row: Row) => ({
      __joinKey: (row as any)[join.dimensionKey],
      ...mapLogicalAttributes(
        row,
        normalizedDimension.table,
        model.attributes,
        requiredAttributes
      ),
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
  tableName: string,
  requiredAttributes: Set<string>
): RowSequence {
  const rows = rowsToEnumerable(db.tables[tableName] ?? []);
  return rows.select((row: Row) =>
    mapLogicalAttributes(row, tableName, model.attributes, requiredAttributes)
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

function aggregateWindowValues(
  values: Array<number | undefined>,
  op: AggregationOperator
): number | undefined {
  const defined = values
    .filter((v): v is number => v !== undefined && !Number.isNaN(v))
    .map((v) => Number(v));

  if (op === "count") return defined.length;
  if (!defined.length) return undefined;

  switch (op) {
    case "sum":
      return defined.reduce((a, b) => a + b, 0);
    case "avg":
      return defined.reduce((a, b) => a + b, 0) / defined.length;
    case "min":
      return Math.min(...defined);
    case "max":
      return Math.max(...defined);
    default:
      return undefined;
  }
}

function validateWindowFrame(frame: WindowFrameSpec): void {
  if (!frame || typeof frame !== "object") {
    throw new Error("windowBy() frame must be provided");
  }

  if (frame.kind === "rolling") {
    if (!Number.isInteger(frame.count) || frame.count <= 0) {
      throw new Error("rolling window count must be a positive integer");
    }
  } else if (frame.kind === "offset") {
    if (!Number.isInteger(frame.offset)) {
      throw new Error("offset window requires an integer offset");
    }
  }
}

function normalizeWindowFrame(
  frame: WindowFrameSpec
): { mode: "offset"; offset: number } | {
  mode: "window";
  frame: { preceding: number; following: number; requireFullWindow: boolean };
} {
  if (frame.kind === "offset") {
    return { mode: "offset", offset: frame.offset };
  }

  if (frame.kind === "rolling") {
    const preceding = Math.max(0, frame.count - 1);
    return {
      mode: "window",
      frame: { preceding, following: 0, requireFullWindow: false },
    };
  }

  // cumulative
  return {
    mode: "window",
    frame: {
      preceding: Number.MAX_SAFE_INTEGER,
      following: 0,
      requireFullWindow: false,
    },
  };
}

function evaluateWindowNode(
  node: Extract<MetricExpr, { kind: "Window" }>,
  ctx: MetricComputationContext,
  evaluator: (node: MetricExpr, ctx: MetricComputationContext) => number | undefined
): number | undefined {
  const runtime = ctx.helpers.runtime;
  const cacheBucket: Map<string, Map<string, number | undefined>> =
    (runtime as any).__windowCache ?? new Map();
  (runtime as any).__windowCache = cacheBucket;

  const cacheKey = JSON.stringify({
    partitionBy: node.partitionBy,
    orderBy: node.orderBy,
    frame: node.frame,
    aggregate: node.aggregate,
    base: node.base,
    grain: runtime.groupDimensions,
  });

  if (!cacheBucket.has(cacheKey)) {
    const normalized = normalizeWindowFrame(node.frame);

    const relationGroups = runtime.relation
      .groupBy((row: Row) => keyFromRow(row, runtime.groupDimensions), (row) => row)
      .toArray();

    const metricCache = new Map<string, number | undefined>();

    const partitionKey = (groupKey: Record<string, any>) => {
      const key: Record<string, any> = {};
      node.partitionBy.forEach((p) => (key[p] = groupKey[p]));
      return JSON.stringify(key);
    };

    const orderValue = (groupKey: Record<string, any>) => groupKey[node.orderBy];

    const rowsWithValues = relationGroups.map((group) => {
      const sample = group.first();
      const groupKey: Record<string, any> = {};
      runtime.groupDimensions.forEach((d) => (groupKey[d] = (sample as any)?.[d]));

      const evalMetricForGroup = (metricName: string) =>
        evaluateMetricRuntime(metricName, runtime, groupKey, group, undefined, metricCache);

      const value = evaluator(node.base, {
        rows: group,
        groupKey,
        evalMetric: evalMetricForGroup,
        helpers: ctx.helpers,
      });

      return { groupKey, value };
    });

    const valuesByGroup = new Map<string, number | undefined>();

    if (normalized.mode === "offset") {
      const groups = Enumerable.from(rowsWithValues)
        .groupBy((g) => partitionKey(g.groupKey))
        .toArray();

      groups.forEach((group) => {
        const ordered = group
          .orderBy((g) => orderValue(g.groupKey))
          .toArray();

        ordered.forEach((item, idx) => {
          const targetIdx = idx + normalized.offset;
          const target = ordered[targetIdx];
          const aggregated = aggregateWindowValues(
            target ? [target.value] : [],
            node.aggregate
          );
          valuesByGroup.set(keyFromGroup(item.groupKey), aggregated);
        });
      });
    } else {
      Enumerable.from(rowsWithValues)
        .windowBy(
          (g) => partitionKey(g.groupKey),
          (g) => orderValue(g.groupKey),
          normalized.frame,
          ({ row, window }) => {
            const aggregated = aggregateWindowValues(
              window.map((w) => w.value),
              node.aggregate
            );
            valuesByGroup.set(keyFromGroup(row.groupKey), aggregated);
            return aggregated;
          }
        )
        .toArray();
    }

    cacheBucket.set(cacheKey, valuesByGroup);
  }

  const cachedMap = cacheBucket.get(cacheKey)!;
  return cachedMap.get(keyFromGroup(ctx.groupKey));
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

    if (node.kind === "Window") {
      if (!node.orderBy) {
        throw new Error("windowBy() requires an orderBy attribute");
      }
      if (!node.frame) {
        throw new Error("windowBy() requires a frame definition");
      }
      if (!node.aggregate) {
        throw new Error("windowBy() requires an aggregate");
      }

      const agg = node.aggregate.toLowerCase();
      if (!("sum|avg|min|max|count".split("|") as string[]).includes(agg)) {
        throw new Error(`Unsupported window aggregate: ${node.aggregate}`);
      }

      validateWindowFrame(node.frame);
      validate(node.base);
    }

    if (node.kind === "BinaryOp") {
      validate(node.left);
      validate(node.right);
    } else if (node.kind === "Call") {
      node.args.forEach(validate);
    } else if (node.kind === "Transform") {
      validate(node.base);
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
      case "Window":
        return evaluateWindowNode(node, ctx, evaluator);
      case "Transform": {
        if (node.transformKind === "table") {
          const cacheLabel = `${node.transformId}:transform`;
          const transformed = ctx.helpers.applyTableTransform(
            node.transformId,
            ctx.groupKey
          );
          if (node.base.kind !== "MetricRef") {
            throw new Error("Table transforms currently require a MetricRef base");
          }
          return evaluateMetricRuntime(
            node.base.name,
            ctx.helpers.runtime,
            ctx.groupKey,
            transformed,
            cacheLabel
          );
        }
        return undefined;
      }
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

export function defineTableTransform(opts: {
  name: string;
  table: TableTransformMappingRow[];
  inputAttr: string;
  outputAttr: string;
  fallbackMappedKey?: any;
}): { name: string; definition: TableTransformDefinition } {
  return {
    name: opts.name,
    definition: {
      table: opts.table,
      inputAttr: opts.inputAttr,
      outputAttr: opts.outputAttr,
      fallbackMappedKey: opts.fallbackMappedKey,
    },
  };
}

export function defineTableTransformFromRelation(opts: {
  name: string;
  relation: string;
  keyColumn: string;
  mappedColumn: string;
  inputAttr: string;
  outputAttr: string;
  fallbackMappedKey?: any;
}): { name: string; definition: TableTransformDefinition } {
  return {
    name: opts.name,
    definition: {
      relation: opts.relation,
      keyColumn: opts.keyColumn,
      mappedColumn: opts.mappedColumn,
      inputAttr: opts.inputAttr,
      outputAttr: opts.outputAttr,
      fallbackMappedKey: opts.fallbackMappedKey,
    },
  };
}

export function tableTransformMetric(opts: {
  name: string;
  baseMetric: string;
  transformId: string;
  baseFact?: string;
  description?: string;
  inputAttr?: string;
  outputAttr?: string;
}): MetricDefinitionV2 {
  const expr = Expr.tableTransform(
    opts.baseMetric,
    opts.transformId,
    opts.inputAttr,
    opts.outputAttr
  );
  return {
    name: opts.name,
    baseFact: opts.baseFact,
    attributes: [opts.inputAttr, opts.outputAttr].filter(
      (a): a is string => Boolean(a)
    ),
    deps: [opts.baseMetric],
    description: opts.description,
    exprAst: expr,
    eval: compileMetricExpr(expr),
  };
}

/* --------------------------------------------------------------------------
 * STATIC ANALYSIS + VALIDATION
 * -------------------------------------------------------------------------- */

export interface ValidationIssue {
  metric: string;
  message: string;
  path?: string[];
  suggestion?: string;
}

export interface ValidationResult {
  metric: string;
  ok: boolean;
  errors: ValidationIssue[];
}

function isAttributeReachable(
  baseFact: string,
  attrName: string,
  model: SemanticModel
): boolean {
  const attr = model.attributes[attrName];
  if (!attr) return false;

  const normalizedFact = normalizeFact(baseFact, model.facts[baseFact]);
  if (attr.table === normalizedFact.table) return true;

  const dimensionId = Object.entries(model.dimensions).find(
    ([id, def]) => normalizeDimension(id, def).table === attr.table
  )?.[0];
  if (!dimensionId) return false;

  return model.joins.some(
    (edge) => edge.fact === baseFact && edge.dimension === dimensionId
  );
}

function collectDependencyGraph(model: SemanticModel): Map<string, string[]> {
  const graph = new Map<string, string[]>();

  for (const [name, metric] of Object.entries(model.metrics)) {
    let deps: string[] = [];
    if ((metric as MetricDefinitionV2).exprAst) {
      const refs = collectMetricReferences((metric as MetricDefinitionV2).exprAst!);
      deps = Array.from(refs.deps);
    } else if (metric.deps) {
      deps = metric.deps;
    }
    graph.set(name, deps);
  }

  return graph;
}

function detectCircularDependencies(
  model: SemanticModel
): Map<string, string[]> {
  const graph = collectDependencyGraph(model);
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const cycles = new Map<string, string[]>();

  function dfs(node: string, stack: string[]) {
    if (visited.has(node)) return;
    if (visiting.has(node)) {
      const idx = stack.indexOf(node);
      const cycle = stack.slice(idx).concat(node);
      cycle.forEach((m) => cycles.set(m, cycle));
      return;
    }

    visiting.add(node);
    const deps = graph.get(node) ?? [];
    deps.forEach((dep) => dfs(dep, [...stack, node]));
    visiting.delete(node);
    visited.add(node);
  }

  for (const metric of graph.keys()) {
    if (!visited.has(metric)) {
      dfs(metric, []);
    }
  }

  return cycles;
}

function metricReferences(
  metric: MetricDefinition | MetricDefinitionV2
): MetricReferenceSummary {
  if ((metric as MetricDefinitionV2).exprAst) {
    return collectMetricReferences((metric as MetricDefinitionV2).exprAst!);
  }

  return {
    attrs: new Set(metric.attributes ?? []),
    deps: new Set(metric.deps ?? []),
    literals: [],
    transforms: new Set<string>(),
    tableTransforms: new Set<string>(),
  };
}

export function validateMetric(
  model: SemanticModel,
  metricName: string
): ValidationResult {
  const metric = model.metrics[metricName];
  const errors: ValidationIssue[] = [];

  if (!metric) {
    return {
      metric: metricName,
      ok: false,
      errors: [
        {
          metric: metricName,
          message: `Metric "${metricName}" is not defined`,
        },
      ],
    };
  }

  const refs = metricReferences(metric as MetricDefinitionV2);

  if ((metric as MetricDefinitionV2).exprAst) {
    for (const dep of refs.deps) {
      if (!model.metrics[dep]) {
        errors.push({
          metric: metricName,
          message: `Metric "${metricName}" references unknown metric "${dep}"`,
          suggestion: "Declare the metric dependency or fix the reference name.",
        });
      }
    }
  }

  const baseFact = metric.baseFact;

  refs.attrs.forEach((attr) => {
    if (!model.attributes[attr]) {
      errors.push({
        metric: metricName,
        message: `Metric "${metricName}" references unknown attribute "${attr}"`,
        suggestion: "Add the attribute to the semantic model or correct the reference.",
      });
      return;
    }

    if (baseFact) {
      const reachable = isAttributeReachable(baseFact, attr, model);
      if (!reachable) {
        errors.push({
          metric: metricName,
          message: `Attribute "${attr}" is not reachable from base fact "${baseFact}"`,
          suggestion: "Add a join to connect the attribute's table to the base fact or choose a compatible base fact.",
        });
      }
    }
  });

  if (!baseFact) {
    const attrTables = new Set(
      Array.from(refs.attrs)
        .map((a) => model.attributes[a]?.table)
        .filter((t): t is string => Boolean(t))
    );

    if (attrTables.size > 1) {
      errors.push({
        metric: metricName,
        message: `Metric "${metricName}" mixes attributes from multiple relations without declaring a base fact`,
        suggestion: "Set baseFact or limit the metric to a single relation.",
      });
    }
  }

  if (baseFact && refs.deps.size) {
    refs.deps.forEach((dep) => {
      const depBase = model.metrics[dep]?.baseFact;
      if (depBase && depBase !== baseFact) {
        errors.push({
          metric: metricName,
          message: `Metric "${metricName}" depends on cross-fact metric "${dep}" (${depBase})`,
          suggestion: "Align baseFact across dependent metrics or split the computation.",
        });
      }
    });
  }

  refs.transforms.forEach((transformId) => {
    const transform = model.rowsetTransforms?.[transformId];
    if (!transform) {
      errors.push({
        metric: metricName,
        message: `Transform "${transformId}" is not defined for metric "${metricName}"`,
        suggestion: "Add the rowset transform configuration to the semantic model.",
      });
      return;
    }

    if (!model.attributes[transform.anchorAttr]) {
      errors.push({
        metric: metricName,
        message: `Transform anchor attribute "${transform.anchorAttr}" is not in the semantic model`,
        suggestion: "Define the anchor attribute so it can be used for period shifting.",
      });
    }

    if (!model.attributes[transform.factAttr]) {
      errors.push({
        metric: metricName,
        message: `Transform fact attribute "${transform.factAttr}" is not in the semantic model`,
        suggestion: "Define the fact attribute or correct the transform configuration.",
      });
    }

    if (baseFact && !isAttributeReachable(baseFact, transform.factAttr, model)) {
      errors.push({
        metric: metricName,
        message: `Transform fact attribute "${transform.factAttr}" is not reachable from base fact "${baseFact}"`,
        suggestion: "Ensure the transform's fact attribute belongs to the metric's base fact or add the necessary join.",
      });
    }
  });

  refs.tableTransforms.forEach((transformId) => {
    const transform = model.tableTransforms?.[transformId];
    if (!transform) {
      errors.push({
        metric: metricName,
        message: `Transform "${transformId}" is not defined for metric "${metricName}"`,
        suggestion: "Add the table transform configuration to the semantic model.",
      });
      return;
    }

    if (!model.attributes[transform.inputAttr]) {
      errors.push({
        metric: metricName,
        message: `Transform input attribute "${transform.inputAttr}" is not in the semantic model`,
        suggestion: "Define the input attribute so it can be used for mapping.",
      });
    }

    if (!model.attributes[transform.outputAttr]) {
      errors.push({
        metric: metricName,
        message: `Transform output attribute "${transform.outputAttr}" is not in the semantic model`,
        suggestion: "Define the output attribute or correct the transform configuration.",
      });
    }

    if (baseFact && !isAttributeReachable(baseFact, transform.outputAttr, model)) {
      errors.push({
        metric: metricName,
        message: `Transform output attribute "${transform.outputAttr}" is not reachable from base fact "${baseFact}"`,
        suggestion: "Ensure the transform output attribute belongs to the metric's base fact or add the necessary join.",
      });
    }

    if (transform.table) {
      const seen = new Map<any, Set<any>>();
      transform.table.forEach((row) => {
        const set = seen.get(row.key) ?? new Set<any>();
        set.add(row.mappedKey);
        seen.set(row.key, set);
      });
      for (const [key, mapped] of seen.entries()) {
        if (mapped.size > 1) {
          errors.push({
            metric: metricName,
            message: `Transform "${transformId}" maps input "${key}" to multiple targets`,
            suggestion: "Confirm the mapping cardinality or mark the transform as intentionally many-to-many.",
          });
          break;
        }
      }
    }
  });

  return { metric: metricName, ok: errors.length === 0, errors };
}

export function validateAll(model: SemanticModel): ValidationResult[] {
  const cycles = detectCircularDependencies(model);
  const results: ValidationResult[] = [];

  for (const metricName of Object.keys(model.metrics)) {
    const result = validateMetric(model, metricName);
    const cyclePath = cycles.get(metricName);
    if (cyclePath) {
      result.errors.push({
        metric: metricName,
        message: `Circular dependency detected: ${cyclePath.join(" -> ")}`,
        path: cyclePath,
        suggestion: "Break the cycle by removing the recursive reference or introducing a base metric.",
      });
    }
    result.ok = result.errors.length === 0;
    results.push(result);
  }

  return results;
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
  //    - rows whose factAttr is in targetAnchors
  //    - rows that match all groupDimensions EXCEPT the anchorAttr (which is shifted)
  const relation = runtime.relation;
  return relation.where((row: Row) => {
    if (!allowedAnchor.has((row as any)[transform.factAttr])) return false;

    return runtime.groupDimensions.every((dim) => {
      if (dim === transform.anchorAttr) return true; // this is the shifted dimension
      return (row as any)[dim] === groupKey[dim];
    });
  });
}

function resolveTableTransformRows(
  transform: TableTransformDefinition,
  db: InMemoryDb
): TableTransformMappingRow[] {
  if (transform.table) return transform.table;

  if (transform.relation) {
    const rows = db.tables[transform.relation] ?? [];
    const keyCol = transform.keyColumn ?? "key";
    const mappedCol = transform.mappedColumn ?? "mappedKey";
    return rows.map((r: Row) => ({ key: (r as any)[keyCol], mappedKey: (r as any)[mappedCol] }));
  }

  return [];
}

function applyTableTransform(
  runtime: MetricRuntime,
  transformId: string,
  groupKey: Record<string, any>
): RowSequence {
  const transform = runtime.model.tableTransforms?.[transformId];
  if (!transform) {
    throw new Error(`Unknown table transform: ${transformId}`);
  }

  const anchorValue = groupKey[transform.inputAttr];
  const mappingRows = resolveTableTransformRows(transform, runtime.db);
  const mappedKeys = mappingRows
    .filter((row) => row.key === anchorValue)
    .map((row) => row.mappedKey);

  if (!mappedKeys.length) {
    if (transform.fallbackMappedKey !== undefined) {
      mappedKeys.push(transform.fallbackMappedKey);
    } else {
      return rowsToEnumerable([]);
    }
  }

  const allowed = new Set(mappedKeys);
  return runtime.relation.where((row: Row) => {
    if (!allowed.has((row as any)[transform.outputAttr])) return false;

    return runtime.groupDimensions.every((dim) => {
      if (dim === transform.inputAttr) return true;
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
    applyTableTransform: (transformId, gk) =>
      applyTableTransform(runtime, transformId, gk),
    bind(name: string) {
      const key = name.startsWith(":") ? name.slice(1) : name;
      if (!(key in runtime.bindings)) {
        throw new Error(`Missing binding '${name}'`);
      }
      return runtime.bindings[key];
    },
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

interface SingleFactResult {
  rows: Row[];
  fact: string | null;
  metricRows: Record<string, Row[]>;
  supportsAllDimensions: boolean;
}

function collectRequiredAttributes(
  dimensions: string[],
  whereFields: Set<string>,
  metricNames: string[],
  model: SemanticModel,
  supportsAllDimensions: boolean
): Set<string> {
  const requiredAttrs = new Set<string>();

  if (supportsAllDimensions) {
    dimensions.forEach((d) => requiredAttrs.add(d));
  }

  whereFields.forEach((f) => requiredAttrs.add(f));

  metricNames.forEach((m) => {
    model.metrics[m]?.attributes?.forEach((a) => requiredAttrs.add(a));
  });

  if (requiredAttrs.size === 0) {
    dimensions.forEach((d) => requiredAttrs.add(d));
  }

  return requiredAttrs;
}

function factSupportsAllDimensions(
  fact: string | null,
  dimensions: string[],
  model: SemanticModel
): boolean {
  if (!fact) return false;
  return dimensions.every((dim) => {
    const attr = model.attributes[dim];
    if (!attr) return false;
    const normalizedFact = normalizeFact(fact, model.facts[fact]);
    if (attr.table === normalizedFact.table) return true;
    const dimensionId = Object.entries(model.dimensions).find(
      ([id, def]) => normalizeDimension(id, def).table === attr.table
    )?.[0];
    const joinKey = `${fact}|${dimensionId}`;
    return model.joins.some((edge) => `${edge.fact}|${edge.dimension}` === joinKey);
  });
}

function metricGrainForQuery(
  metric: MetricDefinition | undefined,
  dimensions: string[],
  supportsAllDimensions: boolean
): string[] {
  const attrs = metric?.attributes;
  if (attrs) {
    const intersection = attrs.filter((a) => dimensions.includes(a));
    if (intersection.length > 0) return intersection;
    if (attrs.length === 0) return [];
  }

  return supportsAllDimensions ? dimensions : [];
}

function runSemanticQueryForBase(
  env: { db: InMemoryDb; model: SemanticModel },
  spec: QuerySpec,
  baseFact: string | null,
  metricNames: string[],
  bindings: Record<string, any>
): SingleFactResult {
  const { db, model } = env;
  const dimensions = spec.dimensions;
  const whereFields = collectFilterFields(spec.where);
  const supportsAllDimensions = factSupportsAllDimensions(
    baseFact,
    dimensions,
    model
  );
  const requiredAttrs = collectRequiredAttributes(
    dimensions,
    whereFields,
    metricNames,
    model,
    supportsAllDimensions
  );

  let baseRelation: RowSequence;
  let runtimeBaseFact: string | undefined = baseFact ?? undefined;

  if (baseFact && model.facts[baseFact]) {
    baseRelation = buildFactBaseRelation(model, db, baseFact, requiredAttrs);
  } else {
    const relationsUsed = new Set<string>();
    requiredAttrs.forEach((attr) => {
      const def = model.attributes[attr];
      if (def) relationsUsed.add(def.table);
    });

    if (baseFact && !model.facts[baseFact]) {
      relationsUsed.add(baseFact);
      runtimeBaseFact = baseFact;
    }

    if (relationsUsed.size === 0) {
      throw new Error("No attributes available to determine a base relation");
    }

    if (relationsUsed.size !== 1) {
      throw new Error(
        `Metrics provided but none declares a baseFact; engine cannot choose a fact table. ` +
          `Please set baseFact on fact metrics or constrain attributes to one relation.`
      );
    }

    const singleRelation = Array.from(relationsUsed)[0];
    baseRelation = buildDimensionBaseRelation(
      model,
      db,
      singleRelation,
      requiredAttrs
    );
    runtimeBaseFact = runtimeBaseFact ?? singleRelation;
  }

  const whereNode = normalizeFilterContext(spec.where);
  const filtered = applyWhereFilter(
    baseRelation,
    whereNode,
    Array.from(requiredAttrs)
  );

  const grouped = filtered.groupBy(
    (row: Row) => keyFromRow(row, dimensions),
    (row: Row) => row
  );

  const frameRows: Row[] = [];
  grouped.forEach((group) => {
    const sample = group.first();
    const groupKey: Record<string, any> = {};
    if (sample) {
      dimensions.forEach((d) => (groupKey[d] = (sample as any)[d]));
    }

    if (supportsAllDimensions) {
      frameRows.push(groupKey);
    }
  });

  const metricRows: Record<string, Row[]> = {};

  for (const metricName of metricNames) {
    const metric = model.metrics[metricName];
    const metricGrain = metricGrainForQuery(
      metric,
      dimensions,
      supportsAllDimensions
    );

    const metricGrouped = filtered.groupBy(
      (row: Row) => keyFromRow(row, metricGrain),
      (row: Row) => row
    );

    const rows: Row[] = [];
    const metricCache = new Map<string, number | undefined>();

    metricGrouped.forEach((group) => {
      const sample = group.first();
      const groupKey: Record<string, any> = {};
      if (sample) {
        metricGrain.forEach((d) => (groupKey[d] = (sample as any)[d]));
      }

      const runtime: MetricRuntime = {
        model,
        db,
        baseFact: runtimeBaseFact,
        relation: baseRelation,
        whereFilter: whereNode,
        bindings,
        groupDimensions: metricGrain,
      };

      rows.push({
        ...groupKey,
        [metricName]: evaluateMetric(
          metricName,
          runtime,
          groupKey,
          group,
          undefined,
          metricCache
        ),
      });
    });

    metricRows[metricName] = rows;
  }

  return { rows: frameRows, fact: baseFact, metricRows, supportsAllDimensions };
}

function dimKeyFromRow(row: Row, dims: string[]): string {
  const obj: Row = {};
  dims.forEach((d) => {
    obj[d] = row[d];
  });
  return JSON.stringify(obj);
}

function pickDims(row: Row, dims: string[]): Row {
  const out: Row = {};
  dims.forEach((d) => {
    out[d] = row[d];
  });
  return out;
}

export function runSemanticQuery(
  env: { db: InMemoryDb; model: SemanticModel },
  spec: QuerySpec,
  options?: ExecutionOptions
): Row[] {
  const { db, model } = env;
  const bindings = options?.bindings ?? {};
  const dimensions = spec.dimensions;

  const rawWhereNode = normalizeFilterContext(spec.where);
  const whereNode = resolveBindingsInFilter(rawWhereNode, bindings);

  const whereFields = collectFilterFields(whereNode);

  // Dimension-only query (no metrics)
  if (spec.metrics.length === 0) {
    const relationsUsed = new Set<string>();
    const requiredAttrs = new Set<string>([...dimensions]);
    whereFields.forEach((f) => requiredAttrs.add(f));

    for (const attr of requiredAttrs) {
      const def = model.attributes[attr];
      if (!def) continue;
      relationsUsed.add(def.table);
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
      whereNode,
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

  const metricsByBase = new Map<string | null, string[]>();

  for (const metricName of spec.metrics) {
    const def = model.metrics[metricName];
    if (!def) {
      throw new Error(`Unknown metric: ${metricName}`);
    }
    const key = def.baseFact ?? null;
    const list = metricsByBase.get(key) ?? [];
    list.push(metricName);
    metricsByBase.set(key, list);
  }

  const perFactResults: SingleFactResult[] = [];

  for (const [baseFact, metricNames] of metricsByBase.entries()) {
    const childSpec: QuerySpec = {
      dimensions: spec.dimensions,
      metrics: metricNames,
      where: whereNode ?? undefined,
      having: undefined,
    };

    perFactResults.push(
      runSemanticQueryForBase(env, childSpec, baseFact, metricNames, bindings)
    );
  }

  const frame = new Map<string, Row>();

  for (const { rows, supportsAllDimensions } of perFactResults) {
    if (!supportsAllDimensions) continue;
    for (const row of rows) {
      const key = dimKeyFromRow(row, dimensions);
      const existing = frame.get(key) ?? {};
      frame.set(key, { ...existing, ...pickDims(row, dimensions) });
    }
  }

  if (frame.size === 0) {
    const relationsUsed = new Set<string>();
    const requiredAttrs = new Set<string>([...dimensions]);
    whereFields.forEach((f) => requiredAttrs.add(f));

    for (const attr of requiredAttrs) {
      const def = model.attributes[attr];
      if (!def) continue;
      relationsUsed.add(def.table);
    }

    if (relationsUsed.size === 1) {
      const singleRelation = Array.from(relationsUsed)[0];
      const relation = buildDimensionBaseRelation(
        model,
        db,
        singleRelation,
        requiredAttrs
      );
      const filtered = applyWhereFilter(
        relation,
        whereNode,
        Array.from(requiredAttrs)
      );
      filtered.forEach((row: Row) => {
        const key = dimKeyFromRow(row, dimensions);
        frame.set(key, pickDims(row, dimensions));
      });
    }
  }

  for (const { metricRows, supportsAllDimensions } of perFactResults) {
    for (const [metricName, rows] of Object.entries(metricRows)) {
      const metric = model.metrics[metricName];
      const metricGrain = metricGrainForQuery(
        metric,
        dimensions,
        supportsAllDimensions
      );

      const factMetricMap = new Map<string, Row>();
      rows.forEach((row) => {
        factMetricMap.set(keyFromRow(row, metricGrain), row);
      });

      frame.forEach((frameRow) => {
        const keyMatch = keyFromRow(frameRow, metricGrain);
        const metricRow = factMetricMap.get(keyMatch);
        if (metricRow) {
          Object.assign(frameRow, metricRow);
        }
      });
    }
  }

  let finalRows = Array.from(frame.values());

  if (spec.having) {
    finalRows = finalRows.filter((r) => {
      const metricValues: Record<string, number | undefined> = {};
      spec.metrics.forEach((m) => {
        metricValues[m] = r[m] as number | undefined;
      });
      return spec.having!(metricValues);
    });
  }

  return finalRows;
}

