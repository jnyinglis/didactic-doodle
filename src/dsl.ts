import {
  FilterContext,
  FilterNode,
  MetricExpr,
  QuerySpecV2,
  SemanticModel,
  MetricRegistry,
  buildMetricFromExpr,
} from "./semanticEngine";

export type ParseResult<T> = { value: T; nextPos: number };
export type Parser<T> = (input: string, pos: number) => ParseResult<T> | null;

function skipWs(input: string, pos: number): number {
  const match = /^\s*/.exec(input.slice(pos));
  return pos + (match ? match[0].length : 0);
}

function map<A, B>(parser: Parser<A>, fn: (value: A) => B): Parser<B> {
  return (input, pos) => {
    const result = parser(input, pos);
    if (!result) return null;
    return { value: fn(result.value), nextPos: result.nextPos };
  };
}

function seq<T extends any[]>(
  ...parsers: { [K in keyof T]: Parser<T[K]> }
): Parser<T> {
  return (input, pos) => {
    const values: any[] = [];
    let nextPos = pos;
    for (const p of parsers) {
      const result = p(input, nextPos);
      if (!result) return null;
      values.push(result.value);
      nextPos = result.nextPos;
    }
    return { value: values as T, nextPos };
  };
}

function choice<T>(...parsers: Parser<T>[]): Parser<T> {
  return (input, pos) => {
    for (const p of parsers) {
      const result = p(input, pos);
      if (result) return result;
    }
    return null;
  };
}

function many<T>(parser: Parser<T>): Parser<T[]> {
  return (input, pos) => {
    const values: T[] = [];
    let nextPos = pos;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const result = parser(input, nextPos);
      if (!result) break;
      values.push(result.value);
      nextPos = result.nextPos;
    }
    return { value: values, nextPos };
  };
}

function opt<T>(parser: Parser<T>): Parser<T | null> {
  return (input, pos) => {
    const result = parser(input, pos);
    if (!result) return { value: null, nextPos: pos };
    return result;
  };
}

function regex(re: RegExp): Parser<string> {
  const anchored = new RegExp("^(?:" + re.source + ")", re.flags);
  return (input, pos) => {
    const start = skipWs(input, pos);
    const slice = input.slice(start);
    const match = anchored.exec(slice);
    if (!match) return null;
    const nextPos = skipWs(input, start + match[0].length);
    return { value: match[0], nextPos };
  };
}

function token(text: string): Parser<string> {
  return (input, pos) => {
    const start = skipWs(input, pos);
    if (input.slice(start).startsWith(text)) {
      const nextPos = skipWs(input, start + text.length);
      return { value: text, nextPos };
    }
    return null;
  };
}

function symbol(text: string): Parser<string> {
  return token(text);
}

function keyword(word: string): Parser<string> {
  const re = new RegExp(word + "(?![A-Za-z0-9_])");
  return regex(re);
}

function between<A>(left: Parser<any>, parser: Parser<A>, right: Parser<any>): Parser<A> {
  return map(seq(left, parser, right), ([, value]) => value);
}

function sepBy<T>(parser: Parser<T>, separator: Parser<any>): Parser<T[]> {
  return (input, pos) => {
    const first = parser(input, pos);
    if (!first) return { value: [], nextPos: pos };
    const values: T[] = [first.value];
    let nextPos = first.nextPos;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const sep = separator(input, nextPos);
      if (!sep) break;
      const next = parser(input, sep.nextPos);
      if (!next) break;
      values.push(next.value);
      nextPos = next.nextPos;
    }
    return { value: values, nextPos };
  };
}

function lazy<T>(fn: () => Parser<T>): Parser<T> {
  return (input, pos) => fn()(input, pos);
}

function chainLeft<T>(parser: Parser<T>, opParser: Parser<string>, combine: (left: T, op: string, right: T) => T): Parser<T> {
  return (input, pos) => {
    let result = parser(input, pos);
    if (!result) return null;
    let value = result.value;
    let nextPos = result.nextPos;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const op = opParser(input, nextPos);
      if (!op) break;
      const right = parser(input, op.nextPos);
      if (!right) break;
      value = combine(value, op.value, right.value);
      nextPos = right.nextPos;
    }
    return { value, nextPos };
  };
}

function parens<T>(parser: Parser<T>): Parser<T> {
  return between(symbol("("), parser, symbol(")"));
}

/* --------------------------------------------------------------------------
 * AST TYPES
 * -------------------------------------------------------------------------- */

export interface MetricDeclAst {
  name: string;
  baseFact?: string;
  expr: MetricExpr;
}

export type MetricHavingAst =
  | { kind: "MetricCmp"; metric: string; op: ">" | ">=" | "<" | "<=" | "==" | "!="; value: number }
  | { kind: "And" | "Or"; items: MetricHavingAst[] };

export interface QueryAst {
  name: string;
  spec: QuerySpecV2;
}

export interface DslFileAst {
  metrics: MetricDeclAst[];
  queries: QueryAst[];
}

/* --------------------------------------------------------------------------
 * LEXER HELPERS
 * -------------------------------------------------------------------------- */

const identifier: Parser<string> = map(regex(/[A-Za-z_][A-Za-z0-9_]*/), (v) => v);
const numberLiteral: Parser<number> = map(regex(/\d+(?:\.\d+)?/), (v) => Number(v));

/* --------------------------------------------------------------------------
 * EXPRESSION PARSER
 * -------------------------------------------------------------------------- */

const expr: Parser<MetricExpr> = lazy(() => additive);

const functionCall: Parser<MetricExpr> = (input, pos) => {
  const nameRes = identifier(input, pos);
  if (!nameRes) return null;
  const lp = symbol("(")(input, nameRes.nextPos);
  if (!lp) return null;

  const fn = nameRes.value;
  let nextPos = lp.nextPos;
  let args: MetricExpr[] = [];

  if (fn === "last_year") {
    const metricArg = identifier(input, nextPos);
    if (!metricArg) return null;
    args.push({ kind: "MetricRef", name: metricArg.value });
    nextPos = metricArg.nextPos;

    const comma = opt(symbol(","))(input, nextPos);
    if (comma) nextPos = comma.nextPos;

    const byKw = keyword("by")(input, nextPos);
    if (!byKw) return null;
    const anchor = identifier(input, byKw.nextPos);
    if (!anchor) return null;
    args.push({ kind: "AttrRef", name: anchor.value });
    nextPos = anchor.nextPos;
  } else {
    const starArg = symbol("*")(input, nextPos);
    if (starArg) {
      args = [{ kind: "AttrRef", name: "*" }];
      nextPos = starArg.nextPos;
    } else {
      const argRes = opt(argList)(input, nextPos);
      if (!argRes) return null;
      args = argRes.value ?? [];
      nextPos = argRes.nextPos;
    }
  }

  const rp = symbol(")")(input, nextPos);
  if (!rp) return null;

  return { value: { kind: "Call", fn, args }, nextPos: rp.nextPos };
};

const primary: Parser<MetricExpr> = choice(
  parens(expr),
  functionCall,
  map(numberLiteral, (n) => ({ kind: "Literal", value: n } as MetricExpr)),
  map(identifier, (name) => ({ kind: "AttrRef", name } as MetricExpr))
);

const multiplicative: Parser<MetricExpr> = chainLeft(
  primary,
  choice(symbol("*"), symbol("/")),
  (left, op, right) => ({ kind: "BinaryOp", op: op as any, left, right })
);

const additive: Parser<MetricExpr> = chainLeft(
  multiplicative,
  choice(symbol("+"), symbol("-")),
  (left, op, right) => ({ kind: "BinaryOp", op: op as any, left, right })
);

const argList: Parser<MetricExpr[]> = sepBy(expr, symbol(","));

/* --------------------------------------------------------------------------
 * METRIC DECL PARSER
 * -------------------------------------------------------------------------- */

export const metricDecl: Parser<MetricDeclAst> = map(
  seq(keyword("metric"), identifier, keyword("on"), identifier, symbol("="), expr),
  ([, name, , baseFact, , expression]) => ({ name, baseFact, expr: expression })
);

/* --------------------------------------------------------------------------
 * FILTER / HAVING PARSERS
 * -------------------------------------------------------------------------- */

const comparator = choice(symbol(">="), symbol("<="), symbol(">"), symbol("<"), symbol("=="), symbol("!="));

const filterLiteral: Parser<number> = map(numberLiteral, (n) => n);

const filterExpression: Parser<FilterNode> = map(
  seq(identifier, comparator, filterLiteral),
  ([field, op, value]) => toFilterExpression(field, op, value)
);

const boolExpr: Parser<FilterNode> = lazy(() => boolOr);

const boolTerm: Parser<FilterNode> = choice(parens(boolExpr), filterExpression);

const boolAnd: Parser<FilterNode> = map(chainLeft(boolTerm, keyword("and"), (left, _, right) => ({ left, right } as any)), (node: any) =>
  foldLogical(node, "and")
);

const boolOr: Parser<FilterNode> = map(chainLeft(boolAnd, keyword("or"), (left, _, right) => ({ left, right } as any)), (node: any) =>
  foldLogical(node, "or")
);

const havingTerm: Parser<MetricHavingAst> = map(
  seq(identifier, comparator, filterLiteral),
  ([metric, op, value]) => ({ kind: "MetricCmp", metric, op: op as any, value })
);

const havingExpr: Parser<MetricHavingAst> = lazy(() => havingOr);

const havingAnd: Parser<MetricHavingAst> = map(
  chainLeft(havingTerm, keyword("and"), (left, _, right) => ({ left, right } as any)),
  (node: any) => foldHaving(node, "And")
);

const havingOr: Parser<MetricHavingAst> = map(
  chainLeft(havingAnd, keyword("or"), (left, _, right) => ({ left, right } as any)),
  (node: any) => foldHaving(node, "Or")
);

/* --------------------------------------------------------------------------
 * QUERY PARSER
 * -------------------------------------------------------------------------- */

const identList = sepBy(identifier, symbol(","));

const dimensionsLine = map(seq(keyword("dimensions"), symbol(":"), identList), ([, , dims]) => ({
  kind: "dimensions" as const,
  values: dims,
}));

const metricsLine = map(seq(keyword("metrics"), symbol(":"), identList), ([, , metrics]) => ({
  kind: "metrics" as const,
  values: metrics,
}));

const whereLine = map(seq(keyword("where"), symbol(":"), boolExpr), ([, , expr]) => ({
  kind: "where" as const,
  value: expr,
}));

const havingLine = map(seq(keyword("having"), symbol(":"), havingExpr), ([, , expr]) => ({
  kind: "having" as const,
  value: expr,
}));

const queryLine: Parser<any> = choice<any>(
  dimensionsLine as Parser<any>,
  metricsLine as Parser<any>,
  whereLine as Parser<any>,
  havingLine as Parser<any>
);

export const queryDecl: Parser<QueryAst> = (input, pos) => {
  const header = seq(keyword("query"), identifier, symbol("{"));
  const headResult = header(input, pos);
  if (!headResult) return null;
  const [, name, ] = headResult.value;
  let nextPos = headResult.nextPos;
  const lines: any[] = [];

  while (true) {
    const close = symbol("}")(input, nextPos);
    if (close) {
      nextPos = close.nextPos;
      break;
    }
    const line = queryLine(input, nextPos);
    if (!line) return null;
    lines.push(line.value);
    nextPos = line.nextPos;
  }

  const spec: QuerySpecV2 = { dimensions: [], metrics: [] };
  lines.forEach((line) => {
    switch (line.kind) {
      case "dimensions":
        spec.dimensions = line.values;
        break;
      case "metrics":
        spec.metrics = line.values;
        break;
      case "where":
        spec.where = line.value as FilterContext;
        break;
      case "having":
        spec.having = compileHaving(line.value as MetricHavingAst);
        break;
    }
  });

  return { value: { name, spec }, nextPos };
};

/* --------------------------------------------------------------------------
 * NAME RESOLUTION / VALIDATION
 * -------------------------------------------------------------------------- */

export function resolveMetricRefs(expr: MetricExpr, metricNames: Set<string>): MetricExpr {
  switch (expr.kind) {
    case "AttrRef":
      if (metricNames.has(expr.name)) {
        return { kind: "MetricRef", name: expr.name };
      }
      return expr;
    case "BinaryOp":
      return {
        kind: "BinaryOp",
        op: expr.op,
        left: resolveMetricRefs(expr.left, metricNames),
        right: resolveMetricRefs(expr.right, metricNames),
      };
    case "Call":
      return {
        kind: "Call",
        fn: expr.fn,
        args: expr.args.map((a) => resolveMetricRefs(a, metricNames)),
      };
    case "MetricRef":
      if (!metricNames.has(expr.name)) {
        throw new Error(`Unknown metric reference: ${expr.name}`);
      }
      return expr;
    default:
      return expr;
  }
}

export function validateMetricExpr(expr: MetricExpr): void {
  if (expr.kind === "BinaryOp") {
    validateMetricExpr(expr.left);
    validateMetricExpr(expr.right);
    return;
  }

  if (expr.kind === "Call") {
    const fn = expr.fn.toLowerCase();
    if (["sum", "avg", "min", "max", "count"].includes(fn)) {
      if (expr.args.length !== 1 || expr.args[0].kind !== "AttrRef") {
        throw new Error(`${fn}() expects a single attribute reference argument`);
      }
    } else if (fn === "last_year") {
      if (
        expr.args.length !== 2 ||
        expr.args[0].kind !== "MetricRef" ||
        expr.args[1].kind !== "AttrRef"
      ) {
        throw new Error("last_year() expects MetricRef and AttrRef arguments");
      }
    }
    expr.args.forEach(validateMetricExpr);
  }
}

/* --------------------------------------------------------------------------
 * FILE PARSER + COMPILATION ENTRY POINTS
 * -------------------------------------------------------------------------- */

const fileParser: Parser<DslFileAst> = (input, pos) => {
  let nextPos = skipWs(input, pos);
  const metrics: MetricDeclAst[] = [];
  const queries: QueryAst[] = [];

  while (nextPos < input.length) {
    const metric = metricDecl(input, nextPos);
    if (metric) {
      metrics.push(metric.value);
      nextPos = metric.nextPos;
      continue;
    }
    const query = queryDecl(input, nextPos);
    if (query) {
      queries.push(query.value);
      nextPos = query.nextPos;
      continue;
    }
    break;
  }

  nextPos = skipWs(input, nextPos);
  return { value: { metrics, queries }, nextPos };
};

export function parseDsl(text: string): DslFileAst {
  const result = fileParser(text, 0);
  if (!result || skipWs(text, result.nextPos) !== text.length) {
    throw new Error("DSL parse error (TODO: better error reporting)");
  }
  return result.value;
}

export interface DslCompileResult {
  model: SemanticModel;
  queries: Record<string, QuerySpecV2>;
}

export function compileDslToModel(
  text: string,
  baseModel: SemanticModel
): DslCompileResult {
  const ast = parseDsl(text);
  const metricNames = new Set([
    ...Object.keys(baseModel.metrics ?? {}),
    ...ast.metrics.map((m) => m.name),
  ]);

  const newMetrics: MetricRegistry = { ...baseModel.metrics };
  ast.metrics.forEach((mAst) => {
    const resolvedExpr = resolveMetricRefs(mAst.expr, metricNames);
    validateMetricExpr(resolvedExpr);
    const def = buildMetricFromExpr({
      name: mAst.name,
      baseFact: mAst.baseFact,
      expr: resolvedExpr,
    });
    newMetrics[def.name] = def;
  });

  const queries: Record<string, QuerySpecV2> = {};
  ast.queries.forEach((qAst) => {
    if (!qAst.spec.dimensions.length || !qAst.spec.metrics.length) {
      throw new Error(`Query ${qAst.name} must specify dimensions and metrics`);
    }
    queries[qAst.name] = qAst.spec;
  });

  const model: SemanticModel = {
    ...baseModel,
    metrics: newMetrics,
  };

  return { model, queries };
}

export function parseAll<T>(parser: Parser<T>, input: string): T {
  const result = parser(input, 0);
  if (!result) throw new Error("Parse failed");
  if (skipWs(input, result.nextPos) !== input.length) {
    throw new Error("Unexpected trailing content");
  }
  return result.value;
}

export function compileHaving(ast: MetricHavingAst): (values: Record<string, number | undefined>) => boolean {
  return (values) => evalHaving(ast, values);
}

function evalHaving(ast: MetricHavingAst, values: Record<string, number | undefined>): boolean {
  if (ast.kind === "MetricCmp") {
    const metricVal = values[ast.metric];
    if (metricVal == null) return false;
    switch (ast.op) {
      case ">":
        return metricVal > ast.value;
      case ">=":
        return metricVal >= ast.value;
      case "<":
        return metricVal < ast.value;
      case "<=":
        return metricVal <= ast.value;
      case "==":
        return metricVal === ast.value;
      case "!=":
        return metricVal !== ast.value;
      default:
        return false;
    }
  }

  if (ast.items.length === 0) return true;
  if (ast.kind === "And") {
    return ast.items.every((item) => evalHaving(item, values));
  }
  return ast.items.some((item) => evalHaving(item, values));
}

function toFilterExpression(field: string, op: string, value: number): FilterNode {
  switch (op) {
    case ">":
      return { kind: "expression", field, op: "gt", value };
    case ">=":
      return { kind: "expression", field, op: "gte", value };
    case "<":
      return { kind: "expression", field, op: "lt", value };
    case "<=":
      return { kind: "expression", field, op: "lte", value };
    case "==":
      return { kind: "expression", field, op: "eq", value };
    case "!=":
      return {
        kind: "or",
        filters: [
          { kind: "expression", field, op: "lt", value },
          { kind: "expression", field, op: "gt", value },
        ],
      } as FilterNode;
    default:
      return { kind: "expression", field, op: "eq", value };
  }
}

function foldLogical(node: any, kind: "and" | "or"): FilterNode {
  if (node.kind) return node as FilterNode;
  const items: FilterNode[] = [];
  let current: any = node;
  while (current) {
    if (current.right) {
      items.push((current.left as any).kind ? current.left : current.left);
      current = current.right;
    } else {
      items.push(current.left ?? current);
      break;
    }
  }
  return { kind, filters: items.map((i) => ((i as any).kind ? (i as any) : i)) } as FilterNode;
}

function foldHaving(node: any, kind: "And" | "Or"): MetricHavingAst {
  if (node.kind) return node as MetricHavingAst;
  const items: MetricHavingAst[] = [];
  let current: any = node;
  while (current) {
    if (current.right) {
      items.push(current.left.kind ? current.left : current.left.value ?? current.left);
      current = current.right;
    } else {
      items.push(current.left ?? current);
      break;
    }
  }
  return { kind, items } as MetricHavingAst;
}
