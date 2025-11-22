import {
  FilterContext,
  FilterNode,
  MetricDefinitionV2,
  MetricEvalV2,
  QuerySpecV2,
  evaluateMetricRuntime,
} from "./semanticEngine";
import type { RowSequence } from "./semanticEngine";

export type Parser<T> = {
  parse(input: string, pos: number): { value: T; nextPos: number } | null;
};

function map<A, B>(parser: Parser<A>, fn: (value: A) => B): Parser<B> {
  return {
    parse(input, pos) {
      const result = parser.parse(input, pos);
      if (!result) return null;
      return { value: fn(result.value), nextPos: result.nextPos };
    },
  };
}

function seq<T extends any[]>(...parsers: { [K in keyof T]: Parser<T[K]> }): Parser<T> {
  return {
    parse(input, pos) {
      const values: any[] = [];
      let nextPos = pos;
      for (const p of parsers) {
        const result = p.parse(input, nextPos);
        if (!result) return null;
        values.push(result.value);
        nextPos = result.nextPos;
      }
      return { value: values as T, nextPos };
    },
  };
}

function choice<T>(...parsers: Parser<T>[]): Parser<T> {
  return {
    parse(input, pos) {
      for (const p of parsers) {
        const result = p.parse(input, pos);
        if (result) return result;
      }
      return null;
    },
  };
}

function many<T>(parser: Parser<T>): Parser<T[]> {
  return {
    parse(input, pos) {
      const values: T[] = [];
      let nextPos = pos;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const result = parser.parse(input, nextPos);
        if (!result) break;
        values.push(result.value);
        nextPos = result.nextPos;
      }
      return { value: values, nextPos };
    },
  };
}

function opt<T>(parser: Parser<T>): Parser<T | null> {
  return {
    parse(input, pos) {
      const result = parser.parse(input, pos);
      if (!result) return { value: null, nextPos: pos };
      return result;
    },
  };
}

function regex(re: RegExp): Parser<string> {
  const anchored = new RegExp("^(?:" + re.source + ")", re.flags);
  return {
    parse(input, pos) {
      const slice = input.slice(pos);
      const match = anchored.exec(slice);
      if (!match) return null;
      const nextPos = pos + match[0].length;
      const after = skipWs(input, nextPos);
      return { value: match[0], nextPos: after };
    },
  };
}

function token(text: string): Parser<string> {
  return {
    parse(input, pos) {
      if (input.slice(pos).startsWith(text)) {
        const nextPos = skipWs(input, pos + text.length);
        return { value: text, nextPos };
      }
      return null;
    },
  };
}

const whitespace: Parser<string> = {
  parse(input, pos) {
    const match = /^\s*/.exec(input.slice(pos));
    const consumed = match ? match[0].length : 0;
    return { value: "", nextPos: pos + consumed };
  },
};

function symbol(text: string) {
  return map(seq(whitespace, token(text)), ([, v]) => v);
}

function keyword(word: string) {
  const re = new RegExp(word + "(?![A-Za-z0-9_])");
  return map(seq(whitespace, regex(re)), ([, v]) => v);
}

function between<A>(left: Parser<any>, parser: Parser<A>, right: Parser<any>): Parser<A> {
  return map(seq(left, parser, right), ([, value]) => value);
}

function sepBy<T>(parser: Parser<T>, separator: Parser<any>): Parser<T[]> {
  return {
    parse(input, pos) {
      const first = parser.parse(input, pos);
      if (!first) return { value: [], nextPos: pos };
      const values: T[] = [first.value];
      let nextPos = first.nextPos;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const sep = separator.parse(input, nextPos);
        if (!sep) break;
        const next = parser.parse(input, sep.nextPos);
        if (!next) break;
        values.push(next.value);
        nextPos = next.nextPos;
      }
      return { value: values, nextPos };
    },
  };
}

function lazy<T>(fn: () => Parser<T>): Parser<T> {
  return {
    parse(input, pos) {
      return fn().parse(input, pos);
    },
  };
}

function chainLeft<T>(parser: Parser<T>, opParser: Parser<any>): Parser<T> {
  return {
    parse(input, pos) {
      let result = parser.parse(input, pos);
      if (!result) return null;
      let value = result.value;
      let nextPos = result.nextPos;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const op = opParser.parse(input, nextPos);
        if (!op) break;
        const right = parser.parse(input, op.nextPos);
        if (!right) break;
        // @ts-ignore
        value = { op: op.value ?? op, left: value, right: right.value } as any;
        nextPos = right.nextPos;
      }
      return { value, nextPos };
    },
  };
}

function skipWs(input: string, pos: number): number {
  const match = /^\s*/.exec(input.slice(pos));
  return pos + (match ? match[0].length : 0);
}

/* --------------------------------------------------------------------------
 * AST TYPES
 * -------------------------------------------------------------------------- */

export type MetricExpr =
  | { kind: "Literal"; value: number }
  | { kind: "AttrRef"; name: string }
  | { kind: "MetricRef"; name: string }
  | { kind: "Call"; fn: string; args: MetricExpr[] }
  | { kind: "BinaryOp"; op: "+" | "-" | "*" | "/"; left: MetricExpr; right: MetricExpr }
  | { kind: "UnresolvedIdent"; name: string };

export interface MetricDefAst {
  name: string;
  baseFact?: string;
  expr: MetricExpr;
}

export type MetricHavingAst =
  | { kind: "MetricCmp"; metric: string; op: ">" | ">=" | "<" | "<=" | "==" | "!="; value: number }
  | { kind: "And" | "Or"; items: MetricHavingAst[] };

export interface QueryAst {
  kind: "Query";
  name: string;
  spec: QuerySpecV2;
}

/* --------------------------------------------------------------------------
 * LEXER HELPERS
 * -------------------------------------------------------------------------- */

const identifier = map(seq(whitespace, regex(/[A-Za-z_][A-Za-z0-9_]*/)), ([, v]) => v);
const numberLiteral = map(seq(whitespace, regex(/\d+(?:\.\d+)?/)), ([, v]) => Number(v));

/* --------------------------------------------------------------------------
 * EXPRESSION PARSER
 * -------------------------------------------------------------------------- */

const expr: Parser<MetricExpr> = lazy(() => additive);

const functionCall: Parser<MetricExpr> = {
  parse(input, pos) {
    const nameRes = identifier.parse(input, pos);
    if (!nameRes) return null;
    const lp = symbol("(").parse(input, nameRes.nextPos);
    if (!lp) return null;

    const fn = nameRes.value;
    let nextPos = lp.nextPos;
    let args: MetricExpr[] = [];

    if (fn === "last_year") {
      const metricArg = expr.parse(input, nextPos);
      if (!metricArg) return null;
      args.push(metricArg.value);
      nextPos = metricArg.nextPos;

      const byKw = keyword("by").parse(input, nextPos);
      if (byKw) {
        const anchor = identifier.parse(input, byKw.nextPos);
        if (!anchor) return null;
        args.push({ kind: "UnresolvedIdent", name: anchor.value });
        nextPos = anchor.nextPos;
      }
    } else {
      const starArg = symbol("*").parse(input, nextPos);
      if (starArg) {
        args = [{ kind: "AttrRef", name: "*" }];
        nextPos = starArg.nextPos;
      } else {
        const argRes = opt(argList).parse(input, nextPos);
        if (!argRes) return null;
        args = argRes.value ?? [];
        nextPos = argRes.nextPos;
      }
    }

    const rp = symbol(")").parse(input, nextPos);
    if (!rp) return null;

    return { value: { kind: "Call", fn, args }, nextPos: rp.nextPos };
  },
};

const primary: Parser<MetricExpr> = choice(
  between(symbol("("), expr, symbol(")")),
  functionCall,
  map(numberLiteral, (n) => ({ kind: "Literal", value: n } as MetricExpr)),
  map(identifier, (name) => ({ kind: "UnresolvedIdent", name } as MetricExpr))
);

const multiplicative: Parser<MetricExpr> = map(
  chainLeft(primary, choice(symbol("*"), symbol("/"))),
  (node: any) => {
    if (node.op) {
      return { kind: "BinaryOp", op: node.op, left: node.left, right: node.right } as MetricExpr;
    }
    return node as MetricExpr;
  }
);

const additive: Parser<MetricExpr> = map(
  chainLeft(multiplicative, choice(symbol("+"), symbol("-"))),
  (node: any) => {
    if (node.op) {
      return { kind: "BinaryOp", op: node.op, left: node.left, right: node.right } as MetricExpr;
    }
    return node as MetricExpr;
  }
);

const argList: Parser<MetricExpr[]> = sepBy(expr, symbol(","));

/* --------------------------------------------------------------------------
 * METRIC DEF PARSER
 * -------------------------------------------------------------------------- */

export const metricDef: Parser<MetricDefAst> = map(
  seq(keyword("metric"), identifier, opt(seq(keyword("on"), identifier)), symbol("="), expr),
  ([, name, onPart, , expression]) => {
    const baseFact = onPart ? onPart[1] : undefined;
    return { name, baseFact, expr: expression };
  }
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

const boolTerm: Parser<FilterNode> = choice(
  between(symbol("("), boolExpr, symbol(")")),
  filterExpression
);

const boolAnd: Parser<FilterNode> = map(
  chainLeft(boolTerm, keyword("and")),
  (node: any) => foldLogical(node, "and")
);

const boolOr: Parser<FilterNode> = map(
  chainLeft(boolAnd, keyword("or")),
  (node: any) => foldLogical(node, "or")
);

const havingTerm: Parser<MetricHavingAst> = map(
  seq(identifier, comparator, filterLiteral),
  ([metric, op, value]) => ({ kind: "MetricCmp", metric, op: op as any, value })
);

const havingExpr: Parser<MetricHavingAst> = lazy(() => havingOr);

const havingAnd: Parser<MetricHavingAst> = map(
  chainLeft(havingTerm, keyword("and")),
  (node: any) => foldHaving(node, "And")
);

const havingOr: Parser<MetricHavingAst> = map(
  chainLeft(havingAnd, keyword("or")),
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

export const queryDef: Parser<QueryAst> = {
  parse(input, pos) {
    const header = seq(keyword("query"), identifier, whitespace, symbol("{"));
    const headResult = header.parse(input, pos);
    if (!headResult) return null;
    const [, name, ,] = headResult.value;
    let nextPos = headResult.nextPos;
    const lines: any[] = [];
    while (true) {
      const close = symbol("}").parse(input, nextPos);
      if (close) {
        nextPos = close.nextPos;
        break;
      }
      const line = queryLine.parse(input, nextPos);
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

    return { value: { kind: "Query", name, spec }, nextPos };
  },
};

/* --------------------------------------------------------------------------
 * NAME RESOLUTION
 * -------------------------------------------------------------------------- */

export function resolveMetricExpr(expr: MetricExpr, metricNames: Set<string>): MetricExpr {
  switch (expr.kind) {
    case "UnresolvedIdent":
      return metricNames.has(expr.name)
        ? { kind: "MetricRef", name: expr.name }
        : { kind: "AttrRef", name: expr.name };
    case "BinaryOp":
      return {
        ...expr,
        left: resolveMetricExpr(expr.left, metricNames),
        right: resolveMetricExpr(expr.right, metricNames),
      };
    case "Call":
      return { ...expr, args: expr.args.map((a) => resolveMetricExpr(a, metricNames)) };
    default:
      return expr;
  }
}

export function collectAttributes(expr: MetricExpr, acc: Set<string> = new Set()): Set<string> {
  switch (expr.kind) {
    case "AttrRef":
      acc.add(expr.name);
      break;
    case "BinaryOp":
      collectAttributes(expr.left, acc);
      collectAttributes(expr.right, acc);
      break;
    case "Call":
      expr.args.forEach((a) => collectAttributes(a, acc));
      break;
  }
  return acc;
}

export function collectDeps(expr: MetricExpr, acc: Set<string> = new Set()): Set<string> {
  switch (expr.kind) {
    case "MetricRef":
      acc.add(expr.name);
      break;
    case "BinaryOp":
      collectDeps(expr.left, acc);
      collectDeps(expr.right, acc);
      break;
    case "Call":
      expr.args.forEach((a) => collectDeps(a, acc));
      break;
  }
  return acc;
}

/* --------------------------------------------------------------------------
 * COMPILATION
 * -------------------------------------------------------------------------- */

function aggregateRows(rows: RowSequence, attr: string | null, op: "sum" | "avg" | "min" | "max" | "count") {
  const values = rows
    .select((r: any) => (attr ? Number(r[attr]) : 1))
    .where((v: number) => !Number.isNaN(v))
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
  }
}

function evalBinary(op: string, left?: number, right?: number): number | undefined {
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
  const evaluator = (node: MetricExpr, ctx: any): number | undefined => {
    switch (node.kind) {
      case "Literal":
        return node.value;
      case "AttrRef": {
        const first = ctx.rows.first ? ctx.rows.first() : ctx.rows.toArray?.()?.[0];
        return first ? Number((first as any)[node.name]) : undefined;
      }
      case "MetricRef":
        return ctx.evalMetric(node.name);
      case "BinaryOp":
        return evalBinary(node.op, evaluator(node.left, ctx), evaluator(node.right, ctx));
      case "Call": {
        const fn = node.fn.toLowerCase();
        if (["sum", "avg", "min", "max", "count"].includes(fn)) {
          const [arg] = node.args;
          const attr = arg?.kind === "AttrRef" ? arg.name : null;
          const effectiveAttr = fn === "count" && (attr === null || attr === "*") ? null : attr;
          return aggregateRows(ctx.rows, effectiveAttr, fn as any);
        }
        if (fn === "last_year") {
          const metricArg = node.args[0];
          if (metricArg?.kind !== "MetricRef") return undefined;
          const cacheLabel = `last_year(${metricArg.name})`;
          const transformed = ctx.helpers.applyRowsetTransform("last_year", ctx.groupKey);
          return evaluateMetricRuntime(
            metricArg.name,
            ctx.helpers.runtime,
            ctx.groupKey,
            transformed,
            cacheLabel
          );
        }
        const evaluatedArgs = node.args.map((a) => evaluator(a, ctx) ?? 0);
        if (fn === "last_year" && evaluatedArgs.length === 1) {
          return evaluatedArgs[0];
        }
        return undefined;
      }
      default:
        return undefined;
    }
  };

  return (ctx) => evaluator(expr, ctx);
}

export function buildMetricDefinition(ast: MetricDefAst, metricNames: Set<string>): MetricDefinitionV2 {
  const resolved = resolveMetricExpr(ast.expr, metricNames);
  const attributes = Array.from(collectAttributes(resolved));
  const deps = Array.from(collectDeps(resolved));
  return {
    name: ast.name,
    baseFact: ast.baseFact,
    attributes,
    deps,
    exprAst: resolved,
    eval: compileMetricExpr(resolved),
  };
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
  return { kind, filters: items.map((i) => (i as any).kind ? (i as any) : i) } as FilterNode;
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

export function parseAll<T>(parser: Parser<T>, input: string): T {
  const result = parser.parse(input, 0);
  if (!result) throw new Error("Parse failed");
  return result.value;
}
