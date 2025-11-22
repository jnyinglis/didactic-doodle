import {
  FilterContext,
  FilterNode,
  MetricExpr,
  MetricDefinitionV2,
  QuerySpecV2,
  SemanticModel,
  MetricRegistry,
  buildMetricFromExpr,
} from "./semanticEngine";

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

export type UnresolvedMetricExpr =
  | MetricExpr
  | { kind: "Call"; fn: string; args: UnresolvedMetricExpr[] }
  | { kind: "BinaryOp"; op: "+" | "-" | "*" | "/"; left: UnresolvedMetricExpr; right: UnresolvedMetricExpr }
  | { kind: "UnresolvedIdent"; name: string };

export interface MetricDefAst {
  name: string;
  baseFact?: string;
  expr: UnresolvedMetricExpr;
}

export interface DslFileAst {
  metrics: MetricDefAst[];
  queries: QueryAst[];
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

const expr: Parser<UnresolvedMetricExpr> = lazy(() => additive);

const functionCall: Parser<UnresolvedMetricExpr> = {
  parse(input, pos) {
    const nameRes = identifier.parse(input, pos);
    if (!nameRes) return null;
    const lp = symbol("(").parse(input, nameRes.nextPos);
    if (!lp) return null;

    const fn = nameRes.value;
    let nextPos = lp.nextPos;
    let args: UnresolvedMetricExpr[] = [];

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

const primary: Parser<UnresolvedMetricExpr> = choice(
  between(symbol("("), expr, symbol(")")),
  functionCall,
  map(numberLiteral, (n) => ({ kind: "Literal", value: n } as UnresolvedMetricExpr)),
  map(identifier, (name) => ({ kind: "UnresolvedIdent", name } as UnresolvedMetricExpr))
);

const multiplicative: Parser<UnresolvedMetricExpr> = map(
  chainLeft(primary, choice(symbol("*"), symbol("/"))),
  (node: any) => {
    if (node.op) {
      return {
        kind: "BinaryOp",
        op: node.op,
        left: node.left,
        right: node.right,
      } as UnresolvedMetricExpr;
    }
    return node as UnresolvedMetricExpr;
  }
);

const additive: Parser<UnresolvedMetricExpr> = map(
  chainLeft(multiplicative, choice(symbol("+"), symbol("-"))),
  (node: any) => {
    if (node.op) {
      return {
        kind: "BinaryOp",
        op: node.op,
        left: node.left,
        right: node.right,
      } as UnresolvedMetricExpr;
    }
    return node as UnresolvedMetricExpr;
  }
);

const argList: Parser<UnresolvedMetricExpr[]> = sepBy(expr, symbol(","));

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

export function resolveMetricExpr(
  expr: UnresolvedMetricExpr,
  metricNames: Set<string>
): MetricExpr {
  switch (expr.kind) {
    case "UnresolvedIdent":
      return metricNames.has(expr.name)
        ? { kind: "MetricRef", name: expr.name }
        : { kind: "AttrRef", name: expr.name };
    case "BinaryOp":
      return {
        kind: "BinaryOp",
        op: expr.op,
        left: resolveMetricExpr(expr.left, metricNames),
        right: resolveMetricExpr(expr.right, metricNames),
      };
    case "Call":
      return {
        kind: "Call",
        fn: expr.fn,
        args: expr.args.map((a) => resolveMetricExpr(a, metricNames)),
      };
    default:
      return expr;
  }
}

/* --------------------------------------------------------------------------
 * COMPILATION
 * -------------------------------------------------------------------------- */

export function buildMetricDefinition(ast: MetricDefAst, metricNames: Set<string>): MetricDefinitionV2 {
  const resolved = resolveMetricExpr(ast.expr, metricNames);
  return buildMetricFromExpr({
    name: ast.name,
    baseFact: ast.baseFact,
    expr: resolved,
  });
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

/* --------------------------------------------------------------------------
 * FILE PARSER + COMPILATION ENTRY POINTS
 * -------------------------------------------------------------------------- */

const fileParser: Parser<DslFileAst> = {
  parse(input, pos) {
    let nextPos = skipWs(input, pos);
    const metrics: MetricDefAst[] = [];
    const queries: QueryAst[] = [];

    while (nextPos < input.length) {
      const metric = metricDef.parse(input, nextPos);
      if (metric) {
        metrics.push(metric.value);
        nextPos = metric.nextPos;
        continue;
      }
      const query = queryDef.parse(input, nextPos);
      if (query) {
        queries.push(query.value);
        nextPos = query.nextPos;
        continue;
      }
      break;
    }

    nextPos = skipWs(input, nextPos);
    return { value: { metrics, queries }, nextPos };
  },
};

export function parseDsl(text: string): DslFileAst {
  const result = fileParser.parse(text, 0);
  if (!result || skipWs(text, result.nextPos) !== text.length) {
    throw new Error("DSL parse error (TODO: better error reporting)");
  }
  return result.value;
}

export function compileDslToModel(
  text: string,
  baseModel: SemanticModel
): { model: SemanticModel; queries: Record<string, QuerySpecV2> } {
  const ast = parseDsl(text);
  const metricNames = new Set([
    ...Object.keys(baseModel.metrics ?? {}),
    ...ast.metrics.map((m) => m.name),
  ]);

  const newMetrics: MetricRegistry = { ...baseModel.metrics };
  ast.metrics.forEach((mAst) => {
    const def = buildMetricFromExpr({
      name: mAst.name,
      baseFact: mAst.baseFact,
      expr: resolveMetricExpr(mAst.expr, metricNames),
    });
    newMetrics[def.name] = def;
  });

  const queries: Record<string, QuerySpecV2> = {};
  ast.queries.forEach((qAst) => {
    queries[qAst.name] = qAst.spec;
  });

  const model: SemanticModel = {
    ...baseModel,
    metrics: newMetrics,
  };

  return { model, queries };
}

export function parseAll<T>(parser: Parser<T>, input: string): T {
  const result = parser.parse(input, 0);
  if (!result) throw new Error("Parse failed");
  return result.value;
}
