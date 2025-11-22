Team,

Below are concrete instructions for implementing the metric/query AST and wiring it into the current semanticEngine.ts + dsl.ts using a parser-combinator approach.

I’ll assume:
	•	semanticEngine.ts is the current minimal engine with:
	•	MetricDefinitionV2 extends MetricDefinition { exprAst?: any; }
	•	aggregateMetric, rowsetTransformMetric, MetricRuntime, QuerySpec etc.
	•	dsl.ts is effectively empty / stubbed and is free to be used for the DSL parser.

⸻

0. High-level shape

We want:
	1.	A typed AST for metric expressions and queries.
	2.	A parser combinator in dsl.ts that:
	•	Parses metric + query definitions into AST.
	3.	A compiler that:
	•	Turns MetricExpr → MetricEvalV2 (the engine’s runtime function).
	•	Turns Query AST → QuerySpecV2 (for runSemanticQuery).

All of this should sit on top of the current engine. Existing programmatic helpers like aggregateMetric keep working.

⸻

1. Add the AST types to semanticEngine.ts

1.1. Metric expression AST

In semanticEngine.ts, near the metric definitions (MetricDefinition, MetricDefinitionV2), add:

// ---------------------------------------------------------------------------
// METRIC EXPRESSION AST
// ---------------------------------------------------------------------------

export type MetricExpr =
  | { kind: "Literal"; value: number }
  | { kind: "AttrRef"; name: string }          // logical attribute name
  | { kind: "MetricRef"; name: string }        // reference to another metric
  | { kind: "Call"; fn: string; args: MetricExpr[] }
  | {
      kind: "BinaryOp";
      op: "+" | "-" | "*" | "/";
      left: MetricExpr;
      right: MetricExpr;
    };

We’ll not support anything more complicated in v1. That’s enough for:
	•	Basic aggregates: sum(sales_amount)
	•	Arithmetic: (sum_sales - sum_sales_last_year) / sum_sales_last_year
	•	Transforms: last_year(sum_sales, by tradyrwkcode) as a Call node.

1.2. Tighten MetricDefinitionV2

Change:

export interface MetricDefinitionV2 extends MetricDefinition {
  exprAst?: any;
}

to:

export interface MetricDefinitionV2 extends MetricDefinition {
  exprAst?: MetricExpr;
}

There may be call sites referring to MetricDefinitionV2 as if it were MetricDefinition. That’s fine; exprAst is optional.

⸻

2. Utility helpers around MetricExpr

We’ll need to:
	•	Extract attributes and metric dependencies from the AST.
	•	This will populate MetricDefinitionV2.attributes and MetricDefinitionV2.deps.

Add the following below the AST types:

function collectAttrsAndDeps(expr: MetricExpr): {
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

We’ll use this when we construct MetricDefinitionV2 from the DSL.

⸻

3. Compiler: MetricExpr → MetricEvalV2

We already have:
	•	MetricEvalV2 (alias of MetricEval).
	•	aggregate helper (attr + op over rows).
	•	rowsetTransformMetric shows how to call applyRowsetTransform + evaluateMetric.

We’ll now implement a generic expression compiler.

Add:

// ---------------------------------------------------------------------------
// METRIC EXPRESSION COMPILER
// ---------------------------------------------------------------------------

export function compileMetricExpr(expr: MetricExpr): MetricEvalV2 {
  return (ctx) => {
    function evalExpr(e: MetricExpr): number | undefined {
      switch (e.kind) {
        case "Literal":
          return e.value;

        case "AttrRef": {
          // Bare attribute reference at top-level is not allowed in v1;
          // attributes should only appear inside aggregate calls.
          throw new Error(
            `Bare AttrRef "${e.name}" not supported outside an aggregate`
          );
        }

        case "MetricRef": {
          // Delegate to dependency metrics by name
          return ctx.evalMetric(e.name);
        }

        case "BinaryOp": {
          const left = evalExpr(e.left);
          const right = evalExpr(e.right);
          if (left === undefined || right === undefined) return undefined;

          switch (e.op) {
            case "+":
              return left + right;
            case "-":
              return left - right;
            case "*":
              return left * right;
            case "/":
              return right === 0 ? undefined : left / right;
          }
        }

        case "Call": {
          const fn = e.fn;

          // Aggregate-style functions: sum(attr), avg(attr), ...
          if (
            fn === "sum" ||
            fn === "avg" ||
            fn === "min" ||
            fn === "max" ||
            fn === "count"
          ) {
            const [arg] = e.args;
            if (!arg || arg.kind !== "AttrRef") {
              throw new Error(`Aggregate ${fn} expects AttrRef argument`);
            }
            return aggregate(ctx.rows, arg.name, fn as AggregationOperator);
          }

          // Time-intelligence transform: last_year(metricName, by anchorAttr)
          if (fn === "last_year") {
            const [metricExpr, anchorExpr] = e.args;

            if (!metricExpr || metricExpr.kind !== "MetricRef") {
              throw new Error(
                "last_year first argument must be MetricRef (metric name)"
              );
            }
            if (!anchorExpr || anchorExpr.kind !== "AttrRef") {
              throw new Error(
                "last_year second argument must be AttrRef (anchor attribute)"
              );
            }

            const metricName = metricExpr.name;
            const anchorAttr = anchorExpr.name;
            const transformId = `last_year:${anchorAttr}`;

            const transformedRows = ctx.helpers.applyRowsetTransform(
              transformId,
              ctx.groupKey
            );

            const cacheLabel = `${metricName}:last_year`;
            const value = evaluateMetric(
              metricName,
              ctx.helpers.runtime,
              ctx.groupKey,
              transformedRows,
              cacheLabel
            );
            return value;
          }

          throw new Error(`Unknown function "${fn}" in metric expression`);
        }
      }
    }

    return evalExpr(expr);
  };
}

Notes:
	•	This compiler assumes that rowset transforms are registered in model.rowsetTransforms with IDs like last_year:<anchorAttr>.
	•	That matches the existing RowsetTransformDefinition + applyRowsetTransform.

⸻

4. Integration: building MetricDefinitionV2 from MetricExpr

We want a helper that takes a parsed metric declaration (from the DSL) and returns a fully wired MetricDefinitionV2.

In semanticEngine.ts, add something like:

export interface MetricFromAstOptions {
  name: string;
  baseFact?: string;
  expr: MetricExpr;
  description?: string;
}

/**
 * Build a MetricDefinitionV2 from a MetricExpr AST.
 */
export function buildMetricFromExpr(
  opts: MetricFromAstOptions
): MetricDefinitionV2 {
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

This is what the DSL parser will call once it’s produced the AST for each metric definition.

⸻

5. dsl.ts: define the DSL AST and parser skeleton

We’ll keep the AST types for metrics/queries in semanticEngine.ts (for now), but dsl.ts should define:
	•	A small AST for metric declarations and query declarations.
	•	A parser combinator core.
	•	Parsers that map input text → these ASTs.
	•	A compile step from DSL AST → engine types (MetricDefinitionV2, QuerySpecV2).

5.1. DSL-level AST in dsl.ts

At the top of dsl.ts, add:

import {
  MetricExpr,
  MetricExpr as EngineMetricExpr,
  MetricDefinitionV2,
  QuerySpecV2,
  buildMetricFromExpr,
} from "./semanticEngine";

/**
 * Raw AST for a metric declaration in the DSL.
 * This is "syntax-level"; we turn it into MetricDefinitionV2 via buildMetricFromExpr.
 */
export interface MetricDeclAst {
  name: string;
  baseFact?: string;
  expr: MetricExpr;
}

/**
 * Raw AST for a query declaration in the DSL.
 */
export interface QueryAst {
  name: string;
  spec: QuerySpecV2;
}

(You can alias MetricExpr as EngineMetricExpr if you want to emphasize that it comes from the engine.)

We also want a “file” AST:

export interface DslFileAst {
  metrics: MetricDeclAst[];
  queries: QueryAst[];
}

5.2. Parser combinator core (minimal)

Still in dsl.ts, define a basic parser type + core combinators (or use a library):

type ParseResult<T> = { value: T; nextPos: number };
type Parser<T> = (input: string, pos: number) => ParseResult<T> | null;

// Basic helpers
function map<A, B>(p: Parser<A>, f: (a: A) => B): Parser<B> { /* ... */ }
function seq<A, B>(p1: Parser<A>, p2: Parser<B>): Parser<[A, B]> { /* ... */ }
function choice<T>(...parsers: Parser<T>[]): Parser<T> { /* ... */ }
function many<T>(p: Parser<T>): Parser<T[]> { /* ... */ }
function opt<T>(p: Parser<T>): Parser<T | null> { /* ... */ }

function token(str: string): Parser<string> { /* keyword/symbol parser */ }
function regex(re: RegExp): Parser<string> { /* low-level matcher */ }

Implementation details are straightforward; the important point is: parsers return typed AST nodes, not strings.

You will also want:
	•	identifier: Parser<string>
	•	numberLiteral: Parser<number>
	•	keyword("metric"), keyword("query"), keyword("on"), keyword("by"), etc.
	•	symbol("="), "(", ")", "{", "}", ",", ":", operators.

All of them should consume trailing whitespace.

5.3. Expression parser → MetricExpr

Implement expression grammar with precedence using combinators.

Key structure:

const expr: Parser<MetricExpr> = additive;

const additive: Parser<MetricExpr> = chainLeft(
  multiplicative,
  choice(symbol("+"), symbol("-")).map(op => op as "+" | "-")
);

const multiplicative: Parser<MetricExpr> = chainLeft(
  primary,
  choice(symbol("*"), symbol("/")).map(op => op as "*" | "/")
);

const primary: Parser<MetricExpr> = choice(
  parens(expr),
  functionCall,
  numberLiteral.map(n => ({ kind: "Literal", value: n })),
  identifier.map(name => ({ kind: "AttrRef", name })) // for v1: treat bare ident as AttrRef; MetricRef will be handled by name resolution later if needed
);

For functionCall:

const functionCall: Parser<MetricExpr> = map(
  seq(identifier, symbol("("), argList, symbol(")")),
  ([fnName, _lp, args, _rp]) =>
    ({ kind: "Call", fn: fnName, args } as MetricExpr)
);

const argList: Parser<MetricExpr[]> = /* expr separated by ',' */;

Special case for last_year(sum_sales, by tradyrwkcode):
	•	Option 1 (simpler): treat by as just an identifier; rely on the parser to see it as a Call("last_year", [MetricRef/AttrRef...]) and enforce structure later.
	•	Option 2 (stricter): create a dedicated parser for last_year that expects "by" keyword between arguments, but still returns a generic Call node.

For v1, it’s fine to parse it as:

Call("last_year", [
  MetricRef("sum_sales"),  // if we treat first arg as MetricRef in a dedicated parser
  AttrRef("tradyrwkcode")
])

or

Call("last_year", [
  AttrRef("sum_sales"),
  AttrRef("by"),
  AttrRef("tradyrwkcode")
])

and then normalize in a validation pass; I’d recommend the first approach if you’re comfortable writing a small special-case parser.

⸻

6. Parse metric declarations

Define a parser that yields MetricDeclAst:

const metricDecl: Parser<MetricDeclAst> = map(
  seq(
    keyword("metric"),
    identifier,                            // metric name
    opt(seq(keyword("on"), identifier)),   // optional "on fact_sales"
    symbol("="),
    expr
  ),
  ([, name, onPart, , expression]) => {
    const baseFact = onPart ? onPart[1] : undefined;
    return { name, baseFact, expr: expression };
  }
);

Later, when you compile DSL AST → engine, you’ll call:

const def = buildMetricFromExpr({
  name: metricAst.name,
  baseFact: metricAst.baseFact,
  expr: metricAst.expr,
});

and register def into SemanticModel.metrics.

⸻

7. Parse query definitions

We want the DSL form:

query weekly_sales_vs_last_year {
  dimensions: tradyrwkcode
  metrics: sum_sales, sum_sales_last_year, sales_growth_pct

  where:
    tradyrwkcode >= 202501
    and tradyrwkcode <= 202552

  having:
    sum_sales > 0
}

In dsl.ts, define:

const queryDecl: Parser<QueryAst> = map(
  seq(
    keyword("query"),
    identifier,
    symbol("{"),
    many(queryLine),
    symbol("}")
  ),
  ([, name, , lines]) => {
    const spec: QuerySpecV2 = {
      dimensions: [],
      metrics: [],
      where: undefined,
      having: undefined,
    };

    for (const line of lines) {
      // line will be discriminated union:
      //   { kind: "dimensions", values: string[] }
      //   { kind: "metrics", values: string[] }
      //   { kind: "where", filter: FilterNode }
      //   { kind: "having", ast: MetricHavingAst }
      // fold into spec
    }

    return { name, spec };
  }
);

Implement queryLine as a choice among four parsers:
	•	dimensions: identList
	•	metrics: identList
	•	where: boolExpr → FilterNode (you already have filter AST in engine)
	•	having: metricBoolExpr → dedicated MetricHavingAst

For where:
	•	You can reuse or adapt the existing filter AST (FilterExpression, FilterConjunction, etc.) as the target.
	•	The parser should produce a FilterNode that you then pass directly into QuerySpecV2.where.

For having:
	•	Define a small AST in dsl.ts:

type MetricHavingAst =
  | { kind: "MetricCmp"; metric: string; op: ">" | ">=" | "<" | "<=" | "==" | "!="; value: number }
  | { kind: "And" | "Or"; items: MetricHavingAst[] };


	•	Parse an expression like sum_sales > 0 and sum_sales_last_year > 0 into this AST.
	•	Then compile MetricHavingAst → (values: Record<string, number | undefined>) => boolean and assign to spec.having.

⸻

8. Top-level parseDsl entry point

Finally, in dsl.ts, provide a single entry point:

const fileParser: Parser<DslFileAst> = /* many(metricDecl | queryDecl) with whitespace, comments, etc. */;

export function parseDsl(text: string): DslFileAst {
  const result = fileParser(text, 0);
  if (!result || result.nextPos !== text.length) {
    throw new Error("DSL parse error (TODO: better error reporting)");
  }
  return result.value;
}

Then, a helper to compile DSL → engine artifacts:

export function compileDslToModel(
  text: string,
  baseModel: SemanticModel
): { model: SemanticModel; queries: Record<string, QuerySpecV2> } {
  const ast = parseDsl(text);

  // 1) Compile metrics
  const newMetrics: MetricRegistry = { ...baseModel.metrics };
  ast.metrics.forEach((mAst) => {
    const def = buildMetricFromExpr({
      name: mAst.name,
      baseFact: mAst.baseFact,
      expr: mAst.expr,
    });
    newMetrics[def.name] = def;
  });

  // 2) Collect queries
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

So client code can:

const { model, queries } = compileDslToModel(dslText, baseModel);
const env = { model, db: inMemoryDb };
const result = runSemanticQuery(env, queries["weekly_sales_vs_last_year"]);


⸻

9. Implementation sequence (recommended)

For the dev team, I’d suggest:
	1.	Step 1 – Engine AST & compiler
	•	Add MetricExpr to semanticEngine.ts.
	•	Implement collectAttrsAndDeps, compileMetricExpr, buildMetricFromExpr.
	•	Write unit tests for compileMetricExpr with manually constructed ASTs.
	2.	Step 2 – DSL expression parser
	•	In dsl.ts, implement parser combinator core.
	•	Implement expr / additive / multiplicative / primary / functionCall.
	•	Unit test: string → MetricExpr.
	3.	Step 3 – Metric declarations
	•	Implement metricDecl parser.
	•	Add parseDsl that returns metrics only.
	•	Use buildMetricFromExpr to compile DSL metrics and run them through the engine.
	4.	Step 4 – Queries (dimensions/metrics only)
	•	Implement queryDecl with dimensions + metrics.
	•	Wire into parseDsl and compileDslToModel.
	•	Confirm runSemanticQuery works with queries defined in DSL.
	5.	Step 5 – where + having
	•	Implement boolean expression parsers for where → FilterNode and having → MetricHavingAst.
	•	Implement compiler for MetricHavingAst → (values) => boolean.
	6.	Step 6 – last_year transform
	•	Ensure compileMetricExpr and applyRowsetTransform interoperate correctly with last_year(...) calls from DSL.
	•	Add tests that:
	•	define sum_sales and sum_sales_last_year in DSL,
	•	load transform table into InMemoryDb,
	•	verify results.

If you share dev feedback about any rough edges or code layout preferences, I can refine this into a repo-ready DSL.md and/or concrete TS snippets for each step.
