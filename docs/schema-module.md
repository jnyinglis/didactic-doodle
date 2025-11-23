Below is a full PRD describing the new Semantic Engine – Schema-First, Module-Based Architecture, including:
	•	Product goals
	•	Architecture overview
	•	Definitions & data structures
	•	API design
	•	Compilation flow
	•	Examples:
	•	Metric-only module
	•	Query module
	•	Multiple DSL modules
	•	Multiple query calls
	•	Parameter binding
	•	Migration notes for the current codebase

This document is ready to hand to your developer.

⸻

PRD – Schema-First, Module-Based Semantic Engine

1. Product Goal

Introduce a unified, modular, and predictable way to load:
	•	Schema (facts, dims, joins, attributes)
	•	Metric libraries (metric-only DSL files)
	•	Query modules (query-only or mixed DSL files)

…into a SemanticEngine that:
	•	Owns the evolving SemanticModel
	•	Owns registered queries
	•	Lets the caller run queries by name
	•	Supports metric-level and query-level parameter binding

This eliminates the existing dual-pipeline problem (parseMetricBlock + compileMetrics vs compileDslToModel) and makes metric libraries first-class modules rather than “magic plumbing.”

⸻

2. Requirements

2.1 Functional Requirements
	1.	Schema is defined separately and contains only structural information:
	•	Facts
	•	Dimensions
	•	Attributes
	•	Join graph
	2.	SemanticEngine is created from a schema + DB handle.
	3.	The engine can load any number of DSL files via:

engine.useDslFile(text)


	4.	Each DSL file may contain:
	•	Only metrics
	•	Only queries
	•	Or both
	5.	Loading a DSL file:
	•	Parses metrics + queries (parseDsl)
	•	Compiles metrics onto the current model (compileDslToModel)
	•	Registers queries
	6.	Multiple DSL modules stack, later ones can reference earlier metrics.
	7.	Queries can be run via:

engine.runQuery("weekly_sales", bindings)


	8.	Bindings apply to:
	•	Query filters
	•	Metric expressions
	•	Context transforms
Everything that currently accepts identifiers or constants should allow parameter placeholders.

⸻

2.2 Non-Functional Requirements
	•	No breaking changes to existing metric AST or query AST formats.
	•	Must support incremental adoption (existing demo code should be easily adapted).
	•	Internals must replace parseMetricBlock and compileMetrics with wrappers around the new unified module pipeline.

⸻

3. Architecture Summary

3.1 Layers

                       +------------------------------+
                       |       DSL Module Files       |
                       |  (metrics, queries, both)    |
                       +------------------------------+
                                      ↓
                       +------------------------------+
                       |      compileDslToModel       |
                       |  - parse metrics & queries   |
                       |  - resolve metric refs       |
                       |  - validate expressions      |
                       |  - merges metrics into model |
                       +------------------------------+
                                      ↓
                       +------------------------------+
                       |     SemanticEngine (API)     |
                       | holds:                       |
                       |   - schema                   |
                       |   - evolving model           |
                       |   - query registry           |
                       +------------------------------+
                                      ↓
                       +------------------------------+
                       |       runSemanticQuery       |
                       +------------------------------+
                                      ↓
                       +------------------------------+
                       |        In-Memory DB          |
                       +------------------------------+


⸻

4. Data Structures

4.1 Schema

export interface Schema {
  facts: Record<string, FactRelation>;
  dimensions: Record<string, { name: string }>;
  attributes: Record<string, LogicalAttribute>;
  joins: JoinEdge[];
}

4.2 SemanticModel

export interface SemanticModel extends Schema {
  metrics: MetricRegistry;
  rowsetTransforms?: Record<string, RowsetTransformDefinition>;
}


⸻

5. Engine API Specification

5.1 Creation

const engine = SemanticEngine.fromSchema(schema, db);

5.2 Loading DSL Modules

engine.useDslFile(text)

	•	Builds DslFileAst via parseDsl
	•	Compiles metrics + query specs via compileDslToModel
	•	Merges metrics into engine.model
	•	Adds query specs to internal registry

5.3 Running Queries

engine.runQuery("weekly_sales", { salesWeek: 202401 });

5.4 Direct Registration APIs (optional)

engine.registerMetric(metricDef);
engine.registerQuery("name", querySpec);


⸻

6. Parameter Binding Design

6.1 Syntax

In DSL:

query sales_for_week {
  dimensions: storeName
  metrics: total_sales
  where: salesWeek = :week
}

In code:

engine.runQuery("sales_for_week", { week: 202512 });

Binding applies at:
	•	WHERE clause
	•	HAVING
	•	Metric expressions (e.g. sum(amount * :fx_rate))
	•	Context transforms

6.2 Internal Mechanics
	•	During runSemanticQuery, resolve any identifier beginning with : to bindings[paramName].
	•	Bindings override literal values.

⸻

7. Examples

7.1 Example 1 – Schema Setup

const schema: Schema = {
  facts: {
    fact_orders: { name: "fact_orders", primaryKey: "orderId" },
    fact_returns: { name: "fact_returns", primaryKey: "returnId" },
  },
  dimensions: {
    dim_store: { name: "dim_store" },
    dim_calendar: { name: "dim_calendar" }
  },
  attributes: {
    storeName: { relation: "dim_store", column: "store_name", baseFact: "fact_orders" },
    region:    { relation: "dim_store", column: "region", baseFact: "fact_orders" },
    orderDate: { relation: "dim_calendar", column: "date", baseFact: "fact_orders" }
  },
  joins: [
    { from: "fact_orders", to: "dim_store", on: [["storeId", "id"]] },
    { from: "fact_orders", to: "dim_calendar", on: [["dateId", "id"]] }
  ]
};


⸻

7.2 Example 2 – Metric-Only Module

coreMetrics.dsl

metric total_sales on fact_orders = sum(amount)
metric orders on fact_orders = count(orderId)

metric avg_ticket on fact_orders =
  total_sales / orders

Load it:

engine.useDslFile(read("coreMetrics.dsl"));


⸻

7.3 Example 3 – Query Module

weeklyQueries.dsl

query weekly_sales {
  dimensions: storeName, region, orderDate
  metrics: total_sales, orders, avg_ticket
  where: orderDate >= :start and orderDate <= :end
}

Load it:

engine.useDslFile(read("weeklyQueries.dsl"));


⸻

7.4 Example 4 – Multiple Modules in Sequence

const engine = SemanticEngine.fromSchema(schema, db)
  .useDslFile(read("coreMetrics.dsl"))
  .useDslFile(read("returnsMetrics.dsl"))   // extra metrics
  .useDslFile(read("weeklyQueries.dsl"))
  .useDslFile(read("promoQueries.dsl"));

Metrics and queries accumulate.

⸻

7.5 Example 5 – Running Multiple Queries

const weekly = engine.runQuery("weekly_sales", {
  start: 20240101,
  end:   20240107,
});

const monthly = engine.runQuery("monthly_sales", {
  month: 202401
});


⸻

7.6 Example 6 – Metric Expression with Binding

DSL

metric sales_fx on fact_orders = total_sales * :fx_rate

Call

engine.runQuery("weekly_sales", {
  start: 20240101,
  end: 20240107,
  fx_rate: 1.34
});


⸻

8. Compilation Flow (Developer Notes)

8.1 useDslFile flow

useDslFile(text)
  ↓
parseDsl(text)
  ↓
DslFileAst { metrics[], queries[] }
  ↓
compileDslToModel(ast, currentModel)
  - resolveMetricRefs
  - validateMetricExpr
  - buildMetricFromExpr
  - merge metrics into model
  - convert queries into QuerySpecV2
  ↓
update engine.metrics and engine.queries


⸻

9. Migration Plan

Step 1 – Create Schema type

Extract structural parts from SemanticModel.

Step 2 – Implement SemanticEngine

Wrap current code. Start with useDslFile.

Step 3 – Replace parseMetricBlock and compileMetrics

Implement them as thin wrappers around:

compileDslToModel(metricText, baseModel)

Step 4 – Modify demo (dslDemo.ts)

Replace:

parseMetricBlock(...)
compileMetrics(...)

With:

const engine = SemanticEngine.fromSchema(schema, db)
  .useDslFile(metricText)
  .useDslFile(queryText);

Step 5 – Add binding support

Modify runSemanticQuery to resolve :paramName.

⸻

End of PRD

If you’d like, I can also generate:
	•	A TypeScript scaffold for the new SemanticEngine
	•	Updated versions of dslDemo.ts and semanticEngine.ts
	•	A migration diff/patch for the current repo
