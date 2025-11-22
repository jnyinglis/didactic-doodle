
⸻

PRD: Parameter Bindings Across DSL, Filters, and Metrics (V2 Engine)

1. Purpose

The semantic engine now supports:
	•	Multi-fact frame construction
	•	Metric grains
	•	Runtime evaluation
	•	Table and rowset transforms

The next capability is parameter bindings, enabling queries and metrics to be parameterized with values supplied at execution time.

Examples:
	•	WHERE year = :year
	•	sales_for_year(:year)
	•	budget > :threshold
	•	HAVING margin > :minMargin

The goal is to support this consistently, both in DSL and semantic API.

⸻

2. Scope

We want to support bindings in the following areas:

✔ DSL WHERE filters

✔ DSL HAVING

✔ Metric definitions (V2 metrics)

✔ Top-level API (runSemanticQuery)

✔ Execution-time binding substitution in filter AST

✔ helpers.bind() inside metrics

Not in scope (for this PRD):
	•	Metric templates (compile-time parameters)
	•	Expression language for computed bindings (e.g. :year - 1)
	•	Persisted binding defaults in the model

⸻

3. Requirements

3.1 Bindings Map (Execution-Time)

Every query execution may supply:

bindings: {
  [name: string]: any
}

Example:

runSemanticQuery(env, spec, {
  bindings: {
    year: 2025,
    region: "NE",
    threshold: 100000
  }
});

This is the single source of truth for all parameters.

⸻

4. Filter Binding Requirements

Bindings appear inside filters as colon-prefixed names, e.g.:

where: f.eq("year", ":year")
where: f.and(f.eq("year", ":year"), f.gt("amount", ":minAmount"))

Engine behavior:
	1.	Parse/normalize filters as usual.
	2.	Before executing:
	•	replace any value "string" starting with ":" with the actual binding value.
	3.	This must work for:
	•	eq
	•	lt, lte, gt, gte
	•	between
	•	in
	•	arrays (f.in("region", [":r1", ":r2"]))

Example

Input filter:

f.and(
  f.eq("year", ":year"),
  f.between("amount", ":minAmount", ":maxAmount")
)

Bindings:

{ year: 2025, minAmount: 100, maxAmount: 500 }

Resolved filter becomes:

f.and(
  f.eq("year", 2025),
  f.between("amount", 100, 500)
)


⸻

5. Metric Binding Requirements

V2 metric definitions must be able to access binding values inside eval.

Add to MetricRuntimeV2:

bindings: Record<string, any>

Add to MetricComputationHelpers:

bind(name: string): any

Where:
	•	If bind(":year") is called → return bindings["year"]
	•	If missing → throw an error

Example metric

sales_for_year: {
  name: "sales_for_year",
  baseFact: "fact_sales",
  attributes: ["year"],   // metric grain
  eval: ({ rows, helpers }) => {
    const y = helpers.bind(":year");
    const filtered = rows.where(r => (r as any).year === y);
    return aggregate(filtered, "sales_amount", "sum");
  }
}

Another example: threshold metric

budget_over_threshold: {
  name: "budget_over_threshold",
  baseFact: "fact_budget",
  attributes: ["week"],
  deps: ["budget"],
  eval: ({ evalMetric, helpers }) => {
    const threshold = helpers.bind(":threshold");
    const budget = evalMetric("budget") ?? 0;
    return budget > threshold ? 1 : 0;
  }
}


⸻

6. HAVING Clause Bindings

Current design:

having?: (values: Record<string, number | undefined>) => boolean;

We do not change the engine for HAVING.
Instead:
	•	The DSL compiler converts HAVING expressions with :bindings into JavaScript closures that close over the bindings object.
	•	For direct API usage, users can close over the bindings themselves:

const bindings = { minMargin: 0.15 };

const spec: QuerySpecV2 = {
  dimensions: ["store"],
  metrics: ["margin_pct"],
  having: (values) => values["margin_pct"]! >= bindings.minMargin,
};

Optional: DSL compiler support for:

HAVING margin_pct >= :minMargin

but that is a DSL responsibility, not engine work.

⸻

7. Necessary Engine Changes (Developer Task List)

7.1 Add bindings to runSemanticQuery

interface ExecutionOptions {
  bindings?: Record<string, any>;
}

runSemanticQuery(env, spec, options?: ExecutionOptions)

Default:

const bindings = options?.bindings ?? {};


⸻

7.2 Add bindings to MetricRuntimeV2

Update:

export interface MetricRuntimeV2 {
  model: SemanticModelV2;
  db: InMemoryDb;
  baseFact?: string;
  relation: RowSequence;
  whereFilter: FilterNode | null;
  groupDimensions: string[];
  bindings: Record<string, any>;  // NEW
}

Pass down into:
	•	single-fact evaluation
	•	multi-fact evaluation
	•	dimension-only evaluation
	•	any recursive calls during metric evaluation

⸻

7.3 Add helpers.bind()

In evaluateMetricV2, where helpers are created:

const helpers = {
  runtime,
  applyRowsetTransform(...),
  bind(name: string) {
    const key = name.startsWith(":") ? name.slice(1) : name;
    if (!(key in runtime.bindings)) {
      throw new Error(`Missing binding '${name}'`);
    }
    return runtime.bindings[key];
  }
}

Expose this in the eval signature.

⸻

7.4 Filter Binding Resolution (must be done BEFORE frame building)

Create a helper:

resolveBindingsInFilter(filterNode: FilterNode | null, bindings: Record<string, any>): FilterNode | null

Rules:
	•	"string" starting with ":" → resolve from bindings
	•	Same for arrays
	•	Same for value2 in between expressions
	•	Recurse through AND/OR nodes

In runSemanticQuery:

const rawWhereNode = normalizeFilterContext(spec.where);
const whereNode = resolveBindingsInFilter(rawWhereNode, bindings);

Use whereNode everywhere (applyWhereFilter, passing to runtime, etc).

⸻

8. Example End-to-End Usage

Metric definitions:

sales_for_year: {
  name: "sales_for_year",
  baseFact: "fact_sales",
  attributes: ["year"],
  eval: ({ rows, helpers }) => {
    const y = helpers.bind(":year");
    const filtered = rows.where(r => r.year === y);
    return aggregate(filtered, "amount", "sum");
  }
},


budget_over_threshold: {
  name: "budget_over_threshold",
  baseFact: "fact_budget",
  attributes: ["week"],
  deps: ["budget"],
  eval: ({ evalMetric, helpers }) => {
    const threshold = helpers.bind(":threshold");
    const budget = evalMetric("budget") ?? 0;
    return budget > threshold ? 1 : 0;
  }
}

Query:

const spec: QuerySpecV2 = {
  dimensions: ["store", "week"],
  metrics: ["sales_for_year", "budget_over_threshold"],
  where: f.eq("region", ":region")
};

const result = runSemanticQuery(env, spec, {
  bindings: {
    year: 2025,
    region: "NE",
    threshold: 100000,
  }
});

Behavior:
	•	Frame built from all facts that support {store, week}
	•	sales_for_year evaluated using bound year = 2025
	•	budget_over_threshold evaluated using bound threshold
	•	Filter region = :region becomes region = 'NE'

⸻

9. Acceptance Criteria

A. Filters with bindings resolve correctly
	•	":year" replaced with actual value
	•	All filter operators supported

B. Metrics can access bindings via helpers.bind
	•	Errors on missing bindings
	•	Works across multi-fact path

C. Bindings propagate everywhere
	•	Filter resolution
	•	Metric evaluation
	•	DSL→QuerySpec→engine

D. No changes to engine semantics outside bindings
	•	Frame logic unchanged
	•	Multi-fact assembly unchanged
	•	Grain alignment unchanged

⸻

10. Future Extensions (Not required now)
	•	Metric templates with compile-time parameters
	•	DSL-side computed bindings (:year - 1)
	•	Scoped bindings (session-level, dashboard-level)

⸻

END OF PRD
