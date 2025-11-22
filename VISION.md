

## 1. Core design (finalized)

### 1.1 Schema & semantic model

We assume a semantic model (outside the DSL) that defines:

* A set of **fact relations**
  (F₁, F₂, …)
  e.g. (F_{\text{sales}}) for `retail_sales_facts`.

* A set of **dimension relations**
  (D₁, D₂, …)
  (store, product, customer, calendar, region, etc.)

* A **join graph** between facts and dimensions:

  * foreign keys, cardinalities,
  * which dimensions are reachable from which fact.

* A mapping from **logical attribute names** (e.g. `store`, `transaction_date`) to:

  * specific relation attributes (columns),
  * and a default base fact they’re associated with.

Important: every column in every relation is an **attribute**.
There is no special “measure type” in the schema.

---

### 1.2 Metrics

A **metric** is:

> A named scalar function that, at runtime, is evaluated over a *group* of fact rows (plus a filter context), and returns a number.

Crucial invariant:

> **Metrics are grain-agnostic.**
> They never store or “hardcode” a grain; they are always recomputed from fact rows at whatever grain the query asks for.

Implementation-wise:

```ts
type MetricEval = (
  ctx: {
    rows: RowSequence;              // the group of fact rows for one output row
    groupKey: Record<string, any>;  // dimensions identifying the group
    evalMetric: (name: string) => number | undefined; // to call other metrics
  }
) => number | undefined;
```

The metric body is defined in your DSL, but its semantics are a function from `rows` → scalar.

---

### 1.3 Queries

A **query** in the DSL looks like:

```dsl
query Q {
  dimensions
    d1, d2, ..., dk

  metrics
    m1, m2, ..., mr

  where
    predicate_attr   -- attribute-level filter

  having
    predicate_metric -- metric-level filter (optional)
}
```

Design commitments:

* **Grain** = the list of dimensions `(d1, …, dk)` – this is explicit.
* The query does **not** mention tables or joins; those are derived from the semantic model.
* The engine:

  * chooses a base fact (F) compatible with all metrics and dimensions,
  * figures out joins to all referenced attributes,
  * runs relational algebra over that.

---

### 1.4 Transforms

We have two conceptually distinct kinds:

1. **Rowset transforms** (table-level, pre-aggregation):
   [
   T: R \to R'
   ]
   e.g. applying time windows, shifting to last year, restricting to finalized sales transactions, etc.

2. **Metric transforms** (expression-level or higher-order):
   They wrap one or more base metrics, but in practice just arrange to call those metrics on a *different* rowset (e.g. last year’s rows).

All of this stays compatible with the “metric = function over group rows” invariant.

---

## 2. Relational-algebra formalization

Let’s define the core relational-algebra pieces that correspond to query + metric execution.

### 2.1 Basic RA notation

We’ll use:

* (R, S) for relations (tables, possibly joined).
* Attributes (columns) as (A, B, …).
* Standard operators:

  * **Selection**: (\sigma_{\varphi}(R)) – filter rows by boolean predicate (\varphi).
  * **Projection**: (\pi_{A_1,\dots,A_k}(R)).
  * **Join**: (R \bowtie_{\theta} S).
  * **Aggregation/group-by** (extended RA):
    [
    \gamma_{G; f_1(\dots), f_2(\dots), \dots}(R)
    ]
    where (G) is a list of group attributes; (f_i) are aggregate functions.

---

### 2.2 Schema level

Each **fact** is a relation:

* Example:
  [
  F_{\text{sales}}(\text{store_id}, \text{transaction_date}, \text{product_id}, \text{units_sold}, \text{net_sales_amount}, \dots)
  ]

Each **dimension** is a relation:

* Example:
  [
  D_{\text{store}}(\text{store_id}, \text{store_name}, \text{region_id}, \dots)
  ]

A **logical attribute name** `store` is mapped (by the semantic model) to some attribute in a relation, e.g.:

* `store` ↦ (D_{\text{store}}.\text{store_name})

And we know how to reach that dimension from a given fact, e.g.:

* (F_{\text{sales}}.\text{store_id} = D_{\text{store}}.\text{store_id})

We won’t write the entire join graph formally, but conceptually: for each fact (F) and each logical attribute (a) usable at query time, there is a defined RA expression (J(F,a)) that brings in the dimension where that attribute lives.

---

### 2.3 Query semantics in RA

Given a query:

```dsl
query Q {
  dimensions d1, ..., dk
  metrics    m1, ..., mr
  where      φ_attr
  having     φ_metric
}
```

1. The engine chooses a **base fact** (F).
   (For now, assume all metrics are defined over the same fact.)

2. It builds a **base joined relation** (R):

   * Start with fact (F).
   * Join any dimension tables needed to resolve:

     * the dimension attributes (d_1, \dots, d_k),
     * any attributes used in `where`.

   Symbolically:
   [
   R := F \bowtie D_{i_1} \bowtie D_{i_2} \dots
   ]
   with appropriate join conditions.

3. Apply the **row-level filter** `where φ_attr` as a selection:
   [
   R' := \sigma_{\varphi_{\text{attr}}}(R)
   ]
   where (\varphi_{\text{attr}}) is the predicate interpreted over attributes of (R).

4. **Group by dimensions**:

   Let (G) be the list of attributes representing the logical dimensions `d1, …, dk` in (R). Then the grouped result (before metrics) is conceptually:
   [
   G_R := \gamma_{G}(R')
   ]
   Here we’re only interested in the grouping partition of (R') induced by (G); the actual metric values will be computed per group.

5. For each group (g \in G_R):

   * Consider the subset of rows:
     [
     R'*g := \sigma*{\wedge_j (G_j = g_j)}(R')
     ]
     i.e. all rows in (R') whose group attributes equal the current group key (g).

   * For each metric (m_i), compute its scalar value:
     [
     v_i(g) := \llbracket m_i \rrbracket(R'_g)
     ]
     where (\llbracket m_i \rrbracket) is the metric’s evaluation function over a relation (below).

   * These become new attributes on the grouped result.

6. **Metric-level filter** (having):

   If we have a `having φ_metric` clause, that predicate uses metric values. So after computing (v_i(g)), we select only those groups (g) for which:
   [
   \varphi_{\text{metric}}(v_1(g), \dots, v_r(g))\ \text{is true}
   ]

7. Final result is a relation:
   [
   Q(F) := { (g, v_1(g), \dots, v_r(g)) \mid g \in G_R\ \text{and passes `having`} }
   ]

This is the **relational-algebra semantics of a query**.

---

### 2.4 Metric semantics in RA

Now: what is (\llbracket m \rrbracket(R'_g)) precisely?

Each metric is defined in the DSL as an expression built from:

* Aggregators: `sum(attrExpr)`, `avg(...)`, `count(...)`, etc.
* Row-level expressions over attributes.
* Other metrics (as scalar dependencies).
* Optionally, metric transforms like `last_year(m, date_attr)`.

We can separate:

1. **Row-level expressions**:
   expressions over attributes, interpreted per row (t \in R'_g).

   * Semantics: (\llbracket e \rrbracket(t)) – standard expression evaluation.

2. **Aggregation operators**:
   Given an attribute-level expression (e) and a group of rows (R'_g), define:

   * (\textsf{SUM}(e, R'*g) := \sum*{t \in R'_g} \llbracket e \rrbracket(t))
   * (\textsf{AVG}(e, R'_g) := \frac{\textsf{SUM}(e, R'_g)}{\textsf{COUNT}(e, R'_g)})
     where (\textsf{COUNT}(e, R'_g)) is the number of non-null evaluations.
   * (\textsf{COUNT}(e, R'_g)) = number of rows with (\llbracket e \rrbracket(t)) not null
     or just (|R'_g|) for `count(*)`.
   * `countDistinct`, `min`, `max` etc. similar.

   In RA, these correspond to the aggregation operator (\gamma) with appropriate aggregate functions.

3. **Metric expressions**:
   A metric’s body is a scalar expression built out of:

   * aggregate calls over row-level expressions,
   * arithmetic/comparison/boolean operators,
   * calls to other metrics at the **same grain** (i.e. same group (R'_g)).

   So:
   [
   \llbracket m \rrbracket(R'_g)
   ]
   is evaluated by recursively interpreting its expression tree as:

   * leaves: literals,
   * aggregator calls: (\textsf{SUM}/\textsf{AVG}/\dots) over (R'_g),
   * metric references: (\llbracket m' \rrbracket(R'_g)),
   * composite operators: arithmetic/boolean over scalar values.

Crucial: note that (R'_g) is entirely determined by the query’s **grain** and `where` conditions; the metric **does not carry its own grouping**.

This is exactly the “always recompute from fact rows at query grain” invariant.

---

## 3. Transforms in relational algebra

Now layer in transforms, including the MicroStrategy-style ones.

### 3.1 Row-level / window filters (LAST_WEEK, etc.)

A simple time window transform:

```dsl
metric total_sales_last_week =
  sum(sales_amount where transaction_date in LAST_WEEK)
```

Semantics:

1. Let (\psi_{\text{last_week}}(t)) be the predicate “`transaction_date` is in the last week relative to the evaluation context” – this is a boolean function over rows.

2. Within group (R'*g), define a restricted relation:
   [
   R'*{g,\text{last_week}} := \sigma_{\psi_{\text{last_week}}}(R'_g)
   ]

3. Then:
   [
   \llbracket \text{total_sales_last_week} \rrbracket(R'*g)
   :=
   \textsf{SUM}(\text{sales_amount}, R'*{g,\text{last_week}})
   ]

General pattern:

* Any `sum(expr where predicate)` becomes:

  * a selection (\sigma_{\text{predicate}}(R'_g)),
  * followed by the aggregate over that restricted relation.

This is pure RA: (\gamma) with an extra (\sigma).

---

### 3.2 MicroStrategy-style transforms (Last Year Week)

Using your week-based example (retail trade calendar):

* Transform table:
  [
  T(\text{tradyrwkcode}, \text{tradyrwkcode_lastyear})
  ]

We want a metric **“Sum Sales Last Year”** that, for each current `tradyrwkcode`, sums fact rows whose `tradyrwkcode` corresponds to the **last year week**.

Let the fact relation be:

[
F(\text{tradyrwkcode}, \text{sales_amount}, \dots)
]

For a given **current** week code (w), define the relational transform:

1. Get the **matching transformation row**:
   [
   T_w := \sigma_{\text{tradyrwkcode} = w}(T)
   ]

2. Join fact with this transform row on the **last-year form**:
   [
   F^{\text{LY}}*w := F \bowtie*{F.\text{tradyrwkcode} = T.\text{tradyrwkcode_lastyear}} T_w
   ]

3. Then the metric at week (w) is:
   [
   \text{SalesLastYear}(w) := \sum_{t \in F^{\text{LY}}_w} t.\text{sales_amount}
   ]

In a grouped RA view (for all weeks at once):

[
R := F \bowtie T
]

We want a relation parameterized by “current code” (c), but fact rows grouped by the **last-year code**. At a higher level, there is an RA expression of the form:

* join: (F \bowtie_{F.\text{tradyrwkcode} = T.\text{tradyrwkcode_lastyear}} T),
* group by `T.tradyrwkcode` (current code),
* aggregate `F.sales_amount`.

That’s exactly what your SQL example does, just with a retail sales column.

### In your engine’s semantics

We can treat this as a **rowset transform**:

For a given group key (g) where one dimension is the “current week” attribute (W), let (w) be its value:

1. Start with the group’s rows (R'_g) (fact rows for current week, or all weeks depending on design).

2. Compute the **last-year rowset** by applying a transform (T_{\text{LY}}):
   [
   R'^{\text{LY}}*g := T*{\text{LY}}(R'_g)
   ]
   where internally:

   * (T_{\text{LY}}) joins with the transform relation (T),
   * and selects rows of (F) matching the last-year keys.

3. The transformed metric “Sum Sales Last Year” is:
   [
   \llbracket m_{\text{LY}} \rrbracket(R'_g) := \textsf{SUM}(\text{sales_amount}, R'^{\text{LY}}_g)
   ]

So:

* In RA: it’s a **join with the transform table + group/aggregate**.
* In your engine: it’s a **rowset transformer** handing modified row groups to the base metric’s aggregator.

This aligns with MicroStrategy’s behavior, but expressed through RA and rowset functions rather than attribute forms.

---

### 3.3 Metric transforms (YOY, etc.)

A metric-level transform (like YoY) is then:

```dsl
metric total_sales        = sum(sales_amount)
metric total_sales_last_y = last_year(total_sales, date_attr = tradyrwkcode)
metric yoy_sales          = (total_sales - total_sales_last_y) / total_sales_last_y
```

RA semantics per group (R'_g):

1. Let:
   [
   v_{\text{curr}} := \llbracket \text{total_sales} \rrbracket(R'_g)
   ]

2. Let:
   [
   v_{\text{ly}} := \llbracket \text{total_sales} \rrbracket(R'^{\text{LY}}_g)
   ]
   where (R'^{\text{LY}}*g) is obtained by a **rowset transform** (T*{\text{LY}}).

3. Then:
   [
   \llbracket \text{yoy_sales} \rrbracket(R'*g) :=
   \frac{v*{\text{curr}} - v_{\text{ly}}}{v_{\text{ly}}}
   ]

This is just a scalar expression over two metric evaluations on two different RA sub-relations: (R'_g) and its transformed counterpart (R'^{\text{LY}}_g).

---

## 4. Relational-algebra summary of the design

Putting it all together:

1. **Schema layer**

   * Facts (F_i) and dimensions (D_j) are relations.
   * Logical attributes map to attributes in these relations.
   * A join graph determines legal RA expressions combining facts & dimensions.

2. **Query layer**
   A query defines:

   * A grain (G = (d_1, \dots, d_k)): **dimensions** (e.g. store, product, transaction_date, region).
   * A set of metrics ((m_1, \dots, m_r)).
   * Attribute-level predicate (\varphi_{\text{attr}}) (`where`).
   * Optional metric-level predicate (\varphi_{\text{metric}}) (`having`).

   Semantics:

   * Build (R) by joining the base retail sales fact with required dimensions.
   * Apply (\sigma_{\varphi_{\text{attr}}}).
   * Group by (G) (RA: (\gamma) operator).
   * For each group (R'_g), evaluate each metric as a function of (R'_g) (and possibly transformed rowsets).
   * Filter groups via (\varphi_{\text{metric}}).

3. **Metric layer**
   Each metric (m) is a scalar expression built from:

   * Aggregators over row-level expressions: RA’s (\gamma).
   * Arithmetic/boolean combos of these scalars.
   * Other metrics at the same grain (scalar dependencies).
   * Optional metric transforms which internally use **rowset transforms**.

   Semantics:

   * (\llbracket m \rrbracket(R'_g)) is always computed from the group’s fact rows (or a transformed version of them) via RA operations.

4. **Transforms layer**

   * **Expression/row filters**: RA selections (\sigma).
   * **Table/rowset transforms**: RA expressions like joins with transform tables, window restrictions, time shifts (e.g. last year, last week, rolling 4 weeks of sales).
   * **Metric transforms**: higher-order metric evaluators that change the rowset argument (e.g. last year, prior period) and then call the base metric evaluator.

5. **Invariant**

   * **Metrics are grain-agnostic**: they are defined only in terms of “given a group of sales fact rows and filters, compute a scalar”.
   * Grain is solely determined by the query’s `dimensions` and the semantic model’s join logic.
   * This guarantees roll-up correctness, because different grains just mean different groups (R'_g).

---

If you’d like, next step could be:

* pick one concrete **retail sales** metric from your domain (e.g. `net_sales`, `units_sold`, or a margin percentage with a last-year variant),
* and I can give you its DSL definition, AST shape, and explicit RA semantics side-by-side, so you can drop it straight into an internal design doc.

---

## Worked “last year” retail metric (unchanged structure, retail terminology)

To keep it aligned with the above, here’s the already-sales-flavored example, just tuned to retail naming.

We’ll base it on weekly retail sales (trade year–week).

### 1. DSL: base metric + last-year metric + query

#### 1.1. Base metric (this year)

```dsl
metric sum_sales =
  sum(sales_amount)
```

Semantics: for whatever grain the query chooses and whatever rows land in the group, just sum `sales_amount`.

#### 1.2. Last-year variant (using a transform)

```dsl
metric sum_sales_last_year =
  last_year(sum_sales, tradyrwkcode)
```

Interpretation:

> “For each current week code, find the corresponding last-year week(s), then evaluate `sum_sales` on the rows whose `tradyrwkcode` is those last-year codes, but report the result at the *current* week grain.”

#### 1.3. Query asking for current & last-year sales

```dsl
query weekly_sales_vs_last_year {
  dimensions
    tradyrwkcode

  metrics
    sum_sales,
    sum_sales_last_year

  where
    tradyrwkcode between 202501 and 202552
}
```

Engine responsibilities (not in DSL):

* Knows there is a fact `fact_sales(tradyrwkcode, sales_amount, …)`.
* Knows there is a transform table `tradyrwk_transform(tradyrwkcode, tradyrwkcode_lastyear)`.

---

### 2. AST view

#### 2.1. Base metric `sum_sales` (unresolved AST)

```ts
const sumSalesMetric: MetricDefNode = {
  kind: "MetricDef",
  name: "sum_sales",
  expression: {
    kind: "Call",
    callee: { kind: "Identifier", name: "sum" },
    args: [
      { kind: "Identifier", name: "sales_amount" }
    ]
  }
};
```

After resolution, `sales_amount` becomes an `AttributeRef`:

```ts
const sumSalesResolvedExpr: ResolvedExprNode = {
  kind: "Call",
  callee: "sum",
  args: [
    { kind: "AttributeRef", logicalName: "sales_amount" }
  ]
};
```

#### 2.2. Last-year metric `sum_sales_last_year`

Unresolved:

```ts
const sumSalesLastYearMetric: MetricDefNode = {
  kind: "MetricDef",
  name: "sum_sales_last_year",
  expression: {
    kind: "Call",
    callee: { kind: "Identifier", name: "last_year" },
    args: [
      { kind: "Identifier", name: "sum_sales" },      // metric ref
      { kind: "Identifier", name: "tradyrwkcode" }    // attribute ref
    ]
  }
};
```

Resolved:

```ts
const sumSalesLastYearResolvedExpr: ResolvedExprNode = {
  kind: "Call",
  callee: "last_year",
  args: [
    { kind: "MetricRef",    metricName: "sum_sales" },
    { kind: "AttributeRef", logicalName: "tradyrwkcode" }
  ]
};
```

So in compiled form, `last_year` is a **higher-order metric function**: it wraps `sum_sales` and changes the rowset it sees.

---

### 3. Relational algebra semantics

#### 3.1. Schema

Fact table:

[
F(\text{tradyrwkcode}, \text{sales_amount}, \dots)
]

Transform table:

[
T(\text{tradyrwkcode}, \text{tradyrwkcode_lastyear})
]

#### 3.2. Query grain & base relation

Query:

```dsl
dimensions tradyrwkcode
metrics   sum_sales, sum_sales_last_year
where     tradyrwkcode between 202501 and 202552
```

1. **Base fact**: choose (F).
2. Build base relation (R) (here we don’t need any extra dimensions):
   [
   R := F
   ]
3. Apply `where` as selection:
   [
   R' := \sigma_{202501 \le \text{tradyrwkcode} \le 202552}(R)
   ]
4. Grain (G = (\text{tradyrwkcode})).
   Conceptually, we partition (R') by each week code (w):
   [
   R'*w := \sigma*{\text{tradyrwkcode} = w}(R')
   ]

---

### 3.3. Metric semantics per group

For each current week (w):

**Base metric `sum_sales`**

[
\llbracket \text{sum_sales} \rrbracket(R'_w)
:= \textsf{SUM}(\text{sales_amount}, R'*w)
= \sum*{t \in R'_w} t.\text{sales_amount}
]

**Rowset transform for “last year”**

Define the **last-year rowset** for week (w) as:

1. Find transform row(s) where `tradyrwkcode = w`:
   [
   T_w := \sigma_{T.\text{tradyrwkcode} = w}(T)
   ]

2. Join fact rows whose `tradyrwkcode` equals `tradyrwkcode_lastyear` from that row:
   [
   R^{\text{LY}}*w := F \bowtie*{F.\text{tradyrwkcode} = T.\text{tradyrwkcode_lastyear}} T_w
   ]

Same logic as the earlier SQL, but with `sales_amount`.

**Last-year metric `sum_sales_last_year`**

[
\llbracket \text{sum_sales_last_year} \rrbracket(R'_w)
:= \textsf{SUM}(\text{sales_amount}, R^{\text{LY}}*w)
= \sum*{t \in R^{\text{LY}}_w} t.\text{sales_amount}
]

In words:

* For each current week (w),

  * build (R^{\text{LY}}_w) by following the transform mapping to last-year weeks,
  * sum `sales_amount` over those last-year rows,
  * but **attach the result back to the current week (w)** (the query grain).

---

### 4. How the engine actually evaluates this

Putting the RA back into your engine’s terms:

For each group key:

```ts
// groupKey.tradyrwkcode = w
const rowsCurrent = R'_w;                     // current-year rows for week w
const rowsLastYear = rowsetTransformLastYear(F, w); // R^{LY}_w

// base metric
const sum_sales = rowsCurrent
  .select(row => row.sales_amount)
  .sum();

// last-year metric via transform
const sum_sales_last_year = rowsLastYear
  .select(row => row.sales_amount)
  .sum();
```

And `rowsetTransformLastYear(F, w)` is the implementation of the relational expression:

[
F \bowtie_{F.\text{tradyrwkcode} = T.\text{tradyrwkcode_lastyear}}
\sigma_{T.\text{tradyrwkcode} = w}(T)
]

All still grain-agnostic: if you later change the query to grain `(region, tradyrwkcode)` or even just `(region)`, the same mechanics apply, you just have different group keys and different (R'_g) / (R^{\text{LY}}_g) sets.
