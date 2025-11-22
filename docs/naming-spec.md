Here’s a concrete implementation doc you can hand to your dev.

---

# Semantic Engine Naming Refactor – Implementation Instructions

## 0. Summary

We’re changing the naming model so that:

* **Registry keys are the only logical IDs** (for attributes, facts, dimensions, metrics, transforms).
* **Physical table/column names are explicit fields**.
* **Attributes** default `column` to their ID.
* **Facts/dimensions** default `table` to their ID.
* We **remove all `name` fields** that just duplicate the keys.

This document describes the exact code changes required.

---

## 1. Data model changes

All changes are in TypeScript. The main file is `semanticEngine.ts`. The demo model in `dslDemo.ts` must be updated to match.

### 1.1 Facts and dimensions

**Before (conceptually):**

```ts
export interface FactRelation {
  name: string;
}

export interface SemanticModel {
  facts: Record<string, FactRelation>;
  dimensions: Record<string, { name: string }>;
  // ...
}
```

**After (new types):**

```ts
export interface FactDefinition {
  /** Physical table name for this fact. If omitted, defaults to the fact ID. */
  table?: string;
}

export interface DimensionDefinition {
  /** Physical table name for this dimension. If omitted, defaults to the dimension ID. */
  table?: string;
}

export interface SemanticModel {
  facts: Record<string, FactDefinition>;
  dimensions: Record<string, DimensionDefinition>;
  attributes: Record<string, LogicalAttribute>;
  joins: JoinEdge[];
  metrics: MetricRegistry;
  rowsetTransforms?: Record<string, RowsetTransformDefinition>;
}
```

#### Normalization helpers (to implement)

Add small helpers inside `semanticEngine.ts`:

```ts
interface NormalizedFact {
  id: string;
  table: string;
}

interface NormalizedDimension {
  id: string;
  table: string;
}

function normalizeFact(id: string, def: FactDefinition): NormalizedFact {
  return { id, table: def.table ?? id };
}

function normalizeDimension(id: string, def: DimensionDefinition): NormalizedDimension {
  return { id, table: def.table ?? id };
}
```

Use these helpers wherever the engine currently assumes `factId` or `dimId` is the table name.

---

### 1.2 Attributes

**Before:**

```ts
export interface LogicalAttribute {
  name: string;
  relation: string; // table name
  column: string;
}
```

**After:**

```ts
export interface LogicalAttribute {
  /** Physical table where this attribute lives (fact or dimension). */
  table: string;

  /** Physical column name. If omitted, defaults to the attribute ID (registry key). */
  column?: string;
}
```

#### Normalization helper

Replace the current `normalizeAttribute` with something like:

```ts
interface NormalizedAttribute {
  id: string;
  table: string;
  column: string;
}

function normalizeAttribute(id: string, def: LogicalAttribute): NormalizedAttribute {
  return {
    id,
    table: def.table,
    column: def.column ?? id,
  };
}
```

> Important: `normalizeAttribute` now takes the **attribute ID** (registry key) as a parameter; `def.name` no longer exists.

Everywhere that previously did `Object.values(attributes).map(normalizeAttribute)` will need to be updated to pass the key.

---

### 1.3 Joins

**Existing design is fine; just clarify semantics:**

```ts
export interface JoinEdge {
  /** Fact ID (key in model.facts). */
  fact: string;

  /** Dimension ID (key in model.dimensions). */
  dimension: string;

  /** Physical column on fact.table. */
  factKey: string;

  /** Physical column on dimension.table. */
  dimensionKey: string;
}
```

No structural change needed, but join resolution must now use `normalizeFact` / `normalizeDimension` to get the actual table names.

---

### 1.4 Rowset transforms

**Before (conceptually):**

```ts
interface RowsetTransformDefinition {
  id: string;
  table: string;
  anchorAttr: string;  // logical attribute
  fromColumn: string;
  toColumn: string;
  factKey: string;     // logical attribute (per comment)
}
```

**After:**

```ts
export interface RowsetTransformDefinition {
  /** Physical transform table, e.g. 'tradyrwk_transform'. */
  table: string;

  /** Attribute ID in the grouped key (e.g. 'salesWeek'). */
  anchorAttr: string;

  /** Attribute ID on fact rows (e.g. 'salesWeek' on fact_orders). */
  factAttr: string;  // rename from factKey

  /** Column on transform.table for the "current" anchor value. */
  fromColumn: string;

  /** Column on transform.table for the shifted/target anchor value. */
  toColumn: string;
}
```

* The **transform ID** is the registry key in `rowsetTransforms`; remove `id` from the interface.
* `anchorAttr` and `factAttr` are **attribute IDs**, not physical column names.
* When the transform logic needs physical columns for fact rows, it should look them up via `model.attributes[anchorAttr]` / `model.attributes[factAttr]`.

---

## 2. Engine internals to update (`semanticEngine.ts`)

### 2.1 Attribute lookup & projection

Anywhere you see logic like:

```ts
const def = model.attributes[attrName]; // attrName from QuerySpec
// using def.name, def.relation, def.column
```

Update to:

* Treat `attrName` as the **attribute ID**.
* Use `normalizeAttribute(attrId, def)` to get `table` and `column`.

#### Example: `attributesForRelation` / `mapLogicalAttributes`

**Before (conceptual):**

```ts
function attributesForRelation(
  relation: string,
  attributes: Record<string, LogicalAttribute>
): LogicalAttribute[] {
  return Object.values(attributes)
    .map(normalizeAttribute)
    .filter((a) => a.relation === relation);
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
```

**After:**

```ts
function attributesForTable(
  table: string,
  attributes: Record<string, LogicalAttribute>
): NormalizedAttribute[] {
  return Object.entries(attributes)
    .map(([id, def]) => normalizeAttribute(id, def))
    .filter((a) => a.table === table);
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
    const logicalId = attr.id;
    if (needed.size && !needed.has(logicalId)) continue;
    mapped[logicalId] = (row as any)[attr.column];
  }
  return mapped;
}
```

Key changes:

* `relation` → `table`
* The **result keys** are now the **attribute IDs** (`attr.id`), not `attr.name`.
* `needed` always contains attribute IDs.

### 2.2 Base relation building

Where you construct base relations for a fact (e.g. `buildFactBaseRelation`):

* Use `normalizeFact(factId, model.facts[factId])` to get `table`.
* When adding projected columns, use `attributesForTable(table, model.attributes)` and the logic above.

### 2.3 Dimension-only queries

If you have a fallback path where no base fact is specified and you derive tables from attributes:

* Use the attributes’ `table` field directly.
* If you need to know whether a table is considered a “dimension” vs “fact”, you’ll use `normalizeFact` / `normalizeDimension` and compare `table` values.

### 2.4 Rowset transform logic

Where rowset transforms are applied:

* Lookup the transform by key:

  ```ts
  const transform = model.rowsetTransforms?.[transformId];
  ```

* Use these fields as **attribute IDs**:

  ```ts
  const anchorAttrId = transform.anchorAttr;
  const factAttrId   = transform.factAttr;
  ```

* When you need the physical columns on the fact rows, resolve them:

  ```ts
  const anchorAttr = normalizeAttribute(anchorAttrId, model.attributes[anchorAttrId]);
  const factAttr   = normalizeAttribute(factAttrId, model.attributes[factAttrId]);
  // anchorAttr.table, anchorAttr.column, etc.
  ```

* For the transform lookup table, use `transform.table` and the given `fromColumn`/`toColumn` as-is (they remain physical).

---

## 3. DSL / QuerySpec expectations

The **public contract** for model authors and DSL users after this change:

* In the DSL and query specs:

  * **Fact names** are fact IDs (keys of `model.facts`).
  * **Dimension names** (in `dimensions: [...]` and filters) are **attribute IDs**.
  * **Metric names** are metric IDs.
  * All field references in filters (`where` / `having`) are **attribute IDs**.

The change is internal; the external DSL syntax doesn’t need to change, but the model definitions do.

---

## 4. Update the demo model (`dslDemo.ts`)

Update the in-file model definition to the new shapes.

### 4.1 Facts & dimensions

**Before:**

```ts
facts: {
  fact_orders: { name: "fact_orders" },
  fact_returns: { name: "fact_returns" },
},
dimensions: {
  dim_store: { name: "dim_store" },
  dim_week: { name: "dim_week" },
},
```

**After:**

```ts
facts: {
  fact_orders: { table: "fact_orders" },
  fact_returns: { table: "fact_returns" },
},
dimensions: {
  dim_store: { table: "dim_store" },
  dim_week:  { table: "dim_week" },
},
```

(You can optionally use `{}` if you implement the `table ?? id` default, but for clarity in the demo, it’s better to show `table` explicitly.)

### 4.2 Attributes

**Before:**

```ts
const attributes: Record<string, LogicalAttribute> = {
  orderId: { name: "orderId", relation: "fact_orders", column: "orderId" },
  storeId: { name: "storeId", relation: "fact_orders", column: "storeId" },
  storeName: { name: "storeName", relation: "dim_store", column: "storeName" },
  region: { name: "region", relation: "dim_store", column: "region" },
  amount: { name: "amount", relation: "fact_orders", column: "amount" },
  refund: { name: "refund", relation: "fact_returns", column: "refund" },
  salesWeek: { name: "salesWeek", relation: "dim_week", column: "code" },
  weekLabel: { name: "weekLabel", relation: "dim_week", column: "label" },
  weekCode: { name: "weekCode", relation: "fact_orders", column: "weekCode" },
  weekCodeReturns: { name: "weekCodeReturns", relation: "fact_returns", column: "weekCode" },
  channel: { name: "channel", relation: "fact_orders", column: "channel" },
};
```

**After:**

```ts
const attributes: Record<string, LogicalAttribute> = {
  orderId:        { table: "fact_orders" },
  storeId:        { table: "fact_orders" },
  amount:         { table: "fact_orders" },
  weekCode:       { table: "fact_orders" },
  channel:        { table: "fact_orders" },

  refund:         { table: "fact_returns" },
  weekCodeReturns:{ table: "fact_returns", column: "weekCode" },

  storeName:      { table: "dim_store" },
  region:         { table: "dim_store" },

  salesWeek:      { table: "dim_week", column: "code" },
  weekLabel:      { table: "dim_week", column: "label" },
};
```

Key points:

* Remove `name` and `relation`.
* Use `table` and (only where needed) `column`.
* Attribute IDs (`orderId`, `storeName`, `salesWeek`, etc.) stay the same and are what the DSL uses.

---

## 5. Sanity checks

Once implemented, validate:

1. **Existing demo queries still run** and return the same logical results, but:

   * Output column names are now attribute IDs exclusively.
2. **Intentionally mismatched column names** (like `salesWeek` → `dim_week.code`) work as expected.
3. You can simulate using the same physical table as both fact and dimension by adding an extra dimension that points to an existing fact table and confirming joins still behave.


