# Naming review (semanticEngine.ts and dsl.ts)

## Attribute identities
- Logical attributes are keyed by map entry (e.g., `attributes.orderId`) but their semantic identity comes from the `name` field that is projected onto rows. The engine maps physical columns to logical names using `attr.name`, so consumers see the `name` values, not the dictionary keys. Duplicating `name` and key works only when they match; any divergence would break lookups because the registry is indexed by key while grouping and filters use `name`. [See `LogicalAttribute` and `mapLogicalAttributes` usage.]
- Physical location is captured by `relation` and `column`. `column` defaults to `name` when omitted, so `name` currently acts as both logical identifier and default physical column.

## Fact/dimension registries
- `SemanticModel.facts` and `SemanticModel.dimensions` store objects with a `name` property, but the engine never reads those `name` values; it only relies on the map keys (`fact_orders`, `dim_store`). This makes the inner `name` redundant unless you intend to support aliases later.

## DSL implications
- The DSL resolves metric and dimension references by the attribute/metric identifiers (strings) without touching fact/dimension names. Metric expressions reference attributes by the logical names (`AttrRef.name`), so keeping dictionary keys identical to `LogicalAttribute.name` avoids confusion.
- Because joins are described with the fact/dimension keys and physical column names, using clearer field names like `table`/`tableName` for `relation` and `column` for the physical column could better signal intent and align with the in-memory DB shape.

## Recommendations
- Pick a single source of truth for logical attribute identity: either drop `LogicalAttribute.name` and use the map key everywhere, or require the map key to mirror `name` and treat `name` as the optional display label. If they can diverge, add validation that enforces consistency between the registry key and `name` before query execution.
- If fact/dimension aliases are unnecessary, remove the nested `name` properties and treat the registry key as the identifier. If aliases are desired, rename the property to `alias` or `tableName` and wire it into resolution/validation.
- Clarify `relation` as `table`/`tableName` in attribute definitions to mirror the DB table keys and reduce ambiguity.
