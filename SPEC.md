# Semantic Engine Refactor Spec

## Purpose
Document the implementation plan for the two outstanding objectives:

1. Adopt the bundled `linq.js` implementation everywhere the engine composes or executes row sets.
2. Collapse the current dimension/fact split into a unified table abstraction similar to MicroStrategy’s semantic layer.

This spec elaborates the design, key changes, data migrations, testing plans, and open questions needed to complete both efforts.

## Progress Checklist

### Objective 1 — Adopt `linq.js`
- [x] Bundled `Enumerable` is imported inside `semanticEngine.ts` along with the `rowsToEnumerable` helper so every caller works with fluent LINQ sequences. 【F:src/semanticEngine.ts†L12-L22】
- [x] The bespoke `RowSequence` implementation has been removed in favor of the `Enumerable` return type, and all callers (`applyContextToTable`, metric evaluators, transforms) now operate directly on those sequences. 【F:src/semanticEngine.ts†L12-L22】【F:src/semanticEngine.ts†L308-L314】
- [x] Execution paths such as `applyContextToTable` and the relational query runner (`runRelationalQuery`) return `Enumerable` instances and compose `where`, `select`, `groupBy`, and aggregate helpers instead of mutating arrays. 【F:src/semanticEngine.ts†L316-L368】【F:src/semanticEngine.ts†L801-L976】
- [x] The repository ships with `src/linq.d.ts`, providing the typings required for the imported operators to satisfy TypeScript. 【F:src/linq.d.ts†L1-L62】
- [ ] Add additional regression coverage that specifically exercises the newly available LINQ helpers (e.g., joins, ordering, nested groupings) beyond the existing unit tests.

### Objective 2 — Unify Tables
- [x] Demo data lives under a single `db.tables` map, and the engine code consumes the unified structure throughout.
- [x] Table metadata is defined through `tableDefinitions` so attributes, measures, relationships, and labels are all described in one place instead of the former fact/dimension split.
- [x] Query APIs (metrics, helper functions, and the relational query flow) reference `table`/`tableForRows` consistently, eliminating the previous `fact*` terminology.
- [x] Execution logic (filtering, grouping, label enrichment) reads from the unified metadata to determine grain and lookup joins.
- [ ] Produce migration guidance / changelog notes that explain how downstream callers should update from `db.dimensions` + `db.facts` to the new unified table layout.

### Combined Testing & Documentation
- [x] Automated tests (`npm test`) cover the LINQ-powered helpers, unified metadata, and context transforms.
- [ ] Perform and document manual regression steps (previously planned for the `/web` playground) so contributors know how to validate behavioral parity outside of the test suite.
- [ ] Resolve the open architectural questions (relationship declaration model, multi-table join scope, handling duplicate measures) and codify the decisions in this spec.

## Current State Summary
- `src/semanticEngine.ts` exports the grain-agnostic runtime, bundled LINQ helpers, and the `runSemanticQuery` entry point now used throughout the repository. 【F:src/semanticEngine.ts†L1-L33】【F:src/semanticEngine.ts†L801-L976】
- Row operations operate directly on `Enumerable` sequences, enabling composable filtering, grouping, and aggregation without the legacy `RowSequence` shim. 【F:src/semanticEngine.ts†L12-L22】【F:src/semanticEngine.ts†L308-L368】
- Demo and test coverage focus exclusively on the engine rather than the superseded semanticEngine prototype. 【F:src/semanticEngineDemo.ts†L8-L58】【F:test/semanticEngine.test.ts†L1-L82】

## Objective 1 — Adopt `linq.js`
### Design Goals
- Remove `RowSequence` and rely on the full `Enumerable` API (filtering, projections, joins, grouping, aggregates).
- Ensure every entry point that returns row collections produces an `Enumerable` instance so higher-level logic can chain operators.
- Provide TypeScript typings (via an existing `linq.d.ts` or new declaration file) so metrics/transforms authored in TS retain IntelliSense and type safety.

### Key Changes
1. **Import & Wire `Enumerable`:**
   - Import the default export from `src/linq.js` inside `semanticEngine.ts`.
   - Provide a helper (`rowsToEnumerable(rows: Row[]): Enumerable<Row>`) to centralize instantiation.

2. **Remove `RowSequence`:**
   - Delete the custom class and update every usage (`applyContextToFact`, metric evaluators, helper utilities) to call LINQ methods.
   - Replace bespoke `sum`, `avg`, `distinctBy` helpers with the equivalent `Enumerable` methods.

3. **Rewrite Execution Paths:**
   - `applyContextToFact` should return an `Enumerable<Row>` rather than a simple array.
   - Metric evaluators (`factMeasure`, `expression`, `derived`, context transform base) should call `sequence.sum(...)`, `sequence.count()`, `sequence.aggregate(...)`, etc.
   - The relational executor must build row groups through `.groupBy`, `.select`, and `.orderBy` for clarity and to reduce mutable state.

4. **Type Support:**
   - Introduce/update declaration files for the `Enumerable` type, exposing at least the methods used by the engine and demo metrics.
   - If necessary, augment the module via `declare module './linq' { ... }` to satisfy the TS compiler.

5. **Testing:**
   - Re-run existing unit/integration checks plus manual smoke tests in the `/web` playground to ensure query results match pre-refactor outputs.

### Acceptance Criteria
- No references to `RowSequence` remain.
- Metrics and transforms can freely chain LINQ methods, enabling future composition (window functions, joins, ranking).
- TypeScript compilation succeeds with proper typings for the LINQ API.

## Objective 2 — Unify Tables
### Design Goals
- Represent all physical data sources (currently `db.dimensions` + `db.facts`) as a single `tables` map.
- Model table metadata in a way that captures grain, measure vs. attribute columns, labels, and relationships without the "dimension vs fact" dichotomy.
- Simplify the query request payload so callers refer to `tableForRows` / `table` rather than `factForRows`.

### Proposed Data Structures
```ts
interface TableColumn {
  role: 'attribute' | 'measure';
  dataType?: 'string' | 'number' | 'date';
  defaultAgg?: 'sum' | 'avg' | 'count' | 'min' | 'max';
  format?: string;
  labelFor?: string; // reference to another column's key when providing display text
}

interface TableDefinition {
  name: string;
  grain: string[]; // column names present per row
  columns: Record<string, TableColumn>;
  relationships?: Record<string, { references: string; column: string }>;
}

interface InMemoryDb {
  tables: Record<string, Row[]>;
}
```
- Dimension metadata (labels, aliases) becomes part of `TableColumn` via `labelFor` or similar.
- Existing fact table measures map directly to columns with `role: 'measure'` and `defaultAgg`.

### Migration Steps
1. **Restructure Data:**
   - Merge `db.dimensions` and `db.facts` into `db.tables`. Each dimension table becomes an entry with `role: 'attribute'` columns.
   - Update demo data importers and the playground to read from the new structure.

2. **Update Metadata:**
   - Replace `factTables` with a `tableDefinitions` object using the new `TableDefinition` interface.
   - Remove `dimensionConfig`; instead, annotate columns with label metadata (`labelFor`, `labelAlias`).

3. **API Changes:**
   - Update metric definitions to reference `table`/`column` rather than `factTable`/`factColumn`.
   - Rename query request properties (e.g., `factForRows` → `tableForRows`).
   - Adjust helper functions (relational query execution, context application, label enrichment) to operate on the unified metadata.

4. **Execution Logic:**
   - Filtering should consult the table definition to know which attributes exist on the requested grain.
   - Grouping logic in the relational executor should dynamically inspect the grain/relationships from the table definition.
   - Label enrichment should use the `labelFor` metadata to join tables for readable captions.

5. **Documentation & Examples:**
   - Update README examples to match the new API shape (unified tables).
   - Provide migration guidance for users of the existing API.

### Acceptance Criteria
- There is a single source of truth for data rows (`db.tables`).
- No code references `dimensionConfig` or `factTables`; all metadata is derived from `tableDefinitions`.
- Queries run solely by referencing table names and columns, mirroring MicroStrategy’s flexible semantic layer.

## Combined Testing Strategy
- Unit tests (if present) updated to reflect new APIs.
- Manual regression via the `/web` playground:
  - Run baseline queries (sales amount, price per unit, time intelligence metrics) before/after changes to confirm parity.
  - Validate label enrichment still works for rows/columns.
- Add targeted tests for LINQ functionality (e.g., verifying `.groupBy` results) if not already covered.

## Risks & Mitigations
| Risk | Impact | Mitigation |
| --- | --- | --- |
| LINQ typings mismatch actual API | Build errors or runtime issues | Validate `linq.js` version, scaffold declaration file, add tsd tests |
| Unified table migration breaks playground data loading | UI regression | Update React state + hooks alongside backend changes, write smoke tests |
| Label enrichment loses aliases when removing `dimensionConfig` | User-visible columns regress | Encode alias/label info directly on columns and add tests |
| Developers rely on old API names | Adoption friction | Provide README/CHANGELOG entries documenting breaking changes and upgrade steps |

## Open Questions
1. Should relationships (foreign keys) be declared centrally to auto-join label tables, or remain implicit via column naming conventions?
2. Do we need to support multi-table joins per query immediately, or can we defer until after the initial refactor?
3. How should we handle measures that exist in multiple tables (e.g., actuals vs. budgets) when removing fact distinctions? Possibly leverage table-scoped namespaces or aliasing.

## Timeline (Suggested)
1. **Phase 1:** Integrate LINQ and remove `RowSequence`. (~1–2 days)
2. **Phase 2:** Restructure data + metadata into unified tables. (~2–3 days)
3. **Phase 3:** Update docs, playground, and add regression tests. (~1 day)
4. **Phase 4:** Polish & document breaking changes (README, CHANGELOG). (~0.5 day)

Each phase should land behind feature branches with targeted PRs for easier review.
