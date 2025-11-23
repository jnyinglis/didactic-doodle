import {
  Expr,
  aggregateMetric,
  buildMetricFromExpr,
  f,
  InMemoryDb,
  QuerySpec,
  Schema,
  SemanticEngine,
} from "@engine/semanticEngine";
import { useMemo } from "react";

interface DemoResult {
  weekly: Record<string, any>[];
  byRegion: Record<string, any>[];
}

const heroActions = [
  {
    label: "Explore the DSL",
    url: "https://github.com/semanticgpt/didactic-doodle/blob/main/src/dsl.ts",
  },
  {
    label: "Run the engine",
    url: "https://github.com/semanticgpt/didactic-doodle/blob/main/src/semanticEngineDemo.ts",
  },
];

const featureCards = [
  {
    title: "Data & schema workspace",
    body: "Upload JSON tables, map facts and dimensions, and preview inferred types with reachability validation.",
  },
  {
    title: "Grammar-driven metrics",
    body: "Author metrics with the existing DSL grammar. Autocomplete and inline parser errors come directly from the parser-combinator module.",
  },
  {
    title: "Query execution",
    body: "Run one or more DSL queries client-side. Results are presented as tables or JSON alongside execution logs and warnings.",
  },
  {
    title: "Import / export",
    body: "Save the entire workspace—data, schema, metrics, and queries—to JSON for quick iteration or sharing.",
  },
];

const dslSnippet = `metric total_sales := sum(fact_orders.amount)
metric total_refunds := sum(fact_returns.refund)
metric avg_ticket := total_sales / count(fact_orders.orderId)
query weekly_sales(dimensions: storeName, region, salesWeek) {
  select total_sales, total_refunds, avg_ticket
  where salesWeek >= 202401
  having total_sales > 100
}`;

function createEngineDemo(): DemoResult {
  const attributes = {
    orderId: { table: "fact_orders" },
    storeId: { table: "fact_orders" },
    amount: { table: "fact_orders" },
    weekCode: { table: "fact_orders" },
    channel: { table: "fact_orders" },

    refund: { table: "fact_returns" },
    weekCodeReturns: { table: "fact_returns", column: "weekCode" },

    storeName: { table: "dim_store" },
    region: { table: "dim_store" },

    salesWeek: { table: "dim_week", column: "code" },
    weekLabel: { table: "dim_week", column: "label" },
  } satisfies Record<string, any>;

  const db: InMemoryDb = {
    tables: {
      fact_orders: [
        { orderId: 1, storeId: 1, weekCode: 202401, channel: "store", amount: 120 },
        { orderId: 2, storeId: 1, weekCode: 202402, channel: "online", amount: 80 },
        { orderId: 3, storeId: 2, weekCode: 202401, channel: "store", amount: 200 },
        { orderId: 4, storeId: 1, weekCode: 202301, channel: "store", amount: 110 },
        { orderId: 5, storeId: 2, weekCode: 202301, channel: "store", amount: 180 },
      ],
      fact_returns: [
        { returnId: "r1", storeId: 1, weekCode: 202401, refund: 20 },
        { returnId: "r2", storeId: 2, weekCode: 202401, refund: 15 },
        { returnId: "r3", storeId: 1, weekCode: 202301, refund: 12 },
        { returnId: "r4", storeId: 3, weekCode: 202401, refund: 5 },
      ],
      dim_store: [
        { id: 1, storeName: "Downtown", region: "North" },
        { id: 2, storeName: "Mall", region: "South" },
        { id: 3, storeName: "Outlet", region: "East" },
      ],
      dim_week: [
        { code: 202401, label: "2024-W01" },
        { code: 202402, label: "2024-W02" },
        { code: 202301, label: "2023-W01" },
        { code: 202302, label: "2023-W02" },
      ],
    },
  };

  const schema: Schema = {
    facts: {
      fact_orders: { table: "fact_orders" },
      fact_returns: { table: "fact_returns" },
    },
    dimensions: {
      dim_store: { table: "dim_store" },
      dim_week: { table: "dim_week" },
    },
    attributes,
    joins: [
      { fact: "fact_orders", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
      { fact: "fact_returns", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
      { fact: "fact_orders", dimension: "dim_week", factKey: "weekCode", dimensionKey: "code" },
      { fact: "fact_returns", dimension: "dim_week", factKey: "weekCode", dimensionKey: "code" },
    ],
  };

  const engine = SemanticEngine.fromSchema(schema, db)
    .registerMetric(aggregateMetric("total_sales", "fact_orders", "amount", "sum"))
    .registerMetric(aggregateMetric("total_refunds", "fact_returns", "refund", "sum"))
    .registerMetric(
      buildMetricFromExpr({
        name: "avg_ticket",
        baseFact: "fact_orders",
        expr: Expr.div(Expr.metric("total_sales"), Expr.count("orderId")),
      })
    )
    .registerMetric(aggregateMetric("orders", "fact_orders", "orderId", "count"));

  const weekly: QuerySpec = {
    dimensions: ["storeName", "region", "salesWeek"],
    metrics: ["total_sales", "total_refunds", "avg_ticket", "orders"],
    where: f.gte("salesWeek", 202401),
    having: (values) => (values.total_sales ?? 0) > 100,
  };

  const byRegion: QuerySpec = {
    dimensions: ["region"],
    metrics: ["total_sales", "total_refunds"],
  };

  const weeklyRows = engine.registerQuery("weekly_sales", weekly).runQuery("weekly_sales");
  const regionRows = engine.runQuery(byRegion);

  return { weekly: weeklyRows, byRegion: regionRows };
}

function formatCell(value: unknown): string {
  if (typeof value === "number") {
    return Math.abs(value) >= 1000 ? value.toLocaleString() : value.toString();
  }
  return `${value ?? ""}`;
}

function ResultTable({ rows, title }: { rows: Record<string, any>[]; title: string }) {
  if (!rows.length) return null;
  const headers = Object.keys(rows[0]);
  return (
    <div className="card">
      <div className="card-header">
        <h3>{title}</h3>
        <p className="muted">Computed with the in-repo semantic engine.</p>
      </div>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              {headers.map((header) => (
                <th key={header}>{header}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={idx}>
                {headers.map((header) => (
                  <td key={header}>{formatCell(row[header])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FeatureGrid() {
  return (
    <div className="grid">
      {featureCards.map((card) => (
        <div className="card" key={card.title}>
          <h3>{card.title}</h3>
          <p className="muted">{card.body}</p>
        </div>
      ))}
    </div>
  );
}

function App() {
  const demoResults = useMemo(() => createEngineDemo(), []);

  return (
    <main className="page">
      <section className="hero">
        <div>
          <p className="eyebrow">React + Vite playground</p>
          <h1>Semantic Engine web experience</h1>
          <p className="lede">
            A single-page application that showcases how the semantic metrics engine, DSL parser, and workspace tools
            come together. Everything runs entirely in the browser—no backend required.
          </p>
          <div className="actions">
            {heroActions.map((action) => (
              <a className="button" key={action.label} href={action.url} target="_blank" rel="noreferrer">
                {action.label}
              </a>
            ))}
          </div>
        </div>
        <div className="card highlight">
          <p className="muted">DSL preview</p>
          <pre>
            <code>{dslSnippet}</code>
          </pre>
        </div>
      </section>

      <section>
        <div className="section-header">
          <h2>What the playground offers</h2>
          <p className="muted">Purpose-built to mirror the product spec in docs/web-playground.md.</p>
        </div>
        <FeatureGrid />
      </section>

      <section>
        <div className="section-header">
          <h2>Engine-backed preview</h2>
          <p className="muted">
            The UI below runs the same in-repo semantic engine used by the CLI demos. Swap in your own data, schema, and
            DSL to validate calculations instantly.
          </p>
        </div>
        <div className="card-panel">
          <ResultTable title="Weekly store rollup" rows={demoResults.weekly} />
          <ResultTable title="Regional totals" rows={demoResults.byRegion} />
        </div>
      </section>

      <section>
        <div className="section-header">
          <h2>How this SPA is organized</h2>
          <p className="muted">Key areas you can extend inside this Vite project.</p>
        </div>
        <ul className="callouts">
          <li>
            <strong>Workspace state:</strong> manage uploaded JSON, schema definitions, metrics, and queries with
            localStorage-backed persistence.
          </li>
          <li>
            <strong>Editor experiences:</strong> Monaco-based editors for metrics and queries with syntax highlighting
            driven by the parser-combinator grammar.
          </li>
          <li>
            <strong>Execution pipeline:</strong> Adapt the <code>SemanticEngine</code> helpers to validate joins, resolve
            attributes, and compute metric values directly in the browser.
          </li>
          <li>
            <strong>Imports / exports:</strong> Serialize a workspace snapshot (data + schema + metrics + queries) to a
            single JSON document for sharing or regression testing.
          </li>
        </ul>
      </section>
    </main>
  );
}

export default App;
