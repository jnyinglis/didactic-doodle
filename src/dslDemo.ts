import {
  MetricDeclAst,
  metricDecl,
  parseAll,
  queryDecl,
  resolveMetricRefs,
  validateMetricExpr,
} from "./dsl";
import {
  InMemoryDb,
  LogicalAttribute,
  MetricDefinition,
  MetricRegistry,
  QuerySpec,
  SemanticModel,
  runSemanticQuery,
} from "./semanticEngine";
import { buildMetricFromExpr } from "./semanticEngine";

function parseMetricBlock(source: string): MetricDeclAst[] {
  const defs: MetricDeclAst[] = [];
  let pos = 0;

  while (pos < source.length) {
    const remaining = source.slice(pos);
    if (!remaining.trim()) break;

    const next = metricDecl(source, pos);
    if (!next) {
      throw new Error(`Could not parse metric definition near: ${remaining.slice(0, 80)}`);
    }
    defs.push(next.value);
    pos = next.nextPos;
  }

  return defs;
}

function compileMetrics(metricAsts: MetricDeclAst[]): MetricRegistry {
  const metricNames = new Set(metricAsts.map((m) => m.name));
  return metricAsts.reduce<MetricRegistry>((registry, ast) => {
    const resolved = resolveMetricRefs(ast.expr, metricNames);
    validateMetricExpr(resolved);
    const compiled = buildMetricFromExpr({
      name: ast.name,
      baseFact: ast.baseFact,
      expr: resolved,
    }) as MetricDefinition;
    registry[compiled.name] = compiled;
    return registry;
  }, {});
}

function runDslDemo() {
  const metricText = `
metric total_sales on fact_orders = sum(amount)
metric total_refunds on fact_returns = sum(refund)
metric avg_ticket on fact_orders = total_sales / count(orderId)
metric orders on fact_orders = count(orderId)
`;

  const queryText = `
query weekly_sales {
  dimensions: storeName, region, salesWeek
  metrics: total_sales, total_refunds, avg_ticket, orders
  where: salesWeek >= 202401
  having: total_sales > 100
}
`;

  const metricDefs = parseMetricBlock(metricText);
  const metrics = compileMetrics(metricDefs);

  // Column defaults to the attribute ID; most entries rely on that shortcut here.
  const attributes: Record<string, LogicalAttribute> = {
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
  };

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

  const model: SemanticModel = {
    facts: {
      // Could also be `{}` thanks to the `table ?? id` default; kept explicit here for clarity.
      fact_orders: { table: "fact_orders" },
      fact_returns: { table: "fact_returns" },
    },
    dimensions: {
      // Likewise, `{}` would resolve to the dimension ID if you prefer the shortcut.
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
    metrics,
  };

  const spec: QuerySpec = parseAll(queryDecl, queryText).spec;
  const rows = runSemanticQuery({ db, model }, spec);

  const unionSpec: QuerySpec = {
    dimensions: ["storeName", "region"],
    metrics: ["total_sales", "total_refunds"],
  };
  const unionRows = runSemanticQuery({ db, model }, unionSpec);

  console.log("DSL metrics parsed:");
  metricDefs.forEach((m) => console.log(`- ${m.name} (base fact: ${m.baseFact ?? "<derived>"})`));
  console.log("\nQuery spec:", JSON.stringify(spec, null, 2));
  console.log("\nDSL demo output:", rows);
  console.log("\nUnion-of-dimensions query spec:", JSON.stringify(unionSpec, null, 2));
  console.log("Union-of-dimensions DSL output:", unionRows);
}

if (require.main === module) {
  runDslDemo();
}

export { runDslDemo };
