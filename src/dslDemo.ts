import {
  MetricDefAst,
  buildMetricDefinition,
  metricDef,
  parseAll,
  queryDef,
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

function parseMetricBlock(source: string): MetricDefAst[] {
  const defs: MetricDefAst[] = [];
  let pos = 0;

  while (pos < source.length) {
    const remaining = source.slice(pos);
    if (!remaining.trim()) break;

    const next = metricDef.parse(source, pos);
    if (!next) {
      throw new Error(`Could not parse metric definition near: ${remaining.slice(0, 80)}`);
    }
    defs.push(next.value);
    pos = next.nextPos;
  }

  return defs;
}

function compileMetrics(metricAsts: MetricDefAst[]): MetricRegistry {
  const metricNames = new Set(metricAsts.map((m) => m.name));
  return metricAsts.reduce<MetricRegistry>((registry, ast) => {
    const compiled = buildMetricDefinition(ast, metricNames) as MetricDefinition;
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
      ],
      dim_store: [
        { id: 1, storeName: "Downtown", region: "North" },
        { id: 2, storeName: "Mall", region: "South" },
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
      fact_orders: { name: "fact_orders" },
      fact_returns: { name: "fact_returns" },
    },
    dimensions: {
      dim_store: { name: "dim_store" },
      dim_week: { name: "dim_week" },
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

  const spec: QuerySpec = parseAll(queryDef, queryText).spec;
  const rows = runSemanticQuery({ db, model }, spec);

  console.log("DSL metrics parsed:");
  metricDefs.forEach((m) => console.log(`- ${m.name} (base fact: ${m.baseFact ?? "<derived>"})`));
  console.log("\nQuery spec:", JSON.stringify(spec, null, 2));
  console.log("\nDSL demo output:", rows);
}

if (require.main === module) {
  runDslDemo();
}

export { runDslDemo };
