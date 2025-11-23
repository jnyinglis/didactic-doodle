import {
  InMemoryDb,
  LogicalAttribute,
  QuerySpec,
  Schema,
  SemanticEngine,
} from "./semanticEngine";

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
    .useDslFile(metricText)
    .useDslFile(queryText);

  const rows = engine.runQuery("weekly_sales");

  const unionSpec: QuerySpec = {
    dimensions: ["storeName", "region"],
    metrics: ["total_sales", "total_refunds"],
  };
  const unionRows = engine.runQuery(unionSpec);

  console.log("DSL demo output:", rows);
  console.log("Union-of-dimensions DSL output:", unionRows);
}

if (require.main === module) {
  runDslDemo();
}

export { runDslDemo };
