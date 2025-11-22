import {
  aggregateMetric,
  InMemoryDb,
  LogicalAttribute,
  QuerySpec,
  SemanticModel,
  runSemanticQuery,
} from "./semanticEngine";

const db: InMemoryDb = {
  tables: {
    fact_orders: [
      { orderId: 1, storeId: 1, amount: 120 },
      { orderId: 2, storeId: 1, amount: 80 },
      { orderId: 3, storeId: 2, amount: 200 },
    ],
    fact_returns: [
      { returnId: "r1", storeId: 1, refund: 20 },
      { returnId: "r2", storeId: 2, refund: 15 },
    ],
    dim_store: [
      { id: 1, storeName: "Downtown" },
      { id: 2, storeName: "Mall" },
    ],
  },
};

const attributes: Record<string, LogicalAttribute> = {
  storeId: { name: "storeId", relation: "fact_orders", column: "storeId" },
  amount: { name: "amount", relation: "fact_orders", column: "amount" },
  refund: { name: "refund", relation: "fact_returns", column: "refund" },
  storeName: { name: "storeName", relation: "dim_store", column: "storeName" },
};

const model: SemanticModel = {
  facts: {
    fact_orders: { name: "fact_orders" },
    fact_returns: { name: "fact_returns" },
  },
  dimensions: { dim_store: { name: "dim_store" } },
  attributes,
  joins: [
    { fact: "fact_orders", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    { fact: "fact_returns", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
  ],
  metrics: {
    totalSales: aggregateMetric("totalSales", "fact_orders", "amount", "sum"),
    totalRefunds: aggregateMetric("totalRefunds", "fact_returns", "refund", "sum"),
  },
};

const spec: QuerySpec = {
  dimensions: ["storeId", "storeName"],
  metrics: ["totalSales", "totalRefunds"],
};

const rows = runSemanticQuery({ db, model }, spec);
console.log("semanticEngine demo output:", rows);
