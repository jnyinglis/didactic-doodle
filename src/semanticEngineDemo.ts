// semanticEngineDemo.ts
//
// POC/demo setup for the relational semantic engine in semanticEngine.ts

import {
  Row,
  InMemoryDb,
  SemanticModel,
  MetricEvaluationEnvironment,
  buildEngine,
  runQuery,
  runRelationalQuery,
  simpleMetric,
  relAggregateMetric,
  relExpressionMetric,
  attr,
  measure,
  f,
} from "./semanticEngine";

// -----------------------------------------------------------------------------
// 1. Demo tables + data
// -----------------------------------------------------------------------------

const demoDb: InMemoryDb = {
  tables: {
    store_dim: [
      { storeId: 1, storeName: "Downtown" },
      { storeId: 2, storeName: "Airport" },
    ],
    product_dim: [
      { productId: 100, productName: "Widget" },
      { productId: 101, productName: "Gadget" },
    ],
    date_dim: [
      { year: 2025, month: 1 },
      { year: 2025, month: 2 },
    ],
    sales: [
      // store 1, product 100
      { storeId: 1, productId: 100, year: 2025, month: 1, amount: 150 },
      { storeId: 1, productId: 100, year: 2025, month: 2, amount: 200 },
      // store 1, product 101
      { storeId: 1, productId: 101, year: 2025, month: 1, amount: 300 },
      { storeId: 1, productId: 101, year: 2025, month: 2, amount: 250 },
      // store 2, product 100
      { storeId: 2, productId: 100, year: 2025, month: 1, amount: 120 },
      { storeId: 2, productId: 100, year: 2025, month: 2, amount: 180 },
      // store 2, product 101
      { storeId: 2, productId: 101, year: 2025, month: 1, amount: 220 },
      { storeId: 2, productId: 101, year: 2025, month: 2, amount: 260 },
    ],
    budget: [
      // yearly budgets per store/product
      { storeId: 1, productId: 100, year: 2025, budgetAmount: 5000 },
      { storeId: 1, productId: 101, year: 2025, budgetAmount: 6000 },
      { storeId: 2, productId: 100, year: 2025, budgetAmount: 4000 },
      { storeId: 2, productId: 101, year: 2025, budgetAmount: 4500 },
    ],
  },
};

// -----------------------------------------------------------------------------
// 2. Table definitions
// -----------------------------------------------------------------------------

const demoTables: SemanticModel["tables"] = {
  store_dim: {
    name: "store_dim",
    grain: ["storeId"],
    columns: {
      storeId: { name: "storeId", type: "number" },
      storeName: { name: "storeName", type: "string" },
    },
  },
  product_dim: {
    name: "product_dim",
    grain: ["productId"],
    columns: {
      productId: { name: "productId", type: "number" },
      productName: { name: "productName", type: "string" },
    },
  },
  date_dim: {
    name: "date_dim",
    grain: ["year", "month"],
    columns: {
      year: { name: "year", type: "number" },
      month: { name: "month", type: "number" },
    },
  },
  sales: {
    name: "sales",
    grain: ["storeId", "productId", "year", "month"],
    columns: {
      storeId: { name: "storeId", type: "number" },
      productId: { name: "productId", type: "number" },
      year: { name: "year", type: "number" },
      month: { name: "month", type: "number" },
      amount: { name: "amount", type: "number", defaultAgg: "sum" },
    },
    relationships: {
      store: { references: "store_dim", column: "storeId" },
      product: { references: "product_dim", column: "productId" },
      date: { references: "date_dim", column: "year" }, // simplified
    },
  },
  budget: {
    name: "budget",
    grain: ["storeId", "productId", "year"],
    columns: {
      storeId: { name: "storeId", type: "number" },
      productId: { name: "productId", type: "number" },
      year: { name: "year", type: "number" },
      budgetAmount: {
        name: "budgetAmount",
        type: "number",
        defaultAgg: "sum",
      },
    },
    relationships: {
      store: { references: "store_dim", column: "storeId" },
      product: { references: "product_dim", column: "productId" },
    },
  },
};

// -----------------------------------------------------------------------------
// 3. Attribute registry
// -----------------------------------------------------------------------------

const demoAttributes: SemanticModel["attributes"] = {
  storeId: attr.id({ name: "storeId", table: "store_dim", column: "storeId" }),
  storeName: attr.id({
    name: "storeName",
    table: "store_dim",
    column: "storeName",
  }),
  productId: attr.id({
    name: "productId",
    table: "product_dim",
    column: "productId",
  }),
  productName: attr.id({
    name: "productName",
    table: "product_dim",
    column: "productName",
  }),
  year: attr.id({ name: "year", table: "date_dim", column: "year" }),
  month: attr.id({ name: "month", table: "date_dim", column: "month" }),
};

// -----------------------------------------------------------------------------
// 4. Measures
// -----------------------------------------------------------------------------

const demoMeasures: SemanticModel["measures"] = {
  amount: measure.fact({
    name: "amount",
    table: "sales",
    column: "amount",
    description: "Sales amount",
    aggregation: "sum",
  }),
  budgetAmount: measure.fact({
    name: "budgetAmount",
    table: "budget",
    column: "budgetAmount",
    description: "Budget amount",
    aggregation: "sum",
  }),
};

// -----------------------------------------------------------------------------
// 5. Metrics
// -----------------------------------------------------------------------------

// Scalar metrics (work with runQuery / Engine.query)
const scalarMetrics: SemanticModel["metrics"] = {
  totalSalesAmountScalar: simpleMetric({
    name: "totalSalesAmountScalar",
    measure: "amount",
    aggregation: "sum",
    description: "Total sales amount (scalar path)",
    format: "currency",
  }),
  totalBudgetScalar: simpleMetric({
    name: "totalBudgetScalar",
    measure: "budgetAmount",
    aggregation: "sum",
    description: "Total budget (scalar path)",
    format: "currency",
  }),
};

// Relational metrics (work with runRelationalQuery)
const relationalMetrics = {
  // Default grain: inherits the frame grain
  totalSalesAmount: relAggregateMetric({
    name: "totalSalesAmount",
    measure: "amount",
    agg: "sum",
    description: "Total sales amount (relational metric)",
    format: "currency",
  }),

  // Example: yearly budget grain, will repeat across finer frames like (store, product, year)
  totalBudget: relAggregateMetric({
    name: "totalBudget",
    measure: "budgetAmount",
    agg: "sum",
    grain: ["year"],
    description: "Total budget per year (relational metric)",
    format: "currency",
  }),

  // Example expression metric: negative of total sales
  negativeTotalSales: relExpressionMetric({
    name: "negativeTotalSales",
    measure: "amount",
    grain: ["storeId", "productId", "year"],
    description: "Negative total sales (relational expression metric)",
    expr: (values) => values.select((v: any) => -Number(v)).sum(),
  }),
};

// -----------------------------------------------------------------------------
// 6. Semantic model + environment
// -----------------------------------------------------------------------------

const demoModel: SemanticModel = {
  tables: demoTables,
  attributes: demoAttributes,
  measures: demoMeasures,
  metrics: scalarMetrics,
  relationalMetrics,
  transforms: {}, // no context transforms in this POC
};

const demoEnv: MetricEvaluationEnvironment = {
  model: demoModel,
  db: demoDb,
};

// -----------------------------------------------------------------------------
// 7. Demo: scalar metric query (existing path)
// -----------------------------------------------------------------------------

function runScalarDemo() {
  const engine = buildEngine(demoEnv);

  const spec = engine
    .query("sales")
    .addAttributes("year", "month")
    .addMetrics("totalSalesAmountScalar")
    .where(f.eq("year", 2025))
    .build();

  const rows = runQuery(demoEnv, spec);

  console.log("=== Scalar metric demo (group by year, month on sales) ===");
  console.table(rows);
}

// -----------------------------------------------------------------------------
// 8. Demo: relational metric query – frame (store, product, year)
// -----------------------------------------------------------------------------

function runRelationalDemoStoreProductYear() {
  // QuerySpec is internal to semanticEngine.ts; we just pass a compatible literal
  const spec = {
    table: "", // not used by runRelationalQuery; frame is derived from attributes
    attributes: ["storeId", "productId", "year"],
    metrics: ["totalSalesAmount", "totalBudget", "negativeTotalSales"],
    filters: f.eq("year", 2025),
  };

  const rows = runRelationalQuery(demoEnv, spec as any);

  console.log(
    "=== Relational demo (frame = store × product × year; metrics joined) ==="
  );
  console.table(rows);
}

// -----------------------------------------------------------------------------
// 9. Demo: relational metric query – frame (year, month)
// -----------------------------------------------------------------------------

function runRelationalDemoYearMonth() {
  const spec = {
    table: "",
    attributes: ["year", "month"],
    metrics: ["totalSalesAmount"],
    filters: f.eq("year", 2025),
  };

  const rows = runRelationalQuery(demoEnv, spec as any);

  console.log(
    "=== Relational demo (frame = year × month; totalSalesAmount joined) ==="
  );
  console.table(rows);
}

// -----------------------------------------------------------------------------
// 10. Run all demos when this file is executed directly
// -----------------------------------------------------------------------------

function main() {
  runScalarDemo();
  runRelationalDemoStoreProductYear();
  runRelationalDemoYearMonth();
}

main();
