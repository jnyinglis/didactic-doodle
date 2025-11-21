// semanticEngineDemo.ts
//
// POC/demo setup for the relational semantic engine in semanticEngine.ts

import {
  InMemoryDb,
  SemanticModel,
  MetricEvaluationEnvironment,
  LogicalAttribute,
  SemanticModelV2,
  QuerySpecV2,
  runRelationalQuery,
  relAggregateMetric,
  relExpressionMetric,
  aggregateMetric,
  rowsetTransformMetric,
  runSemanticQuery,
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
      { year: 2024, month: 1 },
      { year: 2024, month: 2 },
    ],
    sales: [
      // store 1, product 100 (2025)
      { storeId: 1, productId: 100, year: 2025, month: 1, amount: 150 },
      { storeId: 1, productId: 100, year: 2025, month: 2, amount: 200 },
      // store 1, product 101 (2025)
      { storeId: 1, productId: 101, year: 2025, month: 1, amount: 300 },
      { storeId: 1, productId: 101, year: 2025, month: 2, amount: 250 },
      // store 2, product 100 (2025)
      { storeId: 2, productId: 100, year: 2025, month: 1, amount: 120 },
      { storeId: 2, productId: 100, year: 2025, month: 2, amount: 180 },
      // store 2, product 101 (2025)
      { storeId: 2, productId: 101, year: 2025, month: 1, amount: 220 },
      { storeId: 2, productId: 101, year: 2025, month: 2, amount: 260 },
      // store 1, product 100 (2024)
      { storeId: 1, productId: 100, year: 2024, month: 1, amount: 80 },
      { storeId: 1, productId: 100, year: 2024, month: 2, amount: 90 },
      // store 1, product 101 (2024)
      { storeId: 1, productId: 101, year: 2024, month: 1, amount: 120 },
      { storeId: 1, productId: 101, year: 2024, month: 2, amount: 110 },
      // store 2, product 100 (2024)
      { storeId: 2, productId: 100, year: 2024, month: 1, amount: 50 },
      { storeId: 2, productId: 100, year: 2024, month: 2, amount: 60 },
      // store 2, product 101 (2024)
      { storeId: 2, productId: 101, year: 2024, month: 1, amount: 55 },
      { storeId: 2, productId: 101, year: 2024, month: 2, amount: 65 },
    ],
    budget: [
      // yearly budgets per store/product
      { storeId: 1, productId: 100, year: 2025, budgetAmount: 5000 },
      { storeId: 1, productId: 101, year: 2025, budgetAmount: 6000 },
      { storeId: 2, productId: 100, year: 2025, budgetAmount: 4000 },
      { storeId: 2, productId: 101, year: 2025, budgetAmount: 4500 },
    ],
    year_shift: [{ currentYear: 2025, lastYear: 2024 }],
  },
};

// -----------------------------------------------------------------------------
// 2. Table definitions (relational metrics)
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
// 3. Attribute registry (relational metrics)
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
// 4. Measures (relational metrics)
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
// 5. Relational metrics
// -----------------------------------------------------------------------------

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
// 6. Semantic model + environment (relational metrics)
// -----------------------------------------------------------------------------

const relationalModel: SemanticModel = {
  tables: demoTables,
  attributes: demoAttributes,
  measures: demoMeasures,
  metrics: {},
  relationalMetrics,
  transforms: {}, // no context transforms in this POC
};

const relationalEnv: MetricEvaluationEnvironment = {
  model: relationalModel,
  db: demoDb,
};

// -----------------------------------------------------------------------------
// 7. Grain-agnostic semantic DSL setup
// -----------------------------------------------------------------------------

const logicalAttributes: Record<string, LogicalAttribute> = {
  storeId: {
    name: "storeId",
    relation: "sales",
    column: "storeId",
    defaultFact: "sales",
  },
  storeName: {
    name: "storeName",
    relation: "store_dim",
    column: "storeName",
    defaultFact: "sales",
  },
  productId: {
    name: "productId",
    relation: "sales",
    column: "productId",
    defaultFact: "sales",
  },
  productName: {
    name: "productName",
    relation: "product_dim",
    column: "productName",
    defaultFact: "sales",
  },
  year: { name: "year", relation: "sales", column: "year", defaultFact: "sales" },
  month: {
    name: "month",
    relation: "sales",
    column: "month",
    defaultFact: "sales",
  },
  amount: {
    name: "amount",
    relation: "sales",
    column: "amount",
    defaultFact: "sales",
  },
};

const rowsetTransforms = {
  LastYear: {
    id: "LastYear",
    table: "year_shift",
    anchorAttr: "year",
    fromColumn: "currentYear",
    toColumn: "lastYear",
    factKey: "year",
  },
};

const semanticMetrics = {
  totalSales: aggregateMetric("totalSales", "amount", "sum"),
  avgSale: aggregateMetric("avgSale", "amount", "avg"),
  lastYearSales: rowsetTransformMetric({
    name: "lastYearSales",
    baseMetric: "totalSales",
    transformId: "LastYear",
  }),
  salesDelta: {
    name: "salesDelta",
    deps: ["totalSales", "lastYearSales"],
    eval: ({ evalMetric }) => {
      const current = evalMetric("totalSales") ?? 0;
      const lastYear = evalMetric("lastYearSales") ?? 0;
      return current - lastYear;
    },
  },
};

const semanticModel: SemanticModelV2 = {
  facts: { sales: { name: "sales" } },
  dimensions: {
    store_dim: { name: "store_dim" },
    product_dim: { name: "product_dim" },
    date_dim: { name: "date_dim" },
  },
  attributes: logicalAttributes,
  joins: [
    { fact: "sales", dimension: "store_dim", factKey: "storeId", dimensionKey: "storeId" },
    { fact: "sales", dimension: "product_dim", factKey: "productId", dimensionKey: "productId" },
  ],
  metricsV2: semanticMetrics,
  rowsetTransforms,
};

const semanticEnv: MetricEvaluationEnvironment = { model: semanticModel as any, db: demoDb };

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

  const rows = runRelationalQuery(relationalEnv, spec as any);

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

  const rows = runRelationalQuery(relationalEnv, spec as any);

  console.log(
    "=== Relational demo (frame = year × month; totalSalesAmount joined) ==="
  );
  console.table(rows);
}

// -----------------------------------------------------------------------------
// 10. Demo: grain-agnostic semantic DSL – frame (store, year)
// -----------------------------------------------------------------------------

function runSemanticDslDemo() {
  const spec: QuerySpecV2 = {
    dimensions: ["storeId", "year"],
    metrics: ["totalSales", "avgSale", "lastYearSales", "salesDelta"],
    where: f.eq("year", 2025),
  };

  const rows = runSemanticQuery(semanticEnv, semanticModel, spec);

  console.log(
    "=== Semantic DSL demo (frame = store × year; grain-agnostic metrics) ==="
  );
  console.table(rows);
}

// -----------------------------------------------------------------------------
// 11. Run all demos when this file is executed directly
// -----------------------------------------------------------------------------

function main() {
  runRelationalDemoStoreProductYear();
  runRelationalDemoYearMonth();
  runSemanticDslDemo();
}

main();
