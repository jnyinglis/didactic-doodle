import { expect } from "chai";
import {
  attr,
  f,
  InMemoryDb,
  measure,
  MetricEvaluationEnvironment,
  relAggregateMetric,
  relExpressionMetric,
  RelationalMetricRegistry,
  runRelationalQuery,
  SemanticModel,
  projectMeasureValues,
} from "../src/semanticEngine";

const demoDb: InMemoryDb = {
  tables: {
    store_dim: [
      { storeId: 1, storeName: "Downtown" },
      { storeId: 2, storeName: "Airport" },
      { storeId: 3, storeName: "Mall" }, // no sales/budget rows on purpose
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
      { storeId: 1, productId: 100, year: 2025, month: 1, amount: 150 },
      { storeId: 1, productId: 100, year: 2025, month: 2, amount: 200 },
      { storeId: 1, productId: 101, year: 2025, month: 1, amount: 300 },
      { storeId: 1, productId: 101, year: 2025, month: 2, amount: 250 },
      { storeId: 2, productId: 100, year: 2025, month: 1, amount: 120 },
      { storeId: 2, productId: 100, year: 2025, month: 2, amount: 180 },
      { storeId: 2, productId: 101, year: 2025, month: 1, amount: 220 },
      { storeId: 2, productId: 101, year: 2025, month: 2, amount: 260 },
    ],
    budget: [
      { storeId: 1, productId: 100, year: 2025, budgetAmount: 5000 },
      { storeId: 1, productId: 101, year: 2025, budgetAmount: 6000 },
      { storeId: 2, productId: 100, year: 2025, budgetAmount: 4000 },
      { storeId: 2, productId: 101, year: 2025, budgetAmount: 4500 },
    ],
  },
};

const tables: SemanticModel["tables"] = {
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
  },
  budget: {
    name: "budget",
    grain: ["storeId", "productId", "year"],
    columns: {
      storeId: { name: "storeId", type: "number" },
      productId: { name: "productId", type: "number" },
      year: { name: "year", type: "number" },
      budgetAmount: { name: "budgetAmount", type: "number", defaultAgg: "sum" },
    },
  },
};

const attributes: SemanticModel["attributes"] = {
  storeId: attr.id({ name: "storeId", table: "store_dim" }),
  productId: attr.id({ name: "productId", table: "product_dim" }),
  year: attr.id({ name: "year", table: "date_dim" }),
  month: attr.id({ name: "month", table: "date_dim" }),
};

const measures: SemanticModel["measures"] = {
  amount: measure.fact({ name: "amount", table: "sales" }),
  budgetAmount: measure.fact({ name: "budgetAmount", table: "budget" }),
};

const relationalMetrics: RelationalMetricRegistry = {
  totalSalesAmount: relAggregateMetric({
    name: "totalSalesAmount",
    measure: "amount",
    agg: "sum",
  }),
  storeBudget: relAggregateMetric({
    name: "storeBudget",
    measure: "budgetAmount",
    agg: "sum",
    grain: ["storeId", "year"],
  }),
  yearlyBudget: relAggregateMetric({
    name: "yearlyBudget",
    measure: "budgetAmount",
    agg: "sum",
    grain: ["year"],
  }),
  negativeSales: relExpressionMetric({
    name: "negativeSales",
    measure: "amount",
    expr: (values) => values.sum() * -1,
  }),
};

const model: SemanticModel = {
  tables,
  attributes,
  measures,
  metrics: {},
  relationalMetrics,
  transforms: {},
};

const env: MetricEvaluationEnvironment = {
  model,
  db: demoDb,
};

describe("relational semantic engine", () => {
  it("filters rows down to a measure grain when projecting values", () => {
    const values = projectMeasureValues(
      "amount",
      {
        grain: ["storeId", "productId", "year", "month"],
        filter: f.and(f.eq("storeId", 1), f.eq("year", 2025)),
      },
      env
    ).toArray();

    expect(values).to.deep.equal([150, 200, 300, 250]);
  });

  it("aggregates relational metrics per grain and globally", () => {
    const byGrain = relationalMetrics.totalSalesAmount
      .evalEnumerable(
        { grain: ["storeId", "productId", "year"], filter: f.eq("year", 2025) },
        env
      )
      .toArray();

    expect(byGrain).to.deep.include({
      storeId: 1,
      productId: 100,
      year: 2025,
      totalSalesAmount: 350,
    });
    expect(byGrain).to.deep.include({
      storeId: 2,
      productId: 101,
      year: 2025,
      totalSalesAmount: 480,
    });

    const global = relationalMetrics.totalSalesAmount
      .evalEnumerable({ grain: [], filter: f.eq("year", 2025) }, env)
      .toArray();

    expect(global).to.deep.equal([{ totalSalesAmount: 1680 }]);
  });

  it("builds frames via cross join and left joins relational metrics", () => {
    const spec = {
      table: "sales",
      attributes: ["storeId", "productId", "year"],
      metrics: [
        "totalSalesAmount",
        "storeBudget",
        "yearlyBudget",
        "negativeSales",
      ],
      filters: f.eq("year", 2025),
    };

    const rows = runRelationalQuery(env, spec as any);

    expect(rows).to.have.lengthOf(6); // 3 stores × 2 products × 1 year
    expect(rows).to.deep.include({
      storeId: 1,
      productId: 100,
      year: 2025,
      totalSalesAmount: 350,
      storeBudget: 11000,
      yearlyBudget: 19500,
      negativeSales: -350,
    });
    expect(rows).to.deep.include({
      storeId: 3,
      productId: 101,
      year: 2025,
      totalSalesAmount: null,
      storeBudget: null,
      yearlyBudget: 19500,
      negativeSales: null,
    });
  });
});
