import { expect } from "chai";
import {
  aggregateMetric,
  InMemoryDb,
  LogicalAttribute,
  MetricDefinitionV2,
  MetricEvaluationEnvironment,
  MetricRegistryV2,
  QuerySpecV2,
  RowsetTransformDefinition,
  SemanticModelV2,
  runSemanticQuery,
  rowsetTransformMetric,
  f,
} from "../src/semanticEngine";

const db: InMemoryDb = {
  tables: {
    sales: [
      { storeId: 1, year: 2025, month: 1, amount: 150 },
      { storeId: 1, year: 2025, month: 2, amount: 200 },
      { storeId: 2, year: 2025, month: 1, amount: 300 },
      { storeId: 2, year: 2025, month: 2, amount: 100 },
      { storeId: 1, year: 2024, month: 1, amount: 80 },
      { storeId: 1, year: 2024, month: 2, amount: 90 },
      { storeId: 2, year: 2024, month: 1, amount: 50 },
      { storeId: 2, year: 2024, month: 2, amount: 60 },
    ],
    store_dim: [
      { storeId: 1, storeName: "Downtown" },
      { storeId: 2, storeName: "Mall" },
    ],
    year_shift: [{ currentYear: 2025, lastYear: 2024 }],
  },
};

const attributes: Record<string, LogicalAttribute> = {
  storeId: {
    name: "storeId",
    relation: "sales",
    column: "storeId",
    defaultFact: "sales",
  },
  year: {
    name: "year",
    relation: "sales",
    column: "year",
    defaultFact: "sales",
  },
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
  storeName: {
    name: "storeName",
    relation: "store_dim",
    column: "storeName",
    defaultFact: "sales",
  },
};

const rowsetTransforms: Record<string, RowsetTransformDefinition> = {
  LastYear: {
    id: "LastYear",
    table: "year_shift",
    anchorAttr: "year",
    fromColumn: "currentYear",
    toColumn: "lastYear",
    factKey: "year",
  },
};

const baseMetrics: MetricRegistryV2 = {
  totalSales: aggregateMetric("totalSales", "amount", "sum"),
  avgSale: aggregateMetric("avgSale", "amount", "avg"),
};

const transformMetric: MetricDefinitionV2 = rowsetTransformMetric({
  name: "lastYearSales",
  baseMetric: "totalSales",
  transformId: "LastYear",
});

const dependentMetric: MetricDefinitionV2 = {
  name: "highPerformerFlag",
  deps: ["totalSales"],
  eval: ({ evalMetric }) => {
    const total = evalMetric("totalSales") ?? 0;
    return total > 350 ? 1 : 0;
  },
};

const model: SemanticModelV2 = {
  facts: { sales: { name: "sales" } },
  dimensions: { store_dim: { name: "store_dim" } },
  attributes,
  joins: [
    { fact: "sales", dimension: "store_dim", factKey: "storeId", dimensionKey: "storeId" },
  ],
  metricsV2: {
    ...baseMetrics,
    lastYearSales: transformMetric,
    highPerformerFlag: dependentMetric,
  },
  rowsetTransforms,
};

const env: MetricEvaluationEnvironment = { model: model as any, db };

describe("grain-agnostic semantic DSL", () => {
  it("aggregates at the query grain without baking in grain", () => {
    const spec: QuerySpecV2 = {
      dimensions: ["storeId", "year"],
      metrics: ["totalSales", "avgSale"],
      where: f.eq("year", 2025),
    };

    const rows = runSemanticQuery(env, model, spec);

    expect(rows).to.deep.include({ storeId: 1, year: 2025, totalSales: 350, avgSale: 175 });
    expect(rows).to.deep.include({ storeId: 2, year: 2025, totalSales: 400, avgSale: 200 });
  });

  it("allows metrics to depend on other metrics at the same grain and apply having", () => {
    const spec: QuerySpecV2 = {
      dimensions: ["storeId", "year"],
      metrics: ["totalSales", "highPerformerFlag"],
      where: f.eq("year", 2025),
      having: (metrics) => (metrics.totalSales ?? 0) > 350,
    };

    const rows = runSemanticQuery(env, model, spec);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0]).to.deep.equal({
      storeId: 2,
      year: 2025,
      totalSales: 400,
      highPerformerFlag: 1,
    });
  });

  it("applies rowset transforms to evaluate base metrics on shifted rowsets", () => {
    const spec: QuerySpecV2 = {
      dimensions: ["storeId", "year"],
      metrics: ["totalSales", "lastYearSales"],
      where: f.eq("year", 2025),
    };

    const rows = runSemanticQuery(env, model, spec);

    expect(rows).to.deep.include({
      storeId: 1,
      year: 2025,
      totalSales: 350,
      lastYearSales: 170,
    });
    expect(rows).to.deep.include({
      storeId: 2,
      year: 2025,
      totalSales: 400,
      lastYearSales: 110,
    });
  });
});
