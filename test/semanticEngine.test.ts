import { expect } from "chai";
import {
  aggregateMetric,
  InMemoryDb,
  LogicalAttribute,
  MetricDefinition,
  QuerySpec,
  SemanticModel,
  runSemanticQuery,
} from "../src/semanticEngine";

const db: InMemoryDb = {
  tables: {
    fact_sales: [
      { storeId: 1, month: 1, salesAmount: 100 },
      { storeId: 1, month: 2, salesAmount: 50 },
      { storeId: 2, month: 1, salesAmount: 200 },
    ],
    fact_inventory: [
      { storeId: 1, onHand: 5 },
      { storeId: 2, onHand: 10 },
      { storeId: 3, onHand: 7 },
    ],
    dim_store: [
      { id: 1, storeName: "Downtown" },
      { id: 2, storeName: "Mall" },
      { id: 3, storeName: "Airport" },
    ],
  },
};

const attributes: Record<string, LogicalAttribute> = {
  storeId: { name: "storeId", relation: "dim_store", column: "id" },
  month: { name: "month", relation: "fact_sales", column: "month" },
  salesAmount: {
    name: "salesAmount",
    relation: "fact_sales",
    column: "salesAmount",
  },
  onHand: { name: "onHand", relation: "fact_inventory", column: "onHand" },
  storeName: { name: "storeName", relation: "dim_store", column: "storeName" },
};

const totalSales = aggregateMetric("totalSales", "salesAmount", "sum", "fact_sales");
const totalOnHand = aggregateMetric(
  "totalOnHand",
  "onHand",
  "sum",
  "fact_inventory"
);

const storeNameLength: MetricDefinition = {
  name: "storeNameLength",
  attributes: ["storeName"],
  eval: ({ groupKey }) =>
    typeof groupKey.storeName === "string"
      ? (groupKey.storeName as string).length
      : undefined,
};

const model: SemanticModel = {
  facts: { fact_sales: { name: "fact_sales" }, fact_inventory: { name: "fact_inventory" } },
  dimensions: { dim_store: { name: "dim_store" } },
  attributes,
  joins: [
    { fact: "fact_sales", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    {
      fact: "fact_inventory",
      dimension: "dim_store",
      factKey: "storeId",
      dimensionKey: "id",
    },
  ],
  metrics: {
    totalSales,
    totalOnHand,
    storeNameLength,
  },
};

describe("semanticEngine multi-fact", () => {
  it("evaluates metrics per fact and joins by dimensions", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId", "storeName"],
      metrics: ["totalSales", "totalOnHand", "storeNameLength"],
    };

    const rows = runSemanticQuery({ db, model }, spec);

    expect(rows).to.have.lengthOf(2);
    expect(rows).to.deep.include({
      storeId: 1,
      storeName: "Downtown",
      totalSales: 150,
      totalOnHand: 5,
      storeNameLength: "Downtown".length,
    });
    expect(rows).to.deep.include({
      storeId: 2,
      storeName: "Mall",
      totalSales: 200,
      totalOnHand: 10,
      storeNameLength: "Mall".length,
    });
  });

  it("keeps the primary fact as the frame when other facts have extra rows", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId"],
      metrics: ["totalSales", "totalOnHand"],
    };

    const rows = runSemanticQuery({ db, model }, spec);
    const storeIds = rows.map((r) => r.storeId);

    expect(storeIds).to.have.members([1, 2]);
    expect(storeIds).to.not.include(3);
  });
});
