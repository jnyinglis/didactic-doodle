import { expect } from "chai";
import {
  aggregateMetric,
  InMemoryDb,
  LogicalAttribute,
  MetricDefinition,
  MetricComputationContext,
  MetricRuntime,
  MetricExpr,
  Expr,
  QuerySpec,
  SemanticModel,
  buildMetricFromExpr,
  compileMetricExpr,
  evaluateMetricRuntime,
  rowsToEnumerable,
  runSemanticQuery,
  f,
} from "../src/semanticEngine";

const db: InMemoryDb = {
  tables: {
    fact_sales: [
      { storeId: 1, month: 1, salesAmount: 100, week: 202501 },
      { storeId: 1, month: 2, salesAmount: 50, week: 202502 },
      { storeId: 2, month: 1, salesAmount: 200, week: 202501 },
    ],
    fact_inventory: [
      { storeId: 1, onHand: 5 },
      { storeId: 2, onHand: 10 },
      { storeId: 3, onHand: 7 },
    ],
    fact_budget: [
      { week: 202501 },
      { week: 202502 },
    ],
    dim_store: [
      { id: 1, storeName: "Downtown" },
      { id: 2, storeName: "Mall" },
      { id: 3, storeName: "Airport" },
    ],
    dim_week: [
      { id: 202501 },
      { id: 202502 },
    ],
  },
};

const attributes: Record<string, LogicalAttribute> = {
  storeId: { table: "dim_store", column: "id" },
  month: { table: "fact_sales" },
  salesAmount: { table: "fact_sales" },
  onHand: { table: "fact_inventory" },
  storeName: { table: "dim_store" },
  week: { table: "dim_week", column: "id" },
};

const totalSales = aggregateMetric("totalSales", "fact_sales", "salesAmount", "sum");
const totalOnHand = aggregateMetric("totalOnHand", "fact_inventory", "onHand", "sum");
const budgetMetric: MetricDefinition = {
  name: "budget",
  baseFact: "fact_budget",
  attributes: ["week"],
  eval: ({ groupKey }) => ({ 202501: 100, 202502: 150 } as any)[groupKey.week],
};

const storeNameLength: MetricDefinition = {
  name: "storeNameLength",
  attributes: ["storeName"],
  eval: ({ groupKey }) =>
    typeof groupKey.storeName === "string"
      ? (groupKey.storeName as string).length
      : undefined,
};

const salesForYear: MetricDefinition = {
  name: "salesForYear",
  baseFact: "fact_sales",
  attributes: ["storeId", "week", "salesAmount"],
  eval: ({ rows, helpers }) => {
    const year = helpers.bind(":year");
    const amounts = rows
      .where((r) => Math.floor((r as any).week / 100) === year)
      .select((r) => Number((r as any).salesAmount))
      .toArray();

    if (!amounts.length) return undefined;
    return amounts.reduce((a, b) => a + b, 0);
  },
};

const model: SemanticModel = {
  facts: {
    fact_sales: { table: "fact_sales" },
    fact_inventory: { table: "fact_inventory" },
    fact_budget: { table: "fact_budget" },
  },
  dimensions: { dim_store: { table: "dim_store" }, dim_week: { table: "dim_week" } },
  attributes,
  joins: [
    { fact: "fact_sales", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    {
      fact: "fact_inventory",
      dimension: "dim_store",
      factKey: "storeId",
      dimensionKey: "id",
    },
    { fact: "fact_sales", dimension: "dim_week", factKey: "week", dimensionKey: "id" },
    { fact: "fact_budget", dimension: "dim_week", factKey: "week", dimensionKey: "id" },
  ],
  metrics: {
    totalSales,
    totalOnHand,
    storeNameLength,
    budget: budgetMetric,
    salesForYear,
  },
};

describe("metric builders", () => {
  it("creates aggregate metrics backed by MetricExpr", () => {
    const metric = aggregateMetric("avgSales", "fact_sales", "salesAmount", "avg");

    expect(metric.exprAst).to.deep.equal(Expr.avg("salesAmount"));
    expect(metric.attributes).to.deep.equal(["salesAmount"]);
    expect(metric.deps).to.deep.equal([]);
    expect(metric.baseFact).to.equal("fact_sales");
  });
});

describe("semanticEngine multi-fact", () => {
  it("evaluates metrics per fact and joins by dimensions", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId", "storeName"],
      metrics: ["totalSales", "totalOnHand", "storeNameLength"],
    };

    const rows = runSemanticQuery({ db, model }, spec);

    expect(rows).to.have.lengthOf(3);
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
    const airportRow = rows.find((r) => r.storeId === 3);
    expect(airportRow).to.include({ storeId: 3, storeName: "Airport" });
    expect(airportRow?.totalSales).to.be.undefined;
    expect(airportRow?.totalOnHand).to.equal(7);
    expect(airportRow?.storeNameLength).to.equal("Airport".length);
  });

  it("builds the frame from the union of dimension keys across facts", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId"],
      metrics: ["totalSales", "totalOnHand"],
    };

    const rows = runSemanticQuery({ db, model }, spec);
    const storeIds = rows.map((r) => r.storeId);

    expect(storeIds).to.have.members([1, 2, 3]);
    expect(rows.find((r) => r.storeId === 3)?.totalOnHand).to.equal(7);
    expect(rows.find((r) => r.storeId === 3)?.totalSales).to.be.undefined;
  });

  it("joins coarse-grain metrics using their native grain and broadcasts", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId", "week"],
      metrics: ["totalSales", "budget"],
    };

    const rows = runSemanticQuery({ db, model }, spec);

    const store1Week1 = rows.find((r) => r.storeId === 1 && r.week === 202501);
    const store2Week1 = rows.find((r) => r.storeId === 2 && r.week === 202501);

    expect(store1Week1).to.include({ totalSales: 100, budget: 100 });
    expect(store2Week1).to.include({ totalSales: 200, budget: 100 });

    const store1Week2 = rows.find((r) => r.storeId === 1 && r.week === 202502);
    expect(store1Week2).to.include({ totalSales: 50, budget: 150 });
  });
});

describe("bindings", () => {
  it("resolves bindings inside where filters", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId", "week"],
      metrics: ["totalSales"],
      where: f.and(
        f.in("storeId", [":storeOne", ":storeTwo"]),
        f.between("week", ":weekStart" as any, ":weekEnd" as any)
      ),
    };

    const rows = runSemanticQuery(
      { db, model },
      spec,
      { bindings: { storeOne: 1, storeTwo: 2, weekStart: 202501, weekEnd: 202501 } }
    );

    expect(rows).to.have.deep.members([
      { storeId: 1, week: 202501, totalSales: 100 },
      { storeId: 2, week: 202501, totalSales: 200 },
    ]);
  });

  it("makes bindings available to metrics and errors when missing", () => {
    const spec: QuerySpec = {
      dimensions: ["storeId", "week"],
      metrics: ["salesForYear"],
    };

    const rows = runSemanticQuery(
      { db, model },
      spec,
      { bindings: { year: 2025 } }
    );

    expect(rows).to.deep.include({
      storeId: 1,
      week: 202501,
      salesForYear: 100,
    });

    expect(() => runSemanticQuery({ db, model }, spec)).to.throw(
      "Missing binding ':year'"
    );
  });
});

describe("metric expression compiler", () => {
  const dummyRuntime: MetricRuntime = {
    model: { facts: {}, dimensions: {}, attributes: {}, joins: [], metrics: {} },
    db: { tables: {} },
    relation: rowsToEnumerable([]),
    whereFilter: null,
    bindings: {},
    groupDimensions: [],
  };

  const helpers = {
    runtime: dummyRuntime,
    applyRowsetTransform: () => rowsToEnumerable([]),
    bind: () => undefined,
  };

  it("evaluates aggregates from AST", () => {
    const expr: MetricExpr = {
      kind: "Call",
      fn: "sum",
      args: [{ kind: "AttrRef", name: "sales_amount" }],
    };

    const evalFn = compileMetricExpr(expr);
    const rows = rowsToEnumerable([
      { sales_amount: 10 },
      { sales_amount: 5 },
      { sales_amount: 15 },
    ]);

    const ctx: MetricComputationContext = {
      rows,
      groupKey: {},
      evalMetric: () => undefined,
      helpers: { ...helpers, runtime: { ...dummyRuntime, relation: rows } },
    };

    expect(evalFn(ctx)).to.equal(30);
  });

  it("combines metric references with binary ops", () => {
    const expr: MetricExpr = {
      kind: "BinaryOp",
      op: "-",
      left: { kind: "MetricRef", name: "sum_sales" },
      right: { kind: "MetricRef", name: "sum_sales_last_year" },
    };

    const evalFn = compileMetricExpr(expr);
    const ctx: MetricComputationContext = {
      rows: rowsToEnumerable([]),
      groupKey: {},
      evalMetric: (name) => ({ sum_sales: 200, sum_sales_last_year: 150 }[name]),
      helpers,
    };

    expect(evalFn(ctx)).to.equal(50);
  });

  it("applies last_year rowset transforms", () => {
    const db: InMemoryDb = {
      tables: {
        fact_sales: [
          { tradyrwkcode: 202401, sales_amount: 60 },
          { tradyrwkcode: 202501, sales_amount: 100 },
        ],
        tradyrwk_transform: [
          { tradyrwkcode: 202501, tradyrwkcode_lastyear: 202401 },
        ],
      },
    };

    const sumSales = buildMetricFromExpr({
      name: "sum_sales",
      baseFact: "fact_sales",
      expr: { kind: "Call", fn: "sum", args: [{ kind: "AttrRef", name: "sales_amount" }] },
    });

    const model: SemanticModel = {
      facts: { fact_sales: { table: "fact_sales" } },
      dimensions: {},
      attributes: {},
      joins: [],
      metrics: { sum_sales: sumSales },
      rowsetTransforms: {
        "last_year:tradyrwkcode": {
          table: "tradyrwk_transform",
          anchorAttr: "tradyrwkcode",
          fromColumn: "tradyrwkcode",
          toColumn: "tradyrwkcode_lastyear",
          factAttr: "tradyrwkcode",
        },
      },
    };

    const runtime: MetricRuntime = {
      model,
      db,
      baseFact: "fact_sales",
      relation: rowsToEnumerable(db.tables.fact_sales),
      whereFilter: null,
      bindings: {},
      groupDimensions: ["tradyrwkcode"],
    };

    const transformFn = (transformId: string, groupKey: Record<string, any>) => {
      const transform = runtime.model.rowsetTransforms?.[transformId];
      if (!transform) throw new Error("missing transform");

      const anchorValue = groupKey[transform.anchorAttr];
      const transformRows = rowsToEnumerable(runtime.db.tables[transform.table] ?? []);
      const targetAnchors = transformRows
        .where((r: any) => r[transform.fromColumn] === anchorValue)
        .select((r: any) => r[transform.toColumn])
        .toArray();
      const allowed = new Set(targetAnchors);
      return runtime.relation.where(
        (row: any) => allowed.has(row[transform.factAttr])
      );
    };

    const expr: MetricExpr = {
      kind: "Call",
      fn: "last_year",
      args: [
        { kind: "MetricRef", name: "sum_sales" },
        { kind: "AttrRef", name: "tradyrwkcode" },
      ],
    };

    const evalFn = compileMetricExpr(expr);
    const metricCache = new Map<string, number | undefined>();
    const groupRows = rowsToEnumerable(
      db.tables.fact_sales.filter((r) => r.tradyrwkcode === 202501)
    );

    const ctx: MetricComputationContext = {
      rows: groupRows,
      groupKey: { tradyrwkcode: 202501 },
      evalMetric: (name) =>
        evaluateMetricRuntime(name, runtime, { tradyrwkcode: 202501 }, groupRows, undefined, metricCache),
      helpers: { runtime, applyRowsetTransform: transformFn, bind: () => undefined },
    };

    expect(evalFn(ctx)).to.equal(60);
  });
});
