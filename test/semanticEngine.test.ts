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
  validateMetric,
  validateAll,
  lastYearMetric,
  tableTransformMetric,
  defineTableTransform,
  defineTableTransformFromRelation,
  WindowFrameSpec,
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

const validationModelBase: SemanticModel = {
  facts: { fact_sales: { table: "fact_sales" }, fact_inventory: { table: "fact_inventory" } },
  dimensions: { dim_store: { table: "dim_store" }, dim_week: { table: "dim_week" } },
  attributes: {
    storeId: { table: "dim_store", column: "id" },
    salesAmount: { table: "fact_sales" },
    onHand: { table: "fact_inventory" },
    week: { table: "dim_week", column: "id" },
    unreachableAttr: { table: "dim_unreachable", column: "id" },
  },
  joins: [
    { fact: "fact_sales", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    { fact: "fact_sales", dimension: "dim_week", factKey: "week", dimensionKey: "id" },
  ],
  metrics: {},
};

describe("window functions", () => {
  const db: InMemoryDb = {
    tables: {
      fact_sales: [
        { storeId: 1, week: 1, salesAmount: 10 },
        { storeId: 1, week: 2, salesAmount: 20 },
        { storeId: 1, week: 3, salesAmount: 30 },
        { storeId: 2, week: 1, salesAmount: 5 },
        { storeId: 2, week: 2, salesAmount: 15 },
      ],
      dim_store: [
        { id: 1, storeName: "Downtown" },
        { id: 2, storeName: "Mall" },
      ],
      dim_week: [{ id: 1 }, { id: 2 }, { id: 3 }],
    },
  };

  const attributes: Record<string, LogicalAttribute> = {
    storeId: { table: "dim_store", column: "id" },
    salesAmount: { table: "fact_sales" },
    week: { table: "dim_week", column: "id" },
    storeName: { table: "dim_store" },
  };

  const sales = aggregateMetric("sales", "fact_sales", "salesAmount", "sum");

  const model: SemanticModel = {
    facts: { fact_sales: { table: "fact_sales" } },
    dimensions: { dim_store: { table: "dim_store" }, dim_week: { table: "dim_week" } },
    attributes,
    joins: [
      { fact: "fact_sales", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
      { fact: "fact_sales", dimension: "dim_week", factKey: "week", dimensionKey: "id" },
    ],
    metrics: {
      sales,
      rollingSales: buildMetricFromExpr({
        name: "rollingSales",
        baseFact: "fact_sales",
        expr: Expr.window(Expr.metric("sales"), {
          partitionBy: ["storeId"],
          orderBy: "week",
          frame: { kind: "rolling", count: 2 },
          aggregate: "sum",
        }),
      }),
      runningSales: buildMetricFromExpr({
        name: "runningSales",
        baseFact: "fact_sales",
        expr: Expr.window(Expr.metric("sales"), {
          partitionBy: ["storeId"],
          orderBy: "week",
          frame: { kind: "cumulative" },
          aggregate: "sum",
        }),
      }),
      prevSales: buildMetricFromExpr({
        name: "prevSales",
        baseFact: "fact_sales",
        expr: Expr.window(Expr.metric("sales"), {
          partitionBy: ["storeId"],
          orderBy: "week",
          frame: { kind: "offset", offset: -1 },
          aggregate: "sum",
        }),
      }),
    },
  };

  it("computes rolling, cumulative, and offset windows per partition", () => {
    const rows = runSemanticQuery(
      { db, model },
      { dimensions: ["storeId", "week"], metrics: ["sales", "rollingSales", "runningSales", "prevSales"] }
    );

    const getRow = (storeId: number, week: number) =>
      rows.find((r) => r.storeId === storeId && r.week === week) ?? {};

    expect(getRow(1, 1)).to.include({ sales: 10, rollingSales: 10, runningSales: 10 });
    expect(getRow(1, 1).prevSales).to.equal(undefined);
    expect(getRow(1, 2)).to.include({ sales: 20, rollingSales: 30, runningSales: 30 });
    expect(getRow(1, 2).prevSales).to.equal(10);
    expect(getRow(1, 3)).to.include({ sales: 30, rollingSales: 50, runningSales: 60 });
    expect(getRow(1, 3).prevSales).to.equal(20);

    expect(getRow(2, 1)).to.include({ sales: 5, rollingSales: 5, runningSales: 5 });
    expect(getRow(2, 1).prevSales).to.equal(undefined);
    expect(getRow(2, 2)).to.include({ sales: 15, rollingSales: 20, runningSales: 20 });
    expect(getRow(2, 2).prevSales).to.equal(5);
  });

  it("validates window frame inputs", () => {
    const badFrame: WindowFrameSpec = { kind: "rolling", count: 0 } as any;
    expect(() =>
      buildMetricFromExpr({
        name: "bad", baseFact: "fact_sales", expr: Expr.window(Expr.metric("sales"), {
          partitionBy: [],
          orderBy: "week",
          frame: badFrame,
          aggregate: "sum",
        }),
      })
    ).to.throw("rolling window count must be a positive integer");
  });

  it("surfaces validation errors for unreachable window attributes", () => {
    const totalSales = aggregateMetric("totalSales", "fact_sales", "salesAmount", "sum");
    const badMetric = buildMetricFromExpr({
      name: "badWindow",
      baseFact: "fact_sales",
      expr: Expr.window(Expr.metric("totalSales"), {
        partitionBy: ["storeId"],
        orderBy: "unreachableAttr",
        frame: { kind: "rolling", count: 2 },
        aggregate: "sum",
      }),
    });

    const modelWithError: SemanticModel = {
      ...validationModelBase,
      metrics: { totalSales, badWindow: badMetric },
    };

    const result = validateMetric(modelWithError, "badWindow");
    expect(result.ok).to.equal(false);
    expect(result.errors.some((e) => e.message.includes("unreachableAttr"))).to.equal(true);
  });
});

describe("metric builders", () => {
  it("creates aggregate metrics backed by MetricExpr", () => {
    const metric = aggregateMetric("avgSales", "fact_sales", "salesAmount", "avg");

    expect(metric.exprAst).to.deep.equal(Expr.avg("salesAmount"));
    expect(metric.attributes).to.deep.equal(["salesAmount"]);
    expect(metric.deps).to.deep.equal([]);
    expect(metric.baseFact).to.equal("fact_sales");
  });
});

describe("table transforms", () => {
  it("applies inline mapping tables for weekly shifts", () => {
    const db: InMemoryDb = {
      tables: {
        fact_sales: [
          { week: 202501, salesAmount: 100 },
          { week: 202502, salesAmount: 75 },
        ],
        prev_week_map: [{ key: 202502, mappedKey: 202501 }],
      },
    };

    const sales = aggregateMetric("sales", "fact_sales", "salesAmount", "sum");
    const { definition: transformDef } = defineTableTransform({
      name: "prev_week",
      table: db.tables.prev_week_map as any,
      inputAttr: "week",
      outputAttr: "week",
    });

    const model: SemanticModel = {
      facts: { fact_sales: { table: "fact_sales" } },
      dimensions: {},
      attributes: { week: { table: "fact_sales" }, salesAmount: { table: "fact_sales" } },
      joins: [],
      metrics: {
        sales,
        prevWeekSales: tableTransformMetric({
          name: "prevWeekSales",
          baseMetric: "sales",
          transformId: "prev_week",
          baseFact: "fact_sales",
          inputAttr: "week",
          outputAttr: "week",
        }),
      },
      tableTransforms: { prev_week: transformDef },
    };

    const rows = runSemanticQuery(
      { db, model },
      { dimensions: ["week"], metrics: ["sales", "prevWeekSales"] }
    );

    const week1 = rows.find((r) => r.week === 202501);
    const week2 = rows.find((r) => r.week === 202502);

    expect(week1?.sales).to.equal(100);
    expect(week1?.prevWeekSales).to.be.undefined;
    expect(week2?.sales).to.equal(75);
    expect(week2?.prevWeekSales).to.equal(100);
  });

  it("supports many-to-one bucket mappings and missing entries", () => {
    const db: InMemoryDb = {
      tables: {
        fact_sales: [
          { storeId: 1, storeBucket: "urban", salesAmount: 100 },
          { storeId: 2, storeBucket: "urban", salesAmount: 50 },
          { storeId: 3, storeBucket: "airport", salesAmount: 25 },
          { storeId: 4, storeBucket: "suburban", salesAmount: 10 },
        ],
        bucket_map: [
          { store_id: 1, bucket: "urban" },
          { store_id: 2, bucket: "urban" },
          { store_id: 3, bucket: "airport" },
        ],
      },
    };

    const totalSales = aggregateMetric("totalSales", "fact_sales", "salesAmount", "sum");
    const { definition: bucketTransform } = defineTableTransformFromRelation({
      name: "store_bucket",
      relation: "bucket_map",
      keyColumn: "store_id",
      mappedColumn: "bucket",
      inputAttr: "storeId",
      outputAttr: "storeBucket",
    });

    const model: SemanticModel = {
      facts: { fact_sales: { table: "fact_sales" } },
      dimensions: {},
      attributes: {
        storeId: { table: "fact_sales" },
        storeBucket: { table: "fact_sales" },
        salesAmount: { table: "fact_sales" },
      },
      joins: [],
      metrics: {
        totalSales,
        bucketSales: tableTransformMetric({
          name: "bucketSales",
          baseMetric: "totalSales",
          transformId: "store_bucket",
          baseFact: "fact_sales",
          inputAttr: "storeId",
          outputAttr: "storeBucket",
        }),
      },
      tableTransforms: { store_bucket: bucketTransform },
    };

    const rows = runSemanticQuery(
      { db, model },
      { dimensions: ["storeId"], metrics: ["totalSales", "bucketSales"] }
    );

    const urbanStores = rows.filter((r) => r.storeId === 1 || r.storeId === 2);
    urbanStores.forEach((row) => {
      expect(row.bucketSales).to.equal(150);
    });

    const airportStore = rows.find((r) => r.storeId === 3);
    expect(airportStore?.bucketSales).to.equal(25);

    const missingMapping = rows.find((r) => r.storeId === 4);
    expect(missingMapping?.bucketSales).to.be.undefined;
  });

  it("fans out when a key maps to multiple targets", () => {
    const db: InMemoryDb = {
      tables: {
        fact_sales: [
          { week: 202401, salesAmount: 20 },
          { week: 202402, salesAmount: 30 },
          { week: 202501, salesAmount: 15 },
        ],
        multi_map: [
          { key: 202501, mappedKey: 202401 },
          { key: 202501, mappedKey: 202402 },
        ],
      },
    };

    const sales = aggregateMetric("sales", "fact_sales", "salesAmount", "sum");
    const model: SemanticModel = {
      facts: { fact_sales: { table: "fact_sales" } },
      dimensions: {},
      attributes: { week: { table: "fact_sales" }, salesAmount: { table: "fact_sales" } },
      joins: [],
      metrics: {
        sales,
        fanOutSales: tableTransformMetric({
          name: "fanOutSales",
          baseMetric: "sales",
          transformId: "multi",
          baseFact: "fact_sales",
          inputAttr: "week",
          outputAttr: "week",
        }),
      },
      tableTransforms: {
        multi: {
          table: db.tables.multi_map as any,
          inputAttr: "week",
          outputAttr: "week",
        },
      },
    };

    const rows = runSemanticQuery(
      { db, model },
      { dimensions: ["week"], metrics: ["sales", "fanOutSales"] }
    );

    const fanOut = rows.find((r) => r.week === 202501);
    expect(fanOut?.fanOutSales).to.equal(50);
  });
});

describe("static analysis + validation", () => {
  it("flags attributes that are unreachable from the declared base fact", () => {
    const unreachableMetric = buildMetricFromExpr({
      name: "badAttrMetric",
      baseFact: "fact_sales",
      expr: Expr.sum("unreachableAttr"),
    });

    const model: SemanticModel = {
      ...validationModelBase,
      metrics: { badAttrMetric: unreachableMetric },
    };

    const result = validateMetric(model, "badAttrMetric");

    expect(result.ok).to.be.false;
    expect(result.errors[0].message).to.include("not reachable");
  });

  it("detects circular metric dependencies", () => {
    const metricA = buildMetricFromExpr({
      name: "metricA",
      baseFact: "fact_sales",
      expr: Expr.add(Expr.metric("metricB"), Expr.lit(1)),
    });

    const metricB = buildMetricFromExpr({
      name: "metricB",
      baseFact: "fact_sales",
      expr: Expr.add(Expr.metric("metricA"), Expr.lit(1)),
    });

    const model: SemanticModel = {
      ...validationModelBase,
      metrics: { metricA, metricB },
    };

    const results = validateAll(model);
    const metricAIssues = results.find((r) => r.metric === "metricA")?.errors ?? [];
    const messages = metricAIssues.map((e) => e.message).join(" ");

    expect(messages).to.include("Circular dependency");
  });

  it("flags missing rowset transform definitions", () => {
    const lastYear = lastYearMetric("salesLastYear", "fact_sales", "totalSales", "week");

    const model: SemanticModel = {
      ...validationModelBase,
      metrics: { totalSales, salesLastYear: lastYear },
    };

    const result = validateMetric(model, "salesLastYear");

    expect(result.ok).to.be.false;
    expect(result.errors.some((e) => e.message.includes("Transform"))).to.be.true;
  });

  it("prevents cross-fact metric dependencies", () => {
    const salesMetric = aggregateMetric("sales", "fact_sales", "salesAmount", "sum");
    const inventoryMetric = aggregateMetric(
      "inventory",
      "fact_inventory",
      "onHand",
      "sum"
    );

    const mixedMetric = buildMetricFromExpr({
      name: "mixed",
      baseFact: "fact_sales",
      expr: Expr.add(Expr.metric("sales"), Expr.metric("inventory")),
    });

    const model: SemanticModel = {
      ...validationModelBase,
      metrics: { sales: salesMetric, inventory: inventoryMetric, mixed: mixedMetric },
    };

    const result = validateMetric(model, "mixed");

    expect(result.ok).to.be.false;
    expect(result.errors.some((e) => e.message.includes("cross-fact"))).to.be.true;
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
    applyTableTransform: () => rowsToEnumerable([]),
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
      helpers: {
        runtime,
        applyRowsetTransform: transformFn,
        applyTableTransform: () => rowsToEnumerable([]),
        bind: () => undefined,
      },
    };

    expect(evalFn(ctx)).to.equal(60);
  });
});
