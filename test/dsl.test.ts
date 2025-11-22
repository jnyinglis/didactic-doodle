import { expect } from "chai";
import {
  LogicalAttribute,
  SemanticModel,
  InMemoryDb,
  runSemanticQuery,
} from "../src/semanticEngine";
import {
  buildMetricDefinition,
  metricDef,
  parseAll,
  queryDef,
} from "../src/dsl";

describe("DSL parser and compiler", () => {
  const db: InMemoryDb = {
    tables: {
      fact_sales: [
        { storeId: 1, sales_amount: 100 },
        { storeId: 1, sales_amount: 50 },
        { storeId: 2, sales_amount: 25 },
      ],
    },
  };

  const attributes: Record<string, LogicalAttribute> = {
    storeId: { name: "storeId", relation: "fact_sales", column: "storeId" },
    sales_amount: { name: "sales_amount", relation: "fact_sales", column: "sales_amount" },
  };

  it("parses metric and query DSL into executable specs", () => {
    const totalSalesAst = parseAll(
      metricDef,
      "metric total_sales on fact_sales = sum(sales_amount)"
    );
    const avgTicketAst = parseAll(
      metricDef,
      "metric avg_ticket on fact_sales = total_sales / count(*)"
    );

    const metricNames = new Set([totalSalesAst.name, avgTicketAst.name]);
    const totalSalesDef = buildMetricDefinition(totalSalesAst, metricNames);
    const avgTicketDef = buildMetricDefinition(avgTicketAst, metricNames);

    const model: SemanticModel = {
      facts: { fact_sales: { name: "fact_sales" } },
      dimensions: {},
      attributes,
      joins: [],
      metrics: {
        [totalSalesDef.name]: totalSalesDef,
        [avgTicketDef.name]: avgTicketDef,
      },
    };

    const query = parseAll(
      queryDef,
      `query store_sales {
        dimensions: storeId
        metrics: total_sales, avg_ticket
      }`
    );

    const rows = runSemanticQuery({ db, model }, query.spec);
    const store1 = rows.find((r) => r.storeId === 1);
    const store2 = rows.find((r) => r.storeId === 2);

    expect(store1?.total_sales).to.equal(150);
    expect(store1?.avg_ticket).to.equal(75);
    expect(store2?.total_sales).to.equal(25);
    expect(store2?.avg_ticket).to.equal(25);
  });
});
