import { expect } from "chai";
import {
  applyContextToTable,
  createEngine,
  demoDb,
  demoModel,
  demoTableDefinitions,
  FilterContext,
  Row,
} from "../src/semanticEngine";

const engine = createEngine(demoDb, demoModel);

function evaluate(name: string, context: FilterContext) {
  return engine.evaluateMetric(name, { filter: context });
}

describe("semanticEngine", () => {
  describe("FilterRange tagged union", () => {
    const salesRows: Row[] = demoDb.tables.sales;
    const grain = demoTableDefinitions.sales.grain;

    it("supports every range variant", () => {
      const between = applyContextToTable(
        salesRows,
        { month: { kind: "between", from: 1, to: 1 } },
        grain
      ).toArray();
      const betweenPredicate = (row: Row) =>
        row.month >= 1 && row.month <= 1;
      expect(between.every(betweenPredicate)).to.be.true;
      expect(between).to.have.lengthOf(
        salesRows.filter(betweenPredicate).length
      );

      const gte = applyContextToTable(
        salesRows,
        { month: { kind: "gte", value: 2 } },
        grain
      ).toArray();
      const gtePredicate = (row: Row) => row.month >= 2;
      expect(gte.every(gtePredicate)).to.be.true;
      expect(gte).to.have.lengthOf(salesRows.filter(gtePredicate).length);

      const gt = applyContextToTable(
        salesRows,
        { month: { kind: "gt", value: 1 } },
        grain
      ).toArray();
      const gtPredicate = (row: Row) => row.month > 1;
      expect(gt.every(gtPredicate)).to.be.true;
      expect(gt).to.have.lengthOf(salesRows.filter(gtPredicate).length);

      const lte = applyContextToTable(
        salesRows,
        { month: { kind: "lte", value: 1 } },
        grain
      ).toArray();
      const ltePredicate = (row: Row) => row.month <= 1;
      expect(lte.every(ltePredicate)).to.be.true;
      expect(lte).to.have.lengthOf(salesRows.filter(ltePredicate).length);

      const lt = applyContextToTable(
        salesRows,
        { month: { kind: "lt", value: 2 } },
        grain
      ).toArray();
      const ltPredicate = (row: Row) => row.month < 2;
      expect(lt.every(ltPredicate)).to.be.true;
      expect(lt).to.have.lengthOf(salesRows.filter(ltPredicate).length);
    });

    it("rejects ambiguous range payloads", () => {
      expect(() =>
        applyContextToTable(
          salesRows,
          { amount: { from: 400, to: 900 } as any },
          grain
        )
      ).to.throw(/Invalid filter range/);
    });
  });

  describe("applyContextToTable", () => {
    it("ignores filters that are not part of the grain", () => {
      const ctx: FilterContext = { year: 2025, regionId: "NA", productId: 1 };
      const rows = applyContextToTable(
        demoDb.tables.budget,
        ctx,
        demoTableDefinitions.budget.grain
      ).toArray();
      expect(rows).to.have.lengthOf(1);
      expect(rows[0]).to.include({ budgetAmount: 2200 });
    });
  });

  describe("evaluateMetric", () => {
    it("respects metric-level grain overrides on fact measures", () => {
      const value = evaluate("salesAmountYearRegion", {
        year: 2025,
        regionId: "NA",
        month: 2,
      });
      expect(value).to.equal(2550);
    });

    it("applies context transforms before evaluating base metrics", () => {
      const ytd = evaluate("salesAmountYTD", {
        year: 2025,
        regionId: "NA",
        month: 2,
      });
      expect(ytd).to.equal(2550);

      const priorYear = evaluate("salesAmountYTDLastYear", {
        year: 2025,
        regionId: "NA",
        month: 2,
      });
      expect(priorYear).to.equal(1830);
    });

    it("evaluates derived metrics using dependency values", () => {
      const value = evaluate("salesVsBudgetPct", {
        year: 2025,
        regionId: "NA",
        month: 2,
      });
      expect(value).to.be.closeTo((950 / 2200) * 100, 0.0001);
    });

    it("allows multiple metrics to reuse the same measure with different aggregations", () => {
      const total = evaluate("totalBudget", { year: 2025 });
      const average = evaluate("averageBudgetAmount", { year: 2025 });

      expect(total).to.equal(3800);
      expect(average).to.equal(1900);
    });

    it("lets custom metrics request aggregations at runtime", () => {
      const value = evaluate("budgetRange", { year: 2025 });
      expect(value).to.equal(2200 - 1600);
    });
  });

  describe("runQuery", () => {
    it("returns formatted metric values and enriches dimension labels", () => {
      const rows = engine
        .query("sales")
        .addAttributes("regionId")
        .addMetrics("totalSalesAmount")
        .where({ year: 2025, month: 2 })
        .run();

      expect(rows).to.deep.include({
        regionId: "NA",
        regionName: "North America",
        totalSalesAmount: "$950.00",
      });
      expect(rows).to.deep.include({
        regionId: "EU",
        regionName: "Europe",
        totalSalesAmount: "$450.00",
      });
    });

    it("allows base builders to be reused for different slices", () => {
      const base = engine.query("sales").where({ year: 2025 });
      const a = base
        .addAttributes("regionId")
        .addMetrics("totalSalesAmount")
        .where({ month: 2 })
        .run();
      const b = engine
        .query("sales")
        .where({ year: 2025 })
        .addAttributes("regionId")
        .addMetrics("totalSalesAmount")
        .where({ month: 2 })
        .run();
      expect(a).to.deep.equal(b);
    });
  });
});
