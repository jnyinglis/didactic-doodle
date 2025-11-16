import { expect } from "chai";
import {
  applyContextToTable,
  createEngine,
  demoDb,
  demoModel,
  demoTableDefinitions,
  FilterContext,
} from "../src/semanticEngine";

const engine = createEngine(demoDb, demoModel);

function evaluate(name: string, context: FilterContext) {
  return engine.evaluateMetric(name, { filter: context });
}

describe("semanticEngine", () => {
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
