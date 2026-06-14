import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";

// Compiled dashboard TS is not checked in; run via `npm run test:dates` (tsx) or after `next build`.
const require = createRequire(import.meta.url);

let mod;
try {
  mod = require("./dateRangeFilter.ts");
} catch {
  mod = await import("./dateRangeFilter.ts");
}

const {
  isInDateRange,
  normalizeDateRange,
  parseFlexibleDate,
  parseInputSlitFileNameDate,
  toDateInputValue,
} = mod;

test("parseFlexibleDate uses day-first for slash dates", () => {
  const d = parseFlexibleDate("01/06/2026 10:00:00");
  assert.equal(toDateInputValue(d), "2026-06-01");
});

test("parseFlexibleDate handles dotted SAP slit times", () => {
  const d = parseFlexibleDate("28.02.2026 00:56:05");
  assert.equal(toDateInputValue(d), "2026-02-28");
});

test("parseFlexibleDate handles ISO from API", () => {
  const d = parseFlexibleDate("2026-06-14T11:16:59.492Z");
  assert.equal(toDateInputValue(d), "2026-06-14");
});

test("parseFlexibleDate ignores bare six-digit tokens", () => {
  assert.equal(parseFlexibleDate("260614"), null);
});

test("parseInputSlitFileNameDate reads yyMMdd token", () => {
  const d = parseInputSlitFileNameDate("2603516_01_260614_1000059179.csv");
  assert.equal(toDateInputValue(d), "2026-06-14");
});

test("isInDateRange compares calendar days inclusively", () => {
  const range = { startDate: "2026-06-14", endDate: "2026-06-14" };
  assert.equal(isInDateRange("01/06/2026 23:59:00", range), false);
  assert.equal(isInDateRange("14/06/2026 00:01:00", range), true);
  assert.equal(isInDateRange("2026-06-14T12:00:00.000Z", range), true);
});

test("normalizeDateRange swaps inverted bounds", () => {
  assert.deepEqual(normalizeDateRange({ startDate: "2026-06-20", endDate: "2026-06-10" }), {
    startDate: "2026-06-10",
    endDate: "2026-06-20",
  });
});
