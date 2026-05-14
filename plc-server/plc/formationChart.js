"use strict";

/** Mirrors NdtBundleService FormationChartCsvProvider built-in chart + FormationChartLookup rules. */
const BUILT_IN = {
  Default: 20,
  "0.5": 2,
  "0.75": 180,
  "1": 150,
  "1.25": 140,
  "1.5": 120,
  "2": 80,
  "2.4": 60,
  "2.5": 65,
  "3": 45,
  "3.5": 40,
  "4": 35,
  "5": 25,
  "6": 20,
  "8": 13,
};

function normalizePipeSizeKey(raw) {
  if (raw == null || String(raw).trim() === "") return "";
  let s = String(raw).trim().replace(/^"+|"+$/g, "").trim();
  s = s.replace(/\u2033/g, "").replace(/"/g, "").trim();
  if (!s) return "";
  const n = Number.parseFloat(s.replace(",", "."));
  if (Number.isFinite(n) && n === Math.trunc(n) && n >= 0 && n < 10000) {
    return String(Math.trunc(n));
  }
  if (Number.isFinite(n)) return String(n);
  return s;
}

function resolveThreshold(pipeSizeRaw) {
  const key = normalizePipeSizeKey(pipeSizeRaw);
  if (key && Object.prototype.hasOwnProperty.call(BUILT_IN, key)) {
    const t = BUILT_IN[key];
    return t > 0 ? t : 10;
  }
  const trimmed = (pipeSizeRaw && String(pipeSizeRaw).trim()) || "";
  if (trimmed && Object.prototype.hasOwnProperty.call(BUILT_IN, trimmed)) {
    const t = BUILT_IN[trimmed];
    return t > 0 ? t : 10;
  }
  let sizeThreshold = BUILT_IN.Default;
  if (sizeThreshold <= 0) sizeThreshold = 10;
  return sizeThreshold;
}

module.exports = { resolveThreshold, normalizePipeSizeKey, BUILT_IN };
