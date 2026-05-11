"use strict";

const fs = require("fs").promises;
const path = require("path");

/**
 * Primary: WIP_MM_PO_BUNDLE_yymmdd_HHmmss[.csv] e.g. WIP_03_1000058192_2603006978_260511_084825
 * Fallback: WIP_MM_PO_yymmdd_HHmmss.csv (repo sample)
 */
const RE_FULL = /^WIP_(\d{2})_(\d+)_(\d+)_(\d{6})_(\d{6})(\.csv)?$/i;
const RE_SHORT = /^WIP_(\d{2})_(\d+)_(\d{6})_(\d{6})\.csv$/i;

function parseWipFileName(fileName) {
  const base = path.basename(fileName);
  let m = RE_FULL.exec(base);
  if (m) {
    return {
      millNo: parseInt(m[1], 10),
      poNumber: m[2],
      bundleNo: m[3],
      datePart: m[4],
      timePart: m[5],
    };
  }
  m = RE_SHORT.exec(base);
  if (m) {
    return {
      millNo: parseInt(m[1], 10),
      poNumber: m[2],
      bundleNo: `${m[2]}_${m[3]}_${m[4]}`,
      datePart: m[3],
      timePart: m[4],
    };
  }
  return null;
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inQ = !inQ;
      continue;
    }
    if (!inQ && c === ",") {
      out.push(cur.trim());
      cur = "";
      continue;
    }
    cur += c;
  }
  out.push(cur.trim());
  return out;
}

function headerIndex(headers, ...names) {
  const lower = headers.map((h) => h.trim().toLowerCase());
  for (const n of names) {
    const nl = n.toLowerCase();
    const idx = lower.indexOf(nl);
    if (idx >= 0) return idx;
  }
  return -1;
}

/**
 * Read first data row from WIP CSV (same columns as repo WIP_01_*.csv).
 */
async function readWipCsvFields(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const headers = splitCsvLine(lines[0]);
  const cols = splitCsvLine(lines[1]);
  const idx = (names) => {
    const i = headerIndex(headers, ...names);
    return i >= 0 && i < cols.length ? cols[i].trim() : "";
  };
  return {
    poNo: idx("PO_No", "PO No", "PO Number"),
    millNumber: idx("Mill Number", "Mill No"),
    pipeGrade: idx("Pipe Grade"),
    pipeSize: idx("Pipe Size"),
    pipeThickness: idx("Pipe Thickness"),
    pipeLength: idx("Pipe Length"),
    pipeWeightPerMeter: idx("Pipe Weight Per Meter", "Pipe Weight per Meter"),
    pipeType: idx("Pipe Type"),
    piecesPerBundle: idx("Pieces Per Bundle"),
    outputItemcode: idx("Output Itemcode"),
  };
}

/**
 * Latest WIP bundle file for mill (by filename date/time, then mtime).
 */
async function findLatestWipForMill(bundleFolder, millNo) {
  const mm = String(millNo).padStart(2, "0");
  let entries;
  try {
    entries = await fs.readdir(bundleFolder, { withFileTypes: true });
  } catch {
    return null;
  }
  const candidates = [];
  for (const ent of entries) {
    if (!ent.isFile()) continue;
    const name = ent.name;
    if (!/^WIP_/i.test(name)) continue;
    const meta = parseWipFileName(name);
    if (!meta || meta.millNo !== millNo) continue;
    const full = path.join(bundleFolder, name);
    let st;
    try {
      st = await fs.stat(full);
    } catch {
      continue;
    }
    candidates.push({
      fullPath: full,
      meta,
      mtimeMs: st.mtimeMs,
      sortKey: `${meta.datePart}_${meta.timePart}`,
    });
  }
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => {
    if (a.sortKey !== b.sortKey) return a.sortKey < b.sortKey ? 1 : -1;
    return b.mtimeMs - a.mtimeMs;
  });
  const best = candidates[0];
  const fields = await readWipCsvFields(best.fullPath).catch(() => null);
  return {
    filePath: best.fullPath,
    fileName: path.basename(best.fullPath),
    millNo: best.meta.millNo,
    poNumber: best.meta.poNumber,
    bundleNo: best.meta.bundleNo,
    csv: fields,
  };
}

module.exports = { findLatestWipForMill, readWipCsvFields, parseWipFileName };
