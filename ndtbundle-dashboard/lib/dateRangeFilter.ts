/** Inclusive calendar-day range using YYYY-MM-DD (empty = no bound). */
export type DateRange = {
  startDate: string;
  endDate: string;
};

export const EMPTY_DATE_RANGE: DateRange = { startDate: "", endDate: "" };

export function isDateRangeActive(range: DateRange): boolean {
  return !!(range.startDate.trim() || range.endDate.trim());
}

/** Local calendar date as YYYY-MM-DD. */
export function toDateInputValue(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/** Default: last 30 calendar days through today. */
export function defaultDateRange(): DateRange {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 30);
  return { startDate: toDateInputValue(start), endDate: toDateInputValue(end) };
}

/** Ensures start <= end when both bounds are set. */
export function normalizeDateRange(range: DateRange): DateRange {
  const start = range.startDate.trim();
  const end = range.endDate.trim();
  if (!start || !end || start <= end) return { startDate: start, endDate: end };
  return { startDate: end, endDate: start };
}

function startOfDayFromInput(value: string): Date | null {
  const v = value.trim();
  if (!v) return null;
  const [y, m, d] = v.split("-").map((x) => parseInt(x, 10));
  if (!y || !m || !d) return null;
  return new Date(y, m - 1, d, 0, 0, 0, 0);
}

function endOfDayFromInput(value: string): Date | null {
  const v = value.trim();
  if (!v) return null;
  const [y, m, d] = v.split("-").map((x) => parseInt(x, 10));
  if (!y || !m || !d) return null;
  return new Date(y, m - 1, d, 23, 59, 59, 999);
}

/** SAP export date tokens use yyMMdd (e.g. 260614 → 2026-06-14). */
function parseYyMmDdToken(token: string): Date | null {
  if (!/^\d{6}$/.test(token)) return null;
  const yy = parseInt(token.slice(0, 2), 10);
  const month = parseInt(token.slice(2, 4), 10);
  const day = parseInt(token.slice(4, 6), 10);
  const year = yy >= 70 ? 1900 + yy : 2000 + yy;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const dt = new Date(year, month - 1, day, 0, 0, 0, 0);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

/** SAP input slit export names: {slit}_{seq}_{yyMMdd}_{po}.csv */
export function parseInputSlitFileNameDate(fileName: string | null | undefined): Date | null {
  const base = (fileName ?? "").trim().replace(/\.csv$/i, "");
  if (!base) return null;
  const parts = base.split("_");
  if (parts.length >= 3) return parseYyMmDdToken(parts[2] ?? "");
  return null;
}

function parseEuropeanDateTime(
  day: number,
  month: number,
  year: number,
  hour = 0,
  min = 0,
  sec = 0
): Date | null {
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const dt = new Date(year, month - 1, day, hour, min, sec);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function parseIsoLikeDate(raw: string): Date | null {
  // yyyy-MM-dd with optional time and timezone — safe for Date.parse.
  if (!/^\d{4}-\d{2}-\d{2}(?:[T\s]|$)/.test(raw) && !/^\d{4}-\d{2}-\d{2}T.*[Zz+]/.test(raw)) {
    return null;
  }
  const ms = Date.parse(raw);
  return Number.isNaN(ms) ? null : new Date(ms);
}

/** Parse ISO, European dd/MM/yyyy, dd.MM.yyyy, and ddMMyy tokens. */
export function parseFlexibleDate(raw: string | null | undefined): Date | null {
  const s = (raw ?? "").trim();
  if (!s) return null;

  const european = s.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (european) {
    const day = parseInt(european[1], 10);
    const month = parseInt(european[2], 10);
    let year = parseInt(european[3], 10);
    if (year < 100) year += 2000;
    const hour = european[4] ? parseInt(european[4], 10) : 0;
    const min = european[5] ? parseInt(european[5], 10) : 0;
    const sec = european[6] ? parseInt(european[6], 10) : 0;
    return parseEuropeanDateTime(day, month, year, hour, min, sec);
  }

  return parseIsoLikeDate(s);
}

export function isInDateRange(
  raw: string | Date | null | undefined,
  range: DateRange,
  options?: { includeWhenUnparseable?: boolean }
): boolean {
  const normalized = normalizeDateRange(range);
  if (!isDateRangeActive(normalized)) return true;

  const d = raw instanceof Date ? raw : parseFlexibleDate(typeof raw === "string" ? raw : null);
  if (!d) return options?.includeWhenUnparseable ?? false;

  const calendarKey = toDateInputValue(d);
  const startKey = normalized.startDate.trim();
  const endKey = normalized.endDate.trim();
  if (startKey && calendarKey < startKey) return false;
  if (endKey && calendarKey > endKey) return false;
  return true;
}

export function resolveEntryDate(...candidates: (string | null | undefined)[]): Date | null {
  for (const c of candidates) {
    const d = parseFlexibleDate(c);
    if (d) return d;
  }
  return null;
}

export function formatDisplayDate(raw: string | null | undefined): string {
  const d = parseFlexibleDate(raw);
  if (!d) return raw?.trim() || "—";
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
