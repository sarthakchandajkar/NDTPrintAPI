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

/** Parse ISO, yyyy-MM-dd, and common locale date/time strings. */
export function parseFlexibleDate(raw: string | null | undefined): Date | null {
  const s = (raw ?? "").trim();
  if (!s) return null;

  const iso = Date.parse(s);
  if (!Number.isNaN(iso)) return new Date(iso);

  const m = s.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (m) {
    const day = parseInt(m[1], 10);
    const month = parseInt(m[2], 10);
    let year = parseInt(m[3], 10);
    if (year < 100) year += 2000;
    const hour = m[4] ? parseInt(m[4], 10) : 0;
    const min = m[5] ? parseInt(m[5], 10) : 0;
    const sec = m[6] ? parseInt(m[6], 10) : 0;
    const dt = new Date(year, month - 1, day, hour, min, sec);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  return null;
}

export function isInDateRange(
  raw: string | Date | null | undefined,
  range: DateRange,
  options?: { includeWhenUnparseable?: boolean }
): boolean {
  if (!isDateRangeActive(range)) return true;

  const d = raw instanceof Date ? raw : parseFlexibleDate(typeof raw === "string" ? raw : null);
  if (!d) return options?.includeWhenUnparseable ?? false;

  const start = startOfDayFromInput(range.startDate);
  const end = endOfDayFromInput(range.endDate);
  if (start && d < start) return false;
  if (end && d > end) return false;
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
