import type { ReconcileBundle } from "@/lib/api";

import type { DateRange } from "@/lib/dateRangeFilter";
import { isInDateRange, isDateRangeActive } from "@/lib/dateRangeFilter";

export type MillFilterValue = "all" | 1 | 2 | 3 | 4;

export const MILL_FILTER_OPTIONS: { value: MillFilterValue; label: string }[] = [
  { value: "all", label: "All mills" },
  { value: 1, label: "Mill 1" },
  { value: 2, label: "Mill 2" },
  { value: 3, label: "Mill 3" },
  { value: 4, label: "Mill 4" },
];

/** Mill from API field, or digit at position 5 of batch no (12YY + mill + seq). */
export function resolveBundleMillNo(bundle: ReconcileBundle): number {
  const m = bundle.millNo;
  if (typeof m === "number" && m >= 1 && m <= 4) return m;

  const bn = (bundle.bundleNo ?? "").trim();
  if (bn.length >= 5 && bn.startsWith("12")) {
    const digit = parseInt(bn[4], 10);
    if (digit >= 1 && digit <= 4) return digit;
  }

  return 0;
}

export function filterBundlesByMill(
  bundles: ReconcileBundle[],
  mill: MillFilterValue
): ReconcileBundle[] {
  if (mill === "all") return bundles;
  return bundles.filter((b) => resolveBundleMillNo(b) === mill);
}

export function countBundlesByMill(bundles: ReconcileBundle[]): Record<MillFilterValue, number> {
  const counts: Record<MillFilterValue, number> = {
    all: bundles.length,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
  };
  for (const b of bundles) {
    const m = resolveBundleMillNo(b);
    if (m >= 1 && m <= 4) counts[m as 1 | 2 | 3 | 4] += 1;
  }
  return counts;
}

/** Prefer slit finish, then slit start, then tag print time, for bundle date filtering. */
export function resolveBundleEntryDate(bundle: ReconcileBundle): string | null {
  const finish = bundle.slitFinishTime?.trim();
  if (finish) return finish;
  const start = bundle.slitStartTime?.trim();
  if (start) return start;
  const printed = bundle.printedAt?.trim();
  if (printed) return printed;
  return null;
}

export function filterBundlesByDateRange(
  bundles: ReconcileBundle[],
  range: DateRange
): ReconcileBundle[] {
  if (!isDateRangeActive(range)) return bundles;
  return bundles.filter((b) => isInDateRange(resolveBundleEntryDate(b), range));
}
