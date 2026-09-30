"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type ReconcileBundle, type StationPrintedTag } from "@/lib/api";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { MillFilter } from "@/components/MillFilter";
import {
  filterBundlesByDateRange,
  filterBundlesByMill,
  resolveBundleMillNo,
  type MillFilterValue,
} from "@/lib/millFilter";
import {
  EMPTY_DATE_RANGE,
  formatDisplayDate,
  isDateRangeActive,
  isInDateRange,
  type DateRange,
} from "@/lib/dateRangeFilter";

type TagSource = "mill" | "station" | "both";
type StationKindFilter = "all" | "visual" | "revisual" | "hydro";
type VisualStationSubFilter = "all" | "1" | "2";
type HydroSubFilter = "all" | "fourHead" | "big";

function resolveStationMillNo(tag: StationPrintedTag): number {
  const m = tag.millNo;
  if (typeof m === "number" && m >= 1 && m <= 4) return m;
  const bn = (tag.ndtBatchNo ?? "").trim();
  if (bn.length >= 5 && bn.startsWith("12")) {
    const digit = parseInt(bn[4], 10);
    if (digit >= 1 && digit <= 4) return digit;
  }
  return 0;
}

function classifyStationKind(tag: StationPrintedTag): StationKindFilter | "other" {
  const ws = (tag.workStation ?? "").trim().toLowerCase();
  if (ws.startsWith("visual station")) return "visual";
  if (ws.startsWith("revisual station")) return "revisual";
  if (ws === "hydrotesting" || ws.includes("hydro")) return "hydro";
  const hydro = (tag.hydrotestingType ?? "").trim();
  if (hydro) return "hydro";
  return "other";
}

function parseVisualStationNumber(tag: StationPrintedTag): "1" | "2" | null {
  const ws = (tag.workStation ?? "").trim();
  const m = /(?:Visual|Revisual)\s+Station\s+([12])\b/i.exec(ws);
  if (!m) return null;
  return m[1] === "2" ? "2" : "1";
}

function classifyHydroType(tag: StationPrintedTag): HydroSubFilter | "other" {
  const h = (tag.hydrotestingType ?? "").trim().toLowerCase();
  if (h.includes("four") && h.includes("head")) return "fourHead";
  if (h.includes("big")) return "big";
  return "other";
}

function filterStationTagsByMill(
  tags: StationPrintedTag[],
  mill: MillFilterValue
): StationPrintedTag[] {
  if (mill === "all") return tags;
  return tags.filter((t) => resolveStationMillNo(t) === mill);
}

function filterStationTagsByDateRange(
  tags: StationPrintedTag[],
  range: DateRange
): StationPrintedTag[] {
  if (!isDateRangeActive(range)) return tags;
  return tags.filter((t) =>
    isInDateRange(t.bundleEnd || t.importedAtUtc || t.bundleStart || null, range)
  );
}

function filterStationTagsByStation(
  tags: StationPrintedTag[],
  kind: StationKindFilter,
  visualSub: VisualStationSubFilter,
  hydroSub: HydroSubFilter
): StationPrintedTag[] {
  if (kind === "all") return tags;

  return tags.filter((t) => {
    const k = classifyStationKind(t);
    if (k !== kind) return false;

    if (kind === "visual" || kind === "revisual") {
      if (visualSub === "all") return true;
      return parseVisualStationNumber(t) === visualSub;
    }

    if (kind === "hydro") {
      if (hydroSub === "all") return true;
      return classifyHydroType(t) === hydroSub;
    }

    return true;
  });
}

function FilterChip({
  active,
  label,
  onClick,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`px-3 py-1.5 rounded-md text-sm font-medium border ${
        active
          ? "bg-amber-700 text-white border-amber-700"
          : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
      }`}
    >
      {label}
    </button>
  );
}

function PrintStatusBadge({ status }: { status?: string | null }) {
  const s = (status ?? "").trim();
  if (!s) {
    return <span className="text-xs text-gray-400">—</span>;
  }
  const lower = s.toLowerCase();
  const cls =
    lower === "printed"
      ? "bg-green-100 text-green-800"
      : lower === "printfailed" || lower === "print_failed"
        ? "bg-red-100 text-red-800"
        : "bg-gray-100 text-gray-700";
  return (
    <span className={`inline-flex px-1.5 py-0.5 rounded text-[10px] font-medium ${cls}`} title={s}>
      {s}
    </span>
  );
}

export default function PrintedTagsPage() {
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [stationTags, setStationTags] = useState<StationPrintedTag[]>([]);
  const [tagSource, setTagSource] = useState<TagSource>("both");
  const [stationKind, setStationKind] = useState<StationKindFilter>("all");
  const [visualStationSub, setVisualStationSub] = useState<VisualStationSubFilter>("all");
  const [hydroSub, setHydroSub] = useState<HydroSubFilter>("all");
  const [searchBatch, setSearchBatch] = useState("");
  const [searchPo, setSearchPo] = useState("");
  const [millFilter, setMillFilter] = useState<MillFilterValue>("all");
  const [dateRange, setDateRange] = useState<DateRange>(EMPTY_DATE_RANGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);
  const [reprintingKey, setReprintingKey] = useState<string | null>(null);
  const [reprintMessage, setReprintMessage] = useState<string | null>(null);
  const [reprintError, setReprintError] = useState<string | null>(null);

  const searchBatchQ = searchBatch.trim().toLowerCase();
  const searchPoQ = searchPo.trim().toLowerCase();
  const searchActive = Boolean(searchBatchQ || searchPoQ);

  const filteredBundles = useMemo(() => {
    const byMill = filterBundlesByMill(bundles, millFilter);
    const byDate = filterBundlesByDateRange(byMill, dateRange);
    if (!searchActive) return byDate;
    return byDate.filter((b) => {
      const batch = (b.bundleNo ?? "").toLowerCase();
      const po = (b.poNumber ?? "").toLowerCase();
      if (searchBatchQ && !batch.includes(searchBatchQ)) return false;
      if (searchPoQ && !po.includes(searchPoQ)) return false;
      return true;
    });
  }, [bundles, millFilter, dateRange, searchBatchQ, searchPoQ, searchActive]);

  const filteredStationTags = useMemo(() => {
    const byMill = filterStationTagsByMill(stationTags, millFilter);
    const byDate = filterStationTagsByDateRange(byMill, dateRange);
    const byStation = filterStationTagsByStation(byDate, stationKind, visualStationSub, hydroSub);
    if (!searchActive) return byStation;
    return byStation.filter((t) => {
      const batch = (t.ndtBatchNo ?? "").toLowerCase();
      const po = (t.poNumber ?? "").toLowerCase();
      if (searchBatchQ && !batch.includes(searchBatchQ)) return false;
      if (searchPoQ && !po.includes(searchPoQ)) return false;
      return true;
    });
  }, [
    stationTags,
    millFilter,
    dateRange,
    stationKind,
    visualStationSub,
    hydroSub,
    searchBatchQ,
    searchPoQ,
    searchActive,
  ]);

  const showMill = tagSource === "mill" || tagSource === "both";
  const showStation = tagSource === "station" || tagSource === "both";
  const stationFilterActive = stationKind !== "all";
  const filtersActive =
    millFilter !== "all" ||
    isDateRangeActive(dateRange) ||
    (showStation && stationFilterActive) ||
    searchActive;

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const [list, stations] = await Promise.all([
        api.reconcileBundles(),
        api.stationPrintedTags(),
      ]);
      setBundles(Array.isArray(list) ? list : []);
      setStationTags(Array.isArray(stations) ? stations : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
      setBundles([]);
      setStationTags([]);
    } finally {
      setLoading(false);
      setSecondsUntilRefresh(30);
    }
  };

  const reprintTag = async (bundle: ReconcileBundle) => {
    const batchNo = bundle.bundleNo?.trim();
    if (!batchNo) {
      setReprintError("Bundle has no NDT Batch No.");
      return;
    }
    if (bundle.isForming) {
      setReprintError(`Bundle ${batchNo} is still forming and has no printed tag yet.`);
      return;
    }
    if (
      !window.confirm(
        `Reprint mill tag for ${batchNo} (${bundle.totalNdtPcs ?? "?"} pcs)?\n\nThe label will include the Reprint marker.`
      )
    ) {
      return;
    }

    const key = `mill:${batchNo}`;
    setReprintingKey(key);
    setReprintError(null);
    setReprintMessage(null);
    try {
      const res = await api.printReconciledBundle(batchNo);
      setReprintMessage(
        res.message ??
          `Reprint sent for ${res.ndtBatchNo ?? batchNo}${
            res.ndtPcs != null ? ` (${res.ndtPcs} pcs)` : ""
          }.`
      );
    } catch (e) {
      setReprintError(e instanceof Error ? e.message : `Failed to reprint ${batchNo}.`);
    } finally {
      setReprintingKey(null);
    }
  };

  const reprintStationTag = async (tag: StationPrintedTag) => {
    const batchNo = tag.ndtBatchNo?.trim();
    const workStation = tag.workStation?.trim();
    if (!batchNo) {
      setReprintError("Station row has no NDT Batch No.");
      return;
    }
    if (!workStation) {
      setReprintError("Station row has no Work Station.");
      return;
    }
    const okPcs = tag.okPcs ?? 0;
    if (
      !window.confirm(
        `Reprint station tag for ${batchNo}?\n\nStation: ${workStation}` +
          (tag.hydrotestingType?.trim() ? `\nHydro: ${tag.hydrotestingType.trim()}` : "") +
          `\nOK pcs: ${okPcs}\n\nThe label will include the Reprint marker.`
      )
    ) {
      return;
    }

    const key = `st:${tag.id ?? `${batchNo}-${workStation}`}`;
    setReprintingKey(key);
    setReprintError(null);
    setReprintMessage(null);
    try {
      const res = await api.reprintStationTag({
        ndtBatchNo: batchNo,
        workStation,
        hydrotestingType: tag.hydrotestingType,
        okPcs,
        poNumber: tag.poNumber,
        millNo: tag.millNo,
      });
      setReprintMessage(res.message ?? `Station reprint sent for ${batchNo} (${workStation}).`);
      await refresh();
    } catch (e) {
      setReprintError(e instanceof Error ? e.message : `Failed to reprint station tag ${batchNo}.`);
    } finally {
      setReprintingKey(null);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setSecondsUntilRefresh((s) => {
        if (s <= 1) {
          refresh();
          return 30;
        }
        return s - 1;
      });
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const millSummaryCount = showMill ? filteredBundles.length : 0;
  const stationSummaryCount = showStation ? filteredStationTags.length : 0;
  const millTotal = showMill ? bundles.length : 0;
  const stationTotal = showStation ? stationTags.length : 0;

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Printed Tags</h1>
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-500">Next refresh in {secondsUntilRefresh}s</span>
          <button
            onClick={refresh}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {error}
        </div>
      )}

      {reprintError && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {reprintError}
        </div>
      )}

      {reprintMessage && (
        <div className="rounded-md bg-green-50 border border-green-200 p-4 text-green-800 text-sm">
          {reprintMessage}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
        <div className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Show</div>
        <div className="flex flex-wrap gap-2">
          {(
            [
              { value: "both" as const, label: "Mill + station" },
              { value: "mill" as const, label: "Mill bundles only" },
              { value: "station" as const, label: "Station tags only" },
            ] as const
          ).map((opt) => (
            <button
              key={opt.value}
              type="button"
              onClick={() => setTagSource(opt.value)}
              className={`px-3 py-1.5 rounded-md text-sm font-medium border ${
                tagSource === opt.value
                  ? "bg-primary-600 text-white border-primary-600"
                  : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
        <div className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Search</div>
        <div className="flex flex-wrap gap-3 items-end">
          <div>
            <label className="block text-xs text-gray-500 mb-1">NDT Batch No</label>
            <input
              type="text"
              value={searchBatch}
              onChange={(e) => setSearchBatch(e.target.value)}
              placeholder="e.g. 1226100001"
              className="w-48 border border-gray-300 rounded-md px-3 py-2 text-sm font-mono"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">PO Number</label>
            <input
              type="text"
              value={searchPo}
              onChange={(e) => setSearchPo(e.target.value)}
              placeholder="e.g. 1000060591"
              className="w-48 border border-gray-300 rounded-md px-3 py-2 text-sm font-mono"
            />
          </div>
          {(searchBatch || searchPo) && (
            <button
              type="button"
              onClick={() => {
                setSearchBatch("");
                setSearchPo("");
              }}
              className="px-3 py-2 text-sm text-gray-600 border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Clear search
            </button>
          )}
        </div>
      </div>

      <MillFilter
        value={millFilter}
        onChange={setMillFilter}
        bundles={bundles}
        className="bg-white rounded-lg border border-gray-200 shadow-sm p-4"
      />

      {showStation && (
        <div className="bg-white rounded-lg border border-amber-200 shadow-sm p-4 space-y-3">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">
              Station type
            </div>
            <div className="flex flex-wrap gap-2">
              {(
                [
                  { value: "all" as const, label: "All stations" },
                  { value: "visual" as const, label: "Visual" },
                  { value: "revisual" as const, label: "Revisual" },
                  { value: "hydro" as const, label: "Hydrotesting" },
                ] as const
              ).map((opt) => (
                <FilterChip
                  key={opt.value}
                  active={stationKind === opt.value}
                  label={opt.label}
                  onClick={() => {
                    setStationKind(opt.value);
                    if (opt.value !== "visual" && opt.value !== "revisual") {
                      setVisualStationSub("all");
                    }
                    if (opt.value !== "hydro") {
                      setHydroSub("all");
                    }
                  }}
                />
              ))}
            </div>
          </div>

          {(stationKind === "visual" || stationKind === "revisual") && (
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">
                {stationKind === "visual" ? "Visual" : "Revisual"} physical station
              </div>
              <div className="flex flex-wrap gap-2">
                {(
                  [
                    { value: "all" as const, label: "Both stations" },
                    { value: "1" as const, label: "Station 1" },
                    { value: "2" as const, label: "Station 2" },
                  ] as const
                ).map((opt) => (
                  <FilterChip
                    key={opt.value}
                    active={visualStationSub === opt.value}
                    label={opt.label}
                    onClick={() => setVisualStationSub(opt.value)}
                  />
                ))}
              </div>
            </div>
          )}

          {stationKind === "hydro" && (
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">
                Hydrotesting type
              </div>
              <div className="flex flex-wrap gap-2">
                {(
                  [
                    { value: "all" as const, label: "All hydro" },
                    { value: "fourHead" as const, label: "Four Head" },
                    { value: "big" as const, label: "Big" },
                  ] as const
                ).map((opt) => (
                  <FilterChip
                    key={opt.value}
                    active={hydroSub === opt.value}
                    label={opt.label}
                    onClick={() => setHydroSub(opt.value)}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      <DateRangeFilter
        value={dateRange}
        onChange={setDateRange}
        summary={
          filtersActive
            ? `${millSummaryCount + stationSummaryCount} of ${millTotal + stationTotal} row(s)`
            : `${millTotal + stationTotal} row(s)`
        }
        hint="Mill bundles: Slit Finish / Start / PrintedAt. Station tags: Bundle End, then ImportedAt."
      />

      {showMill && (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
            Mill bundles (printed tags)
            {!loading && bundles.length > 0 && filtersActive && (
              <span className="ml-2 text-sm font-normal text-gray-600">
                — {filteredBundles.length} of {bundles.length}
              </span>
            )}
          </h2>
          {loading ? (
            <p className="px-5 py-8 text-gray-500">Loading...</p>
          ) : filteredBundles.length === 0 ? (
            <p className="px-5 py-8 text-gray-500 text-sm">
              {bundles.length === 0
                ? "No printed mill bundles yet."
                : "No mill bundles match the selected mill and date range."}
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">NDT Batch No</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">PO Number</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Mill No</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Slit No</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Slit finish</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">NDT Pipes</th>
                    <th className="px-5 py-2 text-right text-xs font-medium text-gray-500 uppercase">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredBundles.map((b) => {
                    const batchNo = b.bundleNo?.trim() ?? "";
                    const canReprint = Boolean(batchNo) && !b.isForming;
                    const key = `mill:${batchNo}`;
                    const isReprinting = reprintingKey === key;
                    return (
                      <tr key={`mill-${b.bundleNo}-${resolveBundleMillNo(b)}`} className="hover:bg-gray-50">
                        <td className="px-5 py-2 text-sm font-medium text-gray-900">{b.bundleNo}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">{b.poNumber}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">{b.millNo}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">{b.slitNo}</td>
                        <td className="px-5 py-2 text-sm text-gray-700 whitespace-nowrap">
                          {formatDisplayDate(b.slitFinishTime || b.slitStartTime || b.printedAt)}
                        </td>
                        <td className="px-5 py-2 text-sm text-gray-700">{b.totalNdtPcs}</td>
                        <td className="px-5 py-2 text-sm text-right whitespace-nowrap">
                          <button
                            type="button"
                            onClick={() => void reprintTag(b)}
                            disabled={!canReprint || reprintingKey != null}
                            title={
                              b.isForming
                                ? "Still forming — no printed tag yet"
                                : "Reprint ZPL tag with Reprint marker"
                            }
                            className="px-3 py-1.5 rounded-md border border-violet-300 bg-violet-50 text-violet-900 text-xs font-medium hover:bg-violet-100 disabled:opacity-50 disabled:pointer-events-none"
                          >
                            {isReprinting ? "Reprint…" : "Reprint"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {showStation && (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <h2 className="px-5 py-3 bg-amber-50 text-gray-900 font-semibold border-b border-gray-200">
            Station tags (Visual / Hydro / Revisual)
            {!loading && stationTags.length > 0 && filtersActive && (
              <span className="ml-2 text-sm font-normal text-gray-600">
                — {filteredStationTags.length} of {stationTags.length}
              </span>
            )}
          </h2>
          {loading ? (
            <p className="px-5 py-8 text-gray-500">Loading...</p>
          ) : filteredStationTags.length === 0 ? (
            <p className="px-5 py-8 text-gray-500 text-sm">
              {stationTags.length === 0
                ? "No station tag runs recorded yet."
                : "No station tags match the selected mill, station, search, and date range."}
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">NDT Batch No</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">PO Number</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Mill</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Station</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Hydro type</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">OK / Reject</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Print</th>
                    <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
                    <th className="px-5 py-2 text-right text-xs font-medium text-gray-500 uppercase">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredStationTags.map((t) => {
                    const batchNo = t.ndtBatchNo?.trim() ?? "";
                    const workStation = t.workStation?.trim() ?? "";
                    const key = `st:${t.id ?? `${batchNo}-${workStation}`}`;
                    const isReprinting = reprintingKey === key;
                    const canReprint = Boolean(batchNo && workStation);
                    return (
                      <tr key={key} className="hover:bg-gray-50">
                        <td className="px-5 py-2 text-sm font-medium text-gray-900">{t.ndtBatchNo}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">{t.poNumber}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">
                          {t.millNo ?? (resolveStationMillNo(t) || "—")}
                        </td>
                        <td className="px-5 py-2 text-sm text-gray-700">{t.workStation || "—"}</td>
                        <td className="px-5 py-2 text-sm text-gray-700">{t.hydrotestingType?.trim() || "—"}</td>
                        <td className="px-5 py-2 text-sm text-gray-700 tabular-nums">
                          {t.okPcs ?? 0} / {t.rejectPcs ?? 0}
                        </td>
                        <td className="px-5 py-2 text-sm">
                          <PrintStatusBadge status={t.printStatus} />
                          {t.printError ? (
                            <div className="text-[10px] text-red-600 mt-0.5 max-w-xs truncate" title={t.printError}>
                              {t.printError}
                            </div>
                          ) : null}
                        </td>
                        <td className="px-5 py-2 text-sm text-gray-700 whitespace-nowrap">
                          {formatDisplayDate(t.bundleEnd || t.importedAtUtc || t.bundleStart)}
                        </td>
                        <td className="px-5 py-2 text-sm text-right whitespace-nowrap">
                          <button
                            type="button"
                            onClick={() => void reprintStationTag(t)}
                            disabled={!canReprint || reprintingKey != null}
                            title="Reprint station ZPL tag with Reprint marker"
                            className="px-3 py-1.5 rounded-md border border-amber-300 bg-amber-50 text-amber-950 text-xs font-medium hover:bg-amber-100 disabled:opacity-50 disabled:pointer-events-none"
                          >
                            {isReprinting ? "Reprint…" : "Reprint"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
