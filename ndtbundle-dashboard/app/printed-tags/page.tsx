"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type ReconcileBundle, type ReprintPrinterTarget, type StationPrintedTag } from "@/lib/api";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { MillFilter } from "@/components/MillFilter";
import { ReprintPrinterChooser } from "@/components/ReprintPrinterChooser";
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
      className={`px-2 py-1 rounded text-xs font-medium border ${
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
  const [reprintChooserBundle, setReprintChooserBundle] = useState<ReconcileBundle | null>(null);
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

  const openReprintChooser = (bundle: ReconcileBundle) => {
    const batchNo = bundle.bundleNo?.trim();
    if (!batchNo) {
      setReprintError("Bundle has no NDT Batch No.");
      return;
    }
    if (bundle.isForming) {
      setReprintError(`Bundle ${batchNo} is still forming and has no printed tag yet.`);
      return;
    }
    setReprintError(null);
    setReprintMessage(null);
    setReprintChooserBundle(bundle);
  };

  const reprintTag = async (bundle: ReconcileBundle, printerTarget: ReprintPrinterTarget) => {
    const batchNo = bundle.bundleNo?.trim();
    if (!batchNo) {
      setReprintError("Bundle has no NDT Batch No.");
      return;
    }

    const key = `mill:${batchNo}`;
    setReprintingKey(key);
    setReprintError(null);
    setReprintMessage(null);
    try {
      const res = await api.printReconciledBundle(batchNo, printerTarget);
      setReprintChooserBundle(null);
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
  const bothPanels = showMill && showStation;

  return (
    <div className="flex flex-col gap-3 min-h-[calc(100vh-7.5rem)]">
      <div className="flex justify-between items-center shrink-0">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Printed Tags</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {filtersActive
              ? `${millSummaryCount + stationSummaryCount} of ${millTotal + stationTotal} row(s)`
              : `${millTotal + stationTotal} row(s)`}
            {" · "}
            Mill: slit finish/start · Station: bundle end
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500">Refresh {secondsUntilRefresh}s</span>
          <button
            onClick={refresh}
            className="px-3 py-1.5 border border-gray-300 rounded-md text-xs font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {(error || reprintError || reprintMessage) && (
        <div className="space-y-2 shrink-0">
          {error && (
            <div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-red-700 text-sm">
              {error}
            </div>
          )}
          {reprintError && (
            <div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-red-700 text-sm">
              {reprintError}
            </div>
          )}
          {reprintMessage && (
            <div className="rounded-md bg-green-50 border border-green-200 px-3 py-2 text-green-800 text-sm">
              {reprintMessage}
            </div>
          )}
        </div>
      )}

      {/* Compact filter toolbar */}
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm px-3 py-2.5 space-y-2 shrink-0">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="flex items-center gap-1.5">
            <span className="text-[11px] font-semibold uppercase tracking-wide text-gray-500">Show</span>
            {(
              [
                { value: "both" as const, label: "Both" },
                { value: "mill" as const, label: "Mill" },
                { value: "station" as const, label: "Station" },
              ] as const
            ).map((opt) => (
              <button
                key={opt.value}
                type="button"
                onClick={() => setTagSource(opt.value)}
                className={`px-2 py-1 rounded text-xs font-medium border ${
                  tagSource === opt.value
                    ? "bg-primary-600 text-white border-primary-600"
                    : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>

          <div className="h-5 w-px bg-gray-200 hidden sm:block" />

          <MillFilter
            value={millFilter}
            onChange={setMillFilter}
            bundles={bundles}
            className="!gap-1.5 [&_span]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs"
          />

          <div className="h-5 w-px bg-gray-200 hidden md:block" />

          <div className="flex flex-wrap items-end gap-2">
            <div>
              <label className="block text-[10px] text-gray-500 mb-0.5">Batch</label>
              <input
                type="text"
                value={searchBatch}
                onChange={(e) => setSearchBatch(e.target.value)}
                placeholder="12261…"
                className="w-32 border border-gray-300 rounded px-2 py-1 text-xs font-mono"
              />
            </div>
            <div>
              <label className="block text-[10px] text-gray-500 mb-0.5">PO</label>
              <input
                type="text"
                value={searchPo}
                onChange={(e) => setSearchPo(e.target.value)}
                placeholder="10000…"
                className="w-32 border border-gray-300 rounded px-2 py-1 text-xs font-mono"
              />
            </div>
            {(searchBatch || searchPo) && (
              <button
                type="button"
                onClick={() => {
                  setSearchBatch("");
                  setSearchPo("");
                }}
                className="px-2 py-1 text-xs text-gray-600 border border-gray-300 rounded hover:bg-gray-50"
              >
                Clear
              </button>
            )}
          </div>
        </div>

        <div className="flex flex-wrap items-end gap-x-4 gap-y-2 border-t border-gray-100 pt-2">
          <DateRangeFilter
            value={dateRange}
            onChange={setDateRange}
            className="!p-0 !shadow-none !border-0 !bg-transparent !gap-2 flex-1 min-w-0 [&_label]:text-[10px] [&_input]:py-1 [&_input]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs [&_div.text-sm]:text-xs"
            summary={`${millSummaryCount + stationSummaryCount} of ${millTotal + stationTotal}`}
          />

          {showStation && (
            <>
              <div className="h-5 w-px bg-gray-200 hidden lg:block self-center" />
              <div className="flex flex-wrap items-center gap-1.5">
                <span className="text-[11px] font-semibold uppercase tracking-wide text-gray-500">
                  Station
                </span>
                {(
                  [
                    { value: "all" as const, label: "All" },
                    { value: "visual" as const, label: "Visual" },
                    { value: "revisual" as const, label: "Revisual" },
                    { value: "hydro" as const, label: "Hydro" },
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
                {(stationKind === "visual" || stationKind === "revisual") && (
                  <>
                    <span className="text-gray-300 mx-0.5">|</span>
                    {(
                      [
                        { value: "all" as const, label: "St 1+2" },
                        { value: "1" as const, label: "St 1" },
                        { value: "2" as const, label: "St 2" },
                      ] as const
                    ).map((opt) => (
                      <FilterChip
                        key={opt.value}
                        active={visualStationSub === opt.value}
                        label={opt.label}
                        onClick={() => setVisualStationSub(opt.value)}
                      />
                    ))}
                  </>
                )}
                {stationKind === "hydro" && (
                  <>
                    <span className="text-gray-300 mx-0.5">|</span>
                    {(
                      [
                        { value: "all" as const, label: "All hydro" },
                        { value: "fourHead" as const, label: "4-Head" },
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
                  </>
                )}
              </div>
            </>
          )}
        </div>
      </div>

      {/* Side-by-side tables with independent scroll */}
      <div
        className={`flex-1 min-h-0 grid gap-3 ${
          bothPanels ? "lg:grid-cols-2" : "grid-cols-1"
        }`}
      >
        {showMill && (
          <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden flex flex-col min-h-[18rem] max-h-[calc(100vh-13rem)]">
            <h2 className="px-3 py-2 bg-primary-50 text-gray-900 text-sm font-semibold border-b border-gray-200 shrink-0">
              Mill bundles
              {!loading && bundles.length > 0 && (
                <span className="ml-1.5 text-xs font-normal text-gray-600">
                  {filtersActive
                    ? `${filteredBundles.length} of ${bundles.length}`
                    : filteredBundles.length}
                </span>
              )}
            </h2>
            <div className="flex-1 min-h-0 overflow-auto">
              {loading ? (
                <p className="px-3 py-6 text-gray-500 text-sm">Loading...</p>
              ) : filteredBundles.length === 0 ? (
                <p className="px-3 py-6 text-gray-500 text-sm">
                  {bundles.length === 0
                    ? "No printed mill bundles yet."
                    : "No mill bundles match filters."}
                </p>
              ) : (
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="sticky top-0 z-10 bg-gray-50">
                    <tr>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Batch
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        PO
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        M
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase hidden xl:table-cell">
                        Slit
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Finish
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Pcs
                      </th>
                      <th className="px-3 py-1.5 text-right text-[10px] font-medium text-gray-500 uppercase">
                       {" "}
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {filteredBundles.map((b) => {
                      const batchNo = b.bundleNo?.trim() ?? "";
                      const canReprint = Boolean(batchNo) && !b.isForming;
                      const key = `mill:${batchNo}`;
                      const isReprinting = reprintingKey === key;
                      return (
                        <tr
                          key={`mill-${b.bundleNo}-${resolveBundleMillNo(b)}`}
                          className="hover:bg-gray-50"
                        >
                          <td className="px-3 py-1.5 text-xs font-medium text-gray-900 font-mono whitespace-nowrap">
                            {b.bundleNo}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 font-mono whitespace-nowrap">
                            {b.poNumber}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 tabular-nums">{b.millNo}</td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 hidden xl:table-cell whitespace-nowrap">
                            {b.slitNo}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 whitespace-nowrap">
                            {formatDisplayDate(b.slitFinishTime || b.slitStartTime || b.printedAt)}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 tabular-nums">{b.totalNdtPcs}</td>
                          <td className="px-3 py-1.5 text-right whitespace-nowrap">
                            <button
                              type="button"
                              onClick={() => openReprintChooser(b)}
                              disabled={!canReprint || reprintingKey != null || reprintChooserBundle != null}
                              title={
                                b.isForming
                                  ? "Still forming — no printed tag yet"
                                  : "Choose printer and reprint ZPL tag with Reprint marker"
                              }
                              className="px-2 py-0.5 rounded border border-violet-300 bg-violet-50 text-violet-900 text-[11px] font-medium hover:bg-violet-100 disabled:opacity-50 disabled:pointer-events-none"
                            >
                              {isReprinting ? "…" : "Reprint"}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {showStation && (
          <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden flex flex-col min-h-[18rem] max-h-[calc(100vh-13rem)]">
            <h2 className="px-3 py-2 bg-amber-50 text-gray-900 text-sm font-semibold border-b border-gray-200 shrink-0">
              Station tags
              {!loading && stationTags.length > 0 && (
                <span className="ml-1.5 text-xs font-normal text-gray-600">
                  {filtersActive
                    ? `${filteredStationTags.length} of ${stationTags.length}`
                    : filteredStationTags.length}
                </span>
              )}
            </h2>
            <div className="flex-1 min-h-0 overflow-auto">
              {loading ? (
                <p className="px-3 py-6 text-gray-500 text-sm">Loading...</p>
              ) : filteredStationTags.length === 0 ? (
                <p className="px-3 py-6 text-gray-500 text-sm">
                  {stationTags.length === 0
                    ? "No station tag runs recorded yet."
                    : "No station tags match filters."}
                </p>
              ) : (
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="sticky top-0 z-10 bg-gray-50">
                    <tr>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Batch
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Station
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase hidden xl:table-cell">
                        Hydro
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        OK
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase">
                        Print
                      </th>
                      <th className="px-3 py-1.5 text-left text-[10px] font-medium text-gray-500 uppercase hidden 2xl:table-cell">
                        Time
                      </th>
                      <th className="px-3 py-1.5 text-right text-[10px] font-medium text-gray-500 uppercase">
                        {" "}
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {filteredStationTags.map((t) => {
                      const batchNo = t.ndtBatchNo?.trim() ?? "";
                      const workStation = t.workStation?.trim() ?? "";
                      const key = `st:${t.id ?? `${batchNo}-${workStation}`}`;
                      const isReprinting = reprintingKey === key;
                      const canReprint = Boolean(batchNo && workStation);
                      return (
                        <tr key={key} className="hover:bg-gray-50">
                          <td className="px-3 py-1.5 text-xs font-medium text-gray-900 font-mono whitespace-nowrap">
                            <div>{t.ndtBatchNo}</div>
                            <div className="text-[10px] text-gray-500 font-normal">
                              PO {t.poNumber || "—"} · M{(t.millNo ?? resolveStationMillNo(t)) || "—"}
                            </div>
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 whitespace-nowrap">
                            {t.workStation || "—"}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 hidden xl:table-cell whitespace-nowrap">
                            {t.hydrotestingType?.trim() || "—"}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 tabular-nums whitespace-nowrap">
                            {t.okPcs ?? 0}/{t.rejectPcs ?? 0}
                          </td>
                          <td className="px-3 py-1.5 text-xs">
                            <PrintStatusBadge status={t.printStatus} />
                          </td>
                          <td className="px-3 py-1.5 text-xs text-gray-700 whitespace-nowrap hidden 2xl:table-cell">
                            {formatDisplayDate(t.bundleEnd || t.importedAtUtc || t.bundleStart)}
                          </td>
                          <td className="px-3 py-1.5 text-right whitespace-nowrap">
                            <button
                              type="button"
                              onClick={() => void reprintStationTag(t)}
                              disabled={!canReprint || reprintingKey != null}
                              title="Reprint station ZPL tag with Reprint marker"
                              className="px-2 py-0.5 rounded border border-amber-300 bg-amber-50 text-amber-950 text-[11px] font-medium hover:bg-amber-100 disabled:opacity-50 disabled:pointer-events-none"
                            >
                              {isReprinting ? "…" : "Reprint"}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}
      </div>
      <ReprintPrinterChooser
        open={reprintChooserBundle != null}
        millNo={
          reprintChooserBundle != null ? resolveBundleMillNo(reprintChooserBundle) || 1 : 1
        }
        title="Choose reprint printer"
        subtitle={
          reprintChooserBundle
            ? `Bundle ${reprintChooserBundle.bundleNo} (${reprintChooserBundle.totalNdtPcs ?? "?"} pcs). Label includes the Reprint marker.`
            : undefined
        }
        confirmLabel="Reprint"
        busy={reprintingKey != null}
        onCancel={() => {
          if (reprintingKey == null) setReprintChooserBundle(null);
        }}
        onConfirm={(target) => {
          if (reprintChooserBundle) void reprintTag(reprintChooserBundle, target);
        }}
      />
    </div>
  );
}
