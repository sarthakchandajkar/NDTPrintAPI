"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type ReconcileBundle } from "@/lib/api";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { MillFilter } from "@/components/MillFilter";
import {
  filterBundlesByDateRange,
  filterBundlesByMill,
  type MillFilterValue,
} from "@/lib/millFilter";
import { EMPTY_DATE_RANGE, formatDisplayDate, isDateRangeActive, type DateRange } from "@/lib/dateRangeFilter";

export default function PrintedTagsPage() {
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [millFilter, setMillFilter] = useState<MillFilterValue>("all");
  const [dateRange, setDateRange] = useState<DateRange>(EMPTY_DATE_RANGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);
  const [reprintingBatchNo, setReprintingBatchNo] = useState<string | null>(null);
  const [reprintMessage, setReprintMessage] = useState<string | null>(null);
  const [reprintError, setReprintError] = useState<string | null>(null);

  const filteredBundles = useMemo(() => {
    const byMill = filterBundlesByMill(bundles, millFilter);
    return filterBundlesByDateRange(byMill, dateRange);
  }, [bundles, millFilter, dateRange]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.reconcileBundles();
      setBundles(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
      setBundles([]);
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
        `Reprint tag for ${batchNo} (${bundle.totalNdtPcs ?? "?"} pcs)?\n\nThe label will include the Reprint marker.`
      )
    ) {
      return;
    }

    setReprintingBatchNo(batchNo);
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
      setReprintingBatchNo(null);
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

      <MillFilter
        value={millFilter}
        onChange={setMillFilter}
        bundles={bundles}
        className="bg-white rounded-lg border border-gray-200 shadow-sm p-4"
      />

      <DateRangeFilter
        value={dateRange}
        onChange={setDateRange}
        summary={
          isDateRangeActive(dateRange) || millFilter !== "all"
            ? `${filteredBundles.length} of ${bundles.length} bundle(s)`
            : `${bundles.length} bundle(s)`
        }
        hint="Uses Slit Finish Time, then Slit Start Time, then tag print time (PrintedAt). Bundles without any date are hidden when a date range is set."
      />

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          Bundles (printed tags)
          {!loading && bundles.length > 0 && (millFilter !== "all" || isDateRangeActive(dateRange)) && (
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
              ? "No printed bundles yet."
              : "No bundles match the selected mill and date range. Try clearing filters or widening the dates."}
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
                  const isReprinting = reprintingBatchNo === batchNo;
                  return (
                    <tr key={b.bundleNo} className="hover:bg-gray-50">
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
                          disabled={!canReprint || reprintingBatchNo != null}
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
    </div>
  );
}
