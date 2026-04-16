"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type WipByMillRow, type RunningPoNdtSummary } from "@/lib/api";

type MillRowState = {
  row: WipByMillRow;
  ndtPipes: number | null;
};

export default function SummaryPage() {
  const [millRows, setMillRows] = useState<MillRowState[]>([]);
  const [sourcePath, setSourcePath] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dummyPrintStatus, setDummyPrintStatus] = useState<{ success: boolean; message: string } | null>(null);
  const [zplEnabled, setZplEnabled] = useState<boolean | null>(null);
  const [togglingZpl, setTogglingZpl] = useState(false);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const [byMills, runningSummary, zplStatus] = await Promise.all([
        api.wipByMills(),
        api.ndtSummaryRunningPo().catch(() => [] as RunningPoNdtSummary[]),
        api.zplGenerationStatus().catch(() => null),
      ]);

      setSourcePath(typeof byMills.sourcePath === "string" ? byMills.sourcePath : null);
      const mills = Array.isArray(byMills.mills) ? byMills.mills : [];
      const ndtByMill = new Map<number, number>();
      (Array.isArray(runningSummary) ? runningSummary : []).forEach((s) => {
        const m = typeof s.millNo === "number" ? s.millNo : Number.NaN;
        if (Number.isFinite(m) && m >= 1 && m <= 4) {
          ndtByMill.set(m, typeof s.totalNdtPipes === "number" ? s.totalNdtPipes : 0);
        }
      });

      const withNdt: MillRowState[] = mills.map((row) => {
        const po = (row.poNumber ?? "").trim();
        const rawMill = row.millNo;
        let ndtMill: number | null = null;
        if (typeof rawMill === "number" && Number.isFinite(rawMill)) ndtMill = rawMill;
        else if (typeof rawMill === "string" && rawMill.trim() !== "") {
          const p = parseInt(rawMill.trim(), 10);
          if (Number.isFinite(p)) ndtMill = p;
        }
        if (!po || ndtMill == null || ndtMill < 1 || ndtMill > 4) {
          return { row, ndtPipes: null };
        }
        return { row, ndtPipes: ndtByMill.has(ndtMill) ? ndtByMill.get(ndtMill) ?? 0 : null };
      });

      setMillRows(withNdt);
      setZplEnabled(typeof zplStatus?.enabled === "boolean" ? zplStatus.enabled : null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
      setMillRows([]);
      setSourcePath(null);
    } finally {
      setLoading(false);
      setSecondsUntilRefresh(30);
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

  const handlePrintDummyBundle = async () => {
    setDummyPrintStatus(null);
    setError(null);
    try {
      const res = await api.printDummyBundle();
      setDummyPrintStatus({
        success: true,
        message: res.message ?? "Dummy tag sent to printer.",
      });
    } catch (e) {
      let msg = e instanceof Error ? e.message : "Print request failed.";
      const match = msg.match(/^API 500: ([\s\S]+)$/);
      if (match) {
        try {
          const body = JSON.parse(match[1]) as { message?: string };
          if (body.message) msg = body.message;
        } catch {
          // keep msg as is
        }
      }
      setDummyPrintStatus({ success: false, message: msg });
    }
  };

  const handleToggleZpl = async () => {
    if (zplEnabled == null) return;
    setTogglingZpl(true);
    setError(null);
    try {
      const next = !zplEnabled;
      const res = await api.setZplGenerationStatus(next);
      const enabled = !!res.enabled;
      setZplEnabled(enabled);
      setDummyPrintStatus({
        success: true,
        message: enabled
          ? "ZPL generation enabled. New tag actions will generate .zpl preview files."
          : "ZPL generation disabled.",
      });
    } catch (e) {
      setDummyPrintStatus({
        success: false,
        message: e instanceof Error ? e.message : "Failed to update ZPL generation setting.",
      });
    } finally {
      setTogglingZpl(false);
    }
  };

  if (loading && millRows.length === 0 && !error)
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-gray-500">Loading...</div>
      </div>
    );

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Summary</h1>
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

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-5 py-3 bg-primary-50 border-b border-gray-200">
          <h2 className="font-semibold text-gray-900">PO running by mill</h2>
          <p className="text-sm text-gray-600 mt-1">
            Each row is one mill (Mill-1 … Mill-4). When the TM folder is configured, PO details are merged from every WIP
            CSV in that folder (newer files override per mill). NDT pipe counts use slit CSV rows for that PO and mill in
            the input folder.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Mill</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  PO number
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  NDT pipes
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden lg:table-cell">
                  Pipe size
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden lg:table-cell">
                  NDT pcs / bundle
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden lg:table-cell">
                  Pipe length
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden xl:table-cell">
                  Planned month
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden xl:table-cell">
                  Pcs / bundle
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide hidden xl:table-cell">
                  Total pcs
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {millRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-4 py-8 text-gray-500">
                    No mill data. Ensure the PO plan CSV has a &quot;Mill Number&quot; column and rows for mills 1–4.
                  </td>
                </tr>
              ) : (
                millRows.map(({ row, ndtPipes }) => {
                  const m = row.millNo ?? 0;
                  return (
                    <tr key={m} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-semibold text-gray-900 whitespace-nowrap">
                        Mill-{m}
                      </td>
                      <td className="px-4 py-3 text-gray-900 font-medium whitespace-nowrap">
                        {(row.poNumber ?? "").trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-primary-600 font-semibold whitespace-nowrap">
                        {ndtPipes == null ? "—" : ndtPipes}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden lg:table-cell whitespace-nowrap">
                        {row.pipeSize?.trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden lg:table-cell whitespace-nowrap font-medium tabular-nums">
                        {row.ndtPcsPerBundle == null ? "—" : row.ndtPcsPerBundle}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden lg:table-cell whitespace-nowrap">
                        {row.pipeLength?.trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden xl:table-cell whitespace-nowrap">
                        {row.plannedMonth?.trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden xl:table-cell whitespace-nowrap">
                        {row.piecesPerBundle?.trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-gray-700 hidden xl:table-cell whitespace-nowrap">
                        {row.totalPieces?.trim() || "—"}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
        {sourcePath && (
          <p className="px-5 py-2 text-xs text-gray-400 border-t border-gray-100 truncate" title={sourcePath}>
            Plan file: {sourcePath}
          </p>
        )}
      </div>

      {dummyPrintStatus && (
        <div
          className={`rounded-md border p-4 text-sm ${
            dummyPrintStatus.success
              ? "bg-green-50 border-green-200 text-green-800"
              : "bg-red-50 border-red-200 text-red-700"
          }`}
        >
          {dummyPrintStatus.message}
        </div>
      )}

      <div className="flex flex-wrap gap-3">
        <button
          type="button"
          onClick={handleToggleZpl}
          disabled={togglingZpl || zplEnabled == null}
          className={`inline-flex items-center px-4 py-2 rounded-md text-sm font-medium disabled:opacity-50 ${
            zplEnabled
              ? "bg-amber-600 text-white hover:bg-amber-700"
              : "bg-emerald-600 text-white hover:bg-emerald-700"
          }`}
        >
          {togglingZpl
            ? "Updating..."
            : zplEnabled
              ? "Disable ZPL Generation"
              : "Enable ZPL Generation"}
        </button>
        <button
          type="button"
          onClick={handlePrintDummyBundle}
          className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          Print Dummy Bundle
        </button>
        <Link
          href="/po-end"
          className="inline-flex items-center px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600"
        >
          Simulate PO End
        </Link>
      </div>
    </div>
  );
}
