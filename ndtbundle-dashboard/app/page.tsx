"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { api, type FormationChartEntryRow, type WipByMillRow } from "@/lib/api";
import { LineRunningLamp } from "@/components/LineRunningLamp";
import { HooterOutputBadge } from "@/components/HooterOutputBadge";
import { BundleProgressBar } from "@/components/BundleProgressBar";
import { usePlcCountsByMill } from "@/lib/usePlcMillLive";

type MillRowState = {
  row: WipByMillRow;
};

function parseMillNo(row: WipByMillRow): number {
  const m = row.millNo ?? 0;
  if (typeof m === "number" && Number.isFinite(m)) return m;
  if (typeof m === "string") {
    const n = parseInt(m, 10);
    return Number.isFinite(n) ? n : Number.NaN;
  }
  return Number.NaN;
}

function normalizePipeSizeKey(size?: string | null): string {
  return (size ?? "").trim().replace(/^"+|"+$/g, "");
}

export default function SummaryPage() {
  const [millRows, setMillRows] = useState<MillRowState[]>([]);
  const [formationChart, setFormationChart] = useState<FormationChartEntryRow[]>([]);
  const plcLive = usePlcCountsByMill();
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
      const [byMills, zplStatus] = await Promise.all([
        api.wipByMills(),
        api.zplGenerationStatus().catch(() => null),
      ]);

      setSourcePath(typeof byMills.sourcePath === "string" ? byMills.sourcePath : null);
      const mills = Array.isArray(byMills.mills) ? byMills.mills : [];
      setMillRows(mills.map((row) => ({ row })));
      setFormationChart(Array.isArray(byMills.formationChart) ? byMills.formationChart : []);
      setZplEnabled(typeof zplStatus?.enabled === "boolean" ? zplStatus.enabled : null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
      // Keep last good millRows so a transient Shared timeout does not blank the Summary page.
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

  const activePipeSizes = useMemo(
    () =>
      new Set(
        millRows
          .map(({ row }) => normalizePipeSizeKey(row.pipeSize))
          .filter((s) => s.length > 0)
      ),
    [millRows]
  );

  const activeThresholdSummary = useMemo(() => {
    const parts: string[] = [];
    for (const entry of formationChart) {
      const size = normalizePipeSizeKey(entry.pipeSize);
      if (!size || !activePipeSizes.has(size)) continue;
      const pcs = entry.requiredNdtPcs;
      parts.push(pcs != null ? `${entry.pipeSize?.trim() || size} → ${pcs}` : size);
    }
    return parts;
  }, [formationChart, activePipeSizes]);

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
            Live OK / NOK / NDT and line status from{" "}
            <code className="text-xs bg-gray-100 px-1 rounded">/api/Status/plc-live</code>. Bundle formed so far
            (MW56) and threshold (MW58) update every second; hooter reflects Q6.7.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">Mill</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  Line
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  Hooter
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  PO number
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  OK
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  NOK
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  NDT
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wide">
                  Formed / threshold
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
                  <td colSpan={14} className="px-4 py-8 text-gray-500">
                    No mill data. Ensure the PO plan CSV has a &quot;Mill Number&quot; column and rows for mills 1–4.
                  </td>
                </tr>
              ) : (
                millRows.map(({ row }) => {
                  const m = row.millNo ?? 0;
                  const parsedMillNo = parseMillNo(row);
                  const plc = Number.isFinite(parsedMillNo) ? plcLive[parsedMillNo] : undefined;
                  const displayOk = plc?.okCount != null ? plc.okCount : "—";
                  const displayNok = plc?.nokCount != null ? plc.nokCount : "—";
                  const displayNdt = plc?.ndtCount != null ? plc.ndtCount : "—";
                  const thr =
                    plc?.thresholdValue != null
                      ? plc.thresholdValue
                      : row.ndtPcsPerBundle ?? null;
                  const acc = plc?.accumulatedValue ?? null;
                  const hooterOn = !!plc?.hooterActive;
                  return (
                    <tr
                      key={Number.isFinite(parsedMillNo) ? `mill-${parsedMillNo}` : `mill-${String(m)}`}
                      className={`hover:bg-gray-50 ${hooterOn ? "bg-amber-50/80" : ""}`}
                    >
                      <td className="px-4 py-3 font-semibold text-gray-900 whitespace-nowrap">
                        Mill-{m}
                      </td>
                      <td className="px-4 py-3 whitespace-nowrap">
                        <LineRunningLamp
                          running={plc?.lineRunning}
                          connected={plc?.connected}
                        />
                      </td>
                      <td className="px-4 py-3 whitespace-nowrap">
                        <HooterOutputBadge active={hooterOn} />
                      </td>
                      <td className="px-4 py-3 text-gray-900 font-medium whitespace-nowrap">
                        {(row.poNumber ?? "").trim() || "—"}
                      </td>
                      <td className="px-4 py-3 text-emerald-700 font-medium tabular-nums whitespace-nowrap">
                        {displayOk}
                      </td>
                      <td className="px-4 py-3 text-rose-700 font-medium tabular-nums whitespace-nowrap">
                        {displayNok}
                      </td>
                      <td className="px-4 py-3 text-primary-600 font-semibold tabular-nums whitespace-nowrap">
                        {displayNdt}
                      </td>
                      <td className="px-4 py-3 whitespace-nowrap">
                        <BundleProgressBar accumulated={acc} threshold={thr} compact />
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

      <details className="group rounded-lg border border-gray-200 bg-white shadow-sm open:shadow-sm">
        <summary className="flex cursor-pointer list-none items-center gap-3 px-4 py-2.5 text-sm select-none [&::-webkit-details-marker]:hidden">
          <span
            className="inline-block h-1.5 w-1.5 shrink-0 rounded-full bg-gray-400 group-open:bg-primary-500"
            aria-hidden
          />
          <span className="font-medium text-gray-800 shrink-0">Bundle size reference</span>
          <span className="min-w-0 flex-1 truncate text-xs text-gray-500">
            {formationChart.length === 0
              ? "No formation chart loaded"
              : activeThresholdSummary.length > 0
                ? `Active now: ${activeThresholdSummary.join(" · ")}`
                : `${formationChart.length} sizes · expand to glance at NDT pcs / bundle`}
          </span>
          <span className="shrink-0 text-xs text-gray-400 group-open:hidden">Show all</span>
          <span className="hidden shrink-0 text-xs text-gray-400 group-open:inline">Hide</span>
        </summary>
        <div className="border-t border-gray-100 px-4 py-3">
          {formationChart.length === 0 ? (
            <p className="text-xs text-gray-500">
              Check Settings → NDT bundle thresholds / FormationChart.csv.
            </p>
          ) : (
            <>
              <p className="mb-2 text-[11px] text-gray-500">
                Formation chart NDT pcs per bundle. Highlighted sizes are running on a mill. Edit under Settings.
              </p>
              <div className="flex flex-wrap gap-1.5">
                {formationChart.map((entry) => {
                  const size = normalizePipeSizeKey(entry.pipeSize);
                  const active = activePipeSizes.has(size);
                  const millsForSize = millRows
                    .filter(({ row }) => normalizePipeSizeKey(row.pipeSize) === size)
                    .map(({ row }) => row.millNo)
                    .filter((m) => m != null)
                    .join(",");
                  return (
                    <span
                      key={size || entry.pipeSize || "row"}
                      title={
                        active
                          ? `Running on Mill-${millsForSize}`
                          : `${entry.pipeSize?.trim() || size}: ${entry.requiredNdtPcs ?? "—"} NDT pcs / bundle`
                      }
                      className={`inline-flex items-baseline gap-1.5 rounded border px-2 py-1 text-xs tabular-nums ${
                        active
                          ? "border-emerald-300 bg-emerald-50 text-emerald-950"
                          : "border-gray-200 bg-gray-50 text-gray-700"
                      }`}
                    >
                      <span className="font-medium">{entry.pipeSize?.trim() || "—"}</span>
                      <span className={active ? "text-emerald-800 font-semibold" : "text-gray-900 font-semibold"}>
                        {entry.requiredNdtPcs ?? "—"}
                      </span>
                      {active && millsForSize ? (
                        <span className="text-[10px] font-medium text-emerald-700">M{millsForSize}</span>
                      ) : null}
                    </span>
                  );
                })}
              </div>
            </>
          )}
        </div>
      </details>

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
          PO End
        </Link>
      </div>
    </div>
  );
}
