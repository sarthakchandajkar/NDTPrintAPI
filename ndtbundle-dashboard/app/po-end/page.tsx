"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { api, type PoEndPending, type RunningPoNdtSummary } from "@/lib/api";

function parseMillNo(value: number | string | undefined): number | null {
  if (typeof value === "number" && value >= 1 && value <= 4) return value;
  if (typeof value === "string") {
    const n = parseInt(value, 10);
    if (n >= 1 && n <= 4) return n;
  }
  return null;
}

export default function PoEndPage() {
  const [poNumber, setPoNumber] = useState("");
  const [millNo, setMillNo] = useState(1);
  const [loading, setLoading] = useState(false);
  const [pending, setPending] = useState<PoEndPending | null>(null);
  const [runningByMill, setRunningByMill] = useState<RunningPoNdtSummary[]>([]);
  const [message, setMessage] = useState<{ type: "ok" | "warn" | "err"; text: string } | null>(null);

  const loadRunningPo = useCallback(async () => {
    try {
      const rows = await api.ndtSummaryRunningPo();
      setRunningByMill(rows ?? []);
      return rows ?? [];
    } catch {
      setRunningByMill([]);
      return [];
    }
  }, []);

  const refreshPending = useCallback(async (po: string, mill: number) => {
    const trimmed = po.trim();
    if (!trimmed) {
      setPending(null);
      return;
    }
    try {
      const p = await api.poEndPending(trimmed, mill);
      setPending(p);
    } catch {
      setPending(null);
    }
  }, []);

  useEffect(() => {
    loadRunningPo();
  }, [loadRunningPo]);

  useEffect(() => {
    const row = runningByMill.find((r) => parseMillNo(r.millNo) === millNo);
    if (row?.poNumber) setPoNumber(row.poNumber);
  }, [millNo, runningByMill]);

  useEffect(() => {
    const t = setTimeout(() => {
      refreshPending(poNumber, millNo);
    }, 300);
    return () => clearTimeout(t);
  }, [poNumber, millNo, refreshPending]);

  const submit = async () => {
    const po = poNumber.trim();
    if (!po) {
      setMessage({ type: "err", text: "Enter PO Number." });
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const result = await api.poEnd(po, millNo);
      const text = result.message ?? "PO end simulated successfully.";
      const type = result.warning ? "warn" : "ok";
      let full = text;
      if (result.warning) full += ` ${result.warning}`;
      setMessage({ type, text: full });
      await refreshPending(po, millNo);
      await loadRunningPo();
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Request failed." });
    } finally {
      setLoading(false);
    }
  };

  const resumeWip = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const result = await api.resumeWipAfterPoEnd(millNo);
      setMessage({
        type: "ok",
        text: result.message ?? (result.resumed ? "WIP wait cleared." : "No change."),
      });
      await refreshPending(poNumber, millNo);
      await loadRunningPo();
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Resume failed." });
    } finally {
      setLoading(false);
    }
  };

  const activeRow = runningByMill.find((r) => parseMillNo(r.millNo) === millNo);
  const pendingPcs = Math.max(pending?.pendingFromSizeCounts ?? 0, pending?.pendingRunningTotal ?? 0);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Simulate PO End</h1>
      <p className="text-gray-600 text-sm">
        Trigger PO end for the entered PO and mill (1–4). This closes any open partial bundles for that PO/mill,
        prints NDT tags when enabled, and advances batch state. When WIP wait is enabled, the mill pauses slit bundling
        until a new WIP bundle file arrives.
      </p>

      {message && (
        <div
          className={`rounded-md p-4 text-sm ${
            message.type === "err"
              ? "bg-red-50 text-red-700 border border-red-200"
              : message.type === "warn"
                ? "bg-amber-50 text-amber-900 border border-amber-200"
                : "bg-green-50 text-green-800 border border-green-200"
          }`}
        >
          {message.text}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 max-w-lg space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">PO Number</label>
          <input
            type="text"
            value={poNumber}
            onChange={(e) => setPoNumber(e.target.value)}
            placeholder="e.g. 1000055673"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          />
          {activeRow?.poNumber && (
            <p className="mt-1 text-xs text-gray-500">
              Running PO for Mill {millNo}: <span className="font-medium">{activeRow.poNumber}</span>
              {activeRow.totalNdtPipes != null ? ` (${activeRow.totalNdtPipes} NDT pipes from slits)` : ""}
            </p>
          )}
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Mill No</label>
          <select
            value={millNo}
            onChange={(e) => setMillNo(parseInt(e.target.value, 10))}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          >
            {[1, 2, 3, 4].map((n) => (
              <option key={n} value={n}>
                Mill {n}
              </option>
            ))}
          </select>
        </div>

        <div className="rounded-md bg-gray-50 border border-gray-200 px-3 py-2 text-sm text-gray-700 space-y-1">
          <p>
            Open partial bundles for this PO/mill:{" "}
            <span className="font-semibold">{pendingPcs}</span> NDT pipe(s)
          </p>
          {pending?.waitingForNewWip && (
            <p className="text-amber-800">
              This mill is waiting for a new WIP bundle file after a previous PO end.
            </p>
          )}
          {pending && pending.activePoForMill && !pending.poMatchesActiveMill && (
            <p className="text-amber-800">
              Active slit PO for this mill is {pending.activePoForMill} — partials may be stored under that PO instead.
            </p>
          )}
        </div>

        <div className="flex flex-wrap gap-3">
          <button
            onClick={submit}
            disabled={loading}
            className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50"
          >
            {loading ? "Sending…" : "Simulate PO End"}
          </button>
          {pending?.waitingForNewWip && (
            <button
              onClick={resumeWip}
              disabled={loading}
              className="px-4 py-2 border border-amber-300 text-amber-900 text-sm font-medium rounded-md bg-amber-50 hover:bg-amber-100 disabled:opacity-50"
            >
              Resume WIP (undo wait)
            </button>
          )}
          <Link
            href="/"
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Back to Summary
          </Link>
        </div>
      </div>
    </div>
  );
}
