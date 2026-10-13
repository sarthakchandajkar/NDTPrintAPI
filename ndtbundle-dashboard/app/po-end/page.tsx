"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import {
  api,
  type PoEndPending,
  type PlcManualPoEndPending,
  type RunningPoNdtSummary,
} from "@/lib/api";

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
  const [confirming, setConfirming] = useState(false);
  const [pending, setPending] = useState<PoEndPending | null>(null);
  const [manualPending, setManualPending] = useState<PlcManualPoEndPending | null>(null);
  const [runningByMill, setRunningByMill] = useState<RunningPoNdtSummary[]>([]);
  const [message, setMessage] = useState<{ type: "ok" | "warn" | "err"; text: string } | null>(
    null
  );
  const [confirmWorkflow, setConfirmWorkflow] = useState(false);

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

  const refreshManualPending = useCallback(async (mill: number) => {
    try {
      const res = await api.plcManualPoEndPending(mill);
      const items = res.items ?? [];
      setManualPending(items.find((i) => i.millNo === mill) ?? items[0] ?? null);
    } catch {
      setManualPending(null);
    }
  }, []);

  useEffect(() => {
    void loadRunningPo();
  }, [loadRunningPo]);

  useEffect(() => {
    const row = runningByMill.find((r) => parseMillNo(r.millNo) === millNo);
    if (row?.poNumber) setPoNumber(row.poNumber);
  }, [millNo, runningByMill]);

  useEffect(() => {
    const t = setTimeout(() => {
      void refreshPending(poNumber, millNo);
    }, 300);
    return () => clearTimeout(t);
  }, [poNumber, millNo, refreshPending]);

  useEffect(() => {
    void refreshManualPending(millNo);
    const id = setInterval(() => void refreshManualPending(millNo), 2000);
    return () => clearInterval(id);
  }, [millNo, refreshManualPending]);

  const runPoChangeWorkflow = async () => {
    const po = poNumber.trim();
    if (!po) {
      setMessage({ type: "err", text: "Enter the PO Number that is ending (the old / finished PO)." });
      return;
    }
    if (!confirmWorkflow) {
      setMessage({
        type: "err",
        text: "Check the confirm box before running the PO-change workflow.",
      });
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const result = await api.poEnd(po, millNo);
      const text = result.message ?? "PO-change workflow completed.";
      const type = result.warning ? "warn" : "ok";
      let full = text;
      if (result.warning) full += ` ${result.warning}`;
      full +=
        " If the printed/remainder count is wrong, use Reconcile to correct the total and adjust Input Slit files.";
      setMessage({ type, text: full });
      setConfirmWorkflow(false);
      await refreshPending(po, millNo);
      await loadRunningPo();
      await refreshManualPending(millNo);
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

  const confirmManualPoEnd = async (ackOnly = false) => {
    setConfirming(true);
    setMessage(null);
    try {
      const result = await api.confirmPlcManualPoEnd(millNo, ackOnly);
      setMessage({
        type: result.success === false ? "err" : "ok",
        text:
          result.message ??
          (result.success === false
            ? "Confirm failed."
            : ackOnly
              ? "MES ack sent."
              : "PO end confirmed."),
      });
      await refreshManualPending(millNo);
      await refreshPending(poNumber, millNo);
      await loadRunningPo();
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Confirm failed." });
      await refreshManualPending(millNo);
    } finally {
      setConfirming(false);
    }
  };

  const activeRow = runningByMill.find((r) => parseMillNo(r.millNo) === millNo);
  const pendingPcs = Math.max(pending?.pendingFromSizeCounts ?? 0, pending?.pendingRunningTotal ?? 0);
  const resolvedPo =
    manualPending?.poNumber?.trim() ||
    manualPending?.runningPoAtEdge?.trim() ||
    manualPending?.endedPo?.trim() ||
    manualPending?.endedPoAtEdge?.trim() ||
    null;
  const manualPoHint =
    resolvedPo ||
    (typeof manualPending?.poIdAtEdge === "number" && manualPending.poIdAtEdge > 0
      ? String(manualPending.poIdAtEdge)
      : null);
  const waitingForWip =
    manualPending?.waitingForNewWip === true || manualPending?.waitingForNewWipAtEdge === true;
  const endedPoHint =
    manualPending?.endedPo?.trim() || manualPending?.endedPoAtEdge?.trim() || null;
  const expectedPcs = manualPending?.expectedNdtPipeCount;
  const expectedBatch =
    manualPending?.expectedNdtBatchNumber?.trim() || "the NDT batch shown above";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">PO End</h1>
        <p className="text-gray-600 text-sm max-w-3xl mt-1">
          PLC PO-change acknowledgement is <strong>automatic</strong>: when the mill asserts the PO-change
          trigger, MES flushes the open remainder (prints the tag when enabled) and sends the handshake ack.
          Use this page as a <strong>backup</strong> when the PLC trigger is missed (linked / split slits such as{" "}
          <code className="text-xs">2607102_03_91</code>), or when a held manual confirm is armed.
        </p>
      </div>

      {message && (
        <div
          className={`rounded-md p-4 text-sm max-w-3xl ${
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

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4 max-w-3xl">
        <label className="block text-sm font-medium text-gray-700 mb-1">Mill</label>
        <select
          value={millNo}
          onChange={(e) => setMillNo(parseInt(e.target.value, 10))}
          className="w-full max-w-xs border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
        >
          {[1, 2, 3, 4].map((n) => (
            <option key={n} value={n}>
              Mill {n}
            </option>
          ))}
        </select>
        {activeRow?.poNumber && (
          <p className="mt-2 text-xs text-gray-500">
            Running PO for Mill {millNo}: <span className="font-medium">{activeRow.poNumber}</span>
            {activeRow.totalNdtPipes != null
              ? ` (${activeRow.totalNdtPipes} NDT pipes from slits)`
              : ""}
          </p>
        )}
        <p className="mt-3 text-xs text-gray-600 rounded-md bg-sky-50 border border-sky-100 px-3 py-2">
          Auto mode is on for Mill-1. If a PO-end tag quantity is wrong, fix it with{" "}
          <Link href="/reconcile" className="underline font-medium text-sky-900">
            Reconcile
          </Link>{" "}
          (corrected total + Input Slit file sum) rather than holding every PLC ack for a click.
        </p>
      </div>

      {manualPending ? (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-4 space-y-3 max-w-3xl">
          <div>
            <p className="text-sm font-semibold text-amber-950">
              Manual confirm backup — Mill {manualPending.millNo}
              {manualPending.millName ? ` (${manualPending.millName})` : ""}
            </p>
            <p className="mt-1 text-sm text-amber-900">
              A PLC PO-change edge is held for operator confirm (ManualConfirmPoEnd). Confirm after the last
              slit ends to flush remainder and send MES ack.
            </p>
            <ul className="mt-2 text-xs text-amber-800 space-y-0.5 tabular-nums">
              {manualPoHint && <li>PO hint: {manualPoHint}</li>}
              {waitingForWip && (
                <li>
                  Waiting for new WIP after PO end
                  {endedPoHint ? (
                    <>
                      {" "}
                      of <span className="font-semibold">{endedPoHint}</span>
                    </>
                  ) : null}
                </li>
              )}
              {manualPending?.ndtAtEdge != null && <li>NDT at edge: {manualPending.ndtAtEdge}</li>}
              {manualPending.detectedAtUtc && (
                <li>Detected: {new Date(manualPending.detectedAtUtc).toLocaleString()}</li>
              )}
            </ul>
          </div>

          <div className="rounded-md border border-amber-200 bg-white/70 px-3 py-2 space-y-1">
            <p className="text-xs font-semibold uppercase tracking-wide text-amber-900">
              Expected if you confirm
            </p>
            {manualPending.willFormBundle === false ||
            (expectedPcs != null && expectedPcs <= 0) ? (
              <p className="text-sm text-amber-900">
                No NDT batch will be formed (0 pipes in remainder). MES ack will still be sent.
              </p>
            ) : (
              <ul className="text-sm text-amber-950 space-y-0.5 tabular-nums">
                <li>
                  NDT Batch:{" "}
                  <span className="font-semibold">
                    {manualPending.expectedNdtBatchNumber?.trim() || "— (next sequence)"}
                  </span>
                </li>
                <li>
                  PO:{" "}
                  <span className="font-semibold">
                    {manualPending.poNumber?.trim() || manualPoHint || "—"}
                  </span>
                </li>
                <li>
                  NDT pipes: <span className="font-semibold">{expectedPcs ?? "—"}</span>
                </li>
              </ul>
            )}
          </div>

          <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-sm text-sky-950">
            Wrong expected count? Override open accumulation in{" "}
            <Link href="/settings" className="font-medium underline">
              Settings
            </Link>
            , confirm here, then{" "}
            <Link href="/reconcile" className="font-medium underline">
              Reconcile
            </Link>{" "}
            batch <span className="font-semibold">{expectedBatch}</span> if needed.
          </div>

          <div className="flex flex-wrap gap-3">
            <button
              onClick={() => void confirmManualPoEnd(false)}
              disabled={confirming || loading}
              className="px-4 py-2 bg-amber-700 text-white text-sm font-medium rounded-md hover:bg-amber-800 disabled:opacity-50"
            >
              {confirming ? "Confirming…" : "Confirm PO End (flush + ack)"}
            </button>
            <button
              onClick={() => void confirmManualPoEnd(true)}
              disabled={confirming || loading}
              className="px-4 py-2 border border-amber-400 text-amber-950 text-sm font-medium rounded-md bg-white hover:bg-amber-100 disabled:opacity-50"
            >
              MES ack only (no flush)
            </button>
          </div>
        </div>
      ) : (
        <div className="rounded-md border border-emerald-200 bg-emerald-50/60 p-4 max-w-3xl text-sm text-emerald-950">
          No held manual PLC confirm for Mill {millNo}. Automatic handshake is handling M40.6 when the PLC
          asserts it.
        </div>
      )}

      <div className="bg-white rounded-lg border border-indigo-200 shadow-sm p-6 max-w-3xl space-y-4">
        <div>
          <h2 className="text-sm font-semibold text-indigo-950">
            Backup: run PO-change workflow (no PLC trigger)
          </h2>
          <p className="text-xs text-indigo-900/80 mt-1">
            Use when the mill has already moved to a new PO but MES never received M40.6 — common with{" "}
            <strong>linked / split slits</strong> (e.g. <code>2607102_03_91</code>), where the same slit number
            appears on two Input Slit files with different PO numbers. Enter the <strong>ending</strong> PO,
            run the workflow to flush its open remainder, then reconcile if the printed total does not match
            the Input Slit sums for that PO.
          </p>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Ending PO Number</label>
          <input
            type="text"
            value={poNumber}
            onChange={(e) => setPoNumber(e.target.value)}
            placeholder="e.g. 1000062620"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          />
        </div>
        <div className="rounded-md bg-gray-50 border border-gray-200 px-3 py-2 text-sm text-gray-700 space-y-1">
          <p>
            Open remainder for this PO/mill: <span className="font-semibold">{pendingPcs}</span> NDT
            pipe(s)
          </p>
          {pending?.waitingForNewWip && (
            <p className="text-amber-800">
              This mill is waiting for a new WIP bundle file after a previous PO end.
            </p>
          )}
          {pending?.activePoForMill && !pending.poMatchesActiveMill && (
            <p className="text-amber-800">
              Active slit PO for this mill is {pending.activePoForMill} — confirm you entered the PO that
              should close.
            </p>
          )}
        </div>
        <label className="flex items-start gap-2 text-sm text-gray-800">
          <input
            type="checkbox"
            className="mt-1"
            checked={confirmWorkflow}
            onChange={(e) => setConfirmWorkflow(e.target.checked)}
          />
          <span>
            I confirm Mill {millNo} has finished PO <strong>{poNumber.trim() || "…"}</strong> (or linked-slit
            remainder must close) and I want MES to run the PO-change flush now. This does not write the PLC
            MES-ack bit unless a separate held confirm exists.
          </span>
        </label>
        <div className="flex flex-wrap gap-3">
          <button
            onClick={() => void runPoChangeWorkflow()}
            disabled={loading || confirming || !confirmWorkflow}
            className="px-4 py-2 bg-indigo-700 text-white text-sm font-medium rounded-md hover:bg-indigo-800 disabled:opacity-50"
          >
            {loading ? "Running…" : "Run PO-change workflow"}
          </button>
          {pending?.waitingForNewWip && (
            <button
              onClick={() => void resumeWip()}
              disabled={loading || confirming}
              className="px-4 py-2 border border-amber-300 text-amber-900 text-sm font-medium rounded-md bg-amber-50 hover:bg-amber-100 disabled:opacity-50"
            >
              Resume WIP (undo wait)
            </button>
          )}
        </div>
      </div>

      <div>
        <Link
          href="/"
          className="inline-flex px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          Back to Summary
        </Link>
      </div>
    </div>
  );
}
