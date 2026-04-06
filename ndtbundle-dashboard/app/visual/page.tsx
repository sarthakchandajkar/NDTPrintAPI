"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";

export default function VisualPage() {
  const [ndtBatchNo, setNdtBatchNo] = useState("");
  const [poNumber, setPoNumber] = useState<string | null>(null);
  const [millNo, setMillNo] = useState<number | null>(null);
  const [incomingPcs, setIncomingPcs] = useState<number | null>(null);
  const [okPcs, setOkPcs] = useState(0);
  const [rejectedPcs, setRejectedPcs] = useState(0);
  const [printTag, setPrintTag] = useState(true);
  const [loadingContext, setLoadingContext] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const loadContext = async (batch: string) => {
    setLoadingContext(true);
    setError(null);
    setSuccess(null);
    try {
      const ctx = await api.manualStationContext("Visual", batch);
      setPoNumber(ctx.poNumber ?? null);
      setMillNo(typeof ctx.millNo === "number" ? ctx.millNo : null);
      setIncomingPcs(typeof ctx.incomingPcs === "number" ? ctx.incomingPcs : null);
      setOkPcs(typeof ctx.alreadyOkPcs === "number" ? ctx.alreadyOkPcs : 0);
      setRejectedPcs(typeof ctx.alreadyRejectedPcs === "number" ? ctx.alreadyRejectedPcs : 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load context.");
      setPoNumber(null);
      setMillNo(null);
      setIncomingPcs(null);
    } finally {
      setLoadingContext(false);
    }
  };

  const submit = async () => {
    setError(null);
    setSuccess(null);
    const batch = ndtBatchNo.trim();
    if (!batch) {
      setError("Enter/scan NDT Batch No.");
      return;
    }
    const incoming = incomingPcs ?? 0;
    if (okPcs < 0 || rejectedPcs < 0) {
      setError("OK/Rejected must be non-negative.");
      return;
    }
    if (incomingPcs == null) {
      setError("Load bundle context first.");
      return;
    }
    if (okPcs + rejectedPcs !== incoming) {
      setError(`OK (${okPcs}) + Rejected (${rejectedPcs}) must equal Incoming (${incoming}).`);
      return;
    }

    setSubmitting(true);
    try {
      const res = await api.manualStationRecord("Visual", {
        ndtBatchNo: batch,
        okPcs,
        rejectedPcs,
        printTag,
      });
      setSuccess(
        `${res.message ?? "Saved."} Batch: ${res.ndtBatchNo ?? "—"} | Outgoing: ${res.outgoingPcs ?? okPcs}${
          res.csvPath ? ` | CSV: ${res.csvPath}` : ""
        }`
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "Print failed.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Visual</h1>
      <p className="text-gray-600 text-sm">
        Scan/enter the NDT Batch No, then enter OK and Rejected pipe counts for Visual inspection. OK pipes move to the
        next section.
      </p>

      {error && <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">{error}</div>}
      {success && (
        <div className="rounded-md bg-green-50 border border-green-200 p-4 text-green-800 text-sm">{success}</div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 md:p-8 w-full max-w-6xl mx-auto space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">NDT Batch No</label>
          <input
            value={ndtBatchNo}
            onChange={(e) => setNdtBatchNo(e.target.value)}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
            placeholder="e.g. 9260100001"
          />
          <div className="pt-2">
            <button
              type="button"
              onClick={() => loadContext(ndtBatchNo.trim())}
              disabled={loadingContext || !ndtBatchNo.trim()}
              className="text-sm font-medium text-primary-700 hover:text-primary-800 disabled:opacity-50"
            >
              {loadingContext ? "Loading…" : "Load context"}
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 text-sm">
          <div className="rounded-md border border-gray-200 px-3 py-2">
            <div className="text-gray-500">PO Number</div>
            <div className="font-medium text-gray-900">{poNumber ?? "—"}</div>
          </div>
          <div className="rounded-md border border-gray-200 px-3 py-2">
            <div className="text-gray-500">Mill No</div>
            <div className="font-medium text-gray-900">{millNo ?? "—"}</div>
          </div>
          <div className="rounded-md border border-gray-200 px-3 py-2">
            <div className="text-gray-500">Incoming Pcs</div>
            <div className="font-semibold text-gray-900">{incomingPcs ?? "—"}</div>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">OK pcs</label>
            <input
              type="number"
              min={0}
              value={okPcs}
              onChange={(e) => setOkPcs(parseInt(e.target.value, 10) || 0)}
              disabled={incomingPcs == null}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Rejected pcs</label>
            <input
              type="number"
              min={0}
              value={rejectedPcs}
              onChange={(e) => setRejectedPcs(parseInt(e.target.value, 10) || 0)}
              disabled={incomingPcs == null}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
            />
          </div>
        </div>

        <label className="inline-flex items-center gap-2 text-sm text-gray-700">
          <input
            type="checkbox"
            checked={printTag}
            onChange={(e) => setPrintTag(e.target.checked)}
            className="h-4 w-4"
          />
          Print tag for outgoing OK pcs
        </label>

        <button
          onClick={submit}
          disabled={submitting || incomingPcs == null}
          className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50 disabled:pointer-events-none"
        >
          {submitting ? "Saving…" : "Save"}
        </button>
      </div>
    </div>
  );
}

