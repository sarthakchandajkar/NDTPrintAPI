"use client";

import { useEffect, useState } from "react";
import { api, type ReconcileBundle } from "@/lib/api";

export default function ReconcilePage() {
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [selectedBatchNo, setSelectedBatchNo] = useState("");
  const [newNdtPipes, setNewNdtPipes] = useState(0);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [printing, setPrinting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [printMessage, setPrintMessage] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.reconcileBundles();
      setBundles(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load bundles");
      setBundles([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    const b = bundles.find((x) => x.bundleNo === selectedBatchNo);
    if (b != null && typeof b.totalNdtPcs === "number") setNewNdtPipes(b.totalNdtPcs);
  }, [selectedBatchNo, bundles]);

  const submit = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Select a bundle.");
      return;
    }
    setSubmitting(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.reconcile(selectedBatchNo.trim(), newNdtPipes);
      setSuccess(res.message ?? "Bundle reconciled. CSV(s) updated and tag reprinted.");
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reconcile failed.");
    } finally {
      setSubmitting(false);
    }
  };

  const handlePrintBundle = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Select a bundle to print.");
      return;
    }
    setPrinting(true);
    setError(null);
    setPrintMessage(null);
    try {
      const res = await api.printReconciledBundle(selectedBatchNo.trim());
      setPrintMessage(res.message ?? "Bundle tag sent to printer.");
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Print failed.";
      setError(msg);
    } finally {
      setPrinting(false);
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Reconcile Bundle</h1>
      <p className="text-gray-600 text-sm">
        When the actual NDT pipe count in the plant does not match the application, select the NDT Batch No. and enter
        the correct count, then click Reconcile Bundle. Use &quot;Print bundle&quot; to print the selected bundle with its
        current (reconciled) count.
      </p>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {error}
        </div>
      )}
      {success && (
        <div className="rounded-md bg-green-50 border border-green-200 p-4 text-green-800 text-sm">
          {success}
        </div>
      )}
      {printMessage && (
        <div className="rounded-md bg-green-50 border border-green-200 p-4 text-green-800 text-sm">
          {printMessage}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 max-w-xl space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">NDT Batch No.</label>
          <select
            value={selectedBatchNo}
            onChange={(e) => setSelectedBatchNo(e.target.value)}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          >
            <option value="">— Select bundle —</option>
            {bundles.map((b) => (
              <option key={b.bundleNo} value={b.bundleNo}>
                {b.bundleNo} (PO: {b.poNumber}, Mill: {b.millNo}, current: {b.totalNdtPcs} pcs)
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">New NDT Pipes (correct count)</label>
          <input
            type="number"
            min={0}
            value={newNdtPipes}
            onChange={(e) => setNewNdtPipes(parseInt(e.target.value, 10) || 0)}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          />
        </div>
        <div className="flex flex-wrap gap-3">
          <button
            onClick={submit}
            disabled={submitting || !selectedBatchNo.trim()}
            className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50 disabled:pointer-events-none"
          >
            {submitting ? "Reconciling…" : "Reconcile Bundle"}
          </button>
          <button
            type="button"
            onClick={handlePrintBundle}
            disabled={printing || !selectedBatchNo.trim()}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:pointer-events-none"
          >
            {printing ? "Printing…" : "Print bundle"}
          </button>
        </div>
      </div>

      <button
        onClick={refresh}
        className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
      >
        Refresh bundle list
      </button>
    </div>
  );
}
