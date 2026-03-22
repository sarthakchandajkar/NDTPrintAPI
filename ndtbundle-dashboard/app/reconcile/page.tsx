"use client";

import { useEffect, useState } from "react";
import { api, type ReconcileBundle, type ReconcileSlitItem } from "@/lib/api";

export default function ReconcilePage() {
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [selectedBatchNo, setSelectedBatchNo] = useState("");
  const [slits, setSlits] = useState<ReconcileSlitItem[]>([]);
  const [slitsLoading, setSlitsLoading] = useState(false);
  const [selectedSlitNo, setSelectedSlitNo] = useState("");
  const [newSlitNdtPipes, setNewSlitNdtPipes] = useState(0);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [savingSlit, setSavingSlit] = useState(false);
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
    if (!selectedBatchNo) {
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      return;
    }

    setSlitsLoading(true);
    api
      .reconcileBundleSlits(selectedBatchNo)
      .then((res) => {
        setSlits(Array.isArray(res?.slits) ? res.slits : []);
        const firstSlit = (Array.isArray(res?.slits) ? res.slits : []).find((s) => (s.slitNo ?? "").trim() !== "");
        if (firstSlit?.slitNo) {
          setSelectedSlitNo(firstSlit.slitNo);
          setNewSlitNdtPipes(typeof firstSlit.ndtPipes === "number" ? firstSlit.ndtPipes : 0);
        } else {
          setSelectedSlitNo("");
          setNewSlitNdtPipes(0);
        }
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : "Failed to load slit details");
        setSlits([]);
      })
      .finally(() => setSlitsLoading(false));
  }, [selectedBatchNo, bundles]);

  const reconcileBundleTotal = async (newTotal: number) => {
    if (!selectedBatchNo.trim()) {
      setError("Select a bundle.");
      return;
    }
    setSubmitting(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.reconcile(selectedBatchNo.trim(), newTotal);
      setSuccess(res.message ?? "Bundle reconciled. CSV(s) updated and tag reprinted.");
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reconcile failed.");
    } finally {
      setSubmitting(false);
    }
  };

  const saveSlit = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Select a bundle.");
      return;
    }
    if (!selectedSlitNo.trim()) {
      setError("Select a slit.");
      return;
    }
    if (newSlitNdtPipes < 0) {
      setError("NDT pipes must be non-negative.");
      return;
    }

    setSavingSlit(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.reconcileSlit(selectedBatchNo.trim(), selectedSlitNo.trim(), newSlitNdtPipes);
      setSuccess(res.message ?? "Slit reconciled. CSV updated.");
      setSlits(Array.isArray(res?.slits) ? res.slits : slits);
      await refresh();
      // reload slit list to ensure UI matches backend recomputation
      setSlitsLoading(true);
      const details = await api.reconcileBundleSlits(selectedBatchNo.trim());
      setSlits(Array.isArray(details?.slits) ? details.slits : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Slit reconcile failed.");
    } finally {
      setSlitsLoading(false);
      setSavingSlit(false);
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

  const selectedBundle = bundles.find((b) => b.bundleNo === selectedBatchNo) ?? null;
  const computedTotal = slits.reduce((sum, s) => sum + (typeof s.ndtPipes === "number" ? s.ndtPipes : 0), 0);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Reconcile Bundle</h1>
      <p className="text-gray-600 text-sm">
        Select an NDT Batch No. Review bundle totals on the left and slit totals on the right. Use &quot;Edit slit&quot;
        to correct a specific slit&apos;s NDT pipe count; the bundle total will be recomputed from the slit CSV rows.
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

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 max-w-3xl space-y-4">
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
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200 flex items-center justify-between">
            <span>Bundle Details</span>
            <button
              type="button"
              onClick={() => refresh()}
              className="text-sm font-medium text-primary-700 hover:text-primary-800"
            >
              Refresh
            </button>
          </div>
          <div className="p-5 space-y-4">
            {!selectedBundle ? (
              <p className="text-sm text-gray-500">Select a bundle to view details.</p>
            ) : (
              <>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <div className="text-gray-500">Bundle No</div>
                    <div className="font-medium text-gray-900">{selectedBundle.bundleNo}</div>
                  </div>
                  <div>
                    <div className="text-gray-500">PO Number</div>
                    <div className="font-medium text-gray-900">{selectedBundle.poNumber ?? "—"}</div>
                  </div>
                  <div>
                    <div className="text-gray-500">Mill</div>
                    <div className="font-medium text-gray-900">{selectedBundle.millNo ?? "—"}</div>
                  </div>
                  <div>
                    <div className="text-gray-500">Total NDT Pipes</div>
                    <div className="font-semibold text-gray-900">{computedTotal}</div>
                    <div className="text-xs text-gray-500">Computed from slit rows</div>
                  </div>
                </div>

                <div className="flex flex-wrap gap-3 pt-2">
                  <button
                    type="button"
                    onClick={() => reconcileBundleTotal(computedTotal)}
                    disabled={submitting || !selectedBatchNo.trim()}
                    className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50 disabled:pointer-events-none"
                  >
                    {submitting ? "Saving…" : "Save bundle total"}
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
              </>
            )}
          </div>
        </section>

        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
            Slit Details
          </div>
          <div className="p-5 space-y-4">
            {!selectedBatchNo ? (
              <p className="text-sm text-gray-500">Select a bundle to view its slits.</p>
            ) : slitsLoading ? (
              <p className="text-sm text-gray-500">Loading slits…</p>
            ) : slits.length === 0 ? (
              <p className="text-sm text-gray-500">No slit rows found for this bundle.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs">Slit No</th>
                      <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs">NDT Pipes</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {slits.map((s, idx) => (
                      <tr
                        key={`${s.slitNo ?? "—"}-${idx}`}
                        className={`hover:bg-gray-50 cursor-pointer ${selectedSlitNo === s.slitNo ? "bg-primary-50" : ""}`}
                        onClick={() => {
                          const slitNo = s.slitNo ?? "";
                          setSelectedSlitNo(slitNo);
                          setNewSlitNdtPipes(typeof s.ndtPipes === "number" ? s.ndtPipes : 0);
                        }}
                      >
                        <td className="px-3 py-2 text-gray-900 font-medium">{s.slitNo ?? "—"}</td>
                        <td className="px-3 py-2 text-gray-700">{s.ndtPipes ?? 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div className="border-t border-gray-200 pt-4 space-y-3">
              <div className="text-sm font-semibold text-gray-900">Edit slit</div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Slit No</label>
                  <select
                    value={selectedSlitNo}
                    onChange={(e) => {
                      const slitNo = e.target.value;
                      setSelectedSlitNo(slitNo);
                      const current = slits.find((x) => (x.slitNo ?? "") === slitNo);
                      setNewSlitNdtPipes(typeof current?.ndtPipes === "number" ? current.ndtPipes : 0);
                    }}
                    disabled={!selectedBatchNo.trim() || slits.length === 0}
                    className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                  >
                    <option value="">— Select slit —</option>
                    {slits.map((s, idx) => (
                      <option key={`${s.slitNo ?? "—"}-${idx}`} value={s.slitNo ?? ""}>
                        {s.slitNo ?? "—"}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">NDT Pipes</label>
                  <input
                    type="number"
                    min={0}
                    value={newSlitNdtPipes}
                    onChange={(e) => setNewSlitNdtPipes(parseInt(e.target.value, 10) || 0)}
                    disabled={!selectedSlitNo.trim()}
                    className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                  />
                </div>
              </div>
              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={saveSlit}
                  disabled={savingSlit || !selectedBatchNo.trim() || !selectedSlitNo.trim()}
                  className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50 disabled:pointer-events-none"
                >
                  {savingSlit ? "Saving…" : "Save slit"}
                </button>
              </div>
            </div>
          </div>
        </section>
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
