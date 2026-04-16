"use client";

import { useEffect, useState } from "react";
import { api, type ReconcileBundle, type ReconcileSlitItem } from "@/lib/api";

type ReconcileMode = "OutputBundle" | "Visual" | "Hydrotesting" | "Revisual";
type HydroType = "FourHeadHydrotesting" | "BigHydrotesting";

export default function ReconcilePage() {
  const [mode, setMode] = useState<ReconcileMode>("OutputBundle");
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [selectedBatchNo, setSelectedBatchNo] = useState("");
  const [slits, setSlits] = useState<ReconcileSlitItem[]>([]);
  const [slitsLoading, setSlitsLoading] = useState(false);
  const [selectedSlitNo, setSelectedSlitNo] = useState("");
  const [newSlitNdtPipes, setNewSlitNdtPipes] = useState(0);
  const [slitsMarkedForDelete, setSlitsMarkedForDelete] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [savingSlit, setSavingSlit] = useState(false);
  const [deletingSlits, setDeletingSlits] = useState(false);
  const [printing, setPrinting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [printMessage, setPrintMessage] = useState<string | null>(null);

  // Station reconcile (Visual / Hydro / Revisual)
  const [stationBatchNo, setStationBatchNo] = useState("");
  const [hydroType, setHydroType] = useState<HydroType>("FourHeadHydrotesting");
  const [stationPoNumber, setStationPoNumber] = useState<string | null>(null);
  const [stationMillNo, setStationMillNo] = useState<number | null>(null);
  const [stationIncoming, setStationIncoming] = useState<number | null>(null);
  const [stationOk, setStationOk] = useState(0);
  const [stationReject, setStationReject] = useState(0);
  const [stationPrintTag, setStationPrintTag] = useState(true);
  const [stationLoading, setStationLoading] = useState(false);
  const [stationReconciling, setStationReconciling] = useState(false);
  /** Visual / Revisual physical station (1 or 2); used for API and CSV naming. */
  const [stationOperatorNumber, setStationOperatorNumber] = useState<1 | 2>(1);

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
    if (mode !== "OutputBundle") return;
    const b = bundles.find((x) => x.bundleNo === selectedBatchNo);
    if (!selectedBatchNo) {
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      setSlitsMarkedForDelete([]);
      return;
    }

    setSlitsMarkedForDelete([]);
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
  }, [selectedBatchNo, bundles, mode]);

  useEffect(() => {
    // Clear per-mode state when switching reconcile mode
    setError(null);
    setSuccess(null);
    setPrintMessage(null);

    if (mode !== "OutputBundle") {
      setSelectedBatchNo("");
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      setSlitsMarkedForDelete([]);
    } else {
      setStationBatchNo("");
      setStationPoNumber(null);
      setStationMillNo(null);
      setStationIncoming(null);
      setStationOk(0);
      setStationReject(0);
    }
  }, [mode]);

  const loadStationContext = async () => {
    setError(null);
    setSuccess(null);
    const batch = stationBatchNo.trim();
    if (!batch) {
      setError("Enter/scan NDT Batch No.");
      return;
    }

    const station =
      mode === "Visual" ? "Visual" : mode === "Revisual" ? "Revisual" : hydroType;
    const opNum = mode === "Visual" || mode === "Revisual" ? stationOperatorNumber : 1;

    setStationLoading(true);
    try {
      const ctx = await api.manualStationContext(station, batch, opNum);
      setStationPoNumber(ctx.poNumber ?? null);
      setStationMillNo(typeof ctx.millNo === "number" ? ctx.millNo : null);
      setStationIncoming(typeof ctx.incomingPcs === "number" ? ctx.incomingPcs : null);
      setStationOk(typeof ctx.alreadyOkPcs === "number" ? ctx.alreadyOkPcs : 0);
      setStationReject(typeof ctx.alreadyRejectedPcs === "number" ? ctx.alreadyRejectedPcs : 0);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load station context.");
      setStationPoNumber(null);
      setStationMillNo(null);
      setStationIncoming(null);
    } finally {
      setStationLoading(false);
    }
  };

  const reconcileStation = async () => {
    setError(null);
    setSuccess(null);
    const batch = stationBatchNo.trim();
    if (!batch) {
      setError("Enter/scan NDT Batch No.");
      return;
    }
    if (stationIncoming == null) {
      setError("Load context first.");
      return;
    }
    if (stationOk < 0 || stationReject < 0) {
      setError("OK/Reject must be non-negative.");
      return;
    }
    if (stationOk + stationReject !== stationIncoming) {
      setError(`OK (${stationOk}) + Reject (${stationReject}) must equal Incoming (${stationIncoming}).`);
      return;
    }

    const station =
      mode === "Visual" ? "Visual" : mode === "Revisual" ? "Revisual" : hydroType;
    const opNum = mode === "Visual" || mode === "Revisual" ? stationOperatorNumber : 1;

    setStationReconciling(true);
    try {
      const res = await api.manualStationReconcile(station, {
        ndtBatchNo: batch,
        okPcs: stationOk,
        rejectedPcs: stationReject,
        printTag: stationPrintTag,
        operatorStationNumber: opNum,
      });
      setSuccess(
        `${res.message ?? "Reconciled."}${res.csvPath ? ` | CSV: ${res.csvPath}` : ""}${res.printed ? " | Tag sent to printer." : ""}`
      );
      await loadStationContext();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reconcile failed.");
    } finally {
      setStationReconciling(false);
    }
  };

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

  const slitValueForApi = (s: ReconcileSlitItem) => {
    const n = (s.slitNo ?? "").trim();
    return n === "" ? "—" : n;
  };

  const toggleSlitDeleteMark = (s: ReconcileSlitItem) => {
    const v = slitValueForApi(s);
    setSlitsMarkedForDelete((prev) => (prev.includes(v) ? prev.filter((k) => k !== v) : [...prev, v]));
  };

  const deleteMarkedSlits = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Select a bundle.");
      return;
    }
    const slitNos = [...slitsMarkedForDelete];
    if (slitNos.length === 0) {
      setError("Select at least one slit to delete.");
      return;
    }
    if (
      !window.confirm(
        `Remove ${slitNos.length} slit row(s) from output CSV(s), delete matching Output_Slit_Row rows when configured (Input_Slit_Row is not changed), and update the bundle total? This cannot be undone.`
      )
    ) {
      return;
    }

    setDeletingSlits(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.reconcileDeleteSlits(selectedBatchNo.trim(), slitNos);
      setSuccess(
        res.message ??
          `Removed ${typeof res.rowsRemoved === "number" ? res.rowsRemoved : slitNos.length} row(s). New bundle total: ${res.newBundleTotalNdtPcs ?? "—"}.`
      );
      setSlitsMarkedForDelete([]);
      setSlitsLoading(true);
      const details = await api.reconcileBundleSlits(selectedBatchNo.trim());
      setSlits(Array.isArray(details?.slits) ? details.slits : []);
      const firstSlit = (Array.isArray(details?.slits) ? details.slits : []).find((x) => (x.slitNo ?? "").trim() !== "");
      if (firstSlit?.slitNo) {
        setSelectedSlitNo(firstSlit.slitNo);
        setNewSlitNdtPipes(typeof firstSlit.ndtPipes === "number" ? firstSlit.ndtPipes : 0);
      } else {
        setSelectedSlitNo("");
        setNewSlitNdtPipes(0);
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete slits failed.");
    } finally {
      setSlitsLoading(false);
      setDeletingSlits(false);
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
        Use this screen to reconcile output bundle totals (existing workflow) or reconcile station outputs (Visual /
        Hydrotesting / Revisual) for an NDT batch.
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
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Reconcile type</label>
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value as ReconcileMode)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
            >
              <option value="OutputBundle">Output NDT Batch No. (current)</option>
              <option value="Visual">Visual</option>
              <option value="Hydrotesting">Hydrotesting</option>
              <option value="Revisual">Revisual</option>
            </select>
          </div>

          {mode === "OutputBundle" ? (
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
          ) : (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">NDT Batch No.</label>
              <input
                value={stationBatchNo}
                onChange={(e) => setStationBatchNo(e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
                placeholder="Scan/enter batch no (QR)"
              />
              <div className="pt-2">
                <button
                  type="button"
                  onClick={loadStationContext}
                  disabled={stationLoading || !stationBatchNo.trim()}
                  className="text-sm font-medium text-primary-700 hover:text-primary-800 disabled:opacity-50"
                >
                  {stationLoading ? "Loading…" : "Load context"}
                </button>
              </div>
            </div>
          )}
        </div>

        {mode === "Hydrotesting" && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Hydrotesting type</label>
            <select
              value={hydroType}
              onChange={(e) => setHydroType(e.target.value as HydroType)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
            >
              <option value="FourHeadHydrotesting">Four Head Hydrotesting</option>
              <option value="BigHydrotesting">Big Hydrotesting</option>
            </select>
          </div>
        )}

        {(mode === "Visual" || mode === "Revisual") && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              {mode === "Visual" ? "Visual" : "Revisual"} station (for reconcile CSV / tag)
            </label>
            <select
              value={stationOperatorNumber}
              onChange={(e) => setStationOperatorNumber(Number(e.target.value) as 1 | 2)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 max-w-xs"
            >
              <option value={1}>Station 1</option>
              <option value={2}>Station 2</option>
            </select>
          </div>
        )}

        {mode !== "OutputBundle" && (
          <div
            className={`grid grid-cols-1 gap-3 pt-2 ${mode === "Visual" || mode === "Revisual" ? "lg:grid-cols-4" : "lg:grid-cols-3"}`}
          >
            {(mode === "Visual" || mode === "Revisual") && (
              <div className="rounded-md border border-gray-200 px-3 py-2 text-sm bg-gray-50">
                <div className="text-gray-500">{mode === "Visual" ? "Visual" : "Revisual"} station no.</div>
                <div className="font-semibold text-gray-900 tabular-nums">{stationOperatorNumber}</div>
              </div>
            )}
            <div className="rounded-md border border-gray-200 px-3 py-2 text-sm">
              <div className="text-gray-500">PO Number</div>
              <div className="font-medium text-gray-900">{stationPoNumber ?? "—"}</div>
            </div>
            <div className="rounded-md border border-gray-200 px-3 py-2 text-sm">
              <div className="text-gray-500">Mill No</div>
              <div className="font-medium text-gray-900">{stationMillNo ?? "—"}</div>
            </div>
            <div className="rounded-md border border-gray-200 px-3 py-2 text-sm">
              <div className="text-gray-500">Incoming Pcs</div>
              <div className="font-semibold text-gray-900">{stationIncoming ?? "—"}</div>
            </div>
          </div>
        )}

        {mode !== "OutputBundle" && (
          <>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">OK pcs</label>
                <input
                  type="number"
                  min={0}
                  value={stationOk}
                  onChange={(e) => setStationOk(parseInt(e.target.value, 10) || 0)}
                  disabled={stationIncoming == null}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Reject pcs</label>
                <input
                  type="number"
                  min={0}
                  value={stationReject}
                  onChange={(e) => setStationReject(parseInt(e.target.value, 10) || 0)}
                  disabled={stationIncoming == null}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                />
              </div>
            </div>

            <label className="inline-flex items-center gap-2 text-sm text-gray-700">
              <input
                type="checkbox"
                checked={stationPrintTag}
                onChange={(e) => setStationPrintTag(e.target.checked)}
                className="h-4 w-4"
              />
              Reprint tag after reconcile
            </label>

            <div className="flex flex-wrap gap-3">
              <button
                type="button"
                onClick={reconcileStation}
                disabled={stationReconciling || stationIncoming == null}
                className="px-4 py-2 bg-amber-600 text-white text-sm font-medium rounded-md hover:bg-amber-700 disabled:opacity-50 disabled:pointer-events-none"
              >
                {stationReconciling ? "Reconciling…" : "Reconcile station output"}
              </button>
            </div>
          </>
        )}
      </div>

      {mode === "OutputBundle" && (
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
                      <th className="px-2 py-2 w-10 text-left font-medium text-gray-500 uppercase text-xs" scope="col">
                        Del
                      </th>
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
                        <td className="px-2 py-2 align-middle" onClick={(e) => e.stopPropagation()}>
                          <input
                            type="checkbox"
                            className="h-4 w-4 rounded border-gray-300"
                            checked={slitsMarkedForDelete.includes(slitValueForApi(s))}
                            onChange={() => toggleSlitDeleteMark(s)}
                            aria-label={`Mark slit ${slitValueForApi(s)} for deletion`}
                          />
                        </td>
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
                <button
                  type="button"
                  onClick={deleteMarkedSlits}
                  disabled={
                    deletingSlits || !selectedBatchNo.trim() || slits.length === 0 || slitsMarkedForDelete.length === 0
                  }
                  className="px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-md hover:bg-red-700 disabled:opacity-50 disabled:pointer-events-none"
                >
                  {deletingSlits ? "Deleting…" : `Delete selected slit${slitsMarkedForDelete.length === 1 ? "" : "s"}`}
                </button>
              </div>
            </div>
          </div>
        </section>
      </div>
      )}

      {mode === "OutputBundle" && (
        <button
          onClick={refresh}
          className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          Refresh bundle list
        </button>
      )}
    </div>
  );
}
