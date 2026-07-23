"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type ReconcileBundle, type ReconcileSlitItem } from "@/lib/api";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { MillFilter } from "@/components/MillFilter";
import {
  filterBundlesByDateRange,
  filterBundlesByMill,
  type MillFilterValue,
} from "@/lib/millFilter";
import {
  EMPTY_DATE_RANGE,
  formatDisplayDate,
  isDateRangeActive,
  type DateRange,
} from "@/lib/dateRangeFilter";

type ReconcileMode = "OutputBundle" | "Visual" | "Hydrotesting" | "Revisual";
type HydroType = "FourHeadHydrotesting" | "BigHydrotesting";

export default function ReconcilePage() {
  const [mode, setMode] = useState<ReconcileMode>("OutputBundle");
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [millFilter, setMillFilter] = useState<MillFilterValue>("all");
  const [dateRange, setDateRange] = useState<DateRange>(EMPTY_DATE_RANGE);
  const [selectedBatchNo, setSelectedBatchNo] = useState("");

  const filteredBundles = useMemo(() => {
    const byMill = filterBundlesByMill(bundles, millFilter);
    return filterBundlesByDateRange(byMill, dateRange);
  }, [bundles, millFilter, dateRange]);
  const [slits, setSlits] = useState<ReconcileSlitItem[]>([]);
  const [slitsLoading, setSlitsLoading] = useState(false);
  const [selectedSlitNo, setSelectedSlitNo] = useState("");
  const [newSlitNdtPipes, setNewSlitNdtPipes] = useState(0);
  const [slitsMarkedForDelete, setSlitsMarkedForDelete] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [savingSlit, setSavingSlit] = useState(false);
  const [deletingSlits, setDeletingSlits] = useState(false);
  const [manualReconciling, setManualReconciling] = useState(false);
  const [correctedTotal, setCorrectedTotal] = useState(0);
  const [reconcileReason, setReconcileReason] = useState("");
  const [reconciledBy, setReconciledBy] = useState("");
  const [bundleManualRecon, setBundleManualRecon] = useState(false);
  const [bundleManualReconReason, setBundleManualReconReason] = useState<string | null>(null);
  const [bundlePostReconCsvSum, setBundlePostReconCsvSum] = useState<number | null>(null);
  const [reconcileEnabled, setReconcileEnabled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

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
      const next = Array.isArray(list) ? list : [];
      setBundles(next);
      if (mode === "OutputBundle") {
        const filtered = filterBundlesByDateRange(filterBundlesByMill(next, millFilter), dateRange);
        setSelectedBatchNo((prev) => {
          if (!filtered.length) return "";
          if (prev && filtered.some((b) => b.bundleNo === prev)) return prev;
          return filtered[0]?.bundleNo ?? "";
        });
      }
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
    setSelectedBatchNo((prev) => {
      if (!filteredBundles.length) return "";
      if (prev && filteredBundles.some((b) => b.bundleNo === prev)) return prev;
      return filteredBundles[0]?.bundleNo ?? "";
    });
  }, [millFilter, filteredBundles, mode, dateRange]);

  useEffect(() => {
    if (mode !== "OutputBundle") return;
    const b = bundles.find((x) => x.bundleNo === selectedBatchNo);
    if (!selectedBatchNo) {
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      setSlitsMarkedForDelete([]);
      setCorrectedTotal(0);
      setBundleManualRecon(false);
      setBundleManualReconReason(null);
      setBundlePostReconCsvSum(null);
      return;
    }

    setSlitsMarkedForDelete([]);
    setSlitsLoading(true);
    api
      .reconcileBundleSlits(selectedBatchNo)
      .then((res) => {
        const bundleTotal =
          typeof res?.bundle?.totalNdtPcs === "number" && res.bundle.totalNdtPcs > 0
            ? res.bundle.totalNdtPcs
            : 0;
        setCorrectedTotal(bundleTotal);
        setBundleManualRecon(Boolean(res?.bundle?.manualRecon));
        setBundleManualReconReason(res?.bundle?.manualReconReason ?? null);
        setBundlePostReconCsvSum(
          typeof res?.bundle?.postReconCsvSum === "number" ? res.bundle.postReconCsvSum : null
        );
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

  const manualBundleReconcile = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Enter or select an NDT Batch No.");
      return;
    }
    if (correctedTotal < 0) {
      setError("Corrected total must be non-negative.");
      return;
    }
    if (!reconcileReason.trim()) {
      setError("Reason is required.");
      return;
    }
    if (!reconciledBy.trim()) {
      setError("Your name/ID is required (Reconciled by).");
      return;
    }

    setManualReconciling(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.manualBundleReconcile(
        selectedBatchNo.trim(),
        correctedTotal,
        reconcileReason.trim(),
        reconciledBy.trim()
      );
      const printNote = res.printSuccess ? " Tag reprinted." : res.printMessage ? ` Print: ${res.printMessage}` : "";
      setSuccess((res.message ?? "Bundle manually reconciled.") + printNote);
      await refresh();
      setSlitsLoading(true);
      const details = await api.reconcileBundleSlits(selectedBatchNo.trim());
      setSlits(Array.isArray(details?.slits) ? details.slits : []);
      setBundleManualRecon(Boolean(details?.bundle?.manualRecon));
      setBundleManualReconReason(details?.bundle?.manualReconReason ?? null);
      setBundlePostReconCsvSum(
        typeof details?.bundle?.postReconCsvSum === "number" ? details.bundle.postReconCsvSum : null
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "Manual bundle reconcile failed.");
    } finally {
      setManualReconciling(false);
      setSlitsLoading(false);
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
    if (slits.length === 0) {
      setError("No slit rows loaded for this bundle.");
      return;
    }
    const slitForApi = slitValueForApi({ slitNo: selectedSlitNo });
    if (newSlitNdtPipes < 0) {
      setError("NDT pipes must be non-negative.");
      return;
    }

    setSavingSlit(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.reconcileSlit(selectedBatchNo.trim(), slitForApi, newSlitNdtPipes);
      setSuccess(res.message ?? res.warning ?? "Slit reconciled. CSV updated.");
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

  const selectedBundle = bundles.find((b) => b.bundleNo === selectedBatchNo) ?? null;
  const computedTotal = slits.reduce((sum, s) => sum + (typeof s.ndtPipes === "number" ? s.ndtPipes : 0), 0);
  const reconcileModeLabel = reconcileEnabled ? "ON" : "OFF";

  const toggleReconcileEnabled = () => {
    const next = !reconcileEnabled;
    const confirmed = window.confirm(
      next
        ? "Are you sure you want to turn reconcile ON?"
        : "Are you sure you want to turn reconcile OFF?"
    );
    if (!confirmed) return;
    setReconcileEnabled(next);
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Reconcile Bundle</h1>
      <p className="text-gray-600 text-sm">
        Correct a bundle&apos;s total pipe count and reprint its tag in one step — no slit rows required.
        Slit edit/delete below is for traceability on unlocked bundles only.
      </p>

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4 max-w-3xl flex items-center justify-between">
        <div>
          <div className="text-sm font-medium text-gray-700">Reconcile</div>
          <div className={`text-sm font-semibold ${reconcileEnabled ? "text-green-700" : "text-red-700"}`}>
            {reconcileModeLabel}
          </div>
        </div>
        <button
          type="button"
          onClick={toggleReconcileEnabled}
          className={`px-4 py-2 text-sm font-medium rounded-md text-white ${
            reconcileEnabled ? "bg-red-600 hover:bg-red-700" : "bg-green-600 hover:bg-green-700"
          }`}
        >
          Turn Reconcile {reconcileEnabled ? "OFF" : "ON"}
        </button>
      </div>

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
            <div className="sm:col-span-1 rounded-md border border-gray-200 px-3 py-2 bg-gray-50">
              <div className="text-sm text-gray-500">Bundle list</div>
              <div className="text-sm font-medium text-gray-900">
                {filteredBundles.length} shown
                {(millFilter !== "all" || isDateRangeActive(dateRange)) &&
                  ` · ${bundles.length} total`}
              </div>
              <div className="text-xs text-gray-500 pt-1">Select from the left panel below.</div>
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
                disabled={stationReconciling || stationIncoming == null || !reconcileEnabled}
                className="px-4 py-2 bg-amber-600 text-white text-sm font-medium rounded-md hover:bg-amber-700 disabled:opacity-50 disabled:pointer-events-none"
              >
                {stationReconciling ? "Reconciling…" : "Reconcile station output"}
              </button>
            </div>
          </>
        )}
      </div>

      {mode === "OutputBundle" && (
        <>
        <MillFilter
          value={millFilter}
          onChange={setMillFilter}
          bundles={bundles}
          className="bg-white rounded-lg border border-gray-200 shadow-sm p-4"
        />
        <DateRangeFilter
          value={dateRange}
          onChange={setDateRange}
          summary={`${filteredBundles.length} of ${bundles.length} bundle(s)`}
          hint="Uses Slit Finish Time, then Slit Start Time, then tag print time (PrintedAt). Bundles without any date are hidden when a date range is set."
        />
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden lg:col-span-2">
          <div className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
            Bundle total reconcile &amp; reprint
          </div>
          <div className="p-5 space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="sm:col-span-2">
                <label className="block text-sm font-medium text-gray-700 mb-1">NDT Batch No.</label>
                <input
                  value={selectedBatchNo}
                  onChange={(e) => setSelectedBatchNo(e.target.value)}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
                  placeholder="Scan or type batch number (any age)"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Current total (NDT pipes)</label>
                <div className="border border-gray-200 rounded-md px-3 py-2 text-sm bg-gray-50 font-semibold tabular-nums">
                  {selectedBundle?.totalNdtPcs != null && selectedBundle.totalNdtPcs > 0
                    ? selectedBundle.totalNdtPcs
                    : slitsLoading
                      ? "…"
                      : computedTotal > 0
                        ? computedTotal
                        : "—"}
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Corrected total (NDT pipes)</label>
                <input
                  type="number"
                  min={0}
                  value={correctedTotal}
                  onChange={(e) => setCorrectedTotal(parseInt(e.target.value, 10) || 0)}
                  disabled={!selectedBatchNo.trim()}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                />
              </div>
              <div className="sm:col-span-2">
                <label className="block text-sm font-medium text-gray-700 mb-1">Reason (required)</label>
                <input
                  value={reconcileReason}
                  onChange={(e) => setReconcileReason(e.target.value)}
                  disabled={!selectedBatchNo.trim()}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                  placeholder="e.g. Damaged label; count verified on floor"
                />
              </div>
              <div className="sm:col-span-2">
                <label className="block text-sm font-medium text-gray-700 mb-1">Reconciled by (required)</label>
                <input
                  value={reconciledBy}
                  onChange={(e) => setReconciledBy(e.target.value)}
                  disabled={!selectedBatchNo.trim()}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50 max-w-md"
                  placeholder="Operator name or ID"
                />
              </div>
            </div>

            {bundleManualRecon && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                <span className="font-semibold">Manually reconciled (locked).</span>
                {bundleManualReconReason ? ` Reason: ${bundleManualReconReason}.` : ""}
                {bundlePostReconCsvSum != null
                  ? ` Post-recon CSV slit sum: ${bundlePostReconCsvSum}.`
                  : ""}
              </div>
            )}

            <button
              type="button"
              onClick={manualBundleReconcile}
              disabled={manualReconciling || !selectedBatchNo.trim() || !reconcileEnabled}
              className="px-4 py-2 bg-primary-600 text-white text-sm font-medium rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:pointer-events-none"
            >
              {manualReconciling ? "Reconciling & printing…" : "Reconcile & Reprint"}
            </button>
          </div>
        </section>

        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200 flex items-center justify-between">
            <span>
              Bundle picker
              {!loading && bundles.length > 0 && (millFilter !== "all" || isDateRangeActive(dateRange)) && (
                <span className="ml-2 text-sm font-normal text-gray-600">
                  ({filteredBundles.length} of {bundles.length})
                </span>
              )}
            </span>
            <button
              type="button"
              onClick={() => refresh()}
              className="text-sm font-medium text-primary-700 hover:text-primary-800"
            >
              Refresh
            </button>
          </div>
          <div className="p-5 space-y-4">
            {loading ? (
              <p className="text-sm text-gray-500">Loading bundles…</p>
            ) : filteredBundles.length === 0 ? (
              <p className="text-sm text-gray-500">
                {bundles.length === 0
                  ? "No bundles found."
                  : "No bundles match the selected mill and date range. Try clearing filters or widening the dates."}
              </p>
            ) : (
              <div className="max-h-[560px] overflow-y-auto divide-y divide-gray-100 border border-gray-200 rounded-md">
                {filteredBundles.map((b) => {
                  const active = b.bundleNo === selectedBatchNo;
                  return (
                    <button
                      key={b.bundleNo}
                      type="button"
                      onClick={() => {
                        if (b.bundleNo) setSelectedBatchNo(b.bundleNo);
                      }}
                      className={`w-full text-left px-4 py-3 hover:bg-gray-50 ${
                        active ? "bg-primary-50 border-l-4 border-primary-500 pl-3" : ""
                      }`}
                    >
                      <div className="font-semibold text-gray-900">{b.bundleNo}</div>
                      <div className="text-xs text-gray-600 pt-1">
                        PO: {b.poNumber ?? "—"} | Mill: {b.millNo ?? "—"} | Current: {b.totalNdtPcs ?? 0}
                      </div>
                      <div className="text-xs text-gray-400 pt-0.5">
                        {formatDisplayDate(b.slitFinishTime || b.slitStartTime || b.printedAt)}
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </section>

        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 bg-gray-100 text-gray-900 font-semibold border-b border-gray-200">
            Slit traceability (unlocked bundles only)
          </div>
          <div className="p-5 space-y-4">
            {bundleManualRecon ? (
              <p className="text-sm text-amber-800">
                This bundle is manually reconciled. Slit edit/delete is disabled; late CSV rows are recorded for audit
                only.
              </p>
            ) : !selectedBatchNo || !selectedBundle ? (
              <p className="text-sm text-gray-500">Select a bundle to view slit traceability rows.</p>
            ) : (
              <>
                <div className="grid grid-cols-2 gap-4 text-sm border border-gray-200 rounded-md p-3 bg-gray-50">
                  <div>
                    <div className="text-gray-500">Bundle No</div>
                    <div className="font-medium text-gray-900">{selectedBundle.bundleNo}</div>
                  </div>
                  <div>
                    <div className="text-gray-500">Slit sum</div>
                    <div className="font-semibold text-gray-900 tabular-nums">{computedTotal}</div>
                  </div>
                </div>

                {slitsLoading ? (
                  <p className="text-sm text-gray-500">Loading slits…</p>
                ) : slits.length === 0 ? (
                  <p className="text-sm text-gray-500">No slit rows yet — bundle reconcile above does not require them.</p>
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

                {!slitsLoading && slits.length > 0 && (
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
                        disabled={savingSlit || !selectedBatchNo.trim() || !selectedSlitNo.trim() || !reconcileEnabled}
                        className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50 disabled:pointer-events-none"
                      >
                        {savingSlit ? "Saving…" : "Save slit"}
                      </button>
                      <button
                        type="button"
                        onClick={deleteMarkedSlits}
                        disabled={
                          deletingSlits ||
                          !selectedBatchNo.trim() ||
                          slits.length === 0 ||
                          slitsMarkedForDelete.length === 0 ||
                          !reconcileEnabled
                        }
                        className="px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-md hover:bg-red-700 disabled:opacity-50 disabled:pointer-events-none"
                      >
                        {deletingSlits ? "Deleting…" : `Delete selected slit${slitsMarkedForDelete.length === 1 ? "" : "s"}`}
                      </button>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </section>
      </div>
        </>
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
