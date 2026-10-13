"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  getBundleSlitSumForCompare,
  isBundleOvershoot,
  isBundleSlitCountMismatch,
  stationPrintFollowUp,
  type BundleMergePreview,
  type PpcCorrectionItem,
  type ReconcileBundle,
  type ReconcileSlitItem,
  type ReprintPrinterTarget,
  type StationPrintedTag,
} from "@/lib/api";
import Link from "next/link";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { MillFilter } from "@/components/MillFilter";
import { ReprintPrinterChooser } from "@/components/ReprintPrinterChooser";
import {
  filterBundlesByDateRange,
  filterBundlesByMill,
  type MillFilterValue,
} from "@/lib/millFilter";
import {
  EMPTY_DATE_RANGE,
  formatDisplayDate,
  isDateRangeActive,
  isInDateRange,
  type DateRange,
} from "@/lib/dateRangeFilter";

type ReconcileMode = "OutputBundle" | "Visual" | "Hydrotesting" | "Revisual";
type HydroType = "FourHeadHydrotesting" | "BigHydrotesting";
type StationMode = Exclude<ReconcileMode, "OutputBundle">;

function isStationMode(mode: ReconcileMode): mode is StationMode {
  return mode === "Visual" || mode === "Hydrotesting" || mode === "Revisual";
}

function resolveStationMillNo(tag: StationPrintedTag): number {
  const m = tag.millNo;
  if (typeof m === "number" && m >= 1 && m <= 4) return m;
  const bn = (tag.ndtBatchNo ?? "").trim();
  if (bn.length >= 5 && bn.startsWith("12")) {
    const digit = parseInt(bn[4], 10);
    if (digit >= 1 && digit <= 4) return digit;
  }
  return 0;
}

function stationTagMatchesMode(tag: StationPrintedTag, mode: StationMode): boolean {
  const ws = (tag.workStation ?? "").trim().toLowerCase();
  if (mode === "Visual") return ws.startsWith("visual station");
  if (mode === "Revisual") return ws.startsWith("revisual station");
  // Hydrotesting (legacy) or Four Head / Big via hydrotestingType
  if (ws === "hydrotesting" || ws.includes("hydro")) return true;
  return Boolean((tag.hydrotestingType ?? "").trim());
}

function parseStationOperatorNumber(tag: StationPrintedTag): 1 | 2 | null {
  const ws = (tag.workStation ?? "").trim();
  const m = /(?:Visual|Revisual)\s+Station\s+([12])\b/i.exec(ws);
  if (!m) return null;
  return m[1] === "2" ? 2 : 1;
}

function hydroTypeFromTag(tag: StationPrintedTag): HydroType {
  const h = (tag.hydrotestingType ?? "").trim().toLowerCase();
  if (h.includes("big")) return "BigHydrotesting";
  return "FourHeadHydrotesting";
}

function filterStationTagsByMill(
  tags: StationPrintedTag[],
  mill: MillFilterValue
): StationPrintedTag[] {
  if (mill === "all") return tags;
  return tags.filter((t) => resolveStationMillNo(t) === mill);
}

function filterStationTagsByDateRange(
  tags: StationPrintedTag[],
  range: DateRange
): StationPrintedTag[] {
  if (!isDateRangeActive(range)) return tags;
  return tags.filter((t) =>
    isInDateRange(t.bundleEnd || t.importedAtUtc || t.bundleStart || null, range)
  );
}

/** Latest run per batch for the active station mode. */
function latestStationTagsByBatch(tags: StationPrintedTag[]): StationPrintedTag[] {
  const byBatch = new Map<string, StationPrintedTag>();
  for (const tag of tags) {
    const batch = (tag.ndtBatchNo ?? "").trim();
    if (!batch) continue;
    const prev = byBatch.get(batch);
    if (!prev) {
      byBatch.set(batch, tag);
      continue;
    }
    const prevT = Date.parse(prev.importedAtUtc || prev.bundleEnd || "") || 0;
    const nextT = Date.parse(tag.importedAtUtc || tag.bundleEnd || "") || 0;
    if (nextT >= prevT || (tag.id ?? 0) > (prev.id ?? 0)) byBatch.set(batch, tag);
  }
  return Array.from(byBatch.values()).sort((a, b) => {
    const at = Date.parse(a.importedAtUtc || a.bundleEnd || "") || 0;
    const bt = Date.parse(b.importedAtUtc || b.bundleEnd || "") || 0;
    return bt - at;
  });
}

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
  const [manualReconcileWithReprint, setManualReconcileWithReprint] = useState(false);
  const [correctedTotal, setCorrectedTotal] = useState(0);
  const [bundleManualRecon, setBundleManualRecon] = useState(false);
  const [bundleManualReconReason, setBundleManualReconReason] = useState<string | null>(null);
  const [bundleManualReview, setBundleManualReview] = useState(false);
  const [bundlePpcPending, setBundlePpcPending] = useState(false);
  const [ppcItems, setPpcItems] = useState<PpcCorrectionItem[]>([]);
  const [clearingPpcId, setClearingPpcId] = useState<number | null>(null);
  const [bundlePostReconCsvSum, setBundlePostReconCsvSum] = useState<number | null>(null);
  const [reconcileEnabled, setReconcileEnabled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [mergePreview, setMergePreview] = useState<BundleMergePreview | null>(null);
  const [mergeReason, setMergeReason] = useState("");
  const [mergeConfirm, setMergeConfirm] = useState(false);
  const [merging, setMerging] = useState(false);
  const [reprintChooserOpen, setReprintChooserOpen] = useState(false);

  // Station reconcile (Visual / Hydro / Revisual)
  const [stationBatchNo, setStationBatchNo] = useState("");
  const [stationSearchQ, setStationSearchQ] = useState("");
  const [stationTags, setStationTags] = useState<StationPrintedTag[]>([]);
  const [stationTagsLoading, setStationTagsLoading] = useState(false);
  const [hydroType, setHydroType] = useState<HydroType>("FourHeadHydrotesting");
  const [stationPoNumber, setStationPoNumber] = useState<string | null>(null);
  const [stationMillNo, setStationMillNo] = useState<number | null>(null);
  const [stationIncoming, setStationIncoming] = useState<number | null>(null);
  const [stationOk, setStationOk] = useState(0);
  const [stationReject, setStationReject] = useState(0);
  const [stationPrintTag, setStationPrintTag] = useState(true);
  const [stationLoading, setStationLoading] = useState(false);
  const [stationReconciling, setStationReconciling] = useState(false);
  /** When Revisual process CSV is already under Bundle Accepted — mail SAP (amber banner). */
  const [sapMailNotice, setSapMailNotice] = useState(false);
  /** Visual / Revisual physical station (1 or 2); used for API and CSV naming. */
  const [stationOperatorNumber, setStationOperatorNumber] = useState<1 | 2>(1);

  const stationBundlesForMillFilter = useMemo(
    () =>
      stationTags.map(
        (t) =>
          ({
            bundleNo: t.ndtBatchNo ?? "",
            millNo: typeof t.millNo === "number" ? t.millNo : resolveStationMillNo(t) || undefined,
          }) as ReconcileBundle
      ),
    [stationTags]
  );

  const filteredStationTags = useMemo(() => {
    if (!isStationMode(mode)) return [];
    const forMode = stationTags.filter((t) => stationTagMatchesMode(t, mode));
    const byMill = filterStationTagsByMill(forMode, millFilter);
    const byDate = filterStationTagsByDateRange(byMill, dateRange);
    const latest = latestStationTagsByBatch(byDate);
    const q = stationSearchQ.trim().toLowerCase();
    if (!q) return latest;
    return latest.filter((t) => {
      const batch = (t.ndtBatchNo ?? "").toLowerCase();
      const po = (t.poNumber ?? "").toLowerCase();
      return batch.includes(q) || po.includes(q);
    });
  }, [stationTags, mode, millFilter, dateRange, stationSearchQ]);

  const stationOkRejectValid =
    stationIncoming != null && stationOk >= 0 && stationReject >= 0 && stationOk + stationReject === stationIncoming;

  const refresh = async () => {
    setLoading(true);
    setStationTagsLoading(true);
    setError(null);
    try {
      const [list, stations] = await Promise.all([
        api.reconcileBundles(),
        api.stationPrintedTags().catch(() => [] as StationPrintedTag[]),
      ]);
      const next = Array.isArray(list) ? list : [];
      setBundles(next);
      setStationTags(Array.isArray(stations) ? stations : []);
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
      setStationTags([]);
    } finally {
      setLoading(false);
      setStationTagsLoading(false);
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
    if (!selectedBatchNo) {
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      setSlitsMarkedForDelete([]);
      setCorrectedTotal(0);
      setBundleManualRecon(false);
      setBundleManualReconReason(null);
      setBundleManualReview(false);
      setBundlePpcPending(false);
      setPpcItems([]);
      setBundlePostReconCsvSum(null);
      setMergePreview(null);
      setMergeConfirm(false);
      return;
    }

    // Ignore stale responses when selection changes or this effect re-runs (e.g. after refresh()).
    // Without this, an older empty/error response can overwrite a newer successful slits load.
    let cancelled = false;
    const batch = selectedBatchNo;

    setSlitsMarkedForDelete([]);
    setSlitsLoading(true);
    api
      .reconcileBundleSlits(batch)
      .then((res) => {
        if (cancelled) return;
        const bundleTotal =
          typeof res?.bundle?.totalNdtPcs === "number" && res.bundle.totalNdtPcs >= 0
            ? res.bundle.totalNdtPcs
            : 0;
        // Forming: prefer live slit sum when API still returns provisional target.
        const liveSlitSum = (Array.isArray(res?.slits) ? res.slits : []).reduce(
          (sum, s) => sum + (typeof s.ndtPipes === "number" ? s.ndtPipes : 0),
          0
        );
        const initCorrected =
          res?.bundle?.isForming && !res?.bundle?.manualRecon
            ? liveSlitSum
            : bundleTotal;
        setCorrectedTotal(initCorrected);
        setBundleManualRecon(Boolean(res?.bundle?.manualRecon));
        setBundleManualReconReason(res?.bundle?.manualReconReason ?? null);
        setBundleManualReview(Boolean(res?.bundle?.manualReview));
        const ppcPending = Boolean(res?.bundle?.ppcCorrectionPending);
        setBundlePpcPending(ppcPending);
        if (ppcPending) {
          api
            .reconcilePpcCorrections(batch)
            .then((r) => {
              if (!cancelled) setPpcItems(Array.isArray(r?.items) ? r.items : []);
            })
            .catch(() => {
              if (!cancelled) setPpcItems([]);
            });
        } else {
          setPpcItems([]);
        }
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
        if (cancelled) return;
        setError(e instanceof Error ? e.message : "Failed to load slit details");
        setSlits([]);
      })
      .finally(() => {
        if (!cancelled) setSlitsLoading(false);
      });

    setMergePreview(null);
    setMergeConfirm(false);
    api
      .mergePreview(batch)
      .then((p) => {
        if (!cancelled) setMergePreview(p?.targetBundleNo ? p : null);
      })
      .catch(() => {
        if (!cancelled) setMergePreview(null);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedBatchNo, mode]);

  useEffect(() => {
    // Clear per-mode state when switching reconcile mode
    setError(null);
    setSuccess(null);
    setSapMailNotice(false);

    if (mode !== "OutputBundle") {
      setSelectedBatchNo("");
      setSlits([]);
      setSelectedSlitNo("");
      setNewSlitNdtPipes(0);
      setSlitsMarkedForDelete([]);
      setStationBatchNo("");
      setStationSearchQ("");
      setStationPoNumber(null);
      setStationMillNo(null);
      setStationIncoming(null);
      setStationOk(0);
      setStationReject(0);
    } else {
      setStationBatchNo("");
      setStationSearchQ("");
      setStationPoNumber(null);
      setStationMillNo(null);
      setStationIncoming(null);
      setStationOk(0);
      setStationReject(0);
    }
  }, [mode]);

  const loadStationContextFor = async (
    batchRaw: string,
    options?: { operatorStationNumber?: 1 | 2; hydroType?: HydroType }
  ) => {
    setError(null);
    setSuccess(null);
    setSapMailNotice(false);
    const batch = batchRaw.trim();
    if (!batch) {
      setError("Enter/scan NDT Batch No.");
      return;
    }
    if (!isStationMode(mode)) return;

    const nextHydro = options?.hydroType ?? hydroType;
    const nextOp = options?.operatorStationNumber ?? stationOperatorNumber;
    if (options?.hydroType) setHydroType(options.hydroType);
    if (options?.operatorStationNumber) setStationOperatorNumber(options.operatorStationNumber);
    setStationBatchNo(batch);

    const station =
      mode === "Visual" ? "Visual" : mode === "Revisual" ? "Revisual" : nextHydro;
    const opNum = mode === "Visual" || mode === "Revisual" ? nextOp : 1;

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

  const selectStationTag = (tag: StationPrintedTag) => {
    const batch = (tag.ndtBatchNo ?? "").trim();
    if (!batch) return;
    const op = parseStationOperatorNumber(tag);
    const hydro = hydroTypeFromTag(tag);
    void loadStationContextFor(batch, {
      operatorStationNumber: op ?? undefined,
      hydroType: mode === "Hydrotesting" ? hydro : undefined,
    });
  };

  const applyStationSearch = () => {
    const q = stationSearchQ.trim();
    if (!q) return;
    const exact = filteredStationTags.find(
      (t) => (t.ndtBatchNo ?? "").trim().toLowerCase() === q.toLowerCase()
    );
    if (exact) {
      selectStationTag(exact);
      return;
    }
    const partial = filteredStationTags.find((t) =>
      (t.ndtBatchNo ?? "").toLowerCase().includes(q.toLowerCase())
    );
    if (partial) {
      selectStationTag(partial);
      return;
    }
    // Allow loading a batch not in the list (e.g. just scanned)
    void loadStationContextFor(q);
  };

  const reconcileStation = async () => {
    setError(null);
    setSuccess(null);
    setSapMailNotice(false);
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
      const follow = stationPrintFollowUp(stationPrintTag, res);
      const sentToSap = Boolean(res.processFileAlreadySentToSap);
      setSapMailNotice(sentToSap);
      setSuccess(
        `${res.message ?? "Reconciled."}${
          !sentToSap && res.csvPath ? ` | CSV: ${res.csvPath}` : ""
        }${follow.extraSuccess}`
      );
      if (follow.printError) setError(follow.printError);
      await loadStationContextFor(batch);
      try {
        const stations = await api.stationPrintedTags();
        setStationTags(Array.isArray(stations) ? stations : []);
      } catch {
        // list refresh is best-effort after reconcile
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reconcile failed.");
    } finally {
      setStationReconciling(false);
    }
  };

  const mergeIntoPrevious = async () => {
    if (!selectedBatchNo.trim()) {
      setError("Enter or select an NDT Batch No.");
      return;
    }
    if (!mergeReason.trim()) {
      setError("Reason is required to merge.");
      return;
    }
    if (!mergeConfirm) {
      setError("Confirm the merge by checking the box.");
      return;
    }

    setMerging(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.mergeIntoPrevious(selectedBatchNo.trim(), mergeReason.trim());
      const printNote = res.printSuccess
        ? " Target tag reprinted."
        : res.printMessage
          ? ` Print: ${res.printMessage}`
          : "";
      setSuccess(
        `Merged ${res.sourceBundleNo} into ${res.targetBundleNo} (${res.resultingTotal} pcs). ${res.sequenceMessage ?? ""}${printNote}`
      );
      setMergeReason("");
      setMergeConfirm(false);
      setSelectedBatchNo(res.targetBundleNo ?? "");
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Merge failed.");
    } finally {
      setMerging(false);
    }
  };

  const manualBundleReconcile = async (reprint: boolean, printerTarget?: ReprintPrinterTarget) => {
    if (!selectedBatchNo.trim()) {
      setError("Enter or select an NDT Batch No.");
      return;
    }
    if (correctedTotal < 0) {
      setError("Corrected total must be non-negative.");
      return;
    }

    setManualReconciling(true);
    setManualReconcileWithReprint(reprint);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.manualBundleReconcile(selectedBatchNo.trim(), correctedTotal, {
        reprint,
        printerTarget: reprint ? printerTarget : undefined,
      });
      const printNote = !reprint
        ? ""
        : res.printSuccess
          ? " Tag reprinted."
          : res.printMessage
            ? ` Print: ${res.printMessage}`
            : "";
      setSuccess((res.message ?? "Bundle manually reconciled.") + printNote);
      const batch = selectedBatchNo.trim();
      const reconciledTotal =
        typeof res.correctedTotal === "number" && res.correctedTotal >= 0 ? res.correctedTotal : correctedTotal;
      setCorrectedTotal(reconciledTotal);
      setBundleManualRecon(true);
      setBundles((prev) =>
        prev.map((b) =>
          b.bundleNo === batch
            ? { ...b, totalNdtPcs: reconciledTotal, manualRecon: true, isForming: false }
            : b
        )
      );
      await refresh();
      setSlitsLoading(true);
      const details = await api.reconcileBundleSlits(batch);
      setSlits(Array.isArray(details?.slits) ? details.slits : []);
      setBundleManualRecon(Boolean(details?.bundle?.manualRecon));
      setBundleManualReconReason(details?.bundle?.manualReconReason ?? null);
      const detailTotal =
        typeof details?.bundle?.totalNdtPcs === "number" && details.bundle.totalNdtPcs > 0
          ? details.bundle.totalNdtPcs
          : reconciledTotal;
      setCorrectedTotal(detailTotal);
      setBundlePostReconCsvSum(
        typeof details?.bundle?.postReconCsvSum === "number" ? details.bundle.postReconCsvSum : null
      );
      if (reprint) setReprintChooserOpen(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Manual bundle reconcile failed.");
    } finally {
      setManualReconciling(false);
      setManualReconcileWithReprint(false);
      setSlitsLoading(false);
    }
  };

  const formatSlitDateTime = (value?: string | null) => {
    if (!value) return "—";
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? value : d.toLocaleString();
  };

  const slitValueForApi = (s: ReconcileSlitItem) => {
    const n = (s.slitNo ?? "").trim();
    return n === "" ? "—" : n;
  };

  const slitSapStatus = (s: ReconcileSlitItem) => (s.sapStatus ?? "").trim().toLowerCase();
  const slitIsSapAccepted = (s: ReconcileSlitItem) => slitSapStatus(s) === "accepted";

  const sapStatusBadge = (s: ReconcileSlitItem) => {
    const status = slitSapStatus(s);
    if (status === "") return <span className="text-gray-400">—</span>;
    const files = (s.sourceFiles ?? [])
      .map((f) => f.fileName)
      .filter(Boolean)
      .join(", ");
    const title = `SAP ${s.sapStatus}${s.sapStatusAtUtc ? ` since ${new Date(s.sapStatusAtUtc).toLocaleString()}` : ""}${files ? ` — ${files}` : ""}`;
    const style =
      status === "accepted"
        ? "bg-green-100 text-green-800 border-green-200"
        : status === "rejected"
          ? "bg-red-100 text-red-800 border-red-200"
          : "bg-gray-100 text-gray-700 border-gray-200";
    return (
      <span
        title={title}
        className={`inline-block px-2 py-0.5 rounded-full border text-xs font-medium ${style}`}
      >
        {s.sapStatus}
        {status === "rejected" && (s.resubmitCount ?? 0) > 0 ? ` ×${s.resubmitCount}` : ""}
      </span>
    );
  };

  const toggleSlitDeleteMark = (s: ReconcileSlitItem) => {
    if (slitIsSapAccepted(s)) return; // deletion blocked server-side too (409)
    const v = slitValueForApi(s);
    setSlitsMarkedForDelete((prev) => (prev.includes(v) ? prev.filter((k) => k !== v) : [...prev, v]));
  };

  const refreshPpcCorrections = async (batch: string) => {
    try {
      const r = await api.reconcilePpcCorrections(batch);
      setBundlePpcPending(Boolean(r?.ppcCorrectionPending));
      setPpcItems(Array.isArray(r?.items) ? r.items : []);
    } catch {
      setPpcItems([]);
    }
  };

  const clearPpcItem = async (item: PpcCorrectionItem) => {
    if (typeof item.id !== "number") return;
    if (
      !window.confirm(
        `Mark the PPC correction for file ${item.fileName ?? "?"} / slit ${item.slitNo ?? "—"} as cleared?\n\nOnly do this after PPC confirms the change was applied in SAP.`
      )
    ) {
      return;
    }
    setClearingPpcId(item.id);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.clearPpcCorrection(item.id);
      setSuccess(res.message ?? "PPC correction item cleared.");
      await refreshPpcCorrections(selectedBatchNo.trim());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to clear PPC correction item.");
    } finally {
      setClearingPpcId(null);
    }
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
      setBundlePostReconCsvSum(
        typeof details?.bundle?.postReconCsvSum === "number" ? details.bundle.postReconCsvSum : null
      );
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
      setBundlePostReconCsvSum(
        typeof details?.bundle?.postReconCsvSum === "number" ? details.bundle.postReconCsvSum : null
      );
      // A save touching SAP-Accepted data auto-creates a PPC correction item — refresh the status.
      setBundleManualReview(Boolean(details?.bundle?.manualReview));
      if (details?.bundle?.ppcCorrectionPending) {
        await refreshPpcCorrections(selectedBatchNo.trim());
      } else {
        setBundlePpcPending(false);
        setPpcItems([]);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Slit reconcile failed.");
    } finally {
      setSlitsLoading(false);
      setSavingSlit(false);
    }
  };

  const selectedBundle = bundles.find((b) => b.bundleNo === selectedBatchNo) ?? null;
  const selectedIsOvershoot = isBundleOvershoot(selectedBundle);
  const computedTotal = slits.reduce((sum, s) => sum + (typeof s.ndtPipes === "number" ? s.ndtPipes : 0), 0);
  const selectedSlitSumForCompare =
    slits.length > 0 ? computedTotal : getBundleSlitSumForCompare(selectedBundle);
  const selectedIsSlitMismatch = (() => {
    if (!selectedBundle || selectedBundle.isForming) return false;
    if (selectedSlitSumForCompare == null || selectedBundle.totalNdtPcs == null) {
      return isBundleSlitCountMismatch(selectedBundle);
    }
    if (selectedSlitSumForCompare <= 0) return false;
    return selectedBundle.totalNdtPcs !== selectedSlitSumForCompare;
  })();
  const selectedHasSapAcceptedSlit = slits.some((s) => slitIsSapAccepted(s));
  const selectedHasSapPendingSlit = slits.some((s) => {
    const st = slitSapStatus(s);
    return !st || st === "pending";
  });
  const selectedMillNo =
    typeof selectedBundle?.millNo === "number" && selectedBundle.millNo >= 1 && selectedBundle.millNo <= 4
      ? selectedBundle.millNo
      : 0;
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
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-start justify-between gap-2 shrink-0">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Reconcile Bundle</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {isStationMode(mode) ? (
              <>
                {mode} station reconcile — pick a batch from the list or scan · master{" "}
                <span className="font-medium">Reconcile ON</span> required to submit
              </>
            ) : (
              <>
                FillToTarget mills only ·{" "}
                <span className="font-medium">Reconcile</span> locks totals ·{" "}
                <span className="font-medium">Reconcile &amp; Reprint</span> also prints ·{" "}
                <Link href="/input-slits" className="text-primary-700 hover:underline">
                  Manual Input Slit
                </Link>
              </>
            )}
          </p>
        </div>
        <button
          type="button"
          onClick={toggleReconcileEnabled}
          className={`px-3 py-1.5 text-xs font-semibold rounded-md text-white ${
            reconcileEnabled ? "bg-red-600 hover:bg-red-700" : "bg-green-600 hover:bg-green-700"
          }`}
          title="Master switch for reconcile actions"
        >
          Reconcile {reconcileModeLabel}
        </button>
      </div>

      {error && (
        <div
          className={`rounded-md bg-red-50 border border-red-200 text-red-700 shrink-0 ${
            isStationMode(mode) ? "px-4 py-3 text-base" : "px-3 py-2 text-sm"
          }`}
        >
          {error}
        </div>
      )}
      {success && (
        <div
          className={`rounded-md shrink-0 border ${
            isStationMode(mode) ? "px-4 py-3 text-base" : "px-3 py-2 text-sm"
          } ${
            sapMailNotice
              ? "bg-amber-50 border-amber-300 text-amber-950"
              : "bg-green-50 border-green-200 text-green-800"
          }`}
        >
          {success}
        </div>
      )}

      {/* Mode toolbar */}
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm px-3 py-2.5 space-y-2 shrink-0">
        <div className="flex flex-wrap items-end gap-x-4 gap-y-2">
          <div>
            <label className="block text-[10px] font-semibold uppercase tracking-wide text-gray-500 mb-0.5">
              Type
            </label>
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value as ReconcileMode)}
              className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:ring-primary-500 focus:border-primary-500 min-w-[12rem]"
            >
              <option value="OutputBundle">Output NDT Batch</option>
              <option value="Visual">Visual</option>
              <option value="Hydrotesting">Hydrotesting</option>
              <option value="Revisual">Revisual</option>
            </select>
          </div>

          {mode === "OutputBundle" ? (
            <>
              <div className="h-5 w-px bg-gray-200 hidden sm:block self-center" />
              <MillFilter
                value={millFilter}
                onChange={setMillFilter}
                bundles={bundles}
                className="!gap-1.5 [&_span]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs"
              />
              <div className="h-5 w-px bg-gray-200 hidden md:block self-center" />
              <DateRangeFilter
                value={dateRange}
                onChange={setDateRange}
                summary={`${filteredBundles.length} of ${bundles.length}`}
                className="!p-0 !shadow-none !border-0 !bg-transparent !gap-2 flex-1 min-w-0 [&_label]:text-[10px] [&_input]:py-1 [&_input]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs [&_div.text-sm]:text-xs"
              />
            </>
          ) : (
            <>
              <div className="h-5 w-px bg-gray-200 hidden sm:block self-center" />
              <MillFilter
                value={millFilter}
                onChange={setMillFilter}
                bundles={stationBundlesForMillFilter}
                className="!gap-1.5 [&_span]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs"
              />
              <div className="h-5 w-px bg-gray-200 hidden md:block self-center" />
              <DateRangeFilter
                value={dateRange}
                onChange={setDateRange}
                summary={`${filteredStationTags.length} batches`}
                className="!p-0 !shadow-none !border-0 !bg-transparent !gap-2 flex-1 min-w-0 [&_label]:text-[10px] [&_input]:py-1 [&_input]:text-xs [&_button]:px-2 [&_button]:py-1 [&_button]:text-xs [&_div.text-sm]:text-xs"
              />
              <button
                type="button"
                onClick={() => void refresh()}
                className="px-2.5 py-1.5 text-xs font-medium text-primary-700 border border-primary-200 rounded-md hover:bg-primary-50"
              >
                Refresh list
              </button>
            </>
          )}
        </div>
      </div>

      {isStationMode(mode) && (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 items-start pb-8">
          <section className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden flex flex-col lg:col-span-5 lg:sticky lg:top-4 max-h-[min(42rem,calc(100vh-9rem))]">
            <div className="px-4 py-3 bg-primary-50 text-gray-900 border-b border-gray-200 shrink-0">
              <div className="text-base font-semibold">{mode} station runs</div>
              <p className="text-xs text-gray-600 mt-0.5">
                Click a batch to load counts — or scan / search below.
              </p>
            </div>
            <div className="px-4 py-3 border-b border-gray-100 shrink-0 space-y-2">
              <label className="block text-xs font-medium text-gray-600">Scan / search batch or PO</label>
              <div className="flex gap-2">
                <input
                  value={stationSearchQ}
                  onChange={(e) => setStationSearchQ(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      applyStationSearch();
                    }
                  }}
                  className="flex-1 border border-gray-300 rounded-lg px-3 py-2.5 text-base font-mono focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
                  placeholder="Scan barcode or type batch…"
                  autoComplete="off"
                />
                <button
                  type="button"
                  onClick={applyStationSearch}
                  disabled={stationLoading || !stationSearchQ.trim()}
                  className="px-4 py-2.5 text-sm font-semibold text-white bg-primary-600 rounded-lg hover:bg-primary-700 disabled:opacity-50"
                >
                  Load
                </button>
              </div>
            </div>
            <div className="overflow-y-auto min-h-0 flex-1">
              {stationTagsLoading || loading ? (
                <p className="px-4 py-6 text-sm text-gray-500">Loading station runs…</p>
              ) : filteredStationTags.length === 0 ? (
                <p className="px-4 py-6 text-sm text-gray-500">
                  No {mode} runs match the filters. Scan a batch number to load it anyway.
                </p>
              ) : (
                <div className="divide-y divide-gray-100">
                  {filteredStationTags.map((tag) => {
                    const batch = (tag.ndtBatchNo ?? "").trim();
                    const active = batch === stationBatchNo.trim();
                    const mill = resolveStationMillNo(tag);
                    return (
                      <button
                        key={`${tag.id ?? batch}-${tag.workStation ?? ""}`}
                        type="button"
                        onClick={() => selectStationTag(tag)}
                        className={`w-full text-left px-4 py-3.5 hover:bg-gray-50 transition-colors ${
                          active ? "bg-primary-50 border-l-4 border-primary-500 pl-3" : ""
                        }`}
                      >
                        <div className="font-mono text-base font-semibold text-gray-900">{batch || "—"}</div>
                        <div className="text-sm text-gray-600 mt-1 flex flex-wrap gap-x-3 gap-y-0.5">
                          <span>PO {tag.poNumber ?? "—"}</span>
                          <span>M{mill || "—"}</span>
                          <span>
                            OK {tag.okPcs ?? "—"} / Rej {tag.rejectPcs ?? "—"}
                          </span>
                        </div>
                        <div className="text-xs text-gray-400 mt-1">
                          {(tag.workStation ?? "—") +
                            (tag.hydrotestingType ? ` · ${tag.hydrotestingType}` : "")}{" "}
                          · {formatDisplayDate(tag.bundleEnd || tag.importedAtUtc || tag.bundleStart)}
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          </section>

          <section className="bg-white rounded-xl border border-gray-200 shadow-sm lg:col-span-7 overflow-hidden">
            <div className="px-5 py-4 bg-amber-50 border-b border-amber-100">
              <div className="text-sm font-semibold uppercase tracking-wide text-amber-900/80">
                {mode} reconcile
              </div>
              <div className="mt-1 font-mono text-2xl sm:text-3xl font-bold text-gray-900 tracking-tight">
                {stationBatchNo.trim() || "Select a batch"}
              </div>
              {stationLoading && (
                <p className="text-sm text-amber-800 mt-2">Loading station context…</p>
              )}
            </div>

            <div className="p-5 sm:p-6 space-y-6">
              <div className="grid grid-cols-3 gap-3">
                <div className="rounded-xl border border-gray-200 bg-gray-50 px-4 py-3">
                  <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">PO</div>
                  <div className="mt-1 text-lg font-semibold text-gray-900 break-all">
                    {stationPoNumber ?? "—"}
                  </div>
                </div>
                <div className="rounded-xl border border-gray-200 bg-gray-50 px-4 py-3">
                  <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">Mill</div>
                  <div className="mt-1 text-2xl font-bold text-gray-900">{stationMillNo ?? "—"}</div>
                </div>
                <div className="rounded-xl border border-amber-200 bg-amber-50/80 px-4 py-3">
                  <div className="text-xs font-medium text-amber-900/70 uppercase tracking-wide">Incoming</div>
                  <div className="mt-1 text-2xl font-bold text-amber-950">{stationIncoming ?? "—"}</div>
                </div>
              </div>

              {(mode === "Visual" || mode === "Revisual") && (
                <div>
                  <div className="text-sm font-medium text-gray-700 mb-2">
                    {mode} physical station
                  </div>
                  <div className="inline-flex rounded-xl border border-gray-300 overflow-hidden">
                    {([1, 2] as const).map((n) => (
                      <button
                        key={n}
                        type="button"
                        onClick={() => {
                          setStationOperatorNumber(n);
                          if (stationBatchNo.trim()) void loadStationContextFor(stationBatchNo, { operatorStationNumber: n });
                        }}
                        className={`px-6 py-3 text-base font-semibold ${
                          stationOperatorNumber === n
                            ? "bg-primary-600 text-white"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                        }`}
                      >
                        Station {n}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {mode === "Hydrotesting" && (
                <div>
                  <div className="text-sm font-medium text-gray-700 mb-2">Hydro type</div>
                  <div className="inline-flex rounded-xl border border-gray-300 overflow-hidden flex-wrap">
                    {(
                      [
                        ["FourHeadHydrotesting", "Four Head"],
                        ["BigHydrotesting", "Big"],
                      ] as const
                    ).map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        onClick={() => {
                          setHydroType(value);
                          if (stationBatchNo.trim())
                            void loadStationContextFor(stationBatchNo, { hydroType: value });
                        }}
                        className={`px-5 py-3 text-base font-semibold ${
                          hydroType === value
                            ? "bg-primary-600 text-white"
                            : "bg-white text-gray-700 hover:bg-gray-50"
                        }`}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">OK pcs</label>
                  <input
                    type="number"
                    min={0}
                    value={stationOk}
                    onChange={(e) => setStationOk(parseInt(e.target.value, 10) || 0)}
                    disabled={stationIncoming == null}
                    className="w-full border border-gray-300 rounded-xl px-4 py-4 text-3xl font-semibold tabular-nums focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">Reject pcs</label>
                  <input
                    type="number"
                    min={0}
                    value={stationReject}
                    onChange={(e) => setStationReject(parseInt(e.target.value, 10) || 0)}
                    disabled={stationIncoming == null}
                    className="w-full border border-gray-300 rounded-xl px-4 py-4 text-3xl font-semibold tabular-nums focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
                  />
                </div>
              </div>

              {stationIncoming != null && (
                <p
                  className={`text-sm font-medium ${
                    stationOkRejectValid ? "text-green-700" : "text-red-600"
                  }`}
                >
                  {stationOkRejectValid
                    ? `OK + Reject = Incoming (${stationIncoming})`
                    : `OK (${stationOk}) + Reject (${stationReject}) must equal Incoming (${stationIncoming})`}
                </p>
              )}

              <label className="inline-flex items-center gap-3 text-base text-gray-800">
                <input
                  type="checkbox"
                  checked={stationPrintTag}
                  onChange={(e) => setStationPrintTag(e.target.checked)}
                  className="h-5 w-5 rounded border-gray-300"
                />
                Reprint tag after reconcile
              </label>

              <button
                type="button"
                onClick={() => void reconcileStation()}
                disabled={
                  stationReconciling ||
                  stationIncoming == null ||
                  !reconcileEnabled ||
                  !stationOkRejectValid
                }
                className="w-full sm:w-auto min-w-[14rem] px-8 py-4 bg-amber-600 text-white text-lg font-bold rounded-xl hover:bg-amber-700 disabled:opacity-50 disabled:pointer-events-none shadow-sm"
              >
                {stationReconciling
                  ? "Reconciling…"
                  : !reconcileEnabled
                    ? "Turn Reconcile ON first"
                    : "Reconcile station"}
              </button>

              {!reconcileEnabled && (
                <p className="text-sm text-gray-500">
                  Use the green <span className="font-semibold">Reconcile ON</span> button at the top before
                  submitting.
                </p>
              )}
            </div>
          </section>
        </div>
      )}

      {mode === "OutputBundle" && (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-3 items-start">
        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden flex flex-col lg:col-span-4 lg:sticky lg:top-4 max-h-[min(36rem,calc(100vh-10rem))]">
          <div className="px-3 py-2 bg-primary-50 text-gray-900 text-sm font-semibold border-b border-gray-200 flex items-center justify-between shrink-0">
            <span>
              Bundles
              {!loading && bundles.length > 0 && (millFilter !== "all" || isDateRangeActive(dateRange)) && (
                <span className="ml-1.5 text-xs font-normal text-gray-600">
                  {filteredBundles.length}/{bundles.length}
                </span>
              )}
            </span>
            <button
              type="button"
              onClick={() => refresh()}
              className="text-xs font-medium text-primary-700 hover:text-primary-800"
            >
              Refresh
            </button>
          </div>
          <div className="overflow-y-auto min-h-0">
            {loading ? (
              <p className="px-3 py-4 text-xs text-gray-500">Loading…</p>
            ) : filteredBundles.length === 0 ? (
              <p className="px-3 py-4 text-xs text-gray-500">
                {bundles.length === 0
                  ? "No bundles found."
                  : "No bundles match filters."}
              </p>
            ) : (
              <div className="divide-y divide-gray-100">
                {filteredBundles.map((b) => {
                  const active = b.bundleNo === selectedBatchNo;
                  const slitSumCompare = getBundleSlitSumForCompare(b);
                  const showSlitMismatch = isBundleSlitCountMismatch(b);
                  return (
                    <button
                      key={b.bundleNo}
                      type="button"
                      onClick={() => {
                        if (b.bundleNo) setSelectedBatchNo(b.bundleNo);
                      }}
                      className={`w-full text-left px-3 py-2 hover:bg-gray-50 ${
                        active ? "bg-primary-50 border-l-4 border-primary-500 pl-2" : ""
                      }`}
                    >
                      <div className="font-semibold text-gray-900 text-xs flex items-center gap-1.5 flex-wrap font-mono">
                        <span>{b.bundleNo}</span>
                        {b.isForming ? (
                          <span className="text-[9px] font-semibold uppercase tracking-wide px-1 py-0.5 rounded bg-sky-100 text-sky-800">
                            Forming
                          </span>
                        ) : null}
                        {isBundleOvershoot(b) ? (
                          <span className="text-[9px] font-semibold uppercase tracking-wide px-1 py-0.5 rounded bg-red-100 text-red-800">
                            Overshoot
                          </span>
                        ) : null}
                        {showSlitMismatch ? (
                          <span
                            className="text-[9px] font-semibold uppercase tracking-wide px-1 py-0.5 rounded bg-amber-100 text-amber-900"
                            title="Bundle total does not match sum of slit NDT pipes"
                          >
                            Slit mismatch
                          </span>
                        ) : null}
                      </div>
                      <div className="text-[11px] text-gray-600 pt-0.5">
                        PO {b.poNumber ?? "—"} · M{b.millNo ?? "—"} ·{" "}
                        {b.isForming && !b.manualRecon
                          ? `${b.slitSum != null ? b.slitSum : b.totalNdtPcs ?? 0} pcs`
                          : `${b.totalNdtPcs ?? (b.slitSum != null && b.slitSum > 0 ? b.slitSum : 0)} pcs`}
                        {!b.isForming &&
                        slitSumCompare != null &&
                        b.totalNdtPcs != null &&
                        slitSumCompare !== b.totalNdtPcs
                          ? ` (slits ${slitSumCompare})`
                          : ""}
                      </div>
                      <div className="text-[10px] text-gray-400">
                        {formatDisplayDate(b.slitFinishTime || b.slitStartTime || b.printedAt)}
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </section>

        <div className="lg:col-span-8 flex flex-col gap-3 pb-8">
        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-3 py-2 bg-primary-50 text-gray-900 text-sm font-semibold border-b border-gray-200">
            Bundle total reconcile
          </div>
          <div className="p-3 space-y-3">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
              <div className="sm:col-span-1">
                <label className="block text-[10px] text-gray-500 mb-0.5">NDT Batch No.</label>
                <input
                  value={selectedBatchNo}
                  onChange={(e) => setSelectedBatchNo(e.target.value)}
                  className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-xs font-mono focus:ring-primary-500 focus:border-primary-500"
                  placeholder="Scan or type batch"
                />
              </div>
              <div>
                <label className="block text-[10px] text-gray-500 mb-0.5">Current total</label>
                <div className="border border-gray-200 rounded-md px-2 py-1.5 text-xs bg-gray-50 font-semibold tabular-nums">
                  {selectedBundle?.isForming && !selectedBundle?.manualRecon
                    ? slitsLoading
                      ? "…"
                      : computedTotal
                    : selectedBundle?.totalNdtPcs != null && selectedBundle.totalNdtPcs > 0
                      ? selectedBundle.totalNdtPcs
                      : slitsLoading
                        ? "…"
                        : computedTotal > 0
                          ? computedTotal
                          : "—"}
                </div>
              </div>
              <div>
                <label className="block text-[10px] text-gray-500 mb-0.5">Corrected total</label>
                <input
                  type="number"
                  min={0}
                  value={correctedTotal}
                  onChange={(e) => setCorrectedTotal(parseInt(e.target.value, 10) || 0)}
                  disabled={!selectedBatchNo.trim()}
                  className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-xs focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
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

            {selectedIsOvershoot && (
              <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
                <span className="font-semibold">Overshoot:</span> CSV stamped pipes exceed this tag total.
                Reconcile slit counts below (or use Manual Reconcile). Inbox Input Slit files are updated on
                disk; SAP-Accepted files update SQL only (Accepted file unchanged).
              </div>
            )}

            {selectedIsSlitMismatch && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950">
                <span className="font-semibold">Slit mismatch:</span> bundle total{" "}
                <span className="font-mono">{selectedBundle?.totalNdtPcs ?? "—"}</span> does not match slit sum{" "}
                <span className="font-mono">{selectedSlitSumForCompare ?? "—"}</span>.
                {selectedHasSapAcceptedSlit ? (
                  <>
                    {" "}
                    One or more slits are <span className="font-semibold">Sent to SAP (Accepted)</span>. Reconcile
                    MES/SQL slit counts here if needed, then email the PPC team to adjust the accepted SAP slit
                    file(s).
                  </>
                ) : selectedHasSapPendingSlit || slits.length > 0 ? (
                  <>
                    {" "}
                    Slit file status is still <span className="font-semibold">Pending</span> — reconcile the slit
                    counts below now so totals match before SAP pickup.
                  </>
                ) : (
                  <> Load slits below, then reconcile counts so the tag and slit files stay aligned.</>
                )}
              </div>
            )}

            <div className="flex flex-wrap items-end gap-3">
              <button
                type="button"
                onClick={() => void manualBundleReconcile(false)}
                disabled={manualReconciling || !selectedBatchNo.trim() || !reconcileEnabled}
                className="px-4 py-2 bg-white text-primary-800 text-sm font-medium rounded-md border border-primary-300 hover:bg-primary-50 disabled:opacity-50 disabled:pointer-events-none"
                title="Update SQL/CSV totals and lock the bundle without sending a tag to the printer"
              >
                {manualReconciling && !manualReconcileWithReprint ? "Reconciling…" : "Reconcile"}
              </button>
              <button
                type="button"
                onClick={() => setReprintChooserOpen(true)}
                disabled={manualReconciling || !selectedBatchNo.trim() || !reconcileEnabled}
                className="px-4 py-2 bg-primary-600 text-white text-sm font-medium rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:pointer-events-none"
                title="Update SQL/CSV totals, lock the bundle, then choose a printer for the reprint"
              >
                {manualReconciling && manualReconcileWithReprint
                  ? "Reconciling & printing…"
                  : "Reconcile & Reprint"}
              </button>
            </div>

            {mergePreview?.targetBundleNo && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-3 space-y-2">
                <p className="text-sm font-semibold text-amber-950">Merge into previous live bundle</p>
                <p className="text-sm text-amber-900">
                  {mergePreview.sourceBundleNo} ({mergePreview.sourcePcs} pcs) → {mergePreview.targetBundleNo}{" "}
                  ({mergePreview.targetPcs} pcs) = {mergePreview.resultingTotal} pcs.
                  {mergePreview.pipeSize ? ` Size ${mergePreview.pipeSize}.` : ""}{" "}
                  {mergePreview.sequenceMessage}
                  {mergePreview.acceptedFileCount
                    ? ` ${mergePreview.acceptedFileCount} SAP-Accepted file(s) will raise PPC correction.`
                    : ""}
                </p>
                <label className="block text-sm font-medium text-amber-950">
                  Reason
                  <input
                    value={mergeReason}
                    onChange={(e) => setMergeReason(e.target.value)}
                    className="mt-1 w-full border border-amber-300 rounded-md px-3 py-2 text-sm bg-white"
                    placeholder="Why these two bundles are the same physical bundle"
                  />
                </label>
                <label className="flex items-center gap-2 text-sm text-amber-950">
                  <input
                    type="checkbox"
                    checked={mergeConfirm}
                    onChange={(e) => setMergeConfirm(e.target.checked)}
                  />
                  Confirm merge into {mergePreview.targetBundleNo} (source will be voided)
                </label>
                <button
                  type="button"
                  onClick={() => void mergeIntoPrevious()}
                  disabled={merging || !mergeReason.trim() || !mergeConfirm}
                  className="px-4 py-2 bg-amber-700 text-white text-sm font-medium rounded-md hover:bg-amber-800 disabled:opacity-50"
                >
                  {merging ? "Merging…" : "Merge into previous"}
                </button>
              </div>
            )}
          </div>
        </section>

        <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-visible">
          <div className="px-3 py-2 bg-gray-100 text-gray-900 text-sm font-semibold border-b border-gray-200">
            Slit traceability
          </div>
          <div className="p-3 space-y-3">
            {!selectedBatchNo || !selectedBundle ? (
              <p className="text-xs text-gray-500">Select a bundle to view slit rows.</p>
            ) : (
              <>
                {bundleManualRecon && (
                  <p className="text-sm text-amber-800 rounded-md border border-amber-200 bg-amber-50 px-3 py-2">
                    Manually reconciled bundle — slit rows can be edited below. Corrected total above stays locked until
                    you reconcile again (with or without reprint).
                    {bundlePostReconCsvSum != null ? ` Post-recon CSV slit sum: ${bundlePostReconCsvSum}.` : ""}
                  </p>
                )}
                {bundleManualReview && (
                  <p className="text-sm text-purple-900 rounded-md border border-purple-300 bg-purple-50 px-3 py-2">
                    <span className="inline-block mr-2 px-2 py-0.5 rounded-full bg-purple-200 text-purple-900 text-xs font-semibold uppercase">
                      Manual review
                    </span>
                    This bundle was flagged during Closed-PO slit attach — verify slit rows and totals against the
                    physical bundle before reconciling.
                  </p>
                )}
                {bundlePpcPending && (
                  <div className="rounded-md border border-orange-300 bg-orange-50 px-3 py-2 space-y-2">
                    <p className="text-sm text-orange-900">
                      <span className="inline-block mr-2 px-2 py-0.5 rounded-full bg-orange-200 text-orange-900 text-xs font-semibold uppercase">
                        PPC correction pending
                      </span>
                      Local correction(s) touched SAP-Accepted file(s): MES is updated, but SAP still holds the old
                      value. Email PPC with the details below, then mark each item cleared once PPC confirms the SAP
                      change.
                    </p>
                    {ppcItems.length > 0 && (
                      <table className="min-w-full text-sm">
                        <thead>
                          <tr className="text-left text-xs uppercase text-orange-800">
                            <th className="pr-3 py-1 font-medium">File</th>
                            <th className="pr-3 py-1 font-medium">Slit</th>
                            <th className="pr-3 py-1 font-medium">SAP value → Corrected</th>
                            <th className="pr-3 py-1 font-medium">Created</th>
                            <th className="py-1" />
                          </tr>
                        </thead>
                        <tbody>
                          {ppcItems.map((item) => (
                            <tr key={item.id} className="border-t border-orange-200 text-orange-950">
                              <td className="pr-3 py-1 font-mono text-xs">{item.fileName ?? "—"}</td>
                              <td className="pr-3 py-1">{item.slitNo ?? "—"}</td>
                              <td className="pr-3 py-1">
                                {item.oldNdtPipes ?? "?"} → <span className="font-semibold">{item.correctedNdtPipes ?? "?"}</span>
                              </td>
                              <td className="pr-3 py-1">
                                {item.createdAtUtc ? new Date(item.createdAtUtc).toLocaleString() : "—"}
                              </td>
                              <td className="py-1 text-right">
                                <button
                                  type="button"
                                  onClick={() => clearPpcItem(item)}
                                  disabled={clearingPpcId === item.id}
                                  className="px-2 py-1 border border-orange-400 rounded-md text-xs font-medium text-orange-900 bg-white hover:bg-orange-100 disabled:opacity-50"
                                >
                                  {clearingPpcId === item.id ? "Clearing…" : "PPC confirmed — clear"}
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                )}
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
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs">Reject Pipes</th>
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs whitespace-nowrap">
                            Slit Start
                          </th>
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs whitespace-nowrap">
                            Slit End
                          </th>
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs whitespace-nowrap">
                            NDT Short Len
                          </th>
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs whitespace-nowrap">
                            Rej Short Len
                          </th>
                          <th className="px-3 py-2 text-left font-medium text-gray-500 uppercase text-xs">SAP Status</th>
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
                                className="h-4 w-4 rounded border-gray-300 disabled:opacity-40"
                                checked={slitsMarkedForDelete.includes(slitValueForApi(s))}
                                onChange={() => toggleSlitDeleteMark(s)}
                                disabled={slitIsSapAccepted(s)}
                                title={
                                  slitIsSapAccepted(s)
                                    ? "Delete blocked: this slit's data is already posted in SAP (Accepted). Corrections go through PPC."
                                    : undefined
                                }
                                aria-label={`Mark slit ${slitValueForApi(s)} for deletion`}
                              />
                            </td>
                            <td className="px-3 py-2 text-gray-900 font-medium whitespace-nowrap">{s.slitNo ?? "—"}</td>
                            <td className="px-3 py-2 text-gray-700 tabular-nums">{s.ndtPipes ?? 0}</td>
                            <td className="px-3 py-2 text-gray-700 tabular-nums">{s.rejectedPipes ?? 0}</td>
                            <td className="px-3 py-2 text-gray-700 whitespace-nowrap">
                              {formatSlitDateTime(s.slitStartTime)}
                            </td>
                            <td className="px-3 py-2 text-gray-700 whitespace-nowrap">
                              {formatSlitDateTime(s.slitFinishTime)}
                            </td>
                            <td className="px-3 py-2 text-gray-700">{s.ndtShortLengthPipe?.trim() || "—"}</td>
                            <td className="px-3 py-2 text-gray-700">{s.rejectedShortLengthPipe?.trim() || "—"}</td>
                            <td className="px-3 py-2">{sapStatusBadge(s)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {!slitsLoading && slits.length > 0 && (
                  <div className="border-t border-gray-200 pt-4 space-y-3">
                    <div className="text-sm font-semibold text-gray-900">Edit slit</div>
                    {(() => {
                      const current = slits.find((x) => (x.slitNo ?? "") === selectedSlitNo);
                      return current && slitIsSapAccepted(current) ? (
                        <p className="text-sm text-blue-900 rounded-md border border-blue-200 bg-blue-50 px-3 py-2">
                          This slit&apos;s file is <span className="font-semibold">SAP Accepted</span>: saving updates
                          MES/SQL only — the Accepted CSV is left unchanged as the SAP reference. Email the PPC team
                          to correct NDT count for this slit in SAP. Deleting this slit is blocked.
                        </p>
                      ) : null;
                    })()}
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
        </div>
      )}

      <ReprintPrinterChooser
        open={reprintChooserOpen}
        millNo={selectedMillNo || 1}
        title="Choose reprint printer"
        subtitle={`Reconcile ${selectedBatchNo.trim() || "bundle"} to ${correctedTotal} pcs, then print with Reprint marker.`}
        confirmLabel="Reconcile & Reprint"
        busy={manualReconciling}
        onCancel={() => {
          if (!manualReconciling) setReprintChooserOpen(false);
        }}
        onConfirm={(target) => {
          void manualBundleReconcile(true, target);
        }}
      />
    </div>
  );
}
