function getBaseUrl(): string {
  if (typeof window !== "undefined") {
    const win = window as unknown as { __API_BASE__?: string };
    if (win.__API_BASE__ !== undefined && win.__API_BASE__ !== "") return win.__API_BASE__;
    // Empty = same origin; Next.js rewrites /api/* to http://localhost:5000 (no CORS).
    return process.env.NEXT_PUBLIC_API_BASE ?? "";
  }
  return process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:5000";
}

export function setApiBase(base: string): void {
  if (typeof window !== "undefined") {
    (window as unknown as { __API_BASE__?: string }).__API_BASE__ = base;
  }
}

export function getApiBase(): string {
  return getBaseUrl();
}

export async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
  const base = getBaseUrl();
  const res = await fetch(`${base.replace(/\/$/, "")}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...options?.headers },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export interface WipInfo {
  poNumber?: string;
  millNumber?: string;
  plannedMonth?: string;
  pipeGrade?: string;
  pipeSize?: string;
  pipeThickness?: string;
  pipeLength?: string;
  piecesPerBundle?: string;
  totalPieces?: string;
  itemDescription?: string;
}

/** WIP row for one mill (1–4) from the current PO plan CSV. */
export interface WipByMillRow {
  /** API returns a number; string tolerated if serialized differently. */
  millNo?: number | string;
  poNumber?: string;
  plannedMonth?: string;
  pipeGrade?: string;
  pipeSize?: string;
  /** Required NDT pipes per bundle from the NDT Bundle Formation chart (same as bundle engine). */
  ndtPcsPerBundle?: number | null;
  pipeLength?: string;
  piecesPerBundle?: string;
  totalPieces?: string;
}

/** Present on wip-by-mills when MillSlitLive is enabled for a mill (1–4); ndtCount from same PLC DB read as the slit worker. */
export interface LiveMillNdtPayload {
  millNo?: number;
  ndtCount?: number | null;
}

export interface WipByMillsResponse {
  mills?: WipByMillRow[];
  sourcePath?: string;
  liveMillNdt?: LiveMillNdtPayload;
}

/** GET /api/Test/live-mill-ndt — lightweight poll; millNo 0 uses MillSlitLive.ApplyToMillNo on the server. */
export interface LiveMillNdtPollResponse {
  millNo?: number;
  ndtCount?: number | null;
  liveMillConfigured?: number;
  message?: string;
}

export interface NdtSummary {
  poNumber?: string;
  millNo?: number;
  totalNdtPipes?: number;
}

export interface RunningPoNdtSummary {
  millNo?: number;
  poNumber?: string;
  totalNdtPipes?: number;
}

export interface BundleFile {
  fileName?: string;
  fullPath?: string;
}

export interface ReconcileBundle {
  bundleNo?: string;
  poNumber?: string;
  millNo?: number;
  totalNdtPcs?: number;
  slitNo?: string;
}

export interface ReconcileSlitItem {
  slitNo?: string;
  ndtPipes?: number;
}

export interface ReconcileBundleSlitsResponse {
  bundle?: ReconcileBundle;
  slits?: ReconcileSlitItem[];
}

export interface InputSlitFile {
  fileName?: string;
  lastModified?: string;
  size?: number;
}

export interface InputSlitContent {
  header?: string;
  headers?: string[];
  rows?: string[][];
}

export interface PlcStatus {
  /** True only when Modbus TCP is enabled and the last Modbus read succeeded. */
  connected?: boolean;
  lastPlcError?: string | null;
  lastPlcCheckUtc?: string | null;
  poEndActive?: boolean;
  plcPoEndEnabled?: boolean;
  driver?: string;
  poEndByMill?: Record<string, boolean>;
  message?: string;
}

/** NotConfigured | Ready (TCP OK) | Unreachable | Configured (name only, no TCP check). */
export interface PrinterStatus {
  status?: string;
  message?: string;
}

export interface ZplGenerationStatus {
  enabled?: boolean;
}

export interface ManualTagPrintResponse {
  message?: string;
  station?: string;
  ndtBatchNo?: string;
  incomingPcs?: number;
  okPcs?: number;
  rejectedPcs?: number;
  outgoingPcs?: number;
  printed?: boolean;
  csvPath?: string;
}

export interface ManualStationContext {
  station?: string;
  ndtBatchNo?: string;
  poNumber?: string;
  millNo?: number;
  /** Visual / Revisual physical station (1 or 2). */
  operatorStationNumber?: number;
  incomingPcs?: number;
  alreadyOkPcs?: number;
  alreadyRejectedPcs?: number;
  outgoingPcs?: number;
  hydroRedoRequired?: boolean;
  revisualRedoRequired?: boolean;
  hasRecordedThisStation?: boolean;
}

export interface UploadBundleGenerationResponse {
  message?: string;
  filePath?: string;
  rowCount?: number;
}

export const api = {
  wipInfo: () => fetchApi<WipInfo>("/api/Test/wip-info"),
  wipByMills: () => fetchApi<WipByMillsResponse>("/api/Test/wip-by-mills"),
  liveMillNdt: (millNo = 0) =>
    fetchApi<LiveMillNdtPollResponse>(`/api/Test/live-mill-ndt?millNo=${encodeURIComponent(String(millNo))}`),
  ndtSummary: (poNumber: string, millNo: number) =>
    fetchApi<NdtSummary>(`/api/Test/ndt-summary?poNumber=${encodeURIComponent(poNumber)}&millNo=${millNo}`),
  ndtSummaryRunningPo: () => fetchApi<RunningPoNdtSummary[]>("/api/Test/ndt-summary-running-po"),
  bundles: () => fetchApi<BundleFile[]>("/api/Test/bundles"),
  poEnd: (poNumber: string, millNo: number) =>
    fetchApi<{ message?: string }>("/api/Test/po-end", {
      method: "POST",
      body: JSON.stringify({ poNumber, millNo }),
    }),
  reconcileBundles: () => fetchApi<ReconcileBundle[]>("/api/Reconcile/bundles"),
  reconcile: (ndtBatchNo: string, newNdtPipes: number) =>
    fetchApi<{ message?: string; csvFilesUpdated?: number }>("/api/Reconcile/reconcile", {
      method: "POST",
      body: JSON.stringify({ ndtBatchNo, newNdtPipes }),
    }),
  reconcileBundleSlits: (ndtBatchNo: string) =>
    fetchApi<ReconcileBundleSlitsResponse>(`/api/Reconcile/bundles/${encodeURIComponent(ndtBatchNo)}/slits`),
  reconcileSlit: (ndtBatchNo: string, slitNo: string, newNdtPipes: number) =>
    fetchApi<{
      message?: string;
      ndtBatchNo?: string;
      slitNo?: string;
      newNdtPipes?: number;
      newBundleTotalNdtPcs?: number;
      slits?: ReconcileSlitItem[];
    }>("/api/Reconcile/reconcile-slit", {
      method: "POST",
      body: JSON.stringify({ ndtBatchNo, slitNo, newNdtPipes }),
    }),
  reconcileDeleteSlits: (ndtBatchNo: string, slitNos: string[]) =>
    fetchApi<{
      message?: string;
      ndtBatchNo?: string;
      rowsRemoved?: number;
      newBundleTotalNdtPcs?: number;
      bundleSummaryUpdated?: boolean;
      slits?: ReconcileSlitItem[];
    }>("/api/Reconcile/delete-slits", {
      method: "POST",
      body: JSON.stringify({ ndtBatchNo, slitNos }),
    }),
  printReconciledBundle: (ndtBatchNo: string) =>
    fetchApi<{ message?: string; ndtBatchNo?: string; ndtPcs?: number }>("/api/Reconcile/print-bundle", {
      method: "POST",
      body: JSON.stringify({ ndtBatchNo }),
    }),
  inputSlitFiles: () => fetchApi<InputSlitFile[]>("/api/InputSlits/files"),
  inputSlitContent: (fileName: string) => fetchApi<InputSlitContent>(`/api/InputSlits/files/${encodeURIComponent(fileName)}/content`),
  plcStatus: () => fetchApi<PlcStatus>("/api/Status/plc"),
  printerStatus: () => fetchApi<PrinterStatus>("/api/Status/printer"),
  zplGenerationStatus: () => fetchApi<ZplGenerationStatus>("/api/Status/zpl-generation"),
  setZplGenerationStatus: (enabled: boolean) =>
    fetchApi<ZplGenerationStatus>("/api/Status/zpl-generation", {
      method: "POST",
      body: JSON.stringify({ enabled }),
    }),
  printDummyBundle: () =>
    fetchApi<{ message?: string; address?: string; port?: number }>("/api/Test/print-dummy-bundle", {
      method: "POST",
    }),
  manualStationContext: (
    station: "Visual" | "Hydrotesting" | "FourHeadHydrotesting" | "BigHydrotesting" | "Revisual",
    ndtBatchNo: string,
    operatorStationNumber = 1
  ) =>
    fetchApi<ManualStationContext>(
      `/api/ManualTags/${encodeURIComponent(station)}/${encodeURIComponent(ndtBatchNo)}/context?operatorStationNumber=${operatorStationNumber}`
    ),
  manualStationRecord: (
    station: "Visual" | "Hydrotesting" | "FourHeadHydrotesting" | "BigHydrotesting" | "Revisual",
    args: { ndtBatchNo: string; okPcs: number; rejectedPcs: number; printTag: boolean; operatorStationNumber?: number }
  ) =>
    fetchApi<ManualTagPrintResponse>(`/api/ManualTags/${encodeURIComponent(station)}/record`, {
      method: "POST",
      body: JSON.stringify({
        ndtBatchNo: args.ndtBatchNo,
        okPcs: args.okPcs,
        rejectedPcs: args.rejectedPcs,
        printTag: args.printTag,
        operatorStationNumber: args.operatorStationNumber ?? 1,
      }),
    }),
  manualStationReconcile: (
    station: "Visual" | "Hydrotesting" | "FourHeadHydrotesting" | "BigHydrotesting" | "Revisual",
    args: { ndtBatchNo: string; okPcs: number; rejectedPcs: number; printTag: boolean; operatorStationNumber?: number }
  ) =>
    fetchApi<ManualTagPrintResponse>(`/api/ManualTags/${encodeURIComponent(station)}/reconcile`, {
      method: "POST",
      body: JSON.stringify({
        ndtBatchNo: args.ndtBatchNo,
        okPcs: args.okPcs,
        rejectedPcs: args.rejectedPcs,
        printTag: args.printTag,
        operatorStationNumber: args.operatorStationNumber ?? 1,
      }),
    }),
  generateUploadBundleFile: () =>
    fetchApi<UploadBundleGenerationResponse>("/api/UploadNdtBundle/generate-now", {
      method: "POST",
    }),
};
