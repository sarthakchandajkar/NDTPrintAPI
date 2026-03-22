function getBaseUrl(): string {
  if (typeof window !== "undefined") {
    const win = window as unknown as { __API_BASE__?: string };
    if (win.__API_BASE__ !== undefined && win.__API_BASE__ !== "") return win.__API_BASE__;
    // Empty = same origin; Next.js rewrites /api/* to http://localhost:5000 (no CORS).
    return process.env.NEXT_PUBLIC_API_BASE ?? "";
  }
  return process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:5000";
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

export interface NdtSummary {
  poNumber?: string;
  millNo?: number;
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
  connected?: boolean;
  poEndActive?: boolean;
}

export interface PrinterStatus {
  status?: string;
  message?: string;
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

export const api = {
  wipInfo: () => fetchApi<WipInfo>("/api/Test/wip-info"),
  ndtSummary: (poNumber: string, millNo: number) =>
    fetchApi<NdtSummary>(`/api/Test/ndt-summary?poNumber=${encodeURIComponent(poNumber)}&millNo=${millNo}`),
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
  printReconciledBundle: (ndtBatchNo: string) =>
    fetchApi<{ message?: string; ndtBatchNo?: string; ndtPcs?: number }>("/api/Reconcile/print-bundle", {
      method: "POST",
      body: JSON.stringify({ ndtBatchNo }),
    }),
  inputSlitFiles: () => fetchApi<InputSlitFile[]>("/api/InputSlits/files"),
  inputSlitContent: (fileName: string) => fetchApi<InputSlitContent>(`/api/InputSlits/files/${encodeURIComponent(fileName)}/content`),
  plcStatus: () => fetchApi<PlcStatus>("/api/Status/plc"),
  printerStatus: () => fetchApi<PrinterStatus>("/api/Status/printer"),
  printDummyBundle: () =>
    fetchApi<{ message?: string; address?: string; port?: number }>("/api/Test/print-dummy-bundle", {
      method: "POST",
    }),
  manualStationContext: (station: "Visual" | "Hydrotesting" | "Revisual", ndtBatchNo: string) =>
    fetchApi<{
      station?: string;
      ndtBatchNo?: string;
      poNumber?: string;
      millNo?: number;
      incomingPcs?: number;
      alreadyOkPcs?: number;
      alreadyRejectedPcs?: number;
      outgoingPcs?: number;
    }>(`/api/ManualTags/${encodeURIComponent(station)}/${encodeURIComponent(ndtBatchNo)}/context`),
  manualStationRecord: (
    station: "Visual" | "Hydrotesting" | "Revisual",
    args: { ndtBatchNo: string; okPcs: number; rejectedPcs: number; user: string; printTag: boolean }
  ) =>
    fetchApi<ManualTagPrintResponse>(`/api/ManualTags/${encodeURIComponent(station)}/record`, {
      method: "POST",
      body: JSON.stringify(args),
    }),
};
