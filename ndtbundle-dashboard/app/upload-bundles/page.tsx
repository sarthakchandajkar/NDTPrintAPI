"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  api,
  type UploadBundleGeneratedItem,
} from "@/lib/api";

function formatUtc(value?: string | null) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

export default function UploadBundlesPage() {
  const [items, setItems] = useState<UploadBundleGeneratedItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [regeneratingBatch, setRegeneratingBatch] = useState<string | null>(null);
  const [manualBatch, setManualBatch] = useState("");
  const [manualBusy, setManualBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.listGeneratedUploadBundleFiles(500);
      setItems(res.items ?? []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load generated upload files.");
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return items;
    return items.filter((i) => {
      const hay = [
        i.ndtBatchNo,
        i.poNo,
        i.slitNo,
        i.fileName,
        i.millNo != null ? String(i.millNo) : "",
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [items, filter]);

  const regenerate = async (batch: string) => {
    const ndtBatchNo = batch.trim();
    if (!ndtBatchNo) return;
    setError(null);
    setSuccess(null);
    setRegeneratingBatch(ndtBatchNo);
    try {
      const res = await api.generateUploadBundleFile(ndtBatchNo);
      setSuccess(
        `${res.message ?? "Upload file regenerated."}${
          res.filePath ? ` Path: ${res.filePath}` : ""
        }`
      );
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Regenerate failed.");
    } finally {
      setRegeneratingBatch(null);
    }
  };

  const generateManual = async () => {
    const batch = manualBatch.trim();
    if (!batch) {
      setError("Enter an NDT batch number to generate.");
      return;
    }
    setManualBusy(true);
    setError(null);
    setSuccess(null);
    try {
      const res = await api.generateUploadBundleFile(batch);
      setSuccess(
        `${res.message ?? "Upload file generated."}${
          res.filePath ? ` Path: ${res.filePath}` : ""
        }`
      );
      setManualBatch("");
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Generate failed.");
    } finally {
      setManualBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">MES PAS NDT Upload Files</h1>
        <p className="text-gray-600 text-sm mt-1">
          Bundles that already have an upload CSV under{" "}
          <span className="font-mono text-xs">…/MES PAS NDT/Bundle</span>. Regenerate writes a new
          timestamped CSV (older files are left in place). Requires Revisual / NDT process CSV for
          the batch.
        </p>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">{error}</div>
      )}
      {success && (
        <div className="rounded-md bg-green-50 border border-green-200 p-4 text-green-800 text-sm">
          {success}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4 sm:p-6 space-y-4">
        <div className="flex flex-col lg:flex-row gap-3 lg:items-end lg:justify-between">
          <div className="flex-1 max-w-md">
            <label className="block text-sm font-medium text-gray-700 mb-1">Filter</label>
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Batch, PO, slit, file name…"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
            />
          </div>
          <button
            type="button"
            onClick={() => void load()}
            disabled={loading}
            className="inline-flex justify-center rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
          >
            {loading ? "Refreshing…" : "Refresh"}
          </button>
        </div>

        <div className="rounded-md border border-dashed border-gray-300 p-4 space-y-2">
          <div className="text-sm font-medium text-gray-800">Generate / regenerate by batch</div>
          <div className="flex flex-col sm:flex-row gap-2 sm:items-center">
            <input
              value={manualBatch}
              onChange={(e) => setManualBatch(e.target.value)}
              placeholder="NDT Batch No"
              className="sm:max-w-xs w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
            />
            <button
              type="button"
              onClick={() => void generateManual()}
              disabled={manualBusy || !manualBatch.trim()}
              className="inline-flex justify-center rounded-md bg-primary-600 px-4 py-2 text-sm font-medium text-white hover:bg-primary-700 disabled:opacity-50"
            >
              {manualBusy ? "Working…" : "Generate"}
            </button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 text-left text-gray-500">
                <th className="py-2 pr-3 font-medium">Batch</th>
                <th className="py-2 pr-3 font-medium">PO</th>
                <th className="py-2 pr-3 font-medium">Mill</th>
                <th className="py-2 pr-3 font-medium">Slit</th>
                <th className="py-2 pr-3 font-medium">OK pcs</th>
                <th className="py-2 pr-3 font-medium">Generated (UTC)</th>
                <th className="py-2 pr-3 font-medium">File</th>
                <th className="py-2 pr-3 font-medium">On disk</th>
                <th className="py-2 font-medium">Action</th>
              </tr>
            </thead>
            <tbody>
              {loading && items.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-8 text-center text-gray-500">
                    Loading…
                  </td>
                </tr>
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-8 text-center text-gray-500">
                    No generated upload files found.
                  </td>
                </tr>
              ) : (
                filtered.map((row) => {
                  const busy = regeneratingBatch === row.ndtBatchNo;
                  return (
                    <tr key={row.ndtBatchNo} className="border-b border-gray-100 align-top">
                      <td className="py-2 pr-3 font-mono text-gray-900">{row.ndtBatchNo}</td>
                      <td className="py-2 pr-3 font-mono text-gray-800">{row.poNo || "—"}</td>
                      <td className="py-2 pr-3 tabular-nums">{row.millNo ?? "—"}</td>
                      <td className="py-2 pr-3 font-mono text-gray-800">{row.slitNo || "—"}</td>
                      <td className="py-2 pr-3 tabular-nums">
                        {typeof row.numOfPipes === "number" ? row.numOfPipes : "—"}
                      </td>
                      <td className="py-2 pr-3 whitespace-nowrap text-gray-700">
                        {formatUtc(row.generatedAtUtc)}
                      </td>
                      <td className="py-2 pr-3 max-w-xs truncate font-mono text-xs text-gray-600" title={row.filePath}>
                        {row.fileName || "—"}
                      </td>
                      <td className="py-2 pr-3">
                        {row.fileExistsOnDisk ? (
                          <span className="text-green-700">Yes</span>
                        ) : (
                          <span className="text-amber-700">Missing</span>
                        )}
                      </td>
                      <td className="py-2">
                        <button
                          type="button"
                          onClick={() => void regenerate(row.ndtBatchNo)}
                          disabled={busy || regeneratingBatch != null}
                          className="text-sm font-medium text-primary-700 hover:text-primary-800 disabled:opacity-50"
                        >
                          {busy ? "Regenerating…" : "Regenerate"}
                        </button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-gray-500">
          Showing {filtered.length} of {items.length} batch(es).
        </p>
      </div>
    </div>
  );
}
