"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type WipInfo, type NdtSummary, type BundleFile } from "@/lib/api";

export default function SummaryPage() {
  const [wip, setWip] = useState<WipInfo | null>(null);
  const [summary, setSummary] = useState<NdtSummary | null>(null);
  const [bundles, setBundles] = useState<BundleFile[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [poNumber, setPoNumber] = useState("");
  const [millNo, setMillNo] = useState(1);
  const [dummyPrinting, setDummyPrinting] = useState(false);
  const [dummyPrintMessage, setDummyPrintMessage] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    setDummyPrintMessage(null);
    try {
      const [wipRes, bundlesRes] = await Promise.all([
        api.wipInfo().catch(() => null),
        api.bundles().catch(() => []),
      ]);
      setWip(wipRes ?? null);
      setBundles(Array.isArray(bundlesRes) ? bundlesRes : []);
      const po = (wipRes?.poNumber ?? (poNumber || "")).trim();
      if (po) {
        setPoNumber(po);
        const sum = await api.ndtSummary(po, millNo).catch(() => null);
        setSummary(sum ?? null);
      } else {
        setSummary(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  const printDummyBundle = async () => {
    setDummyPrintMessage(null);
    setDummyPrinting(true);
    try {
      const res = await api.printDummyBundle();
      const sent = res.sentToPrinter === true;
      setDummyPrintMessage({
        type: sent ? "ok" : "err",
        text: res.message ?? (sent ? "Dummy tag sent to printer." : "PDF saved; not sent to printer."),
      });
    } catch (e) {
      const isAbort = e instanceof Error && e.name === "AbortError";
      setDummyPrintMessage({
        type: "err",
        text: isAbort
          ? "Request timed out. PDF may have been saved to the output folder; printer may be unreachable."
          : (e instanceof Error ? e.message : "Print failed."),
      });
    } finally {
      setDummyPrinting(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    if (poNumber.trim()) {
      api.ndtSummary(poNumber.trim(), millNo).then(setSummary).catch(() => setSummary(null));
    } else {
      setSummary(null);
    }
  }, [poNumber, millNo]);

  if (loading && !wip && !summary)
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-gray-500">Loading...</div>
      </div>
    );

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Summary</h1>
        <button
          onClick={refresh}
          className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {error}
        </div>
      )}
      {dummyPrintMessage && (
        <div
          className={`rounded-md p-4 text-sm ${
            dummyPrintMessage.type === "ok"
              ? "bg-green-50 text-green-800 border border-green-200"
              : "bg-red-50 text-red-700 border border-red-200"
          }`}
        >
          {dummyPrintMessage.text}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-5">
          <p className="text-sm font-medium text-gray-500">Current PO</p>
          <p className="mt-1 text-xl font-semibold text-gray-900">{wip?.poNumber ?? "—"}</p>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-5">
          <p className="text-sm font-medium text-gray-500">Mill No</p>
          <p className="mt-1 text-xl font-semibold text-gray-900">{wip?.millNumber ?? millNo}</p>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-5 border-l-4 border-l-primary-500">
          <p className="text-sm font-medium text-gray-500">NDT Pipes (this PO / mill)</p>
          <p className="mt-1 text-2xl font-bold text-primary-600">{summary?.totalNdtPipes ?? 0}</p>
        </div>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          WIP / Current PO Plan
        </h2>
        {wip ? (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <tbody className="divide-y divide-gray-200">
                {[
                  ["PO Number", wip.poNumber],
                  ["Mill Number", wip.millNumber],
                  ["Pipe Size", wip.pipeSize],
                  ["Pipe Length", wip.pipeLength],
                  ["Pieces Per Bundle", wip.piecesPerBundle],
                  ["Total Pieces", wip.totalPieces],
                  ["Planned Month", wip.plannedMonth],
                ].map(([label, value]) => (
                  <tr key={String(label)} className="hover:bg-gray-50">
                    <td className="px-5 py-2 text-sm font-medium text-gray-500 w-48">{label}</td>
                    <td className="px-5 py-2 text-sm text-gray-900">{value ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="px-5 py-8 text-gray-500 text-sm">No WIP data. Check service URL and that NdtBundleService is running.</p>
        )}
      </div>

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-5 py-3 bg-primary-50 flex justify-between items-center border-b border-gray-200">
          <h2 className="font-semibold text-gray-900">Bundle CSV Files</h2>
          <span className="text-sm text-gray-600">{bundles.length} file(s)</span>
        </div>
        {bundles.length > 0 ? (
          <ul className="divide-y divide-gray-200 max-h-48 overflow-y-auto">
            {bundles.map((b) => (
              <li key={b.fileName ?? b.fullPath} className="px-5 py-2 text-sm text-gray-700">
                {b.fileName}
              </li>
            ))}
          </ul>
        ) : (
          <p className="px-5 py-8 text-gray-500 text-sm">No bundle files yet.</p>
        )}
      </div>

      <div className="flex flex-wrap gap-3">
        <Link
          href="/po-end"
          className="inline-flex items-center px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600"
        >
          Simulate PO End
        </Link>
        <button
          type="button"
          onClick={printDummyBundle}
          disabled={dummyPrinting}
          className="inline-flex items-center px-4 py-2 border border-primary-500 text-primary-700 bg-white text-sm font-medium rounded-md hover:bg-primary-50 disabled:opacity-50"
        >
          {dummyPrinting ? "Sending…" : "Print dummy bundle"}
        </button>
      </div>
    </div>
  );
}
