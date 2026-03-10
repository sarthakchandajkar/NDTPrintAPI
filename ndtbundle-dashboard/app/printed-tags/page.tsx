"use client";

import { useEffect, useState } from "react";
import { api, type ReconcileBundle } from "@/lib/api";

export default function PrintedTagsPage() {
  const [bundles, setBundles] = useState<ReconcileBundle[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.reconcileBundles();
      setBundles(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
      setBundles([]);
    } finally {
      setLoading(false);
      setSecondsUntilRefresh(30);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setSecondsUntilRefresh((s) => {
        if (s <= 1) {
          refresh();
          return 30;
        }
        return s - 1;
      });
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Printed Tags</h1>
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-500">Next refresh in {secondsUntilRefresh}s</span>
          <button
            onClick={refresh}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {error}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          Bundles (printed tags)
        </h2>
        {loading ? (
          <p className="px-5 py-8 text-gray-500">Loading...</p>
        ) : bundles.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">No printed bundles yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">NDT Batch No</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">PO Number</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Mill No</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Slit No</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">NDT Pipes</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {bundles.map((b) => (
                  <tr key={b.bundleNo} className="hover:bg-gray-50">
                    <td className="px-5 py-2 text-sm font-medium text-gray-900">{b.bundleNo}</td>
                    <td className="px-5 py-2 text-sm text-gray-700">{b.poNumber}</td>
                    <td className="px-5 py-2 text-sm text-gray-700">{b.millNo}</td>
                    <td className="px-5 py-2 text-sm text-gray-700">{b.slitNo}</td>
                    <td className="px-5 py-2 text-sm text-gray-700">{b.totalNdtPcs}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
