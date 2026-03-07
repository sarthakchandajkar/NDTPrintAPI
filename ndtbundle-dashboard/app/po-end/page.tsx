"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type WipInfo } from "@/lib/api";

export default function PoEndPage() {
  const [poNumber, setPoNumber] = useState("");
  const [millNo, setMillNo] = useState(1);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  useEffect(() => {
    api.wipInfo().then((wip: WipInfo) => {
      if (wip?.poNumber) setPoNumber(wip.poNumber);
    }).catch(() => {});
  }, []);

  const submit = async () => {
    const po = poNumber.trim();
    if (!po) {
      setMessage({ type: "err", text: "Enter PO Number." });
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      await api.poEnd(po, millNo);
      setMessage({ type: "ok", text: "PO end simulated successfully." });
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Request failed." });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Simulate PO End</h1>
      <p className="text-gray-600 text-sm">
        Trigger PO end for the current PO and mill. This closes any pending bundles and advances to the next PO plan
        file when using the PO Plan folder.
      </p>

      {message && (
        <div
          className={`rounded-md p-4 text-sm ${
            message.type === "ok" ? "bg-green-50 text-green-800 border border-green-200" : "bg-red-50 text-red-700 border border-red-200"
          }`}
        >
          {message.text}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 max-w-md space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">PO Number</label>
          <input
            type="text"
            value={poNumber}
            onChange={(e) => setPoNumber(e.target.value)}
            placeholder="e.g. 1000055673"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Mill No</label>
          <select
            value={millNo}
            onChange={(e) => setMillNo(parseInt(e.target.value, 10))}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
          >
            {[1, 2, 3, 4].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </div>
        <div className="flex gap-3">
          <button
            onClick={submit}
            disabled={loading}
            className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-md hover:bg-primary-600 disabled:opacity-50"
          >
            {loading ? "Sending…" : "Simulate PO End"}
          </button>
          <Link
            href="/"
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Back to Summary
          </Link>
        </div>
      </div>
    </div>
  );
}
