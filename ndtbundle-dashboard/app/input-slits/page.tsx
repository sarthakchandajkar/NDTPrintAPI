"use client";

import { useEffect, useMemo, useState } from "react";
import { api, type InputSlitFile, type InputSlitContent } from "@/lib/api";

type FileContentState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "loaded"; content: InputSlitContent }
  | { status: "error"; message: string };

type SlitRow = {
  poNumber: string;
  slitNo: string;
  ndtPipes: string;
  rejectedP: string;
  slitStartTime: string;
  slitFinishTime: string;
  millNo: string;
  ndtShortLengthPipe: string;
  rejectedShortLengthPipe: string;
};

const EXCEL_HEADERS: Array<{ key: keyof SlitRow; label: string }> = [
  { key: "poNumber", label: "PO Number" },
  { key: "slitNo", label: "Slit No" },
  { key: "ndtPipes", label: "NDT Pipes" },
  { key: "rejectedP", label: "Rejected P" },
  { key: "slitStartTime", label: "Slit Start Time" },
  { key: "slitFinishTime", label: "Slit Finish Time" },
  { key: "millNo", label: "Mill No" },
  { key: "ndtShortLengthPipe", label: "NDT Short Length Pipe" },
  { key: "rejectedShortLengthPipe", label: "Rejected Short Length Pipe" },
];

function normHeader(h: string): string {
  return (h ?? "").toLowerCase().replace(/\s+/g, " ").trim();
}

function buildHeaderIndex(headers: string[] | undefined): Record<string, number> {
  const idx: Record<string, number> = {};
  (headers ?? []).forEach((h, i) => {
    const k = normHeader(h);
    if (k && idx[k] === undefined) idx[k] = i;
  });
  return idx;
}

function cellAt(row: string[] | undefined, index: number | undefined): string {
  if (index === undefined || index < 0) return "";
  const v = row?.[index];
  return (v ?? "").toString();
}

function mapToSlitRow(content: InputSlitContent, row: string[]): SlitRow {
  const idx = buildHeaderIndex(content.headers);

  // Prefer header-based mapping (case/space-insensitive). If header is missing, return blank for that field.
  return {
    poNumber: cellAt(row, idx[normHeader("PO Number")]),
    slitNo: cellAt(row, idx[normHeader("Slit No")]),
    ndtPipes: cellAt(row, idx[normHeader("NDT Pipes")]),
    rejectedP: cellAt(row, idx[normHeader("Rejected P")]),
    slitStartTime: cellAt(row, idx[normHeader("Slit Start Time")]),
    slitFinishTime: cellAt(row, idx[normHeader("Slit Finish Time")]),
    millNo: cellAt(row, idx[normHeader("Mill No")]),
    ndtShortLengthPipe: cellAt(row, idx[normHeader("NDT Short Length Pipe")]),
    rejectedShortLengthPipe: cellAt(row, idx[normHeader("Rejected Short Length Pipe")]),
  };
}

export default function InputSlitsPage() {
  const [files, setFiles] = useState<InputSlitFile[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);
  const [fileContents, setFileContents] = useState<Record<string, FileContentState>>({});

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.inputSlitFiles();
      setFiles(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load files");
      setFiles([]);
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

  useEffect(() => {
    // Auto-load content for each file so operators can see everything at a glance.
    // We keep previously-loaded content while refreshing the list.
    const fileNames = (files ?? [])
      .map((f) => (f.fileName ?? "").trim())
      .filter((x) => x.length > 0);

    // Drop content state for files no longer present.
    setFileContents((prev) => {
      const next: Record<string, FileContentState> = {};
      for (const name of fileNames) next[name] = prev[name] ?? { status: "idle" };
      return next;
    });

    for (const name of fileNames) {
      setFileContents((prev) => {
        const existing = prev[name];
        if (existing?.status === "loading" || existing?.status === "loaded") return prev;
        return { ...prev, [name]: { status: "loading" } };
      });

      api
        .inputSlitContent(name)
        .then((c) => setFileContents((prev) => ({ ...prev, [name]: { status: "loaded", content: c } })))
        .catch((e) =>
          setFileContents((prev) => ({
            ...prev,
            [name]: { status: "error", message: e instanceof Error ? e.message : "Failed to load content." },
          }))
        );
    }
  }, [files]);

  const excelRows = useMemo(() => {
    const out: Array<SlitRow & { _key: string }> = [];
    const fileNames = (files ?? [])
      .map((f) => (f.fileName ?? "").trim())
      .filter((x) => x.length > 0);

    for (const fileName of fileNames) {
      const st = fileContents[fileName];
      if (!st || st.status !== "loaded") continue;
      const c = st.content;
      const rows = Array.isArray(c?.rows) ? c.rows : [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        out.push({ _key: `${fileName}:${i}`, ...mapToSlitRow(c, r) });
      }
    }
    return out;
  }, [files, fileContents]);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Input Slit Files</h1>
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
          Input Slit Rows (Excel View)
        </h2>
        {loading ? (
          <p className="px-5 py-8 text-gray-500">Loading...</p>
        ) : files.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">No CSV files in input slit folder.</p>
        ) : excelRows.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">No rows loaded yet (files may still be reading).</p>
        ) : (
          <div className="overflow-x-auto max-h-[70vh] overflow-y-auto">
            <table className="min-w-full divide-y divide-gray-200 text-sm">
              <thead className="bg-gray-50 sticky top-0">
                <tr>
                  {EXCEL_HEADERS.map((h) => (
                    <th
                      key={h.key}
                      className="px-3 py-2 text-left font-medium text-gray-500 whitespace-nowrap"
                    >
                      {h.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {excelRows.map((r) => (
                  <tr key={r._key} className="hover:bg-gray-50">
                    {EXCEL_HEADERS.map((h) => (
                      <td key={h.key} className="px-3 py-2 text-gray-700 whitespace-nowrap">
                        {r[h.key]}
                      </td>
                    ))}
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
