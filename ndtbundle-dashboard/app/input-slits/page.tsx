"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { api, type InputSlitFile, type InputSlitContent } from "@/lib/api";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import {
  EMPTY_DATE_RANGE,
  isInDateRange,
  isDateRangeActive,
  parseInputSlitFileNameDate,
  toDateInputValue,
  type DateRange,
} from "@/lib/dateRangeFilter";

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

function resolveSlitRowDate(
  mapped: SlitRow,
  fileName: string,
  fileModified?: string
): string | null {
  const finish = mapped.slitFinishTime?.trim();
  if (finish) return finish;
  const start = mapped.slitStartTime?.trim();
  if (start) return start;
  const fromName = parseInputSlitFileNameDate(fileName);
  if (fromName) return toDateInputValue(fromName);
  const modified = fileModified?.trim();
  return modified || null;
}

/** SAP slit wall-clock: dd.MM.yyyy HH:mm:ss (e.g. 27.09.2026 14:53:31). */
const SAP_SLIT_DATETIME = /^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2}):(\d{2})$/;

/**
 * Normalize a pasted/typed wall-clock value for SAP slit times.
 * Keeps free typing; on paste strips quotes/NBSP and collapses whitespace so
 * `27.09.2026 14:53:31` can be pasted directly from Excel/SAP.
 */
function normalizeSapSlitDateTimeInput(raw: string): string {
  let s = (raw ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/[\t\r\n]+/g, " ")
    .trim();
  if (
    (s.startsWith('"') && s.endsWith('"')) ||
    (s.startsWith("'") && s.endsWith("'"))
  ) {
    s = s.slice(1, -1).trim();
  }
  s = s.replace(/\s+/g, " ");
  return s;
}

function isSapSlitDateTime(value: string): boolean {
  const m = SAP_SLIT_DATETIME.exec(normalizeSapSlitDateTimeInput(value));
  if (!m) return false;
  const day = Number(m[1]);
  const month = Number(m[2]);
  const year = Number(m[3]);
  const hour = Number(m[4]);
  const min = Number(m[5]);
  const sec = Number(m[6]);
  if (month < 1 || month > 12 || day < 1 || day > 31) return false;
  if (hour > 23 || min > 59 || sec > 59) return false;
  const dt = new Date(year, month - 1, day, hour, min, sec);
  return (
    !Number.isNaN(dt.getTime()) &&
    dt.getFullYear() === year &&
    dt.getMonth() === month - 1 &&
    dt.getDate() === day
  );
}

/** YYMMDD from a validated SAP slit datetime string. */
function yyMmDdFromSapSlitDateTime(value: string): string | null {
  const m = SAP_SLIT_DATETIME.exec(normalizeSapSlitDateTimeInput(value));
  if (!m) return null;
  return `${m[3]!.slice(-2)}${m[2]}${m[1]}`;
}

function previewManualFileName(slitNo: string, start: string, po: string): string {
  const s = slitNo.trim();
  const p = po.trim();
  const d = yyMmDdFromSapSlitDateTime(start);
  if (!s || !p || !d) return "SlitNumber_YYMMDD_PONumber";
  return `${s}_${d}_${p}`;
}

function SapSlitDateTimeField({
  id,
  label,
  value,
  onChange,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (next: string) => void;
}) {
  return (
    <div>
      <label htmlFor={id} className="block text-sm font-medium text-gray-700 mb-1">
        {label}
      </label>
      <input
        id={id}
        type="text"
        inputMode="text"
        autoComplete="off"
        spellCheck={false}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onPaste={(e) => {
          const pasted = e.clipboardData.getData("text");
          if (!pasted) return;
          e.preventDefault();
          onChange(normalizeSapSlitDateTimeInput(pasted));
        }}
        onBlur={() => onChange(normalizeSapSlitDateTimeInput(value))}
        placeholder="27.09.2026 14:53:31"
        title="Paste or type dd.MM.yyyy HH:mm:ss"
        className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500 font-mono"
      />
      <p className="mt-1 text-xs text-gray-500">Paste dd.MM.yyyy HH:mm:ss directly</p>
    </div>
  );
}

export default function InputSlitsPage() {
  const [files, setFiles] = useState<InputSlitFile[]>([]);
  const [dateRange, setDateRange] = useState<DateRange>(EMPTY_DATE_RANGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [secondsUntilRefresh, setSecondsUntilRefresh] = useState(30);
  const [fileContents, setFileContents] = useState<Record<string, FileContentState>>({});

  const [manualPo, setManualPo] = useState("");
  const [manualMill, setManualMill] = useState(1);
  const [manualSlitNo, setManualSlitNo] = useState("");
  const [manualNdt, setManualNdt] = useState(0);
  const [manualRejected, setManualRejected] = useState(0);
  const [manualBatchNo, setManualBatchNo] = useState("");
  const [manualStart, setManualStart] = useState("");
  const [manualFinish, setManualFinish] = useState("");
  const [manualShort, setManualShort] = useState("");
  const [manualRejShort, setManualRejShort] = useState("");
  const [manualBusy, setManualBusy] = useState(false);
  const [manualError, setManualError] = useState<string | null>(null);
  const [manualSuccess, setManualSuccess] = useState<string | null>(null);

  const manualFilePreview = previewManualFileName(manualSlitNo, manualStart, manualPo);

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

  const createManualInputSlit = async () => {
    const po = manualPo.trim();
    const batch = manualBatchNo.trim();
    const slitNo = manualSlitNo.trim();
    const start = normalizeSapSlitDateTimeInput(manualStart);
    const finish = normalizeSapSlitDateTimeInput(manualFinish);
    if (!po) {
      setManualError("Please enter a PO Number.");
      setManualSuccess(null);
      return;
    }
    if (!slitNo) {
      setManualError("Please enter a Slit No (used in file name SlitNumber_YYMMDD_PONumber).");
      setManualSuccess(null);
      return;
    }
    if (!batch) {
      setManualError("Please enter an NDT Batch No.");
      setManualSuccess(null);
      return;
    }
    if (!isSapSlitDateTime(start)) {
      setManualError("Slit Start Time is required in format 27.09.2026 14:53:31");
      setManualSuccess(null);
      return;
    }
    if (!isSapSlitDateTime(finish)) {
      setManualError("Slit Finish Time is required in format 27.09.2026 14:53:31");
      setManualSuccess(null);
      return;
    }
    setManualBusy(true);
    setManualError(null);
    setManualSuccess(null);
    try {
      const res = await api.createManualInputSlit({
        poNumber: po,
        millNo: manualMill,
        slitNo,
        ndtPipes: manualNdt,
        rejectedPipes: manualRejected,
        ndtBatchNo: batch,
        slitStartTime: start,
        slitFinishTime: finish,
        ndtShortLengthPipe: manualShort.trim() || undefined,
        rejectedShortLengthPipe: manualRejShort.trim() || undefined,
      });
      const name = res.fileName ? ` (${res.fileName})` : "";
      const folder = res.folder ? ` in ${res.folder}` : "";
      setManualSuccess(
        (res.message ?? "NDT Input Slit output CSV created and Output_Slit_Row recorded.") + name + folder
      );
    } catch (e) {
      setManualError(e instanceof Error ? e.message : "Failed to create NDT Input Slit output CSV.");
    } finally {
      setManualBusy(false);
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
    const out: Array<SlitRow & { _key: string; _fileModified?: string; _fileName: string }> = [];
    const fileNames = (files ?? [])
      .map((f) => (f.fileName ?? "").trim())
      .filter((x) => x.length > 0);

    const fileModifiedByName = new Map(
      files.map((f) => [(f.fileName ?? "").trim(), f.lastModified ?? ""])
    );

    for (const fileName of fileNames) {
      const st = fileContents[fileName];
      if (st?.status !== "loaded") continue;
      const rows = st.content.rows ?? [];
      rows.forEach((row, i) => {
        const mapped = mapToSlitRow(st.content, row);
        out.push({
          ...mapped,
          _key: `${fileName}:${i}`,
          _fileName: fileName,
          _fileModified: fileModifiedByName.get(fileName),
        });
      });
    }

    if (!isDateRangeActive(dateRange)) return out;
    return out.filter((r) => {
      const d = resolveSlitRowDate(r, r._fileName, r._fileModified);
      return isInDateRange(d, dateRange);
    });
  }, [files, fileContents, dateRange]);

  const filesWithRowsInRange = useMemo(() => {
    const names = new Set(excelRows.map((r) => r._fileName));
    return names.size;
  }, [excelRows]);

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

      <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          Manual NDT Input Slit output
        </div>
        <div className="p-5 space-y-4">
          <p className="text-sm text-gray-600">
            Creates a CSV in the NDT Input Slit output folder (
            <code className="text-xs">…\TM\NDT\NDT Input Slit\Input Slit</code>
            ), including <strong>NDT Batch No</strong>, and writes the matching{" "}
            <code className="text-xs">Output_Slit_Row</code> in SQL for SAP. Does{" "}
            <strong>not</strong> write to the SAP Input Slit inbox. File name is always{" "}
            <code className="text-xs">SlitNumber_YYMMDD_PONumber</code> (YYMMDD from Slit Start
            Time). Times must be <code className="text-xs">dd.MM.yyyy HH:mm:ss</code> (e.g.{" "}
            <code className="text-xs">27.09.2026 14:53:31</code>). For wrong printed totals, use{" "}
            <Link href="/reconcile" className="text-primary-700 hover:underline font-medium">
              Reconcile Bundle
            </Link>
            .
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">PO Number</label>
              <input
                value={manualPo}
                onChange={(e) => setManualPo(e.target.value)}
                placeholder="e.g. 1000055673"
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Mill No</label>
              <select
                value={manualMill}
                onChange={(e) => setManualMill(parseInt(e.target.value, 10) || 1)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              >
                {[1, 2, 3, 4].map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">NDT Batch No</label>
              <input
                value={manualBatchNo}
                onChange={(e) => setManualBatchNo(e.target.value)}
                placeholder="Required"
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Slit No</label>
              <input
                value={manualSlitNo}
                onChange={(e) => setManualSlitNo(e.target.value)}
                placeholder="Required (file name segment)"
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">NDT Pipes</label>
              <input
                type="number"
                min={0}
                value={manualNdt}
                onChange={(e) => setManualNdt(parseInt(e.target.value, 10) || 0)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Rejected P</label>
              <input
                type="number"
                min={0}
                value={manualRejected}
                onChange={(e) => setManualRejected(parseInt(e.target.value, 10) || 0)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <SapSlitDateTimeField
              id="manual-slit-start"
              label="Slit Start Time"
              value={manualStart}
              onChange={setManualStart}
            />
            <SapSlitDateTimeField
              id="manual-slit-finish"
              label="Slit Finish Time"
              value={manualFinish}
              onChange={setManualFinish}
            />
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">NDT Short Length Pipe</label>
              <input
                value={manualShort}
                onChange={(e) => setManualShort(e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Rejected Short Length Pipe</label>
              <input
                value={manualRejShort}
                onChange={(e) => setManualRejShort(e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div className="sm:col-span-2 lg:col-span-4">
              <label className="block text-sm font-medium text-gray-700 mb-1">Output file name</label>
              <p className="w-full border border-gray-200 bg-gray-50 rounded-md px-3 py-2 text-sm font-mono text-gray-800">
                {manualFilePreview}
              </p>
              <p className="mt-1 text-xs text-gray-500">
                Built automatically from Slit No + Slit Start Time (YYMMDD) + PO Number. Not editable.
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => void createManualInputSlit()}
            disabled={manualBusy}
            className="px-4 py-2 bg-primary-600 text-white text-sm font-medium rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:pointer-events-none"
          >
            {manualBusy ? "Creating…" : "Create NDT Input Slit CSV"}
          </button>
          {manualError && (
            <div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-sm text-red-700">
              {manualError}
            </div>
          )}
          {manualSuccess && (
            <div className="rounded-md bg-green-50 border border-green-200 px-3 py-2 text-sm text-green-800">
              {manualSuccess}
            </div>
          )}
        </div>
      </section>

      <DateRangeFilter
        value={dateRange}
        onChange={setDateRange}
        summary={
          isDateRangeActive(dateRange)
            ? `${excelRows.length} row(s) · ${filesWithRowsInRange} of ${files.length} file(s)`
            : `${files.length} file(s)`
        }
        hint="Rows match Slit Finish Time, then Slit Start Time, then the yyMMdd token in the file name, then file last-modified."
      />

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          Input Slit Rows (Excel View)
          {isDateRangeActive(dateRange) && (
            <span className="ml-2 text-sm font-normal text-gray-600">
              — {excelRows.length} row(s) in range
            </span>
          )}
        </h2>
        {loading ? (
          <p className="px-5 py-8 text-gray-500">Loading...</p>
        ) : files.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">No CSV files in input slit folder.</p>
        ) : excelRows.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">
            {isDateRangeActive(dateRange)
              ? "No rows in the selected date range (files may still be loading)."
              : "No rows loaded yet."}
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {EXCEL_HEADERS.map((h) => (
                    <th
                      key={h.key}
                      className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider"
                    >
                      {h.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-100">
                {excelRows.map((row) => (
                  <tr key={row._key} className="hover:bg-gray-50">
                    {EXCEL_HEADERS.map((h) => (
                      <td key={h.key} className="px-4 py-2 text-gray-800 whitespace-nowrap">
                        {row[h.key] || "—"}
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
