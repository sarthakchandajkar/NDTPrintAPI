"use client";

import { useEffect, useState } from "react";
import { api, type ReprintPrinterOption, type ReprintPrinterTarget } from "@/lib/api";

function statusBadgeClass(status?: string): string {
  switch (status) {
    case "Ready":
      return "bg-green-100 text-green-800";
    case "Unreachable":
      return "bg-red-100 text-red-800";
    case "NotConfigured":
      return "bg-gray-100 text-gray-600";
    default:
      return "bg-amber-100 text-amber-800";
  }
}

function statusLabel(status?: string): string {
  switch (status) {
    case "Ready":
      return "Reachable";
    case "Unreachable":
      return "Unreachable";
    case "NotConfigured":
      return "Not configured";
    default:
      return status ?? "Checking…";
  }
}

export interface ReprintPrinterChooserProps {
  open: boolean;
  millNo: number;
  title: string;
  subtitle?: string;
  confirmLabel?: string;
  busy?: boolean;
  onCancel: () => void;
  onConfirm: (printerTarget: ReprintPrinterTarget) => void;
}

export function ReprintPrinterChooser({
  open,
  millNo,
  title,
  subtitle,
  confirmLabel = "Print",
  busy = false,
  onCancel,
  onConfirm,
}: ReprintPrinterChooserProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [printers, setPrinters] = useState<ReprintPrinterOption[]>([]);
  const [selected, setSelected] = useState<ReprintPrinterTarget | null>(null);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    setSelected(null);
    setPrinters([]);
    void (async () => {
      try {
        const res = await api.reprintPrinters(millNo);
        if (cancelled) return;
        const list = Array.isArray(res.printers) ? res.printers : [];
        setPrinters(list);
        const firstReady = list.find((p) => p.configured && p.reachable);
        const firstConfigured = list.find((p) => p.configured);
        setSelected(
          (firstReady?.printerTarget ??
            firstConfigured?.printerTarget ??
            list[0]?.printerTarget ??
            null) as ReprintPrinterTarget | null
        );
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to load printer status.");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, millNo]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="reprint-printer-chooser-title"
    >
      <div className="w-full max-w-md rounded-lg border border-gray-200 bg-white shadow-xl">
        <div className="border-b border-gray-100 px-5 py-4">
          <h2 id="reprint-printer-chooser-title" className="text-lg font-semibold text-gray-900">
            {title}
          </h2>
          {subtitle ? <p className="mt-1 text-sm text-gray-600">{subtitle}</p> : null}
        </div>

        <div className="px-5 py-4 space-y-3">
          {loading ? (
            <p className="text-sm text-gray-500">Checking printer reachability…</p>
          ) : error ? (
            <p className="text-sm text-red-700">{error}</p>
          ) : printers.length === 0 ? (
            <p className="text-sm text-gray-500">No reprint printers configured.</p>
          ) : (
            <ul className="space-y-2">
              {printers.map((p) => {
                const target = (p.printerTarget ?? "mill") as ReprintPrinterTarget;
                const disabled = !p.configured || busy;
                const checked = selected === target;
                return (
                  <li key={target}>
                    <label
                      className={`flex cursor-pointer items-start gap-3 rounded-md border px-3 py-2.5 ${
                        checked
                          ? "border-violet-400 bg-violet-50"
                          : "border-gray-200 bg-white hover:bg-gray-50"
                      } ${disabled ? "opacity-60 cursor-not-allowed" : ""}`}
                    >
                      <input
                        type="radio"
                        name="reprint-printer"
                        className="mt-1"
                        disabled={disabled}
                        checked={checked}
                        onChange={() => setSelected(target)}
                      />
                      <span className="flex-1 min-w-0">
                        <span className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-medium text-gray-900">
                            {p.displayName ?? target}
                          </span>
                          <span
                            className={`px-2 py-0.5 rounded-full text-[11px] font-medium ${statusBadgeClass(
                              p.status
                            )}`}
                          >
                            {statusLabel(p.status)}
                          </span>
                        </span>
                        <span className="mt-0.5 block text-xs text-gray-500 font-mono">
                          {(p.address ?? "").trim()
                            ? `${p.address}:${p.port ?? 9100}`
                            : "No IP configured"}
                        </span>
                      </span>
                    </label>
                  </li>
                );
              })}
            </ul>
          )}
        </div>

        <div className="flex justify-end gap-2 border-t border-gray-100 px-5 py-3">
          <button
            type="button"
            onClick={onCancel}
            disabled={busy}
            className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={busy || loading || !selected || !!error}
            onClick={() => selected && onConfirm(selected)}
            className="px-3 py-1.5 text-sm font-medium rounded-md bg-violet-700 text-white hover:bg-violet-800 disabled:opacity-50"
          >
            {busy ? "Printing…" : confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
