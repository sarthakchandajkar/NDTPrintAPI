"use client";

import { useCallback, useEffect, useState } from "react";
import {
  api,
  type FormationChartEntryRow,
  type SettingsPlcDiagnostics,
  type SettingsPrinterMill,
} from "@/lib/api";
import {
  clearSettingsToken,
  getSettingsToken,
  setSettingsToken,
} from "@/lib/settingsAuth";

type Tab = "formation" | "plc" | "printers";

function statusBadgeClass(status?: string): string {
  if (status === "Ready") return "bg-green-100 text-green-800";
  if (status === "Unreachable") return "bg-red-100 text-red-800";
  if (status === "NotConfigured") return "bg-gray-100 text-gray-600";
  return "bg-amber-100 text-amber-800";
}

export default function SettingsPage() {
  const [configured, setConfigured] = useState<boolean | null>(null);
  const [authenticated, setAuthenticated] = useState(false);
  const [password, setPassword] = useState("");
  const [loginError, setLoginError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("formation");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [formationRows, setFormationRows] = useState<FormationChartEntryRow[]>([]);
  const [formationSource, setFormationSource] = useState<string | null>(null);
  const [plc, setPlc] = useState<SettingsPlcDiagnostics | null>(null);
  const [printers, setPrinters] = useState<SettingsPrinterMill[]>([]);

  const refreshStatus = useCallback(async () => {
    try {
      const s = await api.settingsStatus();
      setConfigured(!!s.configured);
      const hasToken = !!getSettingsToken();
      setAuthenticated(hasToken && !!s.authenticated);
    } catch {
      setConfigured(false);
      setAuthenticated(false);
    }
  }, []);

  useEffect(() => {
    void refreshStatus();
  }, [refreshStatus]);

  const loadTabData = useCallback(async () => {
    const t = getSettingsToken();
    if (!t) return;
    setLoading(true);
    setError(null);
    try {
      if (tab === "formation") {
        const r = await api.settingsFormationChart(t);
        setFormationRows(Array.isArray(r.entries) ? r.entries : []);
        setFormationSource(typeof r.sourcePath === "string" ? r.sourcePath : null);
      } else if (tab === "plc") {
        setPlc(await api.settingsPlc(t));
      } else {
        const r = await api.settingsPrinters(t);
        const mills = Array.isArray(r.mills) ? r.mills : [];
        setPrinters(
          mills.length > 0
            ? mills
            : [1, 2, 3, 4].map((m) => ({ millNo: m, address: "", port: 9100 }))
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load settings");
    } finally {
      setLoading(false);
    }
  }, [tab]);

  useEffect(() => {
    if (authenticated) void loadTabData();
  }, [authenticated, loadTabData]);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoginError(null);
    try {
      const r = await api.settingsLogin(password);
      if (!r.token) throw new Error("No token returned");
      setSettingsToken(r.token);
      setAuthenticated(true);
      setPassword("");
    } catch (err) {
      setLoginError(err instanceof Error ? err.message : "Login failed");
    }
  };

  const handleLogout = async () => {
    const t = getSettingsToken();
    if (t) {
      try {
        await api.settingsLogout(t);
      } catch {
        // ignore
      }
    }
    clearSettingsToken();
    setAuthenticated(false);
  };

  const saveFormation = async () => {
    const t = getSettingsToken();
    if (!t) return;
    setMessage(null);
    setError(null);
    try {
      const res = await api.settingsSaveFormationChart(t, formationRows);
      setMessage(res.message ?? "Formation chart saved.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
    }
  };

  const savePrinters = async () => {
    const t = getSettingsToken();
    if (!t) return;
    setMessage(null);
    setError(null);
    try {
      const mills = printers.map((p) => ({
        millNo: p.millNo ?? 0,
        address: (p.address ?? "").trim(),
        port: p.port && p.port > 0 ? p.port : 9100,
      }));
      const res = await api.settingsSavePrinters(t, mills);
      setMessage(res.message ?? "Printer settings saved.");
      await loadTabData();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
    }
  };

  const testPrinter = async (millNo: number) => {
    const t = getSettingsToken();
    if (!t) return;
    setMessage(null);
    try {
      const r = await api.settingsTestPrinter(t, millNo);
      setMessage(`Mill ${millNo}: ${r.status ?? "—"} (${r.address ?? ""}:${r.port ?? 9100})`);
      await loadTabData();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Test failed");
    }
  };

  if (configured === false) {
    return (
      <div className="max-w-lg mx-auto mt-12 rounded-lg border border-amber-200 bg-amber-50 p-6 text-amber-900">
        <h1 className="text-xl font-semibold">Settings unavailable</h1>
        <p className="mt-2 text-sm">
          Set <code className="bg-white px-1 rounded">NdtBundle:DashboardSettings:AdminPassword</code> in{" "}
          <code className="bg-white px-1 rounded">appsettings.Production.json</code> on the server, then restart
          NdtBundleService.
        </p>
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="max-w-md mx-auto mt-12">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">Settings</h1>
        <p className="text-sm text-gray-600 mb-6">Enter the admin password to change formation thresholds, PLC, and printers.</p>
        <form onSubmit={handleLogin} className="bg-white rounded-lg border border-gray-200 shadow-sm p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              autoComplete="current-password"
            />
          </div>
          {loginError && <p className="text-sm text-red-600">{loginError}</p>}
          <button
            type="submit"
            className="w-full px-4 py-2 bg-primary-600 text-white rounded-md text-sm font-medium hover:bg-primary-700"
          >
            Sign in
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
          <p className="text-sm text-gray-600 mt-1">Formation chart, PLC connectivity, and per-mill printers.</p>
        </div>
        <button
          type="button"
          onClick={handleLogout}
          className="text-sm text-gray-600 hover:text-gray-900 underline"
        >
          Sign out
        </button>
      </div>

      {(message || error) && (
        <div
          className={`rounded-md border p-4 text-sm ${
            error ? "bg-red-50 border-red-200 text-red-700" : "bg-green-50 border-green-200 text-green-800"
          }`}
        >
          {error ?? message}
        </div>
      )}

      <div className="flex gap-2 border-b border-gray-200">
        {(
          [
            ["formation", "NDT bundle thresholds"],
            ["plc", "PLC connections"],
            ["printers", "Printers"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => setTab(id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px ${
              tab === id
                ? "border-primary-600 text-primary-700"
                : "border-transparent text-gray-600 hover:text-gray-900"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {loading ? (
        <p className="text-gray-500">Loading…</p>
      ) : tab === "formation" ? (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          {formationSource && (
            <p className="px-5 py-2 text-xs text-gray-500 border-b border-gray-100">File: {formationSource}</p>
          )}
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left font-medium text-gray-500">Pipe size</th>
                  <th className="px-4 py-2 text-left font-medium text-gray-500">NDT pcs / bundle</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {formationRows.map((row, i) => (
                  <tr key={`${row.pipeSize}-${i}`}>
                    <td className="px-4 py-2 font-medium text-gray-900">{row.pipeSize}</td>
                    <td className="px-4 py-2">
                      <input
                        type="number"
                        min={1}
                        className="w-28 border border-gray-300 rounded px-2 py-1"
                        value={row.requiredNdtPcs ?? 0}
                        onChange={(e) => {
                          const v = parseInt(e.target.value, 10);
                          setFormationRows((prev) =>
                            prev.map((r, j) =>
                              j === i ? { ...r, requiredNdtPcs: Number.isFinite(v) ? v : 0 } : r
                            )
                          );
                        }}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-5 py-4 border-t border-gray-100">
            <button
              type="button"
              onClick={saveFormation}
              className="px-4 py-2 bg-primary-600 text-white rounded-md text-sm font-medium hover:bg-primary-700"
            >
              Save thresholds
            </button>
          </div>
        </div>
      ) : tab === "plc" ? (
        <div className="space-y-4">
          <div className="bg-white rounded-lg border border-gray-200 p-5 text-sm space-y-2">
            <p>
              <span className="text-gray-500">Driver:</span>{" "}
              <span className="font-medium">{plc?.driver ?? "—"}</span>
              {plc?.plcPoEndEnabled ? "" : " (disabled)"}
            </p>
            <p>
              <span className="text-gray-500">Last PLC read:</span>{" "}
              <span className={plc?.lastReadOk ? "text-green-700 font-medium" : "text-red-700 font-medium"}>
                {plc?.lastReadOk ? "OK" : "Failed"}
              </span>
              {plc?.lastPlcError ? ` — ${plc.lastPlcError}` : ""}
            </p>
            <p className="text-gray-500 text-xs">Checked: {plc?.lastPlcCheckUtc ?? "—"}</p>
          </div>
          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left">Mill</th>
                  <th className="px-4 py-2 text-left">Host</th>
                  <th className="px-4 py-2 text-left">Port</th>
                  <th className="px-4 py-2 text-left">TCP</th>
                  <th className="px-4 py-2 text-left">PO end signal</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {(plc?.mills ?? []).map((m) => (
                  <tr key={m.millNo}>
                    <td className="px-4 py-2 font-medium">Mill-{m.millNo}</td>
                    <td className="px-4 py-2">{m.host || "—"}</td>
                    <td className="px-4 py-2">{m.port ?? "—"}</td>
                    <td className="px-4 py-2">
                      <span
                        className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                          m.reachable ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
                        }`}
                      >
                        {m.reachable ? "Reachable" : "Unreachable"}
                      </span>
                    </td>
                    <td className="px-4 py-2">
                      {plc?.poEndByMill?.[String(m.millNo)] ? "Active" : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <button
            type="button"
            onClick={loadTabData}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Refresh PLC status
          </button>
        </div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <p className="px-5 py-3 text-sm text-gray-600 border-b border-gray-100">
            One network printer per mill (TCP port 9100). Mill 1 falls back to legacy{" "}
            <code className="text-xs bg-gray-100 px-1">NdtTagPrinterAddress</code> when empty.
          </p>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left">Mill</th>
                  <th className="px-4 py-2 text-left">IP / host</th>
                  <th className="px-4 py-2 text-left">Port</th>
                  <th className="px-4 py-2 text-left">Status</th>
                  <th className="px-4 py-2 text-left">Test</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {printers.map((p, i) => (
                  <tr key={p.millNo ?? i}>
                    <td className="px-4 py-2 font-medium">Mill-{p.millNo}</td>
                    <td className="px-4 py-2">
                      <input
                        type="text"
                        className="w-full min-w-[10rem] border border-gray-300 rounded px-2 py-1"
                        placeholder="192.168.0.125"
                        value={p.address ?? ""}
                        onChange={(e) =>
                          setPrinters((prev) =>
                            prev.map((row, j) => (j === i ? { ...row, address: e.target.value } : row))
                          )
                        }
                      />
                    </td>
                    <td className="px-4 py-2">
                      <input
                        type="number"
                        min={1}
                        className="w-20 border border-gray-300 rounded px-2 py-1"
                        value={p.port ?? 9100}
                        onChange={(e) => {
                          const v = parseInt(e.target.value, 10);
                          setPrinters((prev) =>
                            prev.map((row, j) =>
                              j === i ? { ...row, port: Number.isFinite(v) ? v : 9100 } : row
                            )
                          );
                        }}
                      />
                    </td>
                    <td className="px-4 py-2">
                      <span
                        className={`px-2 py-0.5 rounded-full text-xs font-medium ${statusBadgeClass(p.status)}`}
                      >
                        {p.status ?? "—"}
                      </span>
                    </td>
                    <td className="px-4 py-2">
                      <button
                        type="button"
                        onClick={() => p.millNo && testPrinter(p.millNo)}
                        className="text-primary-600 hover:text-primary-800 text-sm font-medium"
                      >
                        Test
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-5 py-4 border-t border-gray-100 flex gap-3">
            <button
              type="button"
              onClick={savePrinters}
              className="px-4 py-2 bg-primary-600 text-white rounded-md text-sm font-medium hover:bg-primary-700"
            >
              Save printers
            </button>
            <button
              type="button"
              onClick={loadTabData}
              className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              Refresh status
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
