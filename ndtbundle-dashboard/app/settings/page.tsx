"use client";

import { useCallback, useEffect, useState } from "react";
import {
  api,
  type FormationChartEntryRow,
  type SettingsPlcDiagnostics,
  type SettingsPlcMill,
  type SettingsPoChangeTestResult,
  type SettingsPrinterMill,
} from "@/lib/api";
import { LineRunningLamp } from "@/components/LineRunningLamp";
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

function formatUtc(iso?: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

function HooterOutputBadge({ active }: { active?: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-semibold ${
        active
          ? "bg-amber-100 text-amber-900 ring-2 ring-amber-400 animate-pulse"
          : "bg-gray-100 text-gray-600"
      }`}
      title="Q6.7 hooter output"
    >
      <span
        className={`h-2 w-2 rounded-full ${active ? "bg-amber-500" : "bg-gray-400"}`}
      />
      {active ? "HOOTER ON" : "Off"}
    </span>
  );
}

function BundleProgressBar({
  accumulated,
  threshold,
}: {
  accumulated: number;
  threshold: number;
}) {
  if (threshold <= 0) {
    return <span className="text-gray-400 text-xs">—</span>;
  }
  const pct = Math.min(100, Math.round((accumulated / threshold) * 100));
  const near = accumulated > threshold;
  return (
    <div className="min-w-[8rem]">
      <div className="flex justify-between text-[10px] text-gray-500 mb-0.5 tabular-nums">
        <span>{accumulated}</span>
        <span>/ {threshold}</span>
      </div>
      <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
        <div
          className={`h-full transition-all ${near ? "bg-amber-500" : "bg-primary-500"}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      {near && (
        <p className="text-[10px] text-amber-700 font-medium mt-0.5">MW56 &gt; MW58 — hooter may pulse</p>
      )}
    </div>
  );
}

function MillHooterVerificationPanel({ mill }: { mill: SettingsPlcMill }) {
  const mes = mill.mesHooter;
  const plcAcc = mill.accumulatedValue ?? null;
  const plcThr = mill.thresholdValue ?? null;
  const mesAcc = mes?.accumulated ?? null;
  const mesThr = mes?.threshold ?? null;
  const accMatch = mesAcc != null && plcAcc != null && mesAcc === plcAcc;
  const thrMatch = mesThr != null && plcThr != null && mesThr === plcThr;

  return (
    <section className="bg-white rounded-lg border border-violet-200 shadow-sm overflow-hidden">
      <div className="px-5 py-3 bg-violet-50 border-b border-violet-100">
        <h3 className="font-semibold text-violet-950">
          {mill.name ?? `Mill-${mill.millNo}`} — NDT bundle hooter verification
        </h3>
        <p className="text-xs text-violet-800 mt-1">
          MES writes <code className="bg-white/70 px-1 rounded">{mill.hooterThresholdAddress ?? "MW58"}</code> from
          formation chart and <code className="bg-white/70 px-1 rounded">{mill.hooterAccumAddress ?? "MW56"}</code> from
          bundle-engine count. Hooter output{" "}
          <code className="bg-white/70 px-1 rounded">{mill.hooterOutputAddress ?? "Q6.7"}</code> pulses{" "}
          {mill.hooterDurationMs ? `${mill.hooterDurationMs / 1000}s` : "10s"} when MW56 &gt; MW58 and{" "}
          <code className="bg-white/70 px-1 rounded">{mill.hooterPasEnableAddress ?? "DB260.DBX3.6"}</code> is on.
        </p>
      </div>
      <div className="p-5 grid grid-cols-1 lg:grid-cols-2 gap-6 text-sm">
        <div className="space-y-3">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-gray-500">Running PO (MES)</h4>
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5">
            <dt className="text-gray-500">PO</dt>
            <dd className="font-medium text-gray-900">{mes?.poNumber?.trim() || "—"}</dd>
            <dt className="text-gray-500">Pipe size</dt>
            <dd className="font-medium text-gray-900">{mes?.pipeSize?.trim() || "—"}</dd>
            <dt className="text-gray-500">MES threshold</dt>
            <dd className="font-mono tabular-nums">{mesThr ?? "—"}</dd>
            <dt className="text-gray-500">MES accumulated</dt>
            <dd className="font-mono tabular-nums">{mesAcc ?? "—"}</dd>
          </dl>
          {mesAcc != null && mesThr != null && mesThr > 0 && (
            <BundleProgressBar accumulated={mesAcc} threshold={mesThr} />
          )}
        </div>
        <div className="space-y-3">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-gray-500">PLC memory (last handshake write)</h4>
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5">
            <dt className="text-gray-500">{mill.hooterThresholdAddress ?? "MW58"}</dt>
            <dd className="font-mono tabular-nums flex items-center gap-2">
              {plcThr ?? "—"}
              {mesThr != null && plcThr != null && (
                <span className={`text-[10px] px-1.5 py-0.5 rounded ${thrMatch ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
                  {thrMatch ? "matches MES" : "≠ MES"}
                </span>
              )}
            </dd>
            <dt className="text-gray-500">{mill.hooterAccumAddress ?? "MW56"}</dt>
            <dd className="font-mono tabular-nums flex items-center gap-2">
              {plcAcc ?? "—"}
              {mesAcc != null && plcAcc != null && (
                <span className={`text-[10px] px-1.5 py-0.5 rounded ${accMatch ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
                  {accMatch ? "matches MES" : "≠ MES"}
                </span>
              )}
            </dd>
            <dt className="text-gray-500">Hooter</dt>
            <dd>
              <HooterOutputBadge active={mill.hooterActive} />
            </dd>
            <dt className="text-gray-500">DB251 NDT</dt>
            <dd className="font-mono tabular-nums">{mill.ndtCount ?? "—"}</dd>
            <dt className="text-gray-500">Updated</dt>
            <dd className="text-xs text-gray-600">{formatUtc(mill.countsUpdatedUtc)}</dd>
          </dl>
          {plcAcc != null && plcThr != null && plcThr > 0 && (
            <BundleProgressBar accumulated={plcAcc} threshold={plcThr} />
          )}
        </div>
      </div>
      <p className="px-5 pb-4 text-xs text-gray-500">
        After <strong>Test PO change</strong>, MW56 should reset to 0 and MW58 should update for the new running PO&apos;s
        pipe size. Values refresh every 2s while this tab is open.
      </p>
    </section>
  );
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
  const [poChangeTestingMill, setPoChangeTestingMill] = useState<number | null>(null);
  const [poChangeTestResult, setPoChangeTestResult] = useState<SettingsPoChangeTestResult | null>(null);
  const [plcPollTick, setPlcPollTick] = useState(0);

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

  useEffect(() => {
    if (!authenticated || tab !== "plc") return;
    const id = setInterval(() => {
      setPlcPollTick((t) => t + 1);
    }, 2000);
    return () => clearInterval(id);
  }, [authenticated, tab]);

  useEffect(() => {
    if (!authenticated || tab !== "plc" || plcPollTick === 0) return;
    const t = getSettingsToken();
    if (!t) return;
    void api.settingsPlc(t).then(setPlc).catch(() => {
      /* keep last snapshot on transient errors */
    });
  }, [authenticated, tab, plcPollTick]);

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

  const testPoChange = async (millNo: number) => {
    const t = getSettingsToken();
    if (!t) return;
    setPoChangeTestingMill(millNo);
    setPoChangeTestResult(null);
    setError(null);
    setMessage(null);
    try {
      const result = await api.settingsTestPoChange(t, millNo);
      setPoChangeTestResult(result);
      if (result.success) {
        setMessage(result.message ?? `Mill-${millNo} PO change test completed.`);
      } else {
        setError(result.message ?? `Mill-${millNo} PO change test failed.`);
      }
      await loadTabData();
    } catch (e) {
      setError(e instanceof Error ? e.message : "PO change test failed");
    } finally {
      setPoChangeTestingMill(null);
    }
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
              {plc?.plcHandshakeEnabled
                ? " (persistent handshake)"
                : plc?.plcPoEndEnabled
                  ? ""
                  : " (disabled)"}
            </p>
            <p>
              <span className="text-gray-500">S7 handshake:</span>{" "}
              <span className={plc?.lastReadOk ? "text-green-700 font-medium" : "text-red-700 font-medium"}>
                {plc?.lastReadOk ? "All mills connected" : "One or more mills not connected"}
              </span>
              {plc?.lastPlcError ? ` — ${plc.lastPlcError}` : ""}
            </p>
            <p className="text-gray-500 text-xs">
              Checked: {formatUtc(plc?.lastPlcCheckUtc)} · auto-refresh every 2s
            </p>
            <p className="text-gray-600 text-xs pt-1">
              Use <strong>Test PO change</strong> per mill to verify PO-end workflow and MW56/MW58 rewrite. Results appear
              below and in server logs (<code className="text-xs bg-gray-100 px-1 rounded">[Settings test]</code>).
            </p>
          </div>

          {plc?.plcHandshakeEnabled && plc.readLineRunning !== false && (
            <section className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
              <div className="px-5 py-3 bg-gray-50 border-b border-gray-100">
                <h3 className="font-semibold text-gray-900">Line running (SCADA)</h3>
                <p className="text-xs text-gray-600 mt-1">
                  Read from{" "}
                  <code className="bg-gray-100 px-1 rounded">
                    {plc.lineRunningSignal?.address ?? "DB250.DBX2.0"}
                  </code>{" "}
                  on each mill PLC. Green = line running, red = stopped.
                </p>
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 p-5">
                {(plc.mills ?? []).map((m) => (
                  <div
                    key={m.millNo}
                    className="rounded-lg border border-gray-100 bg-gray-50/50 px-4 py-3 flex flex-col gap-2"
                  >
                    <span className="text-sm font-semibold text-gray-900">{m.name ?? `Mill-${m.millNo}`}</span>
                    <LineRunningLamp
                      running={m.lineRunning}
                      connected={m.handshakeConnected ?? false}
                    />
                    <span className="text-[10px] text-gray-400 truncate" title={m.host}>
                      {m.host || "—"}
                    </span>
                  </div>
                ))}
              </div>
            </section>
          )}

          {(plc?.mills ?? [])
            .filter((m) => m.hooterEnabled)
            .map((m) => (
              <MillHooterVerificationPanel key={`hooter-${m.millNo}`} mill={m} />
            ))}

          {poChangeTestResult && (
            <div
              className={`rounded-lg border p-4 text-sm ${
                poChangeTestResult.success
                  ? "bg-green-50 border-green-200 text-green-900"
                  : "bg-red-50 border-red-200 text-red-900"
              }`}
            >
              <p className="font-semibold mb-2">
                {poChangeTestResult.millName ?? `Mill-${poChangeTestResult.millNo}`}:{" "}
                {poChangeTestResult.message}
              </p>
              {poChangeTestResult.poNumber && (
                <p className="mb-2">PO from slit CSV: {poChangeTestResult.poNumber}</p>
              )}
              {Array.isArray(poChangeTestResult.steps) && poChangeTestResult.steps.length > 0 && (
                <ol className="list-decimal list-inside space-y-0.5 text-xs">
                  {poChangeTestResult.steps.map((step, i) => (
                    <li key={i}>{step}</li>
                  ))}
                </ol>
              )}
              {poChangeTestResult.logHint && (
                <p className="mt-2 text-xs opacity-80">{poChangeTestResult.logHint}</p>
              )}
            </div>
          )}

          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left">Mill</th>
                  <th className="px-4 py-2 text-left">Host</th>
                  <th className="px-4 py-2 text-left">S7</th>
                  <th className="px-4 py-2 text-left">Line</th>
                  <th className="px-4 py-2 text-left">MW58</th>
                  <th className="px-4 py-2 text-left">MW56</th>
                  <th className="px-4 py-2 text-left">Hooter</th>
                  <th className="px-4 py-2 text-left">Trigger</th>
                  <th className="px-4 py-2 text-left">Ack</th>
                  <th className="px-4 py-2 text-left">State</th>
                  <th className="px-4 py-2 text-left">Test</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {(plc?.mills ?? []).map((m) => (
                  <tr key={m.millNo}>
                    <td className="px-4 py-2 font-medium whitespace-nowrap">
                      {m.name ?? `Mill-${m.millNo}`}
                      <div className="text-xs text-gray-500 font-normal">
                        {m.poEndAddress} → {m.mesAckAddress}
                      </div>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap">{m.host || "—"}</td>
                    <td className="px-4 py-2">
                      <span
                        className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                          m.handshakeConnected ?? (m.reachable && plc?.plcHandshakeEnabled)
                            ? "bg-green-100 text-green-800"
                            : m.reachable
                              ? "bg-amber-100 text-amber-800"
                              : "bg-red-100 text-red-800"
                        }`}
                      >
                        {m.handshakeConnected
                          ? "Connected"
                          : m.reachable
                            ? "TCP OK"
                            : "Unreachable"}
                      </span>
                    </td>
                    <td className="px-4 py-2">
                      {plc?.readLineRunning !== false ? (
                        <LineRunningLamp
                          running={m.lineRunning}
                          connected={m.handshakeConnected ?? false}
                          compact
                        />
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-4 py-2 font-mono tabular-nums text-xs">
                      {m.hooterEnabled ? (m.thresholdValue ?? "—") : "—"}
                    </td>
                    <td className="px-4 py-2 font-mono tabular-nums text-xs">
                      {m.hooterEnabled ? (m.accumulatedValue ?? "—") : "—"}
                    </td>
                    <td className="px-4 py-2">
                      {m.hooterEnabled ? (
                        <HooterOutputBadge active={m.hooterActive} />
                      ) : (
                        <span className="text-gray-400 text-xs">—</span>
                      )}
                    </td>
                    <td className="px-4 py-2">
                      {m.triggerActive ?? plc?.poEndByMill?.[String(m.millNo)] ? "ON" : "OFF"}
                    </td>
                    <td className="px-4 py-2">{m.ackActive ? "ON" : "OFF"}</td>
                    <td className="px-4 py-2 text-xs max-w-[8rem] truncate" title={m.handshakeState ?? ""}>
                      {m.handshakeState ?? "—"}
                    </td>
                    <td className="px-4 py-2">
                      <button
                        type="button"
                        disabled={
                          poChangeTestingMill !== null ||
                          !(m.testAvailable ?? plc?.plcHandshakeEnabled)
                        }
                        onClick={() => m.millNo && testPoChange(m.millNo)}
                        className="px-3 py-1.5 text-xs font-medium rounded-md border border-primary-300 text-primary-800 bg-primary-50 hover:bg-primary-100 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
                      >
                        {poChangeTestingMill === m.millNo ? "Testing…" : "Test PO change"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
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
