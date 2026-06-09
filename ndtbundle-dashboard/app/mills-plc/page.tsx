"use client";

import {
  PLC_MILL_NUMBERS,
  type PlcMillLiveState,
} from "@/lib/plcTypes";
import { usePlcMillLive } from "@/lib/usePlcMillLive";

const MILL_META: Record<
  number,
  { poEnd: string; ack: string; host: string }
> = {
  1: { poEnd: "M40.6", ack: "M40.7", host: "192.168.0.13" },
  2: { poEnd: "M40.6", ack: "M40.7", host: "192.168.0.60" },
  3: { poEnd: "M20.6", ack: "M22.7", host: "192.168.0.17" },
  4: { poEnd: "M41.6", ack: "M41.7", host: "192.168.0.19" },
};

function MillPlcCard({
  millNo,
  state,
}: {
  millNo: number;
  state: PlcMillLiveState;
}) {
  const meta = MILL_META[millNo];
  const { connection, statusMsg, data, lastPoEnd } = state;

  return (
    <section className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <div className="border-b border-slate-100 bg-slate-50 px-4 py-3">
        <h2 className="text-lg font-semibold text-slate-800">Mill-{millNo}</h2>
        <p className="text-xs text-slate-500 mt-0.5">
          {meta.host} · PO end <strong>{meta.poEnd}</strong> · ack{" "}
          <strong>{meta.ack}</strong> · DB251 OK/NOK/NDT
        </p>
      </div>

      <div className="p-4 space-y-4">
        <div
          className={`rounded-lg border px-3 py-2 text-sm ${
            connection === "live"
              ? "border-green-200 bg-green-50 text-green-900"
              : connection === "connecting"
                ? "border-amber-200 bg-amber-50 text-amber-900"
                : "border-red-200 bg-red-50 text-red-900"
          }`}
        >
          {statusMsg}
        </div>

        {lastPoEnd && (
          <div className="rounded-lg border border-violet-200 bg-violet-50 px-3 py-2 text-sm text-violet-900">
            <span className="font-semibold">Last PO end:</span> PO{" "}
            <span className="tabular-nums">{lastPoEnd.poId}</span>, NDT{" "}
            <span className="tabular-nums">{lastPoEnd.ndtCountFinal}</span> —{" "}
            {lastPoEnd.timestamp}
          </div>
        )}

        <div className="grid grid-cols-3 gap-3">
          <div className="rounded-lg border border-emerald-100 bg-emerald-50/50 p-3">
            <p className="text-xs font-medium uppercase text-emerald-800">OK</p>
            <p className="text-2xl font-bold tabular-nums text-emerald-900">
              {data?.okCount ?? "—"}
            </p>
            <p className="text-[10px] text-emerald-700 mt-1">DB251.DBW2</p>
          </div>
          <div className="rounded-lg border border-rose-100 bg-rose-50/50 p-3">
            <p className="text-xs font-medium uppercase text-rose-800">NOK</p>
            <p className="text-2xl font-bold tabular-nums text-rose-900">
              {data?.nokCount ?? "—"}
            </p>
            <p className="text-[10px] text-rose-700 mt-1">DB251.DBW4</p>
          </div>
          <div className="rounded-lg border border-primary-100 bg-primary-50/50 p-3">
            <p className="text-xs font-medium uppercase text-primary-800">NDT</p>
            <p className="text-2xl font-bold tabular-nums text-primary-900">
              {data?.ndtCount ?? "—"}
            </p>
            <p className="text-[10px] text-primary-700 mt-1">DB251.DBW6</p>
          </div>
        </div>

        {data && (
          <table className="w-full text-left text-sm border border-slate-100 rounded-lg overflow-hidden">
            <tbody className="divide-y divide-slate-100">
              <tr className="bg-slate-50">
                <th className="px-3 py-1.5 font-medium text-slate-600">PO ID</th>
                <td className="px-3 py-1.5 tabular-nums">{data.poId}</td>
              </tr>
              <tr>
                <th className="px-3 py-1.5 font-medium text-slate-600">Slit ID</th>
                <td className="px-3 py-1.5 tabular-nums">{data.slitId}</td>
              </tr>
              <tr className="bg-slate-50">
                <th className="px-3 py-1.5 font-medium text-slate-600">PO end latch</th>
                <td className="px-3 py-1.5">{data.poEndActive ? "ON" : "OFF"}</td>
              </tr>
              <tr>
                <th className="px-3 py-1.5 font-medium text-slate-600">Updated</th>
                <td className="px-3 py-1.5 text-xs">{data.timestamp}</td>
              </tr>
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

export default function MillsPlcPage() {
  const mills = usePlcMillLive();

  return (
    <div className="mx-auto max-w-7xl px-4 py-8">
      <h1 className="text-2xl font-semibold text-slate-800">Mills 1–4 — live PLC</h1>
      <p className="mt-1 text-sm text-slate-600">
        Siemens S7-300 counts from <strong>DB251</strong> (OK / NOK / NDT). When PO handshake is
        enabled, counts come from <strong>NdtBundleService</strong> on the same S7 connection;
        otherwise from plc-server Socket.IO.
      </p>

      <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
        {PLC_MILL_NUMBERS.map((n) => (
          <MillPlcCard key={n} millNo={n} state={mills[n] ?? emptyFallback()} />
        ))}
      </div>
    </div>
  );
}

function emptyFallback(): PlcMillLiveState {
  return {
    connection: "connecting",
    statusMsg: "Loading…",
    data: null,
    lastPoEnd: null,
  };
}
