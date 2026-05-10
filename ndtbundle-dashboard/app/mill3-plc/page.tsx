"use client";

import { useEffect, useState, useCallback } from "react";
import { io, type Socket } from "socket.io-client";

const SOCKET_URL =
  process.env.NEXT_PUBLIC_PLC_SOCKET_URL || "http://localhost:3030";

type PlcUpdate = {
  timestamp: string;
  mill: string;
  poId: number;
  slitId: number;
  ndtCount: number;
  okCount: number;
  nokCount: number;
  status: string;
};

type PlcStatusEvt = {
  status: string;
  message: string;
  timestamp: string;
};

type PlcPoEndEvt = {
  mill: string;
  poId: number;
  ndtCountFinal: number;
  timestamp: string;
};

export default function Mill3PlcPage() {
  const [connection, setConnection] = useState<"connecting" | "live" | "error">(
    "connecting"
  );
  const [statusMsg, setStatusMsg] = useState<string>("Connecting…");
  const [lastStatus, setLastStatus] = useState<PlcStatusEvt | null>(null);
  const [data, setData] = useState<PlcUpdate | null>(null);
  const [lastPoEnd, setLastPoEnd] = useState<PlcPoEndEvt | null>(null);

  const applyStatus = useCallback((s: PlcStatusEvt) => {
    setLastStatus(s);
    if (s.status === "connected") {
      setConnection("live");
      setStatusMsg(s.message || "Connected");
    } else {
      setConnection("error");
      setStatusMsg(s.message || "Disconnected");
    }
  }, []);

  useEffect(() => {
    let socket: Socket | null = null;
    try {
      socket = io(SOCKET_URL, {
        transports: ["websocket", "polling"],
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 2000,
      });

      socket.on("connect", () => {
        setConnection("connecting");
        setStatusMsg("Socket connected — waiting for PLC…");
      });

      socket.on("disconnect", () => {
        setConnection("error");
        setStatusMsg("Socket disconnected from bridge");
      });

      socket.on("connect_error", () => {
        setConnection("error");
        setStatusMsg(
          `Cannot reach PLC bridge at ${SOCKET_URL}. Start plc-server (npm start in plc-server).`
        );
      });

      socket.on("plc:mill3:status", (payload: PlcStatusEvt) => {
        applyStatus(payload);
      });

      socket.on("plc:mill3:update", (payload: PlcUpdate) => {
        setData(payload);
        setConnection("live");
        setStatusMsg("Live (DB251 + M20.6 PO end)");
      });

      socket.on("plc:mill3:po_end", (payload: PlcPoEndEvt) => {
        setLastPoEnd(payload);
      });
    } catch {
      setConnection("error");
      setStatusMsg("Failed to initialize Socket.IO client");
    }

    return () => {
      if (socket) {
        socket.removeAllListeners();
        socket.close();
      }
    };
  }, [applyStatus]);

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-2xl font-semibold text-slate-800">Mill-3 PLC (live)</h1>
      <p className="mt-1 text-sm text-slate-600">
        Siemens S7-300: DB251 counts + <strong>M20.6</strong> PO end (ack{" "}
        <strong>M22.7</strong>). Stream from{" "}
        <code className="rounded bg-slate-100 px-1">{SOCKET_URL}</code>
      </p>

      {lastPoEnd && (
        <div className="mt-4 rounded-lg border border-violet-200 bg-violet-50 px-4 py-3 text-sm text-violet-900">
          <span className="font-semibold">Last PO end:</span> PO{" "}
          <span className="tabular-nums">{lastPoEnd.poId}</span>, NDT at end{" "}
          <span className="tabular-nums">{lastPoEnd.ndtCountFinal}</span> —{" "}
          {lastPoEnd.timestamp}
        </div>
      )}

      <div
        className={`mt-6 rounded-lg border px-4 py-3 text-sm ${
          connection === "live"
            ? "border-green-200 bg-green-50 text-green-900"
            : connection === "connecting"
              ? "border-amber-200 bg-amber-50 text-amber-900"
              : "border-red-200 bg-red-50 text-red-900"
        }`}
      >
        <span className="font-medium">Status:</span> {statusMsg}
        {lastStatus && (
          <span className="ml-2 text-slate-600">
            ({lastStatus.timestamp})
          </span>
        )}
      </div>

      <div className="mt-8 rounded-xl border border-slate-200 bg-white p-8 shadow-sm">
        <p className="text-sm font-medium uppercase tracking-wide text-slate-500">
          NDT pipes (DB251.DBW6 — L1L2_NDTCut)
        </p>
        <p className="mt-2 text-5xl font-bold tabular-nums text-slate-900">
          {data?.ndtCount ?? "—"}
        </p>
      </div>

      {data && (
        <div className="mt-8 overflow-hidden rounded-lg border border-slate-200">
          <table className="w-full text-left text-sm">
            <tbody className="divide-y divide-slate-100">
              <tr className="bg-slate-50">
                <th className="px-4 py-2 font-medium text-slate-600">PO ID</th>
                <td className="px-4 py-2 tabular-nums">{data.poId}</td>
              </tr>
              <tr>
                <th className="px-4 py-2 font-medium text-slate-600">Slit ID</th>
                <td className="px-4 py-2 tabular-nums">{data.slitId}</td>
              </tr>
              <tr className="bg-slate-50">
                <th className="px-4 py-2 font-medium text-slate-600">OK count</th>
                <td className="px-4 py-2 tabular-nums">{data.okCount}</td>
              </tr>
              <tr>
                <th className="px-4 py-2 font-medium text-slate-600">NOK count</th>
                <td className="px-4 py-2 tabular-nums">{data.nokCount}</td>
              </tr>
              <tr className="bg-slate-50">
                <th className="px-4 py-2 font-medium text-slate-600">Last update</th>
                <td className="px-4 py-2">{data.timestamp}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
