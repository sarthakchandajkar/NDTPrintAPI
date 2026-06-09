"use client";

import { useCallback, useEffect, useState } from "react";
import { io, type Socket } from "socket.io-client";
import { api, type PlcLiveMillPayload } from "./api";
import {
  PLC_MILL_NUMBERS,
  PLC_SOCKET_URL,
  emptyPlcMillState,
  plcEventPrefix,
  type PlcMillLiveState,
  type PlcMillPoEndEvt,
  type PlcMillStatusEvt,
  type PlcMillUpdate,
} from "./plcTypes";

function buildInitialMills(): Record<number, PlcMillLiveState> {
  const init: Record<number, PlcMillLiveState> = {};
  for (const n of PLC_MILL_NUMBERS) init[n] = emptyPlcMillState();
  return init;
}

function applyHandshakeMill(
  setMills: React.Dispatch<React.SetStateAction<Record<number, PlcMillLiveState>>>,
  m: PlcLiveMillPayload
) {
  const millNo = typeof m.millNo === "number" ? m.millNo : 0;
  if (millNo < 1 || millNo > 4) return;

  const millLabel = m.millName?.trim() || `Mill-${millNo}`;
  const lastPoEnd: PlcMillPoEndEvt | null = m.lastPoEnd
    ? {
        millNo,
        mill: millLabel,
        poId: m.lastPoEnd.poId ?? 0,
        ndtCountFinal: m.lastPoEnd.ndtCountFinal ?? 0,
        timestamp: m.lastPoEnd.timestamp ?? "",
      }
    : null;

  const hasCounts =
    m.connected &&
    (typeof m.okCount === "number" ||
      typeof m.nokCount === "number" ||
      typeof m.ndtCount === "number");

  const data: PlcMillUpdate | null = hasCounts
    ? {
        millNo,
        timestamp: m.timestamp ?? "",
        mill: millLabel,
        poId: m.poId ?? 0,
        slitId: m.slitId ?? 0,
        ndtCount: m.ndtCount ?? 0,
        okCount: m.okCount ?? 0,
        nokCount: m.nokCount ?? 0,
        poEndActive: m.poEndActive,
        status: m.status ?? "connected",
      }
    : null;

  setMills((prev) => ({
    ...prev,
    [millNo]: {
      connection: m.connected ? "live" : "error",
      statusMsg: m.connected
        ? `Live (DB251 via handshake · ${m.ipAddress ?? ""})`
        : m.lastError?.trim() || "PLC disconnected (handshake)",
      data,
      lastPoEnd: lastPoEnd ?? prev[millNo]?.lastPoEnd ?? null,
    },
  }));
}

/** Live OK/NOK/NDT: polls NdtBundleService when PlcHandshake is enabled, else plc-server Socket.IO. */
export function usePlcMillLive(): Record<number, PlcMillLiveState> {
  const [mills, setMills] = useState<Record<number, PlcMillLiveState>>(buildInitialMills);

  const applyStatus = useCallback((millNo: number, s: PlcMillStatusEvt) => {
    setMills((prev) => {
      const cur = prev[millNo] ?? emptyPlcMillState();
      return {
        ...prev,
        [millNo]: {
          ...cur,
          connection: s.status === "connected" ? "live" : "error",
          statusMsg: s.message || (s.status === "connected" ? "Connected" : "Disconnected"),
        },
      };
    });
  }, []);

  useEffect(() => {
    let cancelled = false;
    let socket: Socket | null = null;
    let pollId: ReturnType<typeof setInterval> | null = null;

    const startSocket = () => {
      try {
        socket = io(PLC_SOCKET_URL, {
          transports: ["websocket", "polling"],
          reconnection: true,
          reconnectionAttempts: Infinity,
          reconnectionDelay: 2000,
        });

        socket.on("connect", () => {
          setMills((prev) => {
            const next = { ...prev };
            for (const n of PLC_MILL_NUMBERS) {
              next[n] = {
                ...(next[n] ?? emptyPlcMillState()),
                connection: "connecting",
                statusMsg: "Socket connected — waiting for PLC…",
              };
            }
            return next;
          });
        });

        socket.on("disconnect", () => {
          setMills((prev) => {
            const next = { ...prev };
            for (const n of PLC_MILL_NUMBERS) {
              next[n] = {
                ...(next[n] ?? emptyPlcMillState()),
                connection: "error",
                statusMsg: "Socket disconnected from bridge",
              };
            }
            return next;
          });
        });

        socket.on("connect_error", () => {
          setMills((prev) => {
            const next = { ...prev };
            for (const n of PLC_MILL_NUMBERS) {
              next[n] = {
                ...(next[n] ?? emptyPlcMillState()),
                connection: "error",
                statusMsg: `Cannot reach PLC bridge at ${PLC_SOCKET_URL}. Start plc-server.`,
              };
            }
            return next;
          });
        });

        for (const millNo of PLC_MILL_NUMBERS) {
          const prefix = plcEventPrefix(millNo);

          socket.on(`${prefix}:status`, (payload: PlcMillStatusEvt) => {
            applyStatus(millNo, payload);
          });

          socket.on(`${prefix}:update`, (payload: PlcMillUpdate) => {
            setMills((prev) => ({
              ...prev,
              [millNo]: {
                ...(prev[millNo] ?? emptyPlcMillState()),
                connection: "live",
                statusMsg: `Live (DB251 + plc-server)`,
                data: payload,
              },
            }));
          });

          socket.on(`${prefix}:po_end`, (payload: PlcMillPoEndEvt) => {
            setMills((prev) => ({
              ...prev,
              [millNo]: {
                ...(prev[millNo] ?? emptyPlcMillState()),
                lastPoEnd: payload,
              },
            }));
          });
        }
      } catch {
        setMills(() => {
          const next = buildInitialMills();
          for (const n of PLC_MILL_NUMBERS) {
            next[n] = {
              ...emptyPlcMillState(),
              connection: "error",
              statusMsg: "Failed to initialize Socket.IO client",
            };
          }
          return next;
        });
      }
    };

    const pollHandshake = async () => {
      try {
        const r = await api.plcLive();
        if (cancelled) return;
        if (!r.plcHandshakeEnabled) return;
        for (const m of r.mills ?? []) applyHandshakeMill(setMills, m);
      } catch {
        if (cancelled) return;
        setMills((prev) => {
          const next = { ...prev };
          for (const n of PLC_MILL_NUMBERS) {
            next[n] = {
              ...(next[n] ?? emptyPlcMillState()),
              connection: "error",
              statusMsg: "Cannot reach API for handshake PLC counts",
            };
          }
          return next;
        });
      }
    };

    const init = async () => {
      try {
        const r = await api.plcLive();
        if (cancelled) return;
        if (r.plcHandshakeEnabled) {
          await pollHandshake();
          pollId = setInterval(() => void pollHandshake(), 1000);
        } else {
          startSocket();
        }
      } catch {
        if (!cancelled) startSocket();
      }
    };

    void init();

    return () => {
      cancelled = true;
      if (pollId) clearInterval(pollId);
      if (socket) {
        socket.removeAllListeners();
        socket.close();
      }
    };
  }, [applyStatus]);

  return mills;
}

export type PlcCountsByMill = Record<
  number,
  { ndtCount: number | null; okCount: number | null; nokCount: number | null }
>;

/** Summary page: OK/NOK/NDT per mill from handshake API or plc-server socket. */
export function usePlcCountsByMill(): PlcCountsByMill {
  const [plcLive, setPlcLive] = useState<PlcCountsByMill>({});

  useEffect(() => {
    let cancelled = false;
    let socket: Socket | null = null;
    let pollId: ReturnType<typeof setInterval> | null = null;

    const toCounts = (m: PlcLiveMillPayload) => {
      const millNo = typeof m.millNo === "number" ? m.millNo : 0;
      if (millNo < 1 || millNo > 4 || !m.connected) return;
      setPlcLive((prev) => ({
        ...prev,
        [millNo]: {
          ndtCount: typeof m.ndtCount === "number" ? Math.trunc(m.ndtCount) : null,
          okCount: typeof m.okCount === "number" ? Math.trunc(m.okCount) : null,
          nokCount: typeof m.nokCount === "number" ? Math.trunc(m.nokCount) : null,
        },
      }));
    };

    const startSocket = () => {
      try {
        socket = io(PLC_SOCKET_URL, {
          transports: ["websocket", "polling"],
          reconnection: true,
          reconnectionAttempts: Infinity,
          reconnectionDelay: 2000,
        });

        const onUpdate = (millNo: number, payload: PlcMillUpdate) => {
          const ndt =
            typeof payload?.ndtCount === "number" && Number.isFinite(payload.ndtCount)
              ? Math.trunc(payload.ndtCount)
              : null;
          const ok =
            typeof payload?.okCount === "number" && Number.isFinite(payload.okCount)
              ? Math.trunc(payload.okCount)
              : null;
          const nok =
            typeof payload?.nokCount === "number" && Number.isFinite(payload.nokCount)
              ? Math.trunc(payload.nokCount)
              : null;
          setPlcLive((prev) => ({
            ...prev,
            [millNo]: { ndtCount: ndt, okCount: ok, nokCount: nok },
          }));
        };

        for (const n of PLC_MILL_NUMBERS) {
          socket.on(`${plcEventPrefix(n)}:update`, (payload: PlcMillUpdate) => {
            onUpdate(n, payload);
          });
        }
      } catch {
        /* socket optional */
      }
    };

    const pollHandshake = async () => {
      try {
        const r = await api.plcLive();
        if (cancelled || !r.plcHandshakeEnabled) return;
        for (const m of r.mills ?? []) toCounts(m);
      } catch {
        /* ignore transient */
      }
    };

    const init = async () => {
      try {
        const r = await api.plcLive();
        if (cancelled) return;
        if (r.plcHandshakeEnabled) {
          await pollHandshake();
          pollId = setInterval(() => void pollHandshake(), 1000);
        } else {
          startSocket();
        }
      } catch {
        if (!cancelled) startSocket();
      }
    };

    void init();

    return () => {
      cancelled = true;
      if (pollId) clearInterval(pollId);
      if (socket) {
        socket.removeAllListeners();
        socket.close();
      }
    };
  }, []);

  return plcLive;
}
