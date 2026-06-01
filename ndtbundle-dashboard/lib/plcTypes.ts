export const PLC_SOCKET_URL =
  process.env.NEXT_PUBLIC_PLC_SOCKET_URL || "http://localhost:3030";

export type PlcMillUpdate = {
  millNo?: number;
  timestamp: string;
  mill: string;
  poId: number;
  slitId: number;
  ndtCount: number;
  okCount: number;
  nokCount: number;
  poEndActive?: boolean;
  status: string;
};

export type PlcMillStatusEvt = {
  millNo?: number;
  mill: string;
  status: string;
  message: string;
  timestamp: string;
  host?: string;
  poEndSignal?: string;
  mesAckCoil?: string;
};

export type PlcMillPoEndEvt = {
  millNo?: number;
  mill: string;
  poId: number;
  ndtCountFinal: number;
  timestamp: string;
};

export type PlcMillLiveState = {
  connection: "connecting" | "live" | "error";
  statusMsg: string;
  data: PlcMillUpdate | null;
  lastPoEnd: PlcMillPoEndEvt | null;
};

export const PLC_MILL_NUMBERS = [1, 2, 3, 4] as const;

export function plcEventPrefix(millNo: number): string {
  return `plc:mill${millNo}`;
}

export function emptyPlcMillState(): PlcMillLiveState {
  return {
    connection: "connecting",
    statusMsg: "Connecting…",
    data: null,
    lastPoEnd: null,
  };
}
