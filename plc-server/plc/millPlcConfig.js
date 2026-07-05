"use strict";

/**
 * Per-mill Siemens S7-300 settings. Counts: DB251 (L1L2_OKCut / NOKCut / NDTCut at INT2/4/6).
 * PO change trigger coils differ by mill (see ladder Networks 95–96 / 103–104).
 */

/** @typedef {{
 *   millNo: number;
 *   millLabel: string;
 *   host: string;
 *   port: number;
 *   rack: number;
 *   slots: number[];
 *   pollMs: number;
 *   retryMs: number;
 *   poEndSignal: string;
 *   mesAckCoil: string;
 *   mesAckPulseMs: number;
 *   enableLabelPrint: boolean;
 * }} MillPlcConfig */

function envInt(name, fallback) {
  const v = process.env[name];
  if (v === undefined || v === "") return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function envBool(name, defaultTrue) {
  const v = (process.env[name] || "").trim().toLowerCase();
  if (!v) return defaultTrue;
  return v !== "0" && v !== "false" && v !== "no";
}

function parseEnabledMillSet() {
  const raw = (process.env.PLC_ENABLED_MILLS || "1,2,3").trim();
  const set = new Set();
  for (const part of raw.split(/[,;\s]+/)) {
    const n = Number(part.trim());
    if (n >= 1 && n <= 4) set.add(n);
  }
  if (set.size === 0) set.add(3);
  return set;
}

/**
 * @returns {MillPlcConfig[]}
 */
function getMillPlcConfigs() {
  const enabled = parseEnabledMillSet();
  const defaultPoll = envInt("PLC_POLL_MS", envInt("PLC_MILL3_POLL_MS", 2000));
  const defaultRetry = envInt("PLC_RETRY_MS", envInt("PLC_MILL3_RETRY_MS", 5000));
  const defaultPort = envInt("PLC_PORT", envInt("PLC_MILL3_PORT", 102));
  const defaultRack = envInt("PLC_RACK", envInt("PLC_MILL3_RACK", 0));
  const defaultSlot = envInt("PLC_SLOT", envInt("PLC_MILL3_SLOT", 2));
  const defaultSlots = [defaultSlot, 1].filter((s, i, a) => a.indexOf(s) === i);

  const labelMill = envInt("PLC_MILL_NUMBER", 3);
  const labelPrintFlag = (process.env.ENABLE_PLC_LABEL_PRINT || "1").trim().toLowerCase();
  const labelPrintGlobally =
    labelPrintFlag !== "0" &&
    labelPrintFlag !== "false" &&
    labelPrintFlag !== "no";

  /** @type {Omit<MillPlcConfig, "enableLabelPrint"> & { envPrefix: string; defaultHost: string; poEndSignal: string; mesAckCoil: string; mesAckPulseMs?: number }[]} */
  const defs = [
    {
      millNo: 1,
      millLabel: "Mill-1",
      envPrefix: "PLC_MILL1",
      defaultHost: "192.168.0.13",
      poEndSignal: "M40.6",
      mesAckCoil: "M40.7",
    },
    {
      millNo: 2,
      millLabel: "Mill-2",
      envPrefix: "PLC_MILL2",
      defaultHost: "192.168.0.60",
      poEndSignal: "M40.6",
      mesAckCoil: "M40.7",
    },
    {
      millNo: 3,
      millLabel: "Mill-3",
      envPrefix: "PLC_MILL3",
      defaultHost: "192.168.0.17",
      poEndSignal: "M20.6",
      mesAckCoil: "M22.7",
    },
    {
      millNo: 4,
      millLabel: "Mill-4",
      envPrefix: "PLC_MILL4",
      defaultHost: "192.168.0.19",
      poEndSignal: "M41.6",
      mesAckCoil: "M41.7",
    },
  ];

  return defs
    .filter((d) => enabled.has(d.millNo))
    .map((d) => {
      const p = d.envPrefix;
      const host = (process.env[`${p}_HOST`] || d.defaultHost).trim();
      const slot = envInt(`${p}_SLOT`, defaultSlot);
      const slots = [slot, 1].filter((s, i, a) => a.indexOf(s) === i);
      return {
        millNo: d.millNo,
        millLabel: d.millLabel,
        host,
        port: envInt(`${p}_PORT`, defaultPort),
        rack: envInt(`${p}_RACK`, defaultRack),
        slots,
        pollMs: envInt(`${p}_POLL_MS`, defaultPoll),
        retryMs: envInt(`${p}_RETRY_MS`, defaultRetry),
        poEndSignal: (process.env[`${p}_PO_END`] || d.poEndSignal).trim(),
        mesAckCoil: (process.env[`${p}_MES_ACK`] || d.mesAckCoil).trim(),
        mesAckPulseMs: envInt(`${p}_ACK_PULSE_MS`, 1000),
        enableLabelPrint:
          labelPrintGlobally &&
          d.millNo === labelMill &&
          envBool(`${p}_LABEL_PRINT`, true),
      };
    });
}

module.exports = { getMillPlcConfigs };
