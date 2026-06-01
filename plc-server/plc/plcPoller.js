"use strict";

const { getMillPlcConfigs } = require("./millPlcConfig");
const { startSingleMillPoller } = require("./singleMillPoller");
const { PlcLabelOrchestrator } = require("./labelOrchestrator");

function formatTimestamp(d = new Date()) {
  const p = (n) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
    `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
  );
}

/**
 * Start one S7 poll loop per enabled mill (1, 2, 3, 4).
 * @param {import('socket.io').Server} io
 */
function startPlcPoller(io) {
  const configs = getMillPlcConfigs();
  if (configs.length === 0) {
    console.warn(
      `[${formatTimestamp()}] [PLC] No mills enabled (PLC_ENABLED_MILLS). Nothing to poll.`
    );
    return;
  }

  const wipBundleFolder =
    process.env.WIP_BUNDLE_FOLDER || "Z:\\To SAP\\TM\\Bundle";
  const printerHost = (
    process.env.PRINTER_HOST ||
    process.env.NDT_TAG_PRINTER_HOST ||
    "192.168.0.125"
  ).trim();
  const printerPort = Number(
    process.env.PRINTER_PORT || process.env.NDT_TAG_PRINTER_PORT || 9100
  );
  const printerBind =
    process.env.PRINTER_LOCAL_BIND ||
    process.env.NDT_TAG_PRINTER_LOCAL_BIND ||
    "";

  const pollers = [];

  for (const cfg of configs) {
    let labelOrch = null;
    if (cfg.enableLabelPrint && wipBundleFolder.trim()) {
      labelOrch = new PlcLabelOrchestrator({
        io,
        bundleFolder: wipBundleFolder.trim(),
        millNo: cfg.millNo,
        printerHost: printerHost.trim(),
        printerPort,
        printerBind: printerBind.trim(),
        stationText: (process.env.NDT_LABEL_STATION_TEXT || "").trim(),
        millLabel: cfg.millLabel,
      });
      console.log(
        `[${formatTimestamp()}] [LABEL] Auto-print on ${cfg.millLabel} | WIP: ${wipBundleFolder} | Printer: ${printerHost}:${printerPort}`
      );
    }

    pollers.push(startSingleMillPoller(io, cfg, labelOrch));
  }

  const shutdownAll = async () => {
    await Promise.all(pollers.map((p) => p.shutdown()));
  };

  process.on("SIGINT", async () => {
    await shutdownAll();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    await shutdownAll();
    process.exit(0);
  });
}

module.exports = { startPlcPoller };
