"use strict";

const { findLatestWipForMill } = require("./wipBundleScanner");
const { resolveThreshold } = require("./formationChart");
const { buildNdtTagZpl } = require("./zplNdtLabelBuilder");
const { sendTcpWithOptionalBind } = require("./printerClient");

function formatTimestamp(d = new Date()) {
  const p = (n) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
    `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
  );
}

/** Same as CsvBundleOutputWriter.FormatNdtBatchNo */
function formatNdtBatchNo(sequenceNumber, millNo) {
  const yy = String(new Date().getFullYear() % 100).padStart(2, "0");
  const millDigit =
    millNo >= 1 && millNo <= 4 ? String(millNo) : "1";
  const seq = String(sequenceNumber).padStart(5, "0");
  return `12${yy}${millDigit}${seq}`;
}

function parsePositiveInt(s) {
  if (s == null || String(s).trim() === "") return 0;
  const n = Number.parseInt(String(s).replace(/,/g, "").trim(), 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function resolvePiecesPerBundle(wip) {
  const csv = wip && wip.csv;
  if (!csv) return resolveThreshold("");
  const fromCsv = parsePositiveInt(csv.piecesPerBundle);
  if (fromCsv > 0) return fromCsv;
  return resolveThreshold(csv.pipeSize);
}

/**
 * PLC-driven NDT label printing: WIP bundle folder + live NDT count (matches NdtBundleEngine overshoot + PO-end partial).
 */
class PlcLabelOrchestrator {
  /**
   * @param {object} opts
   * @param {import('socket.io').Server} opts.io
   * @param {string} opts.bundleFolder
   * @param {number} opts.millNo
   * @param {string} opts.printerHost
   * @param {number} opts.printerPort
   * @param {string} [opts.printerBind]
   * @param {string} [opts.stationText]
   * @param {string} [opts.millLabel] e.g. Mill-3 for logs
   */
  constructor(opts) {
    this.io = opts.io;
    this.bundleFolder = opts.bundleFolder || "";
    this.millNo = opts.millNo || 3;
    this.printerHost = opts.printerHost || "";
    this.printerPort = opts.printerPort || 9100;
    this.printerBind = opts.printerBind || "";
    this.stationText = opts.stationText || "";
    this.millLabel = opts.millLabel || `Mill-${this.millNo}`;

    this.lastPrintNdt = 0;
    this.batchSeq = 0;
    this.prevPlcPoId = null;
    this.lastWipPo = null;
    this.hadWipEver = false;
    this.lastNoWipLog = 0;
  }

  reset() {
    this.lastPrintNdt = 0;
    this.batchSeq = 0;
    this.prevPlcPoId = null;
    this.lastWipPo = null;
    this.hadWipEver = false;
    this.lastNoWipLog = 0;
  }

  emitLabelPrinted(payload) {
    try {
      this.io.emit("plc:mill3:label_printed", {
        mill: this.millLabel,
        timestamp: formatTimestamp(),
        ...payload,
      });
    } catch {
      /* ignore */
    }
  }

  async loadWip() {
    if (!this.bundleFolder || !this.bundleFolder.trim()) return null;
    return findLatestWipForMill(this.bundleFolder.trim(), this.millNo);
  }

  syncPoAndWip(ndtRaw, plcPoId, wip) {
    if (this.prevPlcPoId !== null && plcPoId !== this.prevPlcPoId) {
      this.batchSeq = 0;
      this.lastPrintNdt = ndtRaw;
      console.log(
        `[${formatTimestamp()}] [LABEL] ${this.millLabel} PLC PO ID changed (${this.prevPlcPoId} → ${plcPoId}); primed NDT baseline to ${ndtRaw}.`
      );
    }
    this.prevPlcPoId = plcPoId;

    if (wip && this.lastWipPo !== null && wip.poNumber !== this.lastWipPo) {
      this.batchSeq = 0;
      this.lastPrintNdt = ndtRaw;
      console.log(
        `[${formatTimestamp()}] [LABEL] ${this.millLabel} WIP PO changed (${this.lastWipPo} → ${wip.poNumber}); primed NDT baseline to ${ndtRaw}.`
      );
    }
    if (wip) this.lastWipPo = wip.poNumber;
  }

  /**
   * First time we see a WIP file: do not print retroactive NDT already on the PLC counter.
   */
  primeFirstWip(ndtRaw, wip) {
    this.hadWipEver = true;
    this.lastPrintNdt = ndtRaw;
    this.lastWipPo = wip.poNumber;
    console.log(
      `[${formatTimestamp()}] [LABEL] ${this.millLabel} First WIP file seen (${wip.fileName}); primed NDT baseline to ${ndtRaw} (no retroactive labels).`
    );
  }

  buildZplPayload(wip, ndtBatchNo, pcsInBundle) {
    const csv = wip.csv || {};
    return buildNdtTagZpl({
      ndtBatchNo,
      millNo: wip.millNo,
      poNumber: wip.poNumber || csv.poNo || "",
      pipeGrade: csv.pipeGrade || "",
      pipeSize: csv.pipeSize || "",
      pipeThickness: csv.pipeThickness || "",
      pipeLength: csv.pipeLength || "",
      pipeWeightPerMeter: csv.pipeWeightPerMeter || "",
      pipeType: csv.pipeType || "",
      date: new Date(),
      pcsInBundle,
      isReprint: false,
      stationText: this.stationText || undefined,
    });
  }

  async sendPrint(wip, pcsInBundle, reason) {
    if (!this.printerHost || !this.printerHost.trim()) {
      console.warn(
        `[${formatTimestamp()}] [LABEL] ${this.millLabel} Skip print (${reason}): PRINTER_HOST is not set.`
      );
      return;
    }
    this.batchSeq += 1;
    const ndtBatchNo = formatNdtBatchNo(this.batchSeq, wip.millNo);
    const zpl = this.buildZplPayload(wip, ndtBatchNo, pcsInBundle);
    await sendTcpWithOptionalBind(
      this.printerHost.trim(),
      this.printerPort,
      zpl,
      this.printerBind,
      15000
    );
    console.log(
      `[${formatTimestamp()}] [LABEL] ${this.millLabel} Sent ZPL (${reason}) batch=${ndtBatchNo} pcs=${pcsInBundle} PO=${wip.poNumber} → ${this.printerHost}:${this.printerPort}`
    );
    this.emitLabelPrinted({
      ndtBatchNo,
      pcsInBundle,
      poNumber: wip.poNumber,
      reason,
    });
  }

  /**
   * Normal poll: close full bundle when accumulated NDT − last print ≥ threshold (single flush like NdtBundleEngine).
   */
  async processPoll(ndtRaw, plcPoId) {
    const wip = await this.loadWip();
    if (!wip) {
      const now = Date.now();
      if (now - this.lastNoWipLog > 60000) {
        this.lastNoWipLog = now;
        console.warn(
          `[${formatTimestamp()}] [LABEL] ${this.millLabel} No WIP file in "${this.bundleFolder}" for mill ${String(
            this.millNo
          ).padStart(2, "0")}; waiting (NDT=${ndtRaw}).`
        );
      }
      return;
    }

    if (!this.hadWipEver) {
      this.primeFirstWip(ndtRaw, wip);
      this.prevPlcPoId = plcPoId;
      return;
    }

    this.syncPoAndWip(ndtRaw, plcPoId, wip);

    const threshold = resolvePiecesPerBundle(wip);
    if (threshold <= 0) return;

    let acc = ndtRaw - this.lastPrintNdt;
    if (acc < 0) {
      this.lastPrintNdt = ndtRaw;
      acc = 0;
    }
    if (acc >= threshold) {
      await this.sendPrint(wip, acc, "threshold");
      this.lastPrintNdt = ndtRaw;
    }
  }

  /**
   * Rising PO end: print any NDT counted since the last label (partial bundle).
   */
  async onPoEnd(ndtRaw, plcPoId) {
    const wip = await this.loadWip();
    if (!wip || !this.hadWipEver) {
      console.log(
        `[${formatTimestamp()}] [LABEL] ${this.millLabel} PO end: skip partial print (no WIP context yet). NDT=${ndtRaw}`
      );
      return;
    }

    this.syncPoAndWip(ndtRaw, plcPoId, wip);

    let acc = ndtRaw - this.lastPrintNdt;
    if (acc < 0) {
      this.lastPrintNdt = ndtRaw;
      acc = 0;
    }
    if (acc <= 0) {
      console.log(
        `[${formatTimestamp()}] [LABEL] ${this.millLabel} PO end: no remaining NDT since last print (NDT=${ndtRaw}, lastPrint=${this.lastPrintNdt}).`
      );
      return;
    }

    await this.sendPrint(wip, acc, "po_end_partial");
    this.lastPrintNdt = ndtRaw;
  }
}

module.exports = { PlcLabelOrchestrator, formatNdtBatchNo };
