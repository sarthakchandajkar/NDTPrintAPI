"use strict";

const {
  ITEM_KEYS,
  connectWithSlotFallback,
  readAllItemsPromise,
  writeItemsPromise,
  dropConnectionPromise,
} = require("./plcClient");

function formatTimestamp(d = new Date()) {
  const p = (n) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
    `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
  );
}

function toInt(v) {
  if (v === null || v === undefined) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function toBool(v) {
  if (v === true || v === 1) return true;
  if (v === false || v === 0) return false;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (s === "true" || s === "1") return true;
    if (s === "false" || s === "0") return false;
  }
  return Boolean(v);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * @param {import('socket.io').Server} io
 */
function startPlcPoller(io) {
  const host = process.env.PLC_MILL3_HOST || "192.168.0.17";
  const port = Number(process.env.PLC_MILL3_PORT || 102);
  const rack = Number(process.env.PLC_MILL3_RACK || 0);
  const slotsToTry = [Number(process.env.PLC_MILL3_SLOT || 2), 1].filter(
    (s, i, a) => a.indexOf(s) === i
  );
  const pollMs = Number(process.env.PLC_MILL3_POLL_MS || 2000);
  const retryMs = Number(process.env.PLC_MILL3_RETRY_MS || 5000);

  let conn = null;
  let pollTimer = null;
  let retryTimer = null;
  let connecting = false;

  /** First successful read after connect: learn M20.6 without treating as a rising edge. */
  let poEndPrimed = false;
  let prevPoEndSignal = false;
  /** Cleared when M20.6 goes FALSE; set when we handle a rising edge while M20.6 stays TRUE. */
  let poEndHandled = false;
  /** After PO end: emit ndtCount 0 until DB251 PO ID changes. */
  let suppressNdtUntilPoChange = false;
  let poIdWhenSuppressed = 0;

  function resetPoEndTracking() {
    poEndPrimed = false;
    prevPoEndSignal = false;
    poEndHandled = false;
    suppressNdtUntilPoChange = false;
    poIdWhenSuppressed = 0;
  }

  function clearPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function clearRetry() {
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
  }

  function emitStatus(status, message) {
    try {
      io.emit("plc:mill3:status", {
        status,
        message: message || "",
        timestamp: formatTimestamp(),
      });
    } catch {
      /* never crash server on emit */
    }
  }

  function emitPlcUpdate(values, ndtForDisplay) {
    const ts = formatTimestamp();
    try {
      io.emit("plc:mill3:update", {
        timestamp: ts,
        mill: "Mill-3",
        poId: toInt(values.poId),
        slitId: toInt(values.slitId),
        ndtCount: ndtForDisplay,
        okCount: toInt(values.okCount),
        nokCount: toInt(values.nokCount),
        status: "connected",
      });
    } catch {
      /* ignore */
    }
  }

  function emitPoEnd(poId, ndtCountFinal) {
    try {
      io.emit("plc:mill3:po_end", {
        mill: "Mill-3",
        poId: toInt(poId),
        ndtCountFinal: toInt(ndtCountFinal),
        timestamp: formatTimestamp(),
      });
    } catch {
      /* ignore */
    }
  }

  /**
   * MES → PLC: pulse M22.7 TRUE, wait 1s, FALSE. Retry full sequence once on failure.
   */
  async function runAckSequence(connection) {
    const tag = "M22.7";
    const once = async () => {
      await writeItemsPromise(connection, tag, true);
      await sleep(1000);
      await writeItemsPromise(connection, tag, false);
    };
    try {
      await once();
      console.log(
        `[${formatTimestamp()}] [ACK SENT] M22.7 acknowledgement written to Mill-3 PLC`
      );
    } catch (e1) {
      console.error(
        `[${formatTimestamp()}] [ACK ERR] M22.7 write failed:`,
        e1 && e1.message ? e1.message : String(e1)
      );
      try {
        await once();
        console.log(
          `[${formatTimestamp()}] [ACK SENT] M22.7 acknowledgement written to Mill-3 PLC (after retry)`
        );
      } catch (e2) {
        console.error(
          `[${formatTimestamp()}] [ACK ERR] M22.7 retry failed:`,
          e2 && e2.message ? e2.message : String(e2)
        );
      }
    }
  }

  async function disconnectClient(reasonForEmit) {
    clearPoll();
    resetPoEndTracking();
    if (conn) {
      const c = conn;
      conn = null;
      try {
        await dropConnectionPromise(c);
      } catch {
        /* ignore */
      }
    }
    if (typeof reasonForEmit === "string" && reasonForEmit.length > 0) {
      emitStatus("disconnected", reasonForEmit);
    }
  }

  async function pollOnce() {
    if (!conn) return false;
    try {
      const values = await readAllItemsPromise(conn);
      for (const k of ITEM_KEYS) {
        if (!(k in values)) {
          throw new Error(`Missing key ${k} in PLC response`);
        }
      }

      const poEnd = toBool(values.poEndSignal);

      if (!poEndPrimed) {
        poEndPrimed = true;
        prevPoEndSignal = poEnd;
        if (!poEnd) {
          poEndHandled = false;
        }
        let ndt0 = toInt(values.ndtCount);
        if (suppressNdtUntilPoChange && toInt(values.poId) === poIdWhenSuppressed) {
          ndt0 = 0;
        } else if (suppressNdtUntilPoChange && toInt(values.poId) !== poIdWhenSuppressed) {
          suppressNdtUntilPoChange = false;
        }
        emitPlcUpdate(values, ndt0);
        return true;
      }

      if (!poEnd) {
        poEndHandled = false;
      }

      const risingEdge =
        !prevPoEndSignal && poEnd === true && !poEndHandled;

      if (risingEdge) {
        poEndHandled = true;
        const poIdVal = toInt(values.poId);
        const ndtFinal = toInt(values.ndtCount);
        console.log(
          `[${formatTimestamp()}] [PO END DETECTED] Mill-3 | PO ID: ${poIdVal} | NDT Count at end: ${ndtFinal}`
        );
        suppressNdtUntilPoChange = true;
        poIdWhenSuppressed = poIdVal;
        emitPoEnd(poIdVal, ndtFinal);
        const c = conn;
        if (c) {
          void runAckSequence(c).catch((err) => {
            console.error(
              `[${formatTimestamp()}] [ACK ERR] Unhandled ack sequence error:`,
              err && err.message ? err.message : String(err)
            );
          });
        }
      }

      prevPoEndSignal = poEnd;

      let ndtForDisplay = toInt(values.ndtCount);
      if (suppressNdtUntilPoChange) {
        const curPo = toInt(values.poId);
        if (curPo !== poIdWhenSuppressed) {
          suppressNdtUntilPoChange = false;
        } else {
          ndtForDisplay = 0;
        }
      }

      emitPlcUpdate(values, ndtForDisplay);
      return true;
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      await disconnectClient(`PLC read failed: ${msg}`);
      scheduleReconnect();
      return false;
    }
  }

  function scheduleReconnect() {
    clearRetry();
    retryTimer = setTimeout(() => {
      retryTimer = null;
      connectAndPoll().catch(() => {
        /* handled inside */
      });
    }, retryMs);
  }

  async function connectAndPoll() {
    if (connecting) return;
    connecting = true;
    clearRetry();
    try {
      await disconnectClient();
      const { conn: newConn } = await connectWithSlotFallback(
        host,
        port,
        rack,
        slotsToTry
      );
      conn = newConn;
      emitStatus("connected", `PLC connected at ${host}`);
      const firstOk = await pollOnce();
      if (!firstOk) return;
      clearPoll();
      pollTimer = setInterval(() => {
        pollOnce().catch(() => {
          /* pollOnce handles */
        });
      }, pollMs);
    } catch {
      await disconnectClient(`PLC unreachable at ${host}`);
      scheduleReconnect();
    } finally {
      connecting = false;
    }
  }

  connectAndPoll().catch(() => {
    scheduleReconnect();
  });

  process.on("SIGINT", async () => {
    clearPoll();
    clearRetry();
    await disconnectClient();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    clearPoll();
    clearRetry();
    await disconnectClient();
    process.exit(0);
  });
}

module.exports = { startPlcPoller };
