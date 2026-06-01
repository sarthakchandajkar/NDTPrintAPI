"use strict";

const {
  buildMillVariables,
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
 * One Siemens PLC connection + poll loop for a single mill.
 * @param {import('socket.io').Server} io
 * @param {import('./millPlcConfig').MillPlcConfig} cfg
 * @param {import('./labelOrchestrator').PlcLabelOrchestrator | null} labelOrch
 */
function startSingleMillPoller(io, cfg, labelOrch) {
  const millNo = cfg.millNo;
  const eventPrefix = `plc:mill${millNo}`;
  const variableMap = {
    ...buildMillVariables(cfg.poEndSignal),
    mesAck: cfg.mesAckCoil,
  };
  const readKeys = ["ndtCount", "okCount", "nokCount", "poId", "slitId", "poEndSignal"];
  const itemKeys = Object.keys(variableMap);

  let conn = null;
  let pollTimer = null;
  let retryTimer = null;
  let connecting = false;

  let poEndPrimed = false;
  let prevPoEndSignal = false;
  let poEndHandled = false;
  let suppressNdtUntilPoChange = false;
  let poIdWhenSuppressed = 0;

  function resetPoEndTracking() {
    poEndPrimed = false;
    prevPoEndSignal = false;
    poEndHandled = false;
    suppressNdtUntilPoChange = false;
    poIdWhenSuppressed = 0;
    if (labelOrch) labelOrch.reset();
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
      io.emit(`${eventPrefix}:status`, {
        millNo,
        mill: cfg.millLabel,
        status,
        message: message || "",
        timestamp: formatTimestamp(),
        host: cfg.host,
        poEndSignal: cfg.poEndSignal,
        mesAckCoil: cfg.mesAckCoil,
      });
    } catch {
      /* never crash server on emit */
    }
  }

  function emitPlcUpdate(values, ndtForDisplay) {
    const ts = formatTimestamp();
    try {
      io.emit(`${eventPrefix}:update`, {
        millNo,
        timestamp: ts,
        mill: cfg.millLabel,
        poId: toInt(values.poId),
        slitId: toInt(values.slitId),
        ndtCount: ndtForDisplay,
        okCount: toInt(values.okCount),
        nokCount: toInt(values.nokCount),
        poEndActive: toBool(values.poEndSignal),
        status: "connected",
      });
    } catch {
      /* ignore */
    }
  }

  function emitPoEnd(poId, ndtCountFinal) {
    try {
      io.emit(`${eventPrefix}:po_end`, {
        millNo,
        mill: cfg.millLabel,
        poId: toInt(poId),
        ndtCountFinal: toInt(ndtCountFinal),
        timestamp: formatTimestamp(),
      });
    } catch {
      /* ignore */
    }
  }

  async function runAckSequence(connection) {
    const pulseMs = Math.max(0, cfg.mesAckPulseMs);
    const once = async () => {
      await writeItemsPromise(connection, "mesAck", true);
      if (pulseMs > 0) await sleep(pulseMs);
      await writeItemsPromise(connection, "mesAck", false);
    };
    try {
      await once();
      console.log(
        `[${formatTimestamp()}] [ACK SENT] ${cfg.mesAckCoil} acknowledgement written to ${cfg.millLabel} PLC (${cfg.host})`
      );
    } catch (e1) {
      console.error(
        `[${formatTimestamp()}] [ACK ERR] ${cfg.mesAckCoil} write failed (${cfg.millLabel}):`,
        e1 && e1.message ? e1.message : String(e1)
      );
      try {
        await once();
        console.log(
          `[${formatTimestamp()}] [ACK SENT] ${cfg.mesAckCoil} written to ${cfg.millLabel} (after retry)`
        );
      } catch (e2) {
        console.error(
          `[${formatTimestamp()}] [ACK ERR] ${cfg.mesAckCoil} retry failed (${cfg.millLabel}):`,
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
      for (const k of readKeys) {
        if (!(k in values)) {
          throw new Error(`Missing key ${k} in PLC response`);
        }
      }

      const poEnd = toBool(values.poEndSignal);
      const ndtRaw = toInt(values.ndtCount);
      const poIdVal = toInt(values.poId);

      if (labelOrch) {
        try {
          await labelOrch.processPoll(ndtRaw, poIdVal);
        } catch (labelErr) {
          console.error(
            `[${formatTimestamp()}] [LABEL ERR ${cfg.millLabel}]`,
            labelErr && labelErr.message ? labelErr.message : String(labelErr)
          );
        }
      }

      if (!poEndPrimed) {
        poEndPrimed = true;
        prevPoEndSignal = poEnd;
        if (!poEnd) poEndHandled = false;
        let ndt0 = ndtRaw;
        if (suppressNdtUntilPoChange && poIdVal === poIdWhenSuppressed) {
          ndt0 = 0;
        } else if (suppressNdtUntilPoChange && poIdVal !== poIdWhenSuppressed) {
          suppressNdtUntilPoChange = false;
        }
        emitPlcUpdate(values, ndt0);
        return true;
      }

      if (!poEnd) poEndHandled = false;

      const risingEdge = !prevPoEndSignal && poEnd === true && !poEndHandled;

      if (risingEdge) {
        poEndHandled = true;
        const ndtFinal = ndtRaw;
        console.log(
          `[${formatTimestamp()}] [PO END DETECTED] ${cfg.millLabel} | PO ID: ${poIdVal} | NDT: ${ndtFinal} | signal ${cfg.poEndSignal}`
        );
        if (labelOrch) {
          try {
            await labelOrch.onPoEnd(ndtFinal, poIdVal);
          } catch (labelErr) {
            console.error(
              `[${formatTimestamp()}] [LABEL ERR PO END ${cfg.millLabel}]`,
              labelErr && labelErr.message ? labelErr.message : String(labelErr)
            );
          }
        }
        suppressNdtUntilPoChange = true;
        poIdWhenSuppressed = poIdVal;
        emitPoEnd(poIdVal, ndtFinal);
        const c = conn;
        if (c) {
          void runAckSequence(c).catch((err) => {
            console.error(
              `[${formatTimestamp()}] [ACK ERR ${cfg.millLabel}]`,
              err && err.message ? err.message : String(err)
            );
          });
        }
      }

      prevPoEndSignal = poEnd;

      let ndtForDisplay = ndtRaw;
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
    }, cfg.retryMs);
  }

  async function connectAndPoll() {
    if (connecting) return;
    connecting = true;
    clearRetry();
    try {
      await disconnectClient();
      const { conn: newConn } = await connectWithSlotFallback(
        cfg.host,
        cfg.port,
        cfg.rack,
        cfg.slots,
        variableMap
      );
      conn = newConn;
      emitStatus("connected", `PLC connected at ${cfg.host}`);
      const firstOk = await pollOnce();
      if (!firstOk) return;
      clearPoll();
      pollTimer = setInterval(() => {
        pollOnce().catch(() => {
          /* pollOnce handles */
        });
      }, cfg.pollMs);
    } catch {
      await disconnectClient(`PLC unreachable at ${cfg.host}`);
      scheduleReconnect();
    } finally {
      connecting = false;
    }
  }

  console.log(
    `[${formatTimestamp()}] [PLC] Starting ${cfg.millLabel} poller → ${cfg.host}:${cfg.port} | PO end ${cfg.poEndSignal} | ack ${cfg.mesAckCoil} | poll ${cfg.pollMs}ms`
  );

  connectAndPoll().catch(() => {
    scheduleReconnect();
  });

  return {
    millNo,
    shutdown: async () => {
      clearPoll();
      clearRetry();
      await disconnectClient();
    },
  };
}

module.exports = { startSingleMillPoller };
