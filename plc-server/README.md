# Mill-3 PLC bridge

Reads Siemens S7-300 **DB251** and **M20.6** (PO end) via **nodes7**, broadcasts on **Socket.IO**. Writes **M22.7** only as a **PO-end acknowledgement pulse** (true → 1s → false).

## Socket.IO events

| Event | Purpose |
|-------|---------|
| `plc:mill3:update` | Every poll (~2s): DB251 values; **ndtCount** is held at **0** after PO end until **PO ID** changes. |
| `plc:mill3:po_end` | Once on **M20.6** rising edge: `mill`, `poId`, `ndtCountFinal`, `timestamp`. |
| `plc:mill3:status` | Connection / PLC reachability. |
| `plc:mill3:label_printed` | After a ZPL send: `ndtBatchNo`, `pcsInBundle`, `poNumber`, `reason` (`threshold` or `po_end_partial`). |

## NDT labels from PLC counts

When `ENABLE_PLC_LABEL_PRINT` is not disabled, each poll reads the **live** `ndtCount` from the PLC and:

1. Resolves the current WIP row from **`WIP_BUNDLE_FOLDER`** (latest `WIP_{MM}_…` file for **`PLC_MILL_NUMBER`**), reads **Pipe Size** / **Pieces Per Bundle**, and derives the NDT-per-bundle threshold (same rules as `NdtBundleService` formation chart + optional CSV override).
2. Prints a **full-bundle** ZPL label when accumulated NDT since the last print reaches the threshold (overshoot in one poll is one label, matching `NdtBundleEngine`).
3. On **M20.6** PO end (rising edge), prints any **partial** NDT since the last label, then runs the usual **M22.7** ack pulse.

If the PLC reports NDT counts **before** any WIP file exists for that mill, the first WIP discovery **primes** the baseline to the current count so no retroactive burst prints. **NDT batch numbers** use the same `12YYM#####` format as `CsvBundleOutputWriter`.

## Run

```bash
cd plc-server
npm install
npm start
```

Default: `http://localhost:3030` (Socket.IO). Set `NEXT_PUBLIC_PLC_SOCKET_URL` in the dashboard to match.

### Environment (optional)

| Variable | Default |
|----------|---------|
| `PLC_SERVER_PORT` | `3030` |
| `PLC_MILL3_HOST` | `192.168.0.17` |
| `PLC_MILL3_PORT` | `102` |
| `PLC_MILL3_RACK` | `0` |
| `PLC_MILL3_SLOT` | `2` (then retries slot `1`) |
| `PLC_MILL3_POLL_MS` | `2000` |
| `PLC_MILL3_RETRY_MS` | `5000` |
| `PLC_SERVER_CORS_ORIGIN` | reflect origin (`true`) |
| `ENABLE_PLC_LABEL_PRINT` | `1` (set `0` / `false` / `no` to disable) |
| `PLC_MILL_NUMBER` | `3` (must match WIP filenames, e.g. `WIP_03_…`) |
| `WIP_BUNDLE_FOLDER` | `Z:\To SAP\TM\Bundle` |
| `PRINTER_HOST` / `NDT_TAG_PRINTER_HOST` | `192.168.0.125` (set empty to skip TCP send; threshold logic still runs) |
| `PRINTER_PORT` / `NDT_TAG_PRINTER_PORT` | `9100` |
| `PRINTER_LOCAL_BIND` / `NDT_TAG_PRINTER_LOCAL_BIND` | _(optional source IP)_ |
| `NDT_LABEL_STATION_TEXT` | _(optional ZPL station line)_ |
