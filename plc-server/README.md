# Mills 1–4 PLC bridge

Reads Siemens S7-300 **DB251** (OK / NOK / NDT at DBW2 / DBW4 / DBW6) and per-mill **PO change** coils via **nodes7**, broadcasts on **Socket.IO**. Writes the per-mill **MES ack** coil after PO end (pulse).

| Mill | PLC IP | PO end (PLC→app) | MES ack (app→PLC) |
|------|--------|------------------|-------------------|
| 1 | 192.168.0.13 | M40.6 | M40.7 |
| 2 | 192.168.0.60 | M40.6 | M40.7 |
| 3 | 192.168.0.17 | M20.6 | M22.7 |
| 4 | 192.168.0.19 | M41.6 | M41.7 |

## Socket.IO events (per mill *N* = 1…4)

| Event | Purpose |
|-------|---------|
| `plc:millN:update` | Every poll (~2s): DB251 values; **ndtCount** held at **0** after PO end until **PO ID** changes (Mill-3 behaviour). |
| `plc:millN:po_end` | Once on PO-end coil **rising edge**: `millNo`, `poId`, `ndtCountFinal`, `timestamp`. |
| `plc:millN:status` | Connection / PLC reachability. |
| `plc:mill3:label_printed` | Mill-3 only when label auto-print enabled. |

## NDT labels from PLC counts (Mill-3)

When `ENABLE_PLC_LABEL_PRINT` is not disabled, Mill-3 poll reads live `ndtCount`, WIP folder, threshold labels, and partial print on PO end (see previous Mill-3-only section in git history).

## Run

```bash
cd plc-server
npm install
npm start
```

Default: `http://localhost:3030` (Socket.IO). Set `NEXT_PUBLIC_PLC_SOCKET_URL` in the dashboard to match.

Dashboard: **Mills PLC** (`/mills-plc`) or **Summary** table (OK / NOK / NDT columns).

### Environment (optional)

| Variable | Default |
|----------|---------|
| `PLC_ENABLED_MILLS` | `1,2,3,4` |
| `PLC_MILL1_HOST` … `PLC_MILL4_HOST` | See table above |
| `PLC_POLL_MS` | `2000` |
| `PLC_MILL3_ACK_PULSE_MS` | `1000` (Mill-3); others use `PLC_MILLn_ACK_PULSE_MS` or same default |

Legacy env names `PLC_MILL3_*` still work for Mill-3.
