# Mill-3 PLC bridge

Reads Siemens S7-300 **DB251** and **M20.6** (PO end) via **nodes7**, broadcasts on **Socket.IO**. Writes **M22.7** only as a **PO-end acknowledgement pulse** (true → 1s → false).

## Socket.IO events

| Event | Purpose |
|-------|---------|
| `plc:mill3:update` | Every poll (~2s): DB251 values; **ndtCount** is held at **0** after PO end until **PO ID** changes. |
| `plc:mill3:po_end` | Once on **M20.6** rising edge: `mill`, `poId`, `ndtCountFinal`, `timestamp`. |
| `plc:mill3:status` | Connection / PLC reachability. |

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
