# Dashboard behaviour under five-instance split (Phase 1 gaps)

Dashboard continues to call **Shared** (`:5000`) only. Mill instances listen on localhost `5001`–`5004` for ops/validation — not for the UI.

HTTP write proxy for Settings PLC connect / Test `po-end` is **deferred**. Live OK/NOK/NDT on Shared uses `dbo.Mill_Instance_Status`: each mill copies its in-memory S7 snapshot on a **background** timer (~500ms, 2s SQL timeout). Dashboard poll of `GET /api/Status/plc-live` stays ~1s, same as the monolith. **PO-end, threshold/hooter, slit ingest, and mill tag print stay on each mill’s S7/workers** — they never wait on this table. If SQL publish fails, handshake continues.

## Works on Shared (unchanged)

| Area | Notes |
|---|---|
| Reconcile (list / edit / print / SAP resubmit / PPC) | SQL + UNC CSV folders |
| ManualTags (Visual / Hydro / Revisual) | Must stay single Shared process |
| InputSlits list/read | Shared inbox UNC |
| Upload generate-now / scheduler | Shared only |
| Formation chart GET/PUT | Shared UNC file |
| Printers GET/PUT | SQL `Printer` (mills 1–4 as `MILL_n` plus three station keys). Station writes are Shared-only. One Settings save. |
| ZPL generation toggle | SQL `App_Setting.ZplPhysicalPrintEnabled` — Shared + mills observe the same value |

## Empty / stale / wrong-process on Shared

| Endpoint / UI | Behaviour after split | Follow-up |
|---|---|---|
| `GET /api/Status/plc-live` | Shared reads `Mill_Instance_Status` (mill-n publishes). Tiles match mill RAM within ~1s. | — |
| Settings PLC connect / disconnect / test-po-change | No-op or empty registry on Shared | HTTP write proxy to mill |
| `POST /api/Test/po-end` | Runs against Shared's (empty) mill workers — **do not use for production mills** | Proxy to owned mill |
| `POST /api/Test/resume-wip/{n}` | Same — wrong process | Proxy |
| `GET /api/Test/live-mill-ndt` | Shared uses mill-status NDT when S7 is not on this process | — |
| Hooter / handshake live panels | Shared `plc-live` / `plc` from mill-status | — |

## Mill localhost APIs (non-prod / break-glass)

`Status`, `Settings`, `Test` remain registered on mill instances for validation against `http://127.0.0.1:500n`. Firewall should not expose these ports on the LAN.

## Station printers (Shared-only writes)

Mill isolation stays mill-scoped (`MILL_1`…`MILL_4` in `dbo.Printer`). Stations have no mill, so they use the same table with keys `VISUAL_REVISUAL`, `BIG_HYDRO`, `FOUR_HEAD_HYDRO`. Visual and Revisual are **one row** (same physical printer at point A). Mill instances cannot save station rows.

**Behaviour change:** station tags used to print on the bundle mill via `ResolveForMill(state.MillNo)`. They now print at the inspection point — a Mill-2 bundle at Visual comes out on the Visual/Revisual printer.

ManualTags stays Shared-only. Seed all three at `192.168.0.125:9100`. Empty station row fails with the station display name; no mill fallback.
