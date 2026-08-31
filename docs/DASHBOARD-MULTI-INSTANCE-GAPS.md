# Dashboard behaviour under five-instance split (Phase 1 gaps)

Dashboard continues to call **Shared** (`:5000`) only. Mill instances listen on localhost `5001`–`5004` for ops/validation — not for the UI.

HTTP write proxy and `Mill_Instance_Status` SQL telemetry are **deferred**.

## Works on Shared (unchanged)

| Area | Notes |
|---|---|
| Reconcile (list / edit / print / SAP resubmit / PPC) | SQL + UNC CSV folders |
| ManualTags (Visual / Hydro / Revisual) | Must stay single Shared process |
| InputSlits list/read | Shared inbox UNC |
| Upload generate-now / scheduler | Shared only |
| Formation chart GET/PUT | Shared UNC file |
| Printers GET/PUT | SQL `Mill_Printer` (mills 1–4) plus Shared-only `Station_Printer` (three station codes). One Settings save. |
| ZPL generation toggle | SQL `App_Setting.ZplPhysicalPrintEnabled` — Shared + mills observe the same value |

## Empty / stale / wrong-process on Shared

| Endpoint / UI | Behaviour after split | Follow-up |
|---|---|---|
| `GET /api/Status/plc-live` | Empty or stale — Shared has no S7 mill loops | `Mill_Instance_Status` publish/read |
| Settings PLC connect / disconnect / test-po-change | No-op or empty registry on Shared | HTTP write proxy to mill |
| `POST /api/Test/po-end` | Runs against Shared's (empty) mill workers — **do not use for production mills** | Proxy to owned mill |
| `POST /api/Test/resume-wip/{n}` | Same — wrong process | Proxy |
| `GET /api/Test/live-mill-ndt` | Empty on Shared | Telemetry or proxy |
| Hooter / handshake live panels | Stale | Telemetry |

## Mill localhost APIs (non-prod / break-glass)

`Status`, `Settings`, `Test` remain registered on mill instances for validation against `http://127.0.0.1:500n`. Firewall should not expose these ports on the LAN.

## Station printers (Shared-only table)

`Mill_Printer` isolation keys stay mill-scoped. Stations have no mill, so they live in `dbo.Station_Printer` (`VISUAL_REVISUAL`, `BIG_HYDRO`, `FOUR_HEAD_HYDRO`). Visual and Revisual are **one row** (same physical printer at point A).

**Behaviour change:** station tags used to print on the bundle mill via `ResolveForMill(state.MillNo)`. They now print at the inspection point — a Mill-2 bundle at Visual comes out on the Visual/Revisual printer.

ManualTags stays Shared-only. Seed all three at `192.168.0.125:9100`. Empty station row fails with the station display name; no mill fallback.
