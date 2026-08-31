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
| Printers GET/PUT | SQL `Mill_Printer` (mills 1–4). Station printers are **not** in this table — see follow-up below |
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

## Follow-up: station printers (Shared-only table)

Do **not** generalise `Mill_Printer`. Isolation keys stay mill-scoped.

Today station tags print to the **bundle’s mill printer**: `ManualNdtTagService` → `ResolveForMill(state.MillNo)`. Adding station printers **changes where paper physically comes out** — a Mill-2 bundle at Visual would print on the Visual printer, not Mill-2’s.

Design:

- Separate Shared-only table (e.g. `Station_Printer`), not mill numbers 5–8.
- Four targets resolved **by station**: Visual, Revisual, Big Hydro, Four-Head Hydro.
- Seed Visual and Revisual to the **same IP** (shared physical printer).
- ManualTags stays Shared-only (`[InstanceRole(Monolith, Shared)]`).
