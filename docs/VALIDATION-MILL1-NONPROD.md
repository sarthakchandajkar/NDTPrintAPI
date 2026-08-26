# Non-prod Mill-1 validation (required before production cutover)

**Non-negotiable:** stand up Mill-1 against a **non-production** config while the current monolith's Mill-1 workers are explicitly disabled. Run a real PO transition. Never run monolith + Mill-1 instance against production for the same mill.

## Setup

1. Apply `docs/App_Setting_AddTable.sql` and `docs/Mill_Instance_Lease.sql` on the **non-prod** database.
2. Copy `deploy/instances/mill-1/appsettings.Production.json` to a non-prod content root; point folders + SQL at non-prod.
3. Run `Split-MillStateFiles.ps1 -MillNo 1` against a copy of runtime state (or start with empty mill-1 files + `InitialMillBatchNumbers`).
4. On the **monolith** (or Shared-only stand-in) still pointing at non-prod if needed:
   - `InputSlitProcessMills` must **not** include `1` (e.g. `[]` or omit Mill-1)
   - `PlcHandshake.Mills` Mill-1: `PlcHandshakeEnabled: false`
5. Start **only** `NdtBundleService-M1` (contentRoot = mill-1). Confirm lease claim log for mill 1.
6. Confirm a second start of another process with `OwnedMillNos: [1]` **fails** with "already owned".

## PO transition checklist

1. Place / generate a Mill-1 input slit file for a test PO; confirm Mill-1 instance stamps fill-to-target / Constant as configured and writes NDT Input Slit CSV.
2. Trigger PLC PO-end (or Test `po-end` against **localhost:5001**, not Shared).
3. Confirm bundle close, tag print path, runtime state `NdtBundleRuntimeState-M1.json` sequence advances.
4. Confirm Shared (if running) still serves reconcile/read APIs without mill workers.
5. Stop Mill-1; confirm monolith-with-Mill-1-disabled does **not** process Mill-1 slits while Mill-1 is down (expected gap until Mill-1 restarts).

## Pass criteria

- One crash/kill of Mill-1 leaves Shared + other mills (if running) up.
- Lease prevents dual writers for mill 1.
- Real PO end-to-end on Mill-1 instance completed on non-prod.

Only then schedule production cutover per [DEPLOYMENT-FIVE-INSTANCE.md](./DEPLOYMENT-FIVE-INSTANCE.md).
