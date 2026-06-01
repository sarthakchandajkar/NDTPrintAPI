# Grant SQL access on the production VM (AJS-SOH-VM-PAS-)

On this VM, NetBIOS limits the computer name to **15 characters**:

| Source | Value |
|--------|--------|
| `echo %COMPUTERNAME%` | `AJS-SOH-VM-PAS-` (trailing dash) |
| `whoami` | `ajs-soh-vm-pas-\sarthak` |
| SSMS `CREATE LOGIN` (working) | `[AJS-SOH-VM-PAS-\Sarthak]` |

Do **not** use `AJS-SOH-VM-PAS-DEV` or `AJS-SOH-VM-PAS\Sarthak` without the trailing dash unless SSMS **Search** resolves them.

## 1. API: suggested login names

`GET http://localhost:5000/api/Status/sql-traceability`

Use **`suggestedSqlLogins`** (built from `COMPUTERNAME` + the service account name).

## 2. SSMS script (verified on this VM)

Run as Administrator, connected to **`localhost\SQLEXPRESS`**:

```sql
USE [master];
GO
CREATE LOGIN [AJS-SOH-VM-PAS-\Sarthak] FROM WINDOWS;
GO
USE [JazeeraMES_Prod];
GO
CREATE USER [AJS-SOH-VM-PAS-\Sarthak] FOR LOGIN [AJS-SOH-VM-PAS-\Sarthak];
GO
ALTER ROLE db_datareader ADD MEMBER [AJS-SOH-VM-PAS-\Sarthak];
ALTER ROLE db_datawriter ADD MEMBER [AJS-SOH-VM-PAS-\Sarthak];
GO
```

Shorthand on the same machine: `CREATE LOGIN [.\Sarthak] FROM WINDOWS;`

## 3. Connection string behavior

`NdtBundleService` accepts `Server=AJS-SOH-VM-PAS-\SQLEXPRESS` (and older `AJS-SOH-VM-PAS-DEV\SQLEXPRESS` in env overrides) and rewrites to **`localhost\SQLEXPRESS`** when the app runs on this VM so the service does not depend on SQL Browser resolving the truncated NetBIOS name.

## 4. Optional: fix truncated COMPUTERNAME

Long term, renaming the VM to a name ≤15 characters without a trailing dash (e.g. `AJS-SOH-PAS-DEV`) avoids confusion. That is a Windows admin change, not required for NDT if SQL grants use the names above.
