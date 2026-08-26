using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.InstanceLease;

public sealed class MillLeaseClaimResult
{
    public bool Claimed { get; init; }
    public string? HolderMachineName { get; init; }
    public string? HolderServiceName { get; init; }
    public Guid? HolderInstanceId { get; init; }
}

public enum MillLeaseRenewOutcome
{
    /// <summary>This instance still holds the lease; expiry extended.</summary>
    Renewed = 0,

    /// <summary>UPDATE matched 0 rows — another holder or expired without reclaim. Stop the host immediately.</summary>
    LostLease = 1
}

public interface IMillInstanceLeaseService
{
    Guid InstanceId { get; }

    /// <summary>
    /// Atomic claim: UPDATE expired-or-self row, else INSERT if absent. Never SELECT-then-UPDATE.
    /// </summary>
    Task<MillLeaseClaimResult> TryClaimAsync(
        int millNo,
        string? serviceName,
        int ttlSeconds,
        CancellationToken cancellationToken);

    /// <summary>
    /// Renew for this Instance_Id. Returns <see cref="MillLeaseRenewOutcome.LostLease"/> when rows-affected is 0.
    /// SQL connectivity errors propagate as exceptions (caller may retry then stop host).
    /// </summary>
    Task<MillLeaseRenewOutcome> TryRenewAsync(int millNo, int ttlSeconds, CancellationToken cancellationToken);

    Task ReleaseAsync(int millNo, CancellationToken cancellationToken);
}

public sealed class MillInstanceLeaseService : IMillInstanceLeaseService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<MillInstanceLeaseService> _logger;

    public MillInstanceLeaseService(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<MillInstanceLeaseService> logger)
    {
        _options = options;
        _logger = logger;
        InstanceId = Guid.NewGuid();
    }

    public Guid InstanceId { get; }

    public async Task<MillLeaseClaimResult> TryClaimAsync(
        int millNo,
        string? serviceName,
        int ttlSeconds,
        CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            return new MillLeaseClaimResult { Claimed = false };

        if (!SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue))
        {
            _logger.LogWarning(
                "SQL disabled — skipping Mill_Instance_Lease claim for mill {Mill} (dev/test only).",
                millNo);
            return new MillLeaseClaimResult { Claimed = true };
        }

        var ttl = Math.Clamp(ttlSeconds, 5, 600);
        var machine = Environment.MachineName;
        var service = string.IsNullOrWhiteSpace(serviceName) ? "NdtBundleService" : serviceName.Trim();
        var processStart = DateTime.UtcNow;

        await using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Instance_Lease claim", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
DECLARE @Claimed BIT = 0;
DECLARE @HolderMachine NVARCHAR(128) = NULL;
DECLARE @HolderService NVARCHAR(128) = NULL;
DECLARE @HolderId UNIQUEIDENTIFIER = NULL;

UPDATE dbo.Mill_Instance_Lease WITH (UPDLOCK, HOLDLOCK)
SET Instance_Id = @InstanceId,
    Machine_Name = @Machine,
    Service_Name = @Service,
    Process_Start_AtUtc = @ProcessStart,
    Lease_Acquired_AtUtc = CASE
        WHEN Instance_Id = @InstanceId THEN Lease_Acquired_AtUtc
        ELSE SYSUTCDATETIME()
    END,
    Lease_Renewed_AtUtc = SYSUTCDATETIME(),
    Lease_Expires_AtUtc = DATEADD(SECOND, @Ttl, SYSUTCDATETIME())
WHERE Mill_No = @MillNo
  AND (Lease_Expires_AtUtc < SYSUTCDATETIME() OR Instance_Id = @InstanceId);

IF @@ROWCOUNT = 1
BEGIN
    SET @Claimed = 1;
END
ELSE IF NOT EXISTS (SELECT 1 FROM dbo.Mill_Instance_Lease WITH (UPDLOCK, HOLDLOCK) WHERE Mill_No = @MillNo)
BEGIN
    INSERT INTO dbo.Mill_Instance_Lease (
        Mill_No, Instance_Id, Machine_Name, Service_Name,
        Process_Start_AtUtc, Lease_Acquired_AtUtc, Lease_Renewed_AtUtc, Lease_Expires_AtUtc)
    VALUES (
        @MillNo, @InstanceId, @Machine, @Service,
        @ProcessStart, SYSUTCDATETIME(), SYSUTCDATETIME(), DATEADD(SECOND, @Ttl, SYSUTCDATETIME()));
    SET @Claimed = 1;
END
ELSE
BEGIN
    SELECT
        @HolderMachine = Machine_Name,
        @HolderService = Service_Name,
        @HolderId = Instance_Id
    FROM dbo.Mill_Instance_Lease
    WHERE Mill_No = @MillNo;
END

SELECT @Claimed AS Claimed, @HolderMachine AS HolderMachine, @HolderService AS HolderService, @HolderId AS HolderId;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@MillNo", millNo);
        cmd.Parameters.AddWithValue("@InstanceId", InstanceId);
        cmd.Parameters.AddWithValue("@Machine", machine);
        cmd.Parameters.AddWithValue("@Service", service);
        cmd.Parameters.AddWithValue("@ProcessStart", processStart);
        cmd.Parameters.AddWithValue("@Ttl", ttl);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return new MillLeaseClaimResult { Claimed = false };

        var claimed = reader.GetBoolean(0);
        return new MillLeaseClaimResult
        {
            Claimed = claimed,
            HolderMachineName = reader.IsDBNull(1) ? null : reader.GetString(1),
            HolderServiceName = reader.IsDBNull(2) ? null : reader.GetString(2),
            HolderInstanceId = reader.IsDBNull(3) ? null : reader.GetGuid(3)
        };
    }

    public async Task<MillLeaseRenewOutcome> TryRenewAsync(int millNo, int ttlSeconds, CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue))
            return MillLeaseRenewOutcome.Renewed;

        var ttl = Math.Clamp(ttlSeconds, 5, 600);
        await using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Instance_Lease renew", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
UPDATE dbo.Mill_Instance_Lease
SET Lease_Renewed_AtUtc = SYSUTCDATETIME(),
    Lease_Expires_AtUtc = DATEADD(SECOND, @Ttl, SYSUTCDATETIME())
WHERE Mill_No = @MillNo AND Instance_Id = @InstanceId;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@MillNo", millNo);
        cmd.Parameters.AddWithValue("@InstanceId", InstanceId);
        cmd.Parameters.AddWithValue("@Ttl", ttl);
        var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return n == 1 ? MillLeaseRenewOutcome.Renewed : MillLeaseRenewOutcome.LostLease;
    }

    public async Task ReleaseAsync(int millNo, CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Instance_Lease release", cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
DELETE FROM dbo.Mill_Instance_Lease
WHERE Mill_No = @MillNo AND Instance_Id = @InstanceId;";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@InstanceId", InstanceId);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to release mill lease for mill {Mill}.", millNo);
        }
    }
}
