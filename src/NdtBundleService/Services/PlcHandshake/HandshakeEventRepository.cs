using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Persists <c>dbo.Handshake_Event</c> rows (best-effort; never throws to caller).</summary>
public sealed class HandshakeEventRepository : IHandshakeEventRepository
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<HandshakeEventRepository> _logger;

    public HandshakeEventRepository(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<HandshakeEventRepository> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task UpsertAsync(HandshakeEventRecord record, CancellationToken cancellationToken = default)
    {
        var opt = _options.CurrentValue;
        if (!SqlTraceabilityConnection.IsSqlEnabled(opt))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
MERGE dbo.Handshake_Event AS t
USING (SELECT @CorrelationId AS Correlation_Id) AS s
ON t.Correlation_Id = s.Correlation_Id
WHEN MATCHED THEN UPDATE SET
    Ack_AtUtc = @AckAtUtc,
    Cleared_AtUtc = @ClearedAtUtc,
    Ack_Dropped_AtUtc = @AckDroppedAtUtc,
    Plc_Po_Id = @PlcPoId,
    Plc_Ndt_Count = @PlcNdtCount,
    Outcome = @Outcome,
    Error_Message = @ErrorMessage,
    Updated_AtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (Mill_No, Edge_AtUtc, Ack_AtUtc, Cleared_AtUtc, Ack_Dropped_AtUtc, Plc_Po_Id, Plc_Ndt_Count, Correlation_Id, Outcome, Error_Message)
VALUES
    (@MillNo, @EdgeAtUtc, @AckAtUtc, @ClearedAtUtc, @AckDroppedAtUtc, @PlcPoId, @PlcNdtCount, @CorrelationId, @Outcome, @ErrorMessage);";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MillNo", record.MillNo);
            cmd.Parameters.AddWithValue("@EdgeAtUtc", record.EdgeAtUtc.UtcDateTime);
            cmd.Parameters.AddWithValue("@AckAtUtc", (object?)record.AckAtUtc?.UtcDateTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ClearedAtUtc", (object?)record.ClearedAtUtc?.UtcDateTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@AckDroppedAtUtc", (object?)record.AckDroppedAtUtc?.UtcDateTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PlcPoId", record.PlcPoId);
            cmd.Parameters.AddWithValue("@PlcNdtCount", record.PlcNdtCount);
            cmd.Parameters.AddWithValue("@CorrelationId", record.CorrelationId);
            cmd.Parameters.AddWithValue("@Outcome", record.Outcome.ToString());
            cmd.Parameters.AddWithValue("@ErrorMessage", (object?)record.ErrorMessage ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Handshake_Event upsert failed for CorrelationId {CorrelationId} (run docs/Handshake_Event_AddTable.sql if the table is missing).",
                record.CorrelationId);
        }
    }
}
