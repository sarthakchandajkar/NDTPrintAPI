namespace NdtBundleService.Services;

/// <summary>
/// Last Modbus TCP outcome for PLC status (updated whenever <see cref="ModbusTcpMillPoEndPlcClient"/> performs I/O).
/// </summary>
public sealed class PlcConnectionHealth
{
    private readonly object _sync = new();

    /// <summary>True when the last poll had at least one configured endpoint and all reads succeeded.</summary>
    public bool? LastReadOk { get; private set; }

    public DateTimeOffset? LastUpdateUtc { get; private set; }

    public string? LastError { get; private set; }

    public void RecordModbusPoll(bool anyEndpointAttempted, bool allReadsSucceeded, string? errorDetail = null)
    {
        lock (_sync)
        {
            if (!anyEndpointAttempted)
            {
                LastReadOk = false;
                LastError = errorDetail ?? "No PLC endpoints configured or no I/O attempted.";
            }
            else
            {
                LastReadOk = allReadsSucceeded;
                LastError = allReadsSucceeded ? null : (errorDetail ?? "One or more Modbus reads failed.");
            }

            LastUpdateUtc = DateTimeOffset.UtcNow;
        }
    }
}
