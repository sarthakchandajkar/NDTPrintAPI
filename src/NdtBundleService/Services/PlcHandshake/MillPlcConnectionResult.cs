namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Result of enabling or disabling a single mill's S7 handshake connection.</summary>
public sealed class MillPlcConnectionResult
{
    public bool Success { get; init; }

    public int MillNo { get; init; }

    public string MillName { get; init; } = string.Empty;

    public bool PlcConnectionEnabled { get; init; }

    public bool Connected { get; init; }

    public string Message { get; init; } = string.Empty;
}
