namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Outcome of a settings-initiated PO-change / handshake test for one mill.</summary>
public sealed class PlcPoChangeTestResult
{
    public bool Success { get; init; }

    public string Message { get; init; } = string.Empty;

    public int MillNo { get; init; }

    public string MillName { get; init; } = string.Empty;

    public bool PlcConnected { get; init; }

    public bool TriggerBefore { get; init; }

    public bool TriggerAfter { get; init; }

    public bool AckPulsed { get; init; }

    public bool WorkflowInvoked { get; init; }

    public string? PoNumber { get; init; }

    public IReadOnlyList<string> Steps { get; init; } = Array.Empty<string>();
}
