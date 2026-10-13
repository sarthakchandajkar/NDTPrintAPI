namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Outcome of a settings-initiated NDT bundle hooter pulse test for one mill.</summary>
public sealed class PlcHooterTestResult
{
    public bool Success { get; init; }

    public string Message { get; init; } = string.Empty;

    public int MillNo { get; init; }

    public string MillName { get; init; } = string.Empty;

    public bool PlcConnected { get; init; }

    public bool HooterEnabled { get; init; }

    public string? PoNumber { get; init; }

    public string? PipeSize { get; init; }

    public int Accumulated { get; init; }

    public int Threshold { get; init; }

    public bool AboveThreshold { get; init; }

    public bool RequireThreshold { get; init; }

    public bool PasEnable { get; init; }

    public bool QBefore { get; init; }

    public bool Pulsed { get; init; }

    public int DurationMs { get; init; }

    public string OutputAddress { get; init; } = string.Empty;

    public string PasEnableAddress { get; init; } = string.Empty;

    public IReadOnlyList<string> Steps { get; init; } = Array.Empty<string>();
}
