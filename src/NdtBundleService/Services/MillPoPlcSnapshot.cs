namespace NdtBundleService.Services;

/// <summary>
/// One Modbus read cycle of PO-related tags for a single mill (independent instance data per mill).
/// </summary>
public sealed class MillPoPlcSnapshot
{
    public int PoId { get; init; }

    public int? PoTypeId { get; init; }

    /// <summary>When the mill does not configure a slit-valid coil, this is true.</summary>
    public bool SlitEntryValid { get; init; } = true;

    public int? SlitEntryCount { get; init; }

    public bool ReadOk { get; init; } = true;
}
