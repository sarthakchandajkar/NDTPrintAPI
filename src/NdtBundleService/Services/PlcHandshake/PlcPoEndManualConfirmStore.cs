namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Pending PLC PO-end that waits for an operator dashboard confirm (flush + MES ack)
/// when <see cref="Configuration.PlcHandshakeOptions.ManualConfirmPoEnd"/> is enabled.
/// </summary>
public interface IPlcPoEndManualConfirmStore
{
    void Arm(PlcPoEndManualConfirmPending pending);

    bool TryGet(int millNo, out PlcPoEndManualConfirmPending pending);

    bool TryTake(int millNo, out PlcPoEndManualConfirmPending pending);

    IReadOnlyList<PlcPoEndManualConfirmPending> List();

    void Clear(int millNo);
}

public sealed record PlcPoEndManualConfirmPending(
    int MillNo,
    string MillName,
    int PoIdAtEdge,
    int NdtAtEdge,
    Guid CorrelationId,
    DateTimeOffset DetectedAtUtc,
    string? RunningPoAtEdge);

public sealed class PlcPoEndManualConfirmStore : IPlcPoEndManualConfirmStore
{
    private readonly object _gate = new();
    private readonly Dictionary<int, PlcPoEndManualConfirmPending> _byMill = new();

    public void Arm(PlcPoEndManualConfirmPending pending)
    {
        if (pending.MillNo is < 1 or > 4)
            return;
        lock (_gate)
            _byMill[pending.MillNo] = pending;
    }

    public bool TryGet(int millNo, out PlcPoEndManualConfirmPending pending)
    {
        lock (_gate)
            return _byMill.TryGetValue(millNo, out pending!);
    }

    public bool TryTake(int millNo, out PlcPoEndManualConfirmPending pending)
    {
        lock (_gate)
        {
            if (!_byMill.TryGetValue(millNo, out pending!))
                return false;
            _byMill.Remove(millNo);
            return true;
        }
    }

    public IReadOnlyList<PlcPoEndManualConfirmPending> List()
    {
        lock (_gate)
            return _byMill.Values.OrderBy(p => p.MillNo).ToList();
    }

    public void Clear(int millNo)
    {
        lock (_gate)
            _byMill.Remove(millNo);
    }
}
