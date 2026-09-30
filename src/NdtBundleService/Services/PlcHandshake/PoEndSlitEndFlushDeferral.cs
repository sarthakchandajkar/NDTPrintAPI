namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Arms Immediate PO-end remainder flush to run after the next PLC slit-end (e.g. L1_ButtEnd)
/// when M40.6 arrived while a slit was still producing pipes (live NDT &gt; 0).
/// </summary>
public interface IPoEndSlitEndFlushDeferral
{
    void Arm(PoEndSlitEndFlushPending pending);

    bool TryGet(int millNo, out PoEndSlitEndFlushPending pending);

    bool TryTake(int millNo, out PoEndSlitEndFlushPending pending);

    IReadOnlyList<PoEndSlitEndFlushPending> SnapshotExpired(DateTime utcNow, TimeSpan maxWait);
}

public sealed record PoEndSlitEndFlushPending(
    int MillNo,
    string PoNumber,
    int? PlcNdtAtArm,
    Guid? CorrelationId,
    DateTime ArmedAtUtc);

public sealed class PoEndSlitEndFlushDeferral : IPoEndSlitEndFlushDeferral
{
    private readonly object _gate = new();
    private readonly Dictionary<int, PoEndSlitEndFlushPending> _byMill = new();

    public void Arm(PoEndSlitEndFlushPending pending)
    {
        if (pending.MillNo is < 1 or > 4 || string.IsNullOrWhiteSpace(pending.PoNumber))
            return;

        lock (_gate)
            _byMill[pending.MillNo] = pending;
    }

    public bool TryGet(int millNo, out PoEndSlitEndFlushPending pending)
    {
        lock (_gate)
            return _byMill.TryGetValue(millNo, out pending!);
    }

    public bool TryTake(int millNo, out PoEndSlitEndFlushPending pending)
    {
        lock (_gate)
        {
            if (!_byMill.TryGetValue(millNo, out pending!))
                return false;
            _byMill.Remove(millNo);
            return true;
        }
    }

    public IReadOnlyList<PoEndSlitEndFlushPending> SnapshotExpired(DateTime utcNow, TimeSpan maxWait)
    {
        lock (_gate)
        {
            return _byMill.Values
                .Where(p => utcNow - p.ArmedAtUtc >= maxWait)
                .ToList();
        }
    }
}
