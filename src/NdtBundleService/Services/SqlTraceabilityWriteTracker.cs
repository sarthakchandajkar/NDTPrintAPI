namespace NdtBundleService.Services;

/// <summary>Last-known result of traceability SQL writes (for diagnostics when CSV/labels succeed but SQL does not).</summary>
public interface ISqlTraceabilityWriteTracker
{
    void RecordSuccess(string operation, string? detail = null);
    void RecordFailure(string operation, string error, string? detail = null);
    IReadOnlyList<SqlTraceabilityWriteResult> GetRecentResults();
}

public sealed class SqlTraceabilityWriteResult
{
    public DateTime Utc { get; init; }
    public string Operation { get; init; } = string.Empty;
    public bool Success { get; init; }
    public string? Detail { get; init; }
    public string? Error { get; init; }
}

public sealed class SqlTraceabilityWriteTracker : ISqlTraceabilityWriteTracker
{
    private readonly object _lock = new();
    private readonly List<SqlTraceabilityWriteResult> _recent = new();
    private const int MaxRecent = 20;

    public void RecordSuccess(string operation, string? detail = null)
    {
        lock (_lock)
        {
            _recent.Insert(0, new SqlTraceabilityWriteResult
            {
                Utc = DateTime.UtcNow,
                Operation = operation,
                Success = true,
                Detail = detail
            });
            Trim();
        }
    }

    public void RecordFailure(string operation, string error, string? detail = null)
    {
        lock (_lock)
        {
            _recent.Insert(0, new SqlTraceabilityWriteResult
            {
                Utc = DateTime.UtcNow,
                Operation = operation,
                Success = false,
                Detail = detail,
                Error = error
            });
            Trim();
        }
    }

    public IReadOnlyList<SqlTraceabilityWriteResult> GetRecentResults()
    {
        lock (_lock)
        {
            return _recent.ToList();
        }
    }

    private void Trim()
    {
        while (_recent.Count > MaxRecent)
            _recent.RemoveAt(_recent.Count - 1);
    }
}
