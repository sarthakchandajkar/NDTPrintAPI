using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface INdtBundleRuntimeStateStore
{
    Task EnsureInitializedAsync(CancellationToken cancellationToken);

    /// <summary>Sum of open size counts (derived; not stored).</summary>
    int GetRunningTotal(string poNumber, int millNo)
    {
        var sum = 0;
        foreach (var v in GetSizeCounts(poNumber, millNo).Values)
        {
            if (v > 0)
                sum += v;
        }

        return sum;
    }

    void ClearOpenAccumulation(string poNumber, int millNo);

    DateTime GetLastActivityUtc(string poNumber, int millNo);

    /// <summary>
    /// Additive write: <c>Pcs = Pcs + delta</c> in one MERGE when SQL is on.
    /// Default in-memory path is get+add under the store lock (tests).
    /// </summary>
    void IncrementSizeCount(string poNumber, int millNo, string sizeKey, int delta)
    {
        var counts = GetSizeCounts(poNumber, millNo);
        counts.TryGetValue(sizeKey, out var current);
        var next = current + delta;
        if (next > 0)
            counts[sizeKey] = next;
        else
            counts.Remove(sizeKey);
        SetSizeCounts(poNumber, millNo, counts);
    }

    Dictionary<string, int> GetSizeCounts(string poNumber, int millNo);

    void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts);

    InputSlitRecord? GetLastRecord(string poNumber, int millNo);

    void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record);

    Task SaveAsync(CancellationToken cancellationToken);

    bool HasUnsafeOpenStateForFillCutover(int? millNo = null) => false;

    void ArmCloseSize(string poNumber, int millNo, string? sizeKey) { }

    Task DeleteArmedSizeInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string poNumber,
        int millNo,
        CancellationToken cancellationToken) =>
        Task.CompletedTask;
}

/// <summary>
/// Open-bundle remainder in <c>Bundle_Accumulation</c> (SQL write-through) with an in-memory cache.
/// When SQL is disabled (tests), cache only — never INSERT on a zero-pcs read.
/// </summary>
public sealed class NdtBundleRuntimeStateStore : INdtBundleRuntimeStateStore
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IMillOwnership _millOwnership;
    private readonly ILogger<NdtBundleRuntimeStateStore> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly object _stateLock = new();

    private readonly Dictionary<string, Dictionary<string, int>> _sizeCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, InputSlitRecord?> _lastRecord = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DateTime> _lastActivity = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string?> _armedCloseSize = new(StringComparer.OrdinalIgnoreCase);
    private bool _initialized;

    public NdtBundleRuntimeStateStore(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IMillOwnership millOwnership,
        ILogger<NdtBundleRuntimeStateStore> logger)
    {
        _optionsMonitor = optionsMonitor;
        _millOwnership = millOwnership;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;
    private bool SqlEnabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
                return;

            if (SqlEnabled)
                await HydrateFromSqlAsync(cancellationToken).ConfigureAwait(false);

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public int GetRunningTotal(string poNumber, int millNo)
    {
        var sum = 0;
        foreach (var v in GetSizeCounts(poNumber, millNo).Values)
        {
            if (v > 0)
                sum += v;
        }

        return sum;
    }

    public void ClearOpenAccumulation(string poNumber, int millNo)
    {
        EnsureMillAllowed(millNo, write: true);
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
        {
            _sizeCounts.Remove(SlotKey(po, millNo));
            _lastRecord.Remove(SlotKey(po, millNo));
            _lastActivity.Remove(SlotKey(po, millNo));
        }

        if (!SqlEnabled)
            return;

        Execute(conn =>
        {
            using var delSizes = new SqlCommand(BundleAccumulationSql.DeleteAllSizesForPo, conn);
            BundleAccumulationSql.AddMillPo(delSizes, millNo, po);
            delSizes.ExecuteNonQuery();
            using var delCtx = new SqlCommand(BundleAccumulationSql.DeleteContext, conn);
            BundleAccumulationSql.AddMillPo(delCtx, millNo, po);
            delCtx.ExecuteNonQuery();
        });
    }

    public DateTime GetLastActivityUtc(string poNumber, int millNo)
    {
        if (!Allows(millNo))
            return default;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
            return _lastActivity.GetValueOrDefault(SlotKey(po, millNo));
    }

    public void IncrementSizeCount(string poNumber, int millNo, string sizeKey, int delta)
    {
        EnsureMillAllowed(millNo, write: true);
        if (delta == 0)
            return;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        var key = string.IsNullOrWhiteSpace(sizeKey) ? "Default" : sizeKey;

        lock (_stateLock)
            ApplyDeltaInMemory(po, millNo, key, delta);

        if (!SqlEnabled)
            return;

        Execute(conn =>
        {
            using var cmd = new SqlCommand(BundleAccumulationSql.IncrementMerge, conn);
            BundleAccumulationSql.AddMillPo(cmd, millNo, po);
            cmd.Parameters.AddWithValue("@Size", key);
            cmd.Parameters.AddWithValue("@Delta", delta);
            cmd.ExecuteNonQuery();
        });
    }

    public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
    {
        if (!Allows(millNo))
            return new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
        {
            if (!_sizeCounts.TryGetValue(SlotKey(po, millNo), out var counts))
                return new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            return new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        }
    }

    public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts)
    {
        EnsureMillAllowed(millNo, write: true);
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        var cleaned = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in counts)
        {
            if (v > 0)
                cleaned[k] = v;
        }

        Dictionary<string, int> previous;
        lock (_stateLock)
        {
            var slot = SlotKey(po, millNo);
            previous = _sizeCounts.TryGetValue(slot, out var cur)
                ? new Dictionary<string, int>(cur, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            if (cleaned.Count == 0)
                _sizeCounts.Remove(slot);
            else
                _sizeCounts[slot] = cleaned;
            Touch(po, millNo);
        }

        if (!SqlEnabled)
            return;

        var keys = new HashSet<string>(previous.Keys, StringComparer.OrdinalIgnoreCase);
        foreach (var k in cleaned.Keys)
            keys.Add(k);

        Execute(conn =>
        {
            foreach (var sizeKey in keys)
            {
                cleaned.TryGetValue(sizeKey, out var pcs);
                using var cmd = new SqlCommand(BundleAccumulationSql.AbsoluteMerge, conn);
                BundleAccumulationSql.AddMillPo(cmd, millNo, po);
                cmd.Parameters.AddWithValue("@Size", sizeKey);
                cmd.Parameters.AddWithValue("@Pcs", pcs);
                cmd.ExecuteNonQuery();
            }
        });
    }

    public InputSlitRecord? GetLastRecord(string poNumber, int millNo)
    {
        if (!Allows(millNo))
            return null;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
            return _lastRecord.GetValueOrDefault(SlotKey(po, millNo));
    }

    public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record)
    {
        EnsureMillAllowed(millNo, write: true);
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
        {
            _lastRecord[SlotKey(po, millNo)] = record;
            Touch(po, millNo);
        }

        if (!SqlEnabled || record is null)
            return;

        Execute(conn =>
        {
            using var cmd = new SqlCommand(BundleAccumulationSql.UpsertContext, conn);
            BundleAccumulationSql.AddMillPo(cmd, millNo, po);
            cmd.Parameters.AddWithValue("@SlitNo", (object?)record.SlitNo ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Rejected", record.RejectedPipes);
            cmd.Parameters.AddWithValue("@Start", (object?)record.SlitStartTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Finish", (object?)record.SlitFinishTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@NdtShort", (object?)record.NdtShortLengthPipe ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@RejShort", (object?)record.RejectedShortLengthPipe ?? DBNull.Value);
            cmd.ExecuteNonQuery();
        });
    }

    public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public bool HasUnsafeOpenStateForFillCutover(int? millNo = null)
    {
        if (SqlEnabled)
        {
            var open = false;
            Execute(conn =>
            {
                if (millNo is int mill)
                {
                    if (!Allows(mill))
                        return;
                    using var cmd = new SqlCommand(BundleAccumulationSql.ExistsOpenForMill, conn);
                    cmd.Parameters.AddWithValue("@Mill", mill);
                    open = cmd.ExecuteScalar() is not null and not DBNull;
                }
                else
                {
                    using var cmd = new SqlCommand(BundleAccumulationSql.ExistsOpenAny, conn);
                    open = cmd.ExecuteScalar() is not null and not DBNull;
                }
            });
            return open;
        }

        lock (_stateLock)
        {
            foreach (var (slot, counts) in _sizeCounts)
            {
                if (millNo.HasValue && !slot.EndsWith("|" + millNo.Value.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal))
                    continue;
                if (counts.Values.Any(v => v > 0))
                    return true;
            }

            return false;
        }
    }

    public void ArmCloseSize(string poNumber, int millNo, string? sizeKey)
    {
        if (!Allows(millNo))
            return;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        lock (_stateLock)
            _armedCloseSize[SlotKey(po, millNo)] = sizeKey;
    }

    public async Task DeleteArmedSizeInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        EnsureMillAllowed(millNo, write: true);
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        string? sizeKey;
        lock (_stateLock)
        {
            _armedCloseSize.TryGetValue(SlotKey(po, millNo), out sizeKey);
            _armedCloseSize.Remove(SlotKey(po, millNo));
        }

        if (string.IsNullOrEmpty(sizeKey))
        {
            await using var delAll = new SqlCommand(BundleAccumulationSql.DeleteAllSizesForPo, conn, tx);
            BundleAccumulationSql.AddMillPo(delAll, millNo, po);
            await delAll.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await using var del = new SqlCommand(BundleAccumulationSql.DeleteSize, conn, tx);
            BundleAccumulationSql.AddMillPo(del, millNo, po);
            del.Parameters.AddWithValue("@Size", sizeKey);
            await del.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var remaining = new SqlCommand(BundleAccumulationSql.RemainingSizeCount, conn, tx);
        BundleAccumulationSql.AddMillPo(remaining, millNo, po);
        var left = Convert.ToInt32(await remaining.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0);
        if (left == 0)
        {
            await using var delCtx = new SqlCommand(BundleAccumulationSql.DeleteContext, conn, tx);
            BundleAccumulationSql.AddMillPo(delCtx, millNo, po);
            await delCtx.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        lock (_stateLock)
        {
            var slot = SlotKey(po, millNo);
            if (string.IsNullOrEmpty(sizeKey))
                _sizeCounts.Remove(slot);
            else if (_sizeCounts.TryGetValue(slot, out var counts))
            {
                counts.Remove(sizeKey);
                if (counts.Count == 0)
                    _sizeCounts.Remove(slot);
            }

            if (left == 0)
            {
                _lastRecord.Remove(slot);
                _lastActivity.Remove(slot);
            }
        }
    }

    private async Task HydrateFromSqlAsync(CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Bundle_Accumulation hydrate", cancellationToken)
            .ConfigureAwait(false);

        foreach (var mill in Enumerable.Range(1, 4).Where(Allows))
        {
            await using var cmd = new SqlCommand(BundleAccumulationSql.SelectOpenForMill, conn);
            cmd.Parameters.AddWithValue("@Mill", mill);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var po = reader.GetString(0);
                var size = reader.GetString(1);
                var pcs = reader.GetInt32(2);
                var activity = reader.GetDateTime(3);
                lock (_stateLock)
                {
                    var slot = SlotKey(po, mill);
                    if (!_sizeCounts.TryGetValue(slot, out var counts))
                    {
                        counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                        _sizeCounts[slot] = counts;
                    }

                    counts[size] = pcs;
                    if (!_lastActivity.TryGetValue(slot, out var prev) || activity > prev)
                        _lastActivity[slot] = activity;
                }
            }
        }

        foreach (var mill in Enumerable.Range(1, 4).Where(Allows))
        {
            await using var cmd = new SqlCommand(
                @"SELECT Po_Number, Slit_No, Rejected_Pipes, Slit_Start_Time, Slit_Finish_Time,
                         Ndt_Short_Length_Pipe, Rejected_Short_Length_Pipe
                  FROM dbo.Bundle_Accumulation_Context WHERE Mill_No = @Mill;",
                conn);
            cmd.Parameters.AddWithValue("@Mill", mill);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var po = reader.GetString(0);
                var record = new InputSlitRecord
                {
                    PoNumber = po,
                    MillNo = mill,
                    SlitNo = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    RejectedPipes = reader.GetInt32(2),
                    SlitStartTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    SlitFinishTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    NdtShortLengthPipe = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                    RejectedShortLengthPipe = reader.IsDBNull(6) ? string.Empty : reader.GetString(6)
                };
                lock (_stateLock)
                    _lastRecord[SlotKey(po, mill)] = record;
            }
        }
    }

    private void ApplyDeltaInMemory(string po, int millNo, string sizeKey, int delta)
    {
        var slot = SlotKey(po, millNo);
        if (!_sizeCounts.TryGetValue(slot, out var counts))
        {
            counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            _sizeCounts[slot] = counts;
        }

        counts.TryGetValue(sizeKey, out var current);
        var next = current + delta;
        if (next > 0)
            counts[sizeKey] = next;
        else
            counts.Remove(sizeKey);

        if (counts.Count == 0)
            _sizeCounts.Remove(slot);

        Touch(po, millNo);
    }

    private void Touch(string po, int millNo) =>
        _lastActivity[SlotKey(po, millNo)] = DateTime.UtcNow;

    private bool Allows(int millNo) => _millOwnership.Allows(millNo);

    private void EnsureMillAllowed(int millNo, bool write)
    {
        if (Allows(millNo))
            return;

        throw new InvalidOperationException(
            $"Mill {millNo} is not owned by this instance; refusing Bundle_Accumulation {(write ? "write" : "read")}.");
    }

    private void Execute(Action<SqlConnection> action)
    {
        using var conn = SqlTraceabilityConnection.Create(Opt);
        conn.Open();
        action(conn);
    }

    private static string SlotKey(string po, int millNo) => $"{po}|{millNo}";
}
