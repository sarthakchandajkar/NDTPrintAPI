using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed record MillPrinterEndpoint(int MillNo, string Address, int Port);

public interface IMillPrinterSettingsService
{
    Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken);

    Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken);

    (string Address, int Port, bool Configured) ResolveForMill(int millNo);
}

/// <summary>
/// Per-mill ZPL printer endpoints in <c>dbo.Mill_Printer</c>. Shared writes all four rows;
/// mill-n reads/writes only its mill. Mill 1 falls back to <see cref="NdtBundleOptions.NdtTagPrinterAddress"/>
/// when its row is missing. Mills 2–4 never fall back to another mill's printer.
/// Cross-process reads use a 2s TTL (same as the ZPL toggle).
/// </summary>
public sealed class MillPrinterSettingsService : IMillPrinterSettingsService
{
    internal static readonly TimeSpan CacheTtl = TimeSpan.FromSeconds(2);

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IMillOwnership _millOwnership;
    private readonly ILogger<MillPrinterSettingsService> _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly IMillPrinterBackingStore _store;
    private readonly object _cacheLock = new();
    private Dictionary<int, MillPrinterEndpoint>? _cached;
    private DateTime _cacheExpiresUtc = DateTime.MinValue;

    public MillPrinterSettingsService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IMillOwnership millOwnership,
        ILogger<MillPrinterSettingsService> logger)
        : this(optionsMonitor, millOwnership, logger, utcNow: null, store: null)
    {
    }

    internal MillPrinterSettingsService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IMillOwnership millOwnership,
        ILogger<MillPrinterSettingsService> logger,
        Func<DateTime>? utcNow,
        IMillPrinterBackingStore? store)
    {
        _optionsMonitor = optionsMonitor;
        _millOwnership = millOwnership;
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _store = store ?? CreateDefaultStore(optionsMonitor, logger);
    }

    public Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken)
    {
        var map = LoadCached();
        var list = new List<MillPrinterEndpoint>(4);
        for (var m = 1; m <= 4; m++)
        {
            if (map.TryGetValue(m, out var row) && Allows(m))
                list.Add(row);
            else
                list.Add(new MillPrinterEndpoint(m, string.Empty, 9100));
        }

        return Task.FromResult<IReadOnlyList<MillPrinterEndpoint>>(list);
    }

    public Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken)
    {
        var owned = mills.Where(x => x.MillNo is >= 1 and <= 4).ToList();
        foreach (var m in owned)
        {
            if (!Allows(m.MillNo))
            {
                throw new InvalidOperationException(
                    $"Mill {m.MillNo} is not owned by this instance; refusing Mill_Printer write.");
            }
        }

        _store.Save(owned);

        lock (_cacheLock)
        {
            _cached = ToMap(owned);
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }

        _logger.LogInformation("Saved mill printer settings to Mill_Printer ({Count} mill(s)).", owned.Count);
        return Task.CompletedTask;
    }

    public (string Address, int Port, bool Configured) ResolveForMill(int millNo)
    {
        if (millNo is < 1 or > 4 || !Allows(millNo))
            return (string.Empty, 9100, false);

        var map = LoadCached();
        if (map.TryGetValue(millNo, out var row))
        {
            var addr = (row.Address ?? string.Empty).Trim();
            if (IsUsableAddress(addr))
                return (addr, row.Port > 0 ? row.Port : 9100, true);
        }

        if (millNo == 1)
        {
            var legacy = (_optionsMonitor.CurrentValue.NdtTagPrinterAddress ?? string.Empty).Trim();
            var port = _optionsMonitor.CurrentValue.NdtTagPrinterPort > 0
                ? _optionsMonitor.CurrentValue.NdtTagPrinterPort
                : 9100;
            if (IsUsableAddress(legacy))
                return (legacy, port, true);
        }

        return (string.Empty, 9100, false);
    }

    private Dictionary<int, MillPrinterEndpoint> LoadCached()
    {
        lock (_cacheLock)
        {
            if (_cached is not null && _utcNow() < _cacheExpiresUtc)
                return _cached;
        }

        var loaded = FilterOwned(_store.Load());
        lock (_cacheLock)
        {
            _cached = loaded;
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }

        return loaded;
    }

    private Dictionary<int, MillPrinterEndpoint> FilterOwned(IReadOnlyDictionary<int, MillPrinterEndpoint> raw)
    {
        var map = new Dictionary<int, MillPrinterEndpoint>();
        foreach (var (mill, row) in raw)
        {
            if (Allows(mill))
                map[mill] = row;
        }

        return map;
    }

    private static Dictionary<int, MillPrinterEndpoint> ToMap(IEnumerable<MillPrinterEndpoint> mills)
    {
        var map = new Dictionary<int, MillPrinterEndpoint>();
        foreach (var m in mills.Where(x => x.MillNo is >= 1 and <= 4))
        {
            map[m.MillNo] = new MillPrinterEndpoint(
                m.MillNo,
                (m.Address ?? string.Empty).Trim(),
                m.Port > 0 ? m.Port : 9100);
        }

        return map;
    }

    private bool Allows(int millNo) => _millOwnership.Allows(millNo);

    private static IMillPrinterBackingStore CreateDefaultStore(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger logger)
    {
        if (SqlTraceabilityConnection.IsSqlEnabled(options.CurrentValue))
            return new SqlMillPrinterBackingStore(options, logger);
        return new InMemoryMillPrinterBackingStore();
    }

    private static bool IsUsableAddress(string address) =>
        !string.IsNullOrWhiteSpace(address) &&
        !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
}

internal interface IMillPrinterBackingStore
{
    IReadOnlyDictionary<int, MillPrinterEndpoint> Load();
    void Save(IReadOnlyList<MillPrinterEndpoint> mills);
}

internal sealed class InMemoryMillPrinterBackingStore : IMillPrinterBackingStore
{
    private readonly Dictionary<int, MillPrinterEndpoint> _rows = new();
    public int LoadCalls { get; private set; }

    public IReadOnlyDictionary<int, MillPrinterEndpoint> Load()
    {
        LoadCalls++;
        return new Dictionary<int, MillPrinterEndpoint>(_rows);
    }

    public void Save(IReadOnlyList<MillPrinterEndpoint> mills)
    {
        foreach (var m in mills)
            _rows[m.MillNo] = m;
    }
}

internal sealed class SqlMillPrinterBackingStore : IMillPrinterBackingStore
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger _logger;

    public SqlMillPrinterBackingStore(IOptionsMonitor<NdtBundleOptions> options, ILogger logger)
    {
        _options = options;
        _logger = logger;
    }

    public IReadOnlyDictionary<int, MillPrinterEndpoint> Load()
    {
        var map = new Dictionary<int, MillPrinterEndpoint>();
        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand("SELECT Mill_No, Address, Port FROM dbo.Mill_Printer;", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var mill = reader.GetInt32(0);
                map[mill] = new MillPrinterEndpoint(
                    mill,
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    reader.GetInt32(2));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load Mill_Printer; mill 1 may fall back to NdtTagPrinterAddress.");
        }

        return map;
    }

    public void Save(IReadOnlyList<MillPrinterEndpoint> mills)
    {
        using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        conn.Open();
        foreach (var m in mills)
        {
            using var cmd = new SqlCommand(@"
MERGE dbo.Mill_Printer WITH (HOLDLOCK) AS t
USING (SELECT @Mill AS Mill_No) AS s
ON t.Mill_No = s.Mill_No
WHEN MATCHED THEN UPDATE SET
    Address = @Address,
    Port = @Port,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = N'Dashboard'
WHEN NOT MATCHED THEN INSERT (Mill_No, Address, Port, Updated_By)
VALUES (@Mill, @Address, @Port, N'Dashboard');", conn);
            cmd.Parameters.AddWithValue("@Mill", m.MillNo);
            cmd.Parameters.AddWithValue("@Address", (m.Address ?? string.Empty).Trim());
            cmd.Parameters.AddWithValue("@Port", m.Port > 0 ? m.Port : 9100);
            cmd.ExecuteNonQuery();
        }
    }
}
