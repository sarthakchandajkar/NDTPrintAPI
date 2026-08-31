using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed record StationPrinterEndpoint(string StationCode, string Address, int Port);

public interface IStationPrinterSettingsService
{
    Task<IReadOnlyList<StationPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken);

    Task SaveAllAsync(IReadOnlyList<StationPrinterEndpoint> stations, CancellationToken cancellationToken);

    (string Address, int Port, bool Configured) Resolve(string stationCode);
}

/// <summary>
/// Per-station ZPL printer endpoints in <c>dbo.Station_Printer</c>. Shared/Monolith only.
/// No mill fallback. Missing or blank row is not configured. 2s read-through cache.
/// </summary>
public sealed class StationPrinterSettingsService : IStationPrinterSettingsService
{
    internal static readonly TimeSpan CacheTtl = TimeSpan.FromSeconds(2);

    private readonly IOptions<InstanceRoleOptions> _role;
    private readonly ILogger<StationPrinterSettingsService> _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly IStationPrinterBackingStore _store;
    private readonly object _cacheLock = new();
    private Dictionary<string, StationPrinterEndpoint>? _cached;
    private DateTime _cacheExpiresUtc = DateTime.MinValue;

    public StationPrinterSettingsService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IOptions<InstanceRoleOptions> role,
        ILogger<StationPrinterSettingsService> logger)
        : this(optionsMonitor, role, logger, utcNow: null, store: null)
    {
    }

    internal StationPrinterSettingsService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IOptions<InstanceRoleOptions> role,
        ILogger<StationPrinterSettingsService> logger,
        Func<DateTime>? utcNow,
        IStationPrinterBackingStore? store)
    {
        _ = optionsMonitor;
        _role = role;
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _store = store ?? CreateDefaultStore(optionsMonitor, logger);
    }

    public Task<IReadOnlyList<StationPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken)
    {
        var map = LoadCached();
        var list = new List<StationPrinterEndpoint>(StationPrinterTarget.All.Length);
        foreach (var code in StationPrinterTarget.All)
        {
            if (map.TryGetValue(code, out var row))
                list.Add(row);
            else
                list.Add(new StationPrinterEndpoint(code, string.Empty, 9100));
        }

        return Task.FromResult<IReadOnlyList<StationPrinterEndpoint>>(list);
    }

    public Task SaveAllAsync(IReadOnlyList<StationPrinterEndpoint> stations, CancellationToken cancellationToken)
    {
        EnsureSharedWrite();
        var owned = new List<StationPrinterEndpoint>();
        foreach (var s in stations)
        {
            if (!StationPrinterTarget.IsKnown(s.StationCode))
            {
                throw new InvalidOperationException(
                    $"Unknown station printer code '{s.StationCode}'.");
            }

            owned.Add(new StationPrinterEndpoint(
                StationPrinterTarget.Normalize(s.StationCode),
                (s.Address ?? string.Empty).Trim(),
                s.Port > 0 ? s.Port : 9100));
        }

        _store.Save(owned);

        lock (_cacheLock)
        {
            _cached = ToMap(owned);
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }

        _logger.LogInformation("Saved station printer settings to Station_Printer ({Count} station(s)).", owned.Count);
        return Task.CompletedTask;
    }

    public (string Address, int Port, bool Configured) Resolve(string stationCode)
    {
        if (!StationPrinterTarget.IsKnown(stationCode))
            return (string.Empty, 9100, false);

        var code = StationPrinterTarget.Normalize(stationCode);
        var map = LoadCached();
        if (map.TryGetValue(code, out var row))
        {
            var addr = (row.Address ?? string.Empty).Trim();
            if (IsUsableAddress(addr))
                return (addr, row.Port > 0 ? row.Port : 9100, true);
        }

        return (string.Empty, 9100, false);
    }

    private Dictionary<string, StationPrinterEndpoint> LoadCached()
    {
        lock (_cacheLock)
        {
            if (_cached is not null && _utcNow() < _cacheExpiresUtc)
                return _cached;
        }

        var loaded = ToMap(_store.Load().Values);
        lock (_cacheLock)
        {
            _cached = loaded;
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }

        return loaded;
    }

    private void EnsureSharedWrite()
    {
        var role = _role.Value;
        if (role.IsShared || role.IsMonolith)
            return;

        throw new InvalidOperationException("Station_Printer writes are Shared-only.");
    }

    private static Dictionary<string, StationPrinterEndpoint> ToMap(IEnumerable<StationPrinterEndpoint> stations)
    {
        var map = new Dictionary<string, StationPrinterEndpoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var s in stations)
        {
            if (!StationPrinterTarget.IsKnown(s.StationCode))
                continue;
            var code = StationPrinterTarget.Normalize(s.StationCode);
            map[code] = new StationPrinterEndpoint(
                code,
                (s.Address ?? string.Empty).Trim(),
                s.Port > 0 ? s.Port : 9100);
        }

        return map;
    }

    private static IStationPrinterBackingStore CreateDefaultStore(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger logger)
    {
        if (SqlTraceabilityConnection.IsSqlEnabled(options.CurrentValue))
            return new SqlStationPrinterBackingStore(options, logger);
        return new InMemoryStationPrinterBackingStore();
    }

    private static bool IsUsableAddress(string address) =>
        !string.IsNullOrWhiteSpace(address) &&
        !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
}

internal interface IStationPrinterBackingStore
{
    IReadOnlyDictionary<string, StationPrinterEndpoint> Load();
    void Save(IReadOnlyList<StationPrinterEndpoint> stations);
}

internal sealed class InMemoryStationPrinterBackingStore : IStationPrinterBackingStore
{
    private readonly Dictionary<string, StationPrinterEndpoint> _rows =
        new(StringComparer.OrdinalIgnoreCase);

    public int LoadCalls { get; private set; }

    public IReadOnlyDictionary<string, StationPrinterEndpoint> Load()
    {
        LoadCalls++;
        return new Dictionary<string, StationPrinterEndpoint>(_rows, StringComparer.OrdinalIgnoreCase);
    }

    public void Save(IReadOnlyList<StationPrinterEndpoint> stations)
    {
        foreach (var s in stations)
            _rows[s.StationCode] = s;
    }
}

internal sealed class SqlStationPrinterBackingStore : IStationPrinterBackingStore
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger _logger;

    public SqlStationPrinterBackingStore(IOptionsMonitor<NdtBundleOptions> options, ILogger logger)
    {
        _options = options;
        _logger = logger;
    }

    public IReadOnlyDictionary<string, StationPrinterEndpoint> Load()
    {
        var map = new Dictionary<string, StationPrinterEndpoint>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand("SELECT Station_Code, Address, Port FROM dbo.Station_Printer;", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var code = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
                if (!StationPrinterTarget.IsKnown(code))
                    continue;
                map[StationPrinterTarget.Normalize(code)] = new StationPrinterEndpoint(
                    StationPrinterTarget.Normalize(code),
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    reader.GetInt32(2));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load Station_Printer.");
        }

        return map;
    }

    public void Save(IReadOnlyList<StationPrinterEndpoint> stations)
    {
        using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        conn.Open();
        foreach (var s in stations)
        {
            using var cmd = new SqlCommand(@"
MERGE dbo.Station_Printer WITH (HOLDLOCK) AS t
USING (SELECT @Code AS Station_Code) AS s
ON t.Station_Code = s.Station_Code
WHEN MATCHED THEN UPDATE SET
    Address = @Address,
    Port = @Port,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = N'Dashboard'
WHEN NOT MATCHED THEN INSERT (Station_Code, Address, Port, Updated_By)
VALUES (@Code, @Address, @Port, N'Dashboard');", conn);
            cmd.Parameters.AddWithValue("@Code", s.StationCode);
            cmd.Parameters.AddWithValue("@Address", (s.Address ?? string.Empty).Trim());
            cmd.Parameters.AddWithValue("@Port", s.Port > 0 ? s.Port : 9100);
            cmd.ExecuteNonQuery();
        }
    }
}
