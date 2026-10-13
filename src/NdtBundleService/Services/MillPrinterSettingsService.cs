using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed record MillPrinterEndpoint(int MillNo, string Address, int Port);

public sealed record NamedPrinterEndpoint(string Key, string Address, int Port);

public interface IMillPrinterSettingsService
{
    Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken);

    Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken);

    (string Address, int Port, bool Configured) ResolveForMill(int millNo);

    Task<NamedPrinterEndpoint> GetMill1WipAsync(CancellationToken cancellationToken);

    Task<NamedPrinterEndpoint> GetMill1FgAsync(CancellationToken cancellationToken);

    Task<NamedPrinterEndpoint> GetPasRoomAsync(CancellationToken cancellationToken);

    Task SaveReprintPrintersAsync(
        NamedPrinterEndpoint? mill1Wip,
        NamedPrinterEndpoint? mill1Fg,
        NamedPrinterEndpoint? pasRoom,
        CancellationToken cancellationToken);

    /// <summary>
    /// Resolves a reprint destination. <paramref name="printerTarget"/>:
    /// <c>wip</c>, <c>fg</c>, <c>pas</c>, or <c>mill</c>/null (default mill printer).
    /// </summary>
    (string Address, int Port, bool Configured, string DisplayName) ResolveReprintTarget(
        int millNo,
        string? printerTarget);
}

/// <summary>
/// Per-mill ZPL printer endpoints in <c>dbo.Printer</c> (<c>Kind=Mill</c>, keys <c>MILL_1</c>…<c>MILL_4</c>),
/// plus Mill-1 WIP/FG and PAS Room reprint printers. Shared writes all four mill rows; mill-n reads/writes
/// only its mill. Mill 1 auto-print prefers <c>MILL_1_WIP</c>, then <c>MILL_1</c>, then
/// <see cref="NdtBundleOptions.NdtTagPrinterAddress"/>. Cross-process reads use a 2s TTL.
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
    private Dictionary<string, NamedPrinterEndpoint>? _namedCached;
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
        EnsureCacheFresh();
        var map = _cached ?? new Dictionary<int, MillPrinterEndpoint>();
        var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
        var list = new List<MillPrinterEndpoint>(4);
        for (var m = 1; m <= 4; m++)
        {
            if (!Allows(m))
            {
                list.Add(new MillPrinterEndpoint(m, string.Empty, 9100));
                continue;
            }

            if (m == 1 &&
                named.TryGetValue(SqlPrinterTable.KeyMill1Wip, out var wip) &&
                IsUsableAddress(wip.Address))
            {
                list.Add(new MillPrinterEndpoint(1, wip.Address.Trim(), wip.Port > 0 ? wip.Port : 9100));
                continue;
            }

            if (map.TryGetValue(m, out var row))
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
                    $"Mill {m.MillNo} is not owned by this instance; refusing Printer write.");
            }
        }

        _store.Save(owned);

        // Keep Mill-1 WIP in sync with the Mill-1 row so auto-print and Settings WIP stay aligned.
        var mill1 = owned.FirstOrDefault(m => m.MillNo == 1);
        if (mill1 is not null && Allows(1))
        {
            _store.SaveNamed(
            [
                (
                    SqlPrinterTable.KeyMill1Wip,
                    SqlPrinterTable.KindMill,
                    (mill1.Address ?? string.Empty).Trim(),
                    mill1.Port > 0 ? mill1.Port : 9100)
            ]);
        }

        InvalidateCache();
        _logger.LogInformation("Saved mill printer settings to Printer ({Count} mill(s)).", owned.Count);
        return Task.CompletedTask;
    }

    public (string Address, int Port, bool Configured) ResolveForMill(int millNo)
    {
        if (millNo is < 1 or > 4 || !Allows(millNo))
            return (string.Empty, 9100, false);

        EnsureCacheFresh();
        var map = _cached ?? new Dictionary<int, MillPrinterEndpoint>();
        var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();

        if (millNo == 1 &&
            named.TryGetValue(SqlPrinterTable.KeyMill1Wip, out var wip) &&
            IsUsableAddress(wip.Address))
        {
            return (wip.Address.Trim(), wip.Port > 0 ? wip.Port : 9100, true);
        }

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

    public Task<NamedPrinterEndpoint> GetMill1WipAsync(CancellationToken cancellationToken)
    {
        EnsureCacheFresh();
        var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
        if (named.TryGetValue(SqlPrinterTable.KeyMill1Wip, out var wip) && IsUsableAddress(wip.Address))
            return Task.FromResult(wip);

        var mill = ResolveForMill(1);
        return Task.FromResult(new NamedPrinterEndpoint(
            SqlPrinterTable.KeyMill1Wip,
            mill.Configured ? mill.Address : string.Empty,
            mill.Port));
    }

    public Task<NamedPrinterEndpoint> GetMill1FgAsync(CancellationToken cancellationToken)
    {
        EnsureCacheFresh();
        var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
        if (named.TryGetValue(SqlPrinterTable.KeyMill1Fg, out var fg))
            return Task.FromResult(fg);
        return Task.FromResult(new NamedPrinterEndpoint(SqlPrinterTable.KeyMill1Fg, string.Empty, 9100));
    }

    public Task<NamedPrinterEndpoint> GetPasRoomAsync(CancellationToken cancellationToken)
    {
        EnsureCacheFresh();
        var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
        if (named.TryGetValue(SqlPrinterTable.KeyPasRoom, out var pas))
            return Task.FromResult(pas);
        return Task.FromResult(new NamedPrinterEndpoint(SqlPrinterTable.KeyPasRoom, string.Empty, 9100));
    }

    public Task SaveReprintPrintersAsync(
        NamedPrinterEndpoint? mill1Wip,
        NamedPrinterEndpoint? mill1Fg,
        NamedPrinterEndpoint? pasRoom,
        CancellationToken cancellationToken)
    {
        var rows = new List<(string Key, string Kind, string Address, int Port)>();

        if (mill1Wip is not null)
        {
            if (!Allows(1))
            {
                throw new InvalidOperationException(
                    "Mill 1 is not owned by this instance; refusing Mill-1 WIP printer write.");
            }

            var addr = (mill1Wip.Address ?? string.Empty).Trim();
            var port = mill1Wip.Port > 0 ? mill1Wip.Port : 9100;
            rows.Add((SqlPrinterTable.KeyMill1Wip, SqlPrinterTable.KindMill, addr, port));
            // Keep legacy MILL_1 aligned with WIP for older readers / auto-print fallback.
            _store.Save([new MillPrinterEndpoint(1, addr, port)]);
        }

        if (mill1Fg is not null)
        {
            if (!Allows(1))
            {
                throw new InvalidOperationException(
                    "Mill 1 is not owned by this instance; refusing Mill-1 FG printer write.");
            }

            rows.Add((
                SqlPrinterTable.KeyMill1Fg,
                SqlPrinterTable.KindMill,
                (mill1Fg.Address ?? string.Empty).Trim(),
                mill1Fg.Port > 0 ? mill1Fg.Port : 9100));
        }

        if (pasRoom is not null)
        {
            // Shared (empty owned set) and Monolith (owns 1–4) may write PAS Room; mill-n may not.
            if (!(Allows(1) && Allows(2) && Allows(3) && Allows(4)))
            {
                throw new InvalidOperationException(
                    "PAS Room printer can only be saved on Shared.");
            }

            rows.Add((
                SqlPrinterTable.KeyPasRoom,
                SqlPrinterTable.KindPasRoom,
                (pasRoom.Address ?? string.Empty).Trim(),
                pasRoom.Port > 0 ? pasRoom.Port : 9100));
        }

        if (rows.Count > 0)
            _store.SaveNamed(rows);

        InvalidateCache();
        _logger.LogInformation("Saved reprint printer settings ({Count} row(s)).", rows.Count);
        return Task.CompletedTask;
    }

    public (string Address, int Port, bool Configured, string DisplayName) ResolveReprintTarget(
        int millNo,
        string? printerTarget)
    {
        var target = ReprintPrinterTarget.Normalize(printerTarget);
        var display = ReprintPrinterTarget.DisplayName(target);

        if (target == ReprintPrinterTarget.Pas)
        {
            EnsureCacheFresh();
            var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
            if (named.TryGetValue(SqlPrinterTable.KeyPasRoom, out var pas) && IsUsableAddress(pas.Address))
                return (pas.Address.Trim(), pas.Port > 0 ? pas.Port : 9100, true, display);
            return (string.Empty, 9100, false, display);
        }

        if (target == ReprintPrinterTarget.Wip)
        {
            if (millNo != 1 || !Allows(1))
                return (string.Empty, 9100, false, display);

            EnsureCacheFresh();
            var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
            if (named.TryGetValue(SqlPrinterTable.KeyMill1Wip, out var wip) && IsUsableAddress(wip.Address))
                return (wip.Address.Trim(), wip.Port > 0 ? wip.Port : 9100, true, display);

            var mill = ResolveForMill(1);
            return (mill.Address, mill.Port, mill.Configured, display);
        }

        if (target == ReprintPrinterTarget.Fg)
        {
            if (millNo != 1 || !Allows(1))
                return (string.Empty, 9100, false, display);

            EnsureCacheFresh();
            var named = _namedCached ?? new Dictionary<string, NamedPrinterEndpoint>();
            if (named.TryGetValue(SqlPrinterTable.KeyMill1Fg, out var fg) && IsUsableAddress(fg.Address))
                return (fg.Address.Trim(), fg.Port > 0 ? fg.Port : 9100, true, display);
            return (string.Empty, 9100, false, display);
        }

        var resolved = ResolveForMill(millNo);
        return (resolved.Address, resolved.Port, resolved.Configured, ReprintPrinterTarget.DisplayName(ReprintPrinterTarget.Mill));
    }

    private void EnsureCacheFresh()
    {
        lock (_cacheLock)
        {
            if (_cached is not null && _namedCached is not null && _utcNow() < _cacheExpiresUtc)
                return;
        }

        var loaded = FilterOwned(_store.Load());
        var named = new Dictionary<string, NamedPrinterEndpoint>(
            _store.LoadNamed(),
            StringComparer.OrdinalIgnoreCase);
        lock (_cacheLock)
        {
            _cached = loaded;
            _namedCached = named;
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }
    }

    private void InvalidateCache()
    {
        lock (_cacheLock)
        {
            _cached = null;
            _namedCached = null;
            _cacheExpiresUtc = DateTime.MinValue;
        }
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
    IReadOnlyDictionary<string, NamedPrinterEndpoint> LoadNamed();
    void SaveNamed(IReadOnlyList<(string Key, string Kind, string Address, int Port)> rows);
}

internal sealed class InMemoryMillPrinterBackingStore : IMillPrinterBackingStore
{
    private readonly Dictionary<int, MillPrinterEndpoint> _rows = new();
    private readonly Dictionary<string, NamedPrinterEndpoint> _named = new(StringComparer.OrdinalIgnoreCase);
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

    public IReadOnlyDictionary<string, NamedPrinterEndpoint> LoadNamed() =>
        new Dictionary<string, NamedPrinterEndpoint>(_named, StringComparer.OrdinalIgnoreCase);

    public void SaveNamed(IReadOnlyList<(string Key, string Kind, string Address, int Port)> rows)
    {
        foreach (var row in rows)
        {
            _named[row.Key] = new NamedPrinterEndpoint(
                row.Key,
                (row.Address ?? string.Empty).Trim(),
                row.Port > 0 ? row.Port : 9100);
        }
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
        foreach (var (key, address, port) in SqlPrinterTable.Load(_options, _logger, SqlPrinterTable.KindMill))
        {
            if (SqlPrinterTable.TryParseMillKey(key, out var mill))
                map[mill] = new MillPrinterEndpoint(mill, address, port);
        }

        return map;
    }

    public void Save(IReadOnlyList<MillPrinterEndpoint> mills)
    {
        var rows = mills
            .Where(m => m.MillNo is >= 1 and <= 4)
            .Select(m => (
                Key: SqlPrinterTable.MillKey(m.MillNo),
                Kind: SqlPrinterTable.KindMill,
                Address: (m.Address ?? string.Empty).Trim(),
                Port: m.Port > 0 ? m.Port : 9100))
            .ToList();
        SqlPrinterTable.Upsert(_options, rows);
    }

    public IReadOnlyDictionary<string, NamedPrinterEndpoint> LoadNamed()
    {
        var map = new Dictionary<string, NamedPrinterEndpoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in new[]
                 {
                     SqlPrinterTable.KeyMill1Wip,
                     SqlPrinterTable.KeyMill1Fg,
                     SqlPrinterTable.KeyPasRoom
                 })
        {
            var loaded = SqlPrinterTable.TryLoadKey(_options, _logger, key);
            if (loaded is null)
                continue;
            map[key] = new NamedPrinterEndpoint(key, loaded.Value.Address, loaded.Value.Port);
        }

        return map;
    }

    public void SaveNamed(IReadOnlyList<(string Key, string Kind, string Address, int Port)> rows)
    {
        SqlPrinterTable.Upsert(_options, rows);
    }
}
