using System.Text.Json;
using System.Text.Json.Serialization;
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
/// Per-mill ZPL printer endpoints (TCP 9100). Persisted JSON; mill 1 falls back to <see cref="NdtBundleOptions.NdtTagPrinterAddress"/> when unset.
/// </summary>
public sealed class MillPrinterSettingsService : IMillPrinterSettingsService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<MillPrinterSettingsService> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private PersistedMillPrinters? _cached;

    public MillPrinterSettingsService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ILogger<MillPrinterSettingsService> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken)
    {
        await EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);
        return BuildListFromState(_cached!);
    }

    public async Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var state = new PersistedMillPrinters();
            foreach (var m in mills.Where(x => x.MillNo is >= 1 and <= 4))
            {
                state.Mills[m.MillNo] = new MillPrinterRow
                {
                    Address = (m.Address ?? string.Empty).Trim(),
                    Port = m.Port > 0 ? m.Port : 9100
                };
            }

            var path = GetSettingsFilePath();
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            await File.WriteAllTextAsync(path, JsonSerializer.Serialize(state, JsonOptions), cancellationToken)
                .ConfigureAwait(false);
            _cached = state;
            _logger.LogInformation("Saved mill printer settings to {Path}.", path);
        }
        finally
        {
            _lock.Release();
        }
    }

    public (string Address, int Port, bool Configured) ResolveForMill(int millNo)
    {
        EnsureLoadedSync();
        var state = _cached;
        if (state?.Mills.TryGetValue(millNo, out var row) == true)
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

    private async Task EnsureLoadedAsync(CancellationToken cancellationToken)
    {
        if (_cached is not null)
            return;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cached is not null)
                return;

            var path = GetSettingsFilePath();
            if (File.Exists(path))
            {
                try
                {
                    var json = await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false);
                    _cached = JsonSerializer.Deserialize<PersistedMillPrinters>(json, JsonOptions) ?? new PersistedMillPrinters();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to load mill printer settings from {Path}; using defaults.", path);
                    _cached = new PersistedMillPrinters();
                }
            }
            else
            {
                _cached = CreateDefaultFromLegacy();
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private PersistedMillPrinters CreateDefaultFromLegacy()
    {
        var state = new PersistedMillPrinters();
        var legacy = (_optionsMonitor.CurrentValue.NdtTagPrinterAddress ?? string.Empty).Trim();
        var port = _optionsMonitor.CurrentValue.NdtTagPrinterPort > 0
            ? _optionsMonitor.CurrentValue.NdtTagPrinterPort
            : 9100;
        if (IsUsableAddress(legacy))
        {
            state.Mills[1] = new MillPrinterRow { Address = legacy, Port = port };
        }

        return state;
    }

    private IReadOnlyList<MillPrinterEndpoint> BuildListFromState(PersistedMillPrinters state)
    {
        var list = new List<MillPrinterEndpoint>(4);
        for (var m = 1; m <= 4; m++)
        {
            if (state.Mills.TryGetValue(m, out var row))
                list.Add(new MillPrinterEndpoint(m, row.Address ?? string.Empty, row.Port > 0 ? row.Port : 9100));
            else
                list.Add(new MillPrinterEndpoint(m, string.Empty, 9100));
        }

        return list;
    }

    private void EnsureLoadedSync()
    {
        if (_cached is not null)
            return;
        EnsureLoadedAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    private string GetSettingsFilePath()
    {
        var opt = _optionsMonitor.CurrentValue;
        var configured = (opt.NdtBundleRuntimeStateFile ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(configured))
        {
            var dir = Path.GetDirectoryName(configured);
            if (!string.IsNullOrEmpty(dir))
                return Path.Combine(dir, "MillPrinterSettings.json");
        }

        var output = (opt.OutputBundleFolder ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(output))
            return Path.Combine(output, "MillPrinterSettings.json");

        return Path.Combine(AppContext.BaseDirectory, "MillPrinterSettings.json");
    }

    private static bool IsUsableAddress(string address) =>
        !string.IsNullOrWhiteSpace(address) &&
        !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);

    private sealed class PersistedMillPrinters
    {
        public Dictionary<int, MillPrinterRow> Mills { get; set; } = new();
    }

    private sealed class MillPrinterRow
    {
        public string Address { get; set; } = string.Empty;
        public int Port { get; set; } = 9100;
    }
}
