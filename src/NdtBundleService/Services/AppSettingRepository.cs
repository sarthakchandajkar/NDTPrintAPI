using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IAppSettingRepository
{
    Task<string?> GetValueAsync(string settingKey, CancellationToken cancellationToken);
    Task SetValueAsync(string settingKey, string settingValue, string? updatedBy, CancellationToken cancellationToken);
}

/// <summary>Key/value settings in <c>dbo.App_Setting</c> (cross-instance ZPL toggle, etc.).</summary>
public sealed class AppSettingRepository : IAppSettingRepository
{
    public const string ZplPhysicalPrintEnabledKey = "ZplPhysicalPrintEnabled";

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<AppSettingRepository> _logger;

    public AppSettingRepository(IOptionsMonitor<NdtBundleOptions> options, ILogger<AppSettingRepository> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task<string?> GetValueAsync(string settingKey, CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue) || string.IsNullOrWhiteSpace(settingKey))
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "App_Setting read", cancellationToken)
                .ConfigureAwait(false);

            await using var cmd = new SqlCommand(
                "SELECT Setting_Value FROM dbo.App_Setting WHERE Setting_Key = @Key;",
                conn);
            cmd.Parameters.AddWithValue("@Key", settingKey.Trim());
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is string s ? s : result?.ToString()?.Trim();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "App_Setting read failed for key {Key} (run docs/App_Setting_AddTable.sql if missing).", settingKey);
            return null;
        }
    }

    public async Task SetValueAsync(string settingKey, string settingValue, string? updatedBy, CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue))
            throw new InvalidOperationException("SQL is not configured; cannot persist App_Setting.");

        await using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "App_Setting upsert", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
MERGE dbo.App_Setting AS t
USING (SELECT @Key AS Setting_Key) AS s
ON t.Setting_Key = s.Setting_Key
WHEN MATCHED THEN UPDATE SET
    Setting_Value = @Value,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = @UpdatedBy
WHEN NOT MATCHED THEN INSERT (Setting_Key, Setting_Value, Updated_AtUtc, Updated_By)
VALUES (@Key, @Value, SYSUTCDATETIME(), @UpdatedBy);";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Key", settingKey);
        cmd.Parameters.AddWithValue("@Value", settingValue);
        cmd.Parameters.AddWithValue("@UpdatedBy", (object?)updatedBy ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// Global ZPL/physical-print flag stored in SQL so Shared + mill instances observe the same safety toggle.
/// Falls back to <see cref="NdtBundleOptions.EnableNdtTagZplAndPrint"/> when SQL is unavailable or row is missing.
/// </summary>
public sealed class SqlZplGenerationToggle : IZplGenerationToggle
{
    internal static readonly TimeSpan CacheTtl = TimeSpan.FromSeconds(2);

    private readonly IAppSettingRepository _settings;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<SqlZplGenerationToggle> _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly object _cacheLock = new();
    private bool? _cachedEnabled;
    private DateTime _cacheExpiresUtc = DateTime.MinValue;

    public SqlZplGenerationToggle(
        IAppSettingRepository settings,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<SqlZplGenerationToggle> logger,
        Func<DateTime>? utcNow = null)
    {
        _settings = settings;
        _options = options;
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    public bool IsEnabled
    {
        get
        {
            lock (_cacheLock)
            {
                if (_cachedEnabled.HasValue && _utcNow() < _cacheExpiresUtc)
                    return _cachedEnabled.Value;
            }

            var enabled = ReadEnabledSync();
            lock (_cacheLock)
            {
                _cachedEnabled = enabled;
                _cacheExpiresUtc = _utcNow().Add(CacheTtl);
            }

            return enabled;
        }
    }

    public bool SetEnabled(bool enabled)
    {
        var value = enabled ? "true" : "false";
        _settings.SetValueAsync(AppSettingRepository.ZplPhysicalPrintEnabledKey, value, "dashboard", CancellationToken.None)
            .GetAwaiter().GetResult();

        lock (_cacheLock)
        {
            _cachedEnabled = enabled;
            _cacheExpiresUtc = _utcNow().Add(CacheTtl);
        }

        _logger.LogInformation("ZPL physical print toggle set to {Enabled} (persisted to App_Setting).", enabled);
        return enabled;
    }

    private bool ReadEnabledSync()
    {
        var configDefault = _options.CurrentValue.EnableNdtTagZplAndPrint;
        var raw = _settings.GetValueAsync(AppSettingRepository.ZplPhysicalPrintEnabledKey, CancellationToken.None)
            .GetAwaiter().GetResult();

        if (string.IsNullOrWhiteSpace(raw))
            return configDefault;

        if (bool.TryParse(raw.Trim(), out var parsed))
            return parsed;

        _logger.LogWarning(
            "App_Setting {Key} has invalid value '{Value}'; using config default {Default}.",
            AppSettingRepository.ZplPhysicalPrintEnabledKey,
            raw,
            configDefault);
        return configDefault;
    }
}
