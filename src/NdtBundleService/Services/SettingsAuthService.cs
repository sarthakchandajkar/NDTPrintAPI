using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed class SettingsAuthService
{
    private readonly DashboardSettingsOptions _options;
    private readonly ConcurrentDictionary<string, DateTime> _tokens = new(StringComparer.Ordinal);

    public SettingsAuthService(IOptions<NdtBundleOptions> options)
    {
        _options = options.Value.DashboardSettings ?? new DashboardSettingsOptions();
    }

    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(_options.AdminPassword);

    public bool TryLogin(string? password, out string? token, out DateTime expiresUtc)
    {
        token = null;
        expiresUtc = default;

        if (!IsConfigured)
            return false;

        var provided = password ?? string.Empty;
        var expected = _options.AdminPassword;
        var providedBytes = Encoding.UTF8.GetBytes(provided);
        var expectedBytes = Encoding.UTF8.GetBytes(expected);
        if (providedBytes.Length != expectedBytes.Length ||
            !CryptographicOperations.FixedTimeEquals(providedBytes, expectedBytes))
            return false;

        token = Guid.NewGuid().ToString("N");
        var hours = Math.Clamp(_options.SessionHours, 1, 72);
        expiresUtc = DateTime.UtcNow.AddHours(hours);
        _tokens[token] = expiresUtc;
        PruneExpired();
        return true;
    }

    public bool ValidateToken(string? token)
    {
        if (string.IsNullOrWhiteSpace(token))
            return false;

        if (!_tokens.TryGetValue(token, out var expires))
            return false;

        if (expires <= DateTime.UtcNow)
        {
            _tokens.TryRemove(token, out _);
            return false;
        }

        return true;
    }

    public void Revoke(string? token)
    {
        if (!string.IsNullOrWhiteSpace(token))
            _tokens.TryRemove(token, out _);
    }

    private void PruneExpired()
    {
        var now = DateTime.UtcNow;
        foreach (var kv in _tokens)
        {
            if (kv.Value <= now)
                _tokens.TryRemove(kv.Key, out _);
        }
    }
}
