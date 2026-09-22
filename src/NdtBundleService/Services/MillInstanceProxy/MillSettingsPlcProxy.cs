using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.MillInstanceProxy;

public sealed record MillProxyHttpResult(int StatusCode, string ContentType, string Body);

/// <summary>
/// Forwards Settings PLC write calls from Shared to the mill process that owns the S7 handshake.
/// </summary>
public interface IMillSettingsPlcProxy
{
    bool ShouldProxy(int millNo);

    Task<MillProxyHttpResult> ForwardAsync(
        int millNo,
        HttpMethod method,
        string relativePath,
        string? jsonBody,
        CancellationToken cancellationToken);
}

public sealed class MillSettingsPlcProxy : IMillSettingsPlcProxy
{
    public const string HttpClientName = "MillSettingsPlcProxy";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IOptionsMonitor<NdtBundleOptions> _bundle;
    private readonly IOptionsMonitor<InstanceRoleOptions> _role;
    private readonly ILogger<MillSettingsPlcProxy> _logger;
    private readonly ConcurrentDictionary<int, CachedToken> _tokens = new();

    public MillSettingsPlcProxy(
        IHttpClientFactory httpClientFactory,
        IOptionsMonitor<NdtBundleOptions> bundle,
        IOptionsMonitor<InstanceRoleOptions> role,
        ILogger<MillSettingsPlcProxy> logger)
    {
        _httpClientFactory = httpClientFactory;
        _bundle = bundle;
        _role = role;
        _logger = logger;
    }

    public bool ShouldProxy(int millNo)
    {
        if (!_role.CurrentValue.IsShared)
            return false;
        var proxy = _bundle.CurrentValue.MillInstanceProxy ?? new MillInstanceProxyOptions();
        return proxy.Enabled && proxy.TryGetBaseUrl(millNo, out _);
    }

    public async Task<MillProxyHttpResult> ForwardAsync(
        int millNo,
        HttpMethod method,
        string relativePath,
        string? jsonBody,
        CancellationToken cancellationToken)
    {
        var proxy = _bundle.CurrentValue.MillInstanceProxy ?? new MillInstanceProxyOptions();
        if (!proxy.TryGetBaseUrl(millNo, out var baseUri) || baseUri is null)
        {
            return ErrorJson(
                503,
                $"Mill {millNo} proxy base URL is not configured (NdtBundle:MillInstanceProxy:BaseUrls).");
        }

        var client = _httpClientFactory.CreateClient(HttpClientName);
        client.Timeout = TimeSpan.FromSeconds(Math.Clamp(proxy.TimeoutSeconds, 5, 120));

        string token;
        try
        {
            token = await EnsureMillTokenAsync(client, baseUri, millNo, forceRefresh: false, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Mill {Mill} Settings login via proxy failed at {Base}.", millNo, baseUri);
            return ErrorJson(
                502,
                $"Cannot reach Mill-{millNo} at {baseUri.AbsoluteUri.TrimEnd('/')} for Settings login. Is the mill service running?",
                ex.Message);
        }

        var path = relativePath.TrimStart('/');
        var uri = new Uri(baseUri, path);

        try
        {
            using var response = await SendWithTokenAsync(
                    client, method, uri, token, jsonBody, cancellationToken)
                .ConfigureAwait(false);
            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                _tokens.TryRemove(millNo, out _);
                token = await EnsureMillTokenAsync(client, baseUri, millNo, forceRefresh: true, cancellationToken)
                    .ConfigureAwait(false);
                using var retryResponse = await SendWithTokenAsync(
                        client, method, uri, token, jsonBody, cancellationToken)
                    .ConfigureAwait(false);
                return await ToResultAsync(retryResponse, cancellationToken).ConfigureAwait(false);
            }

            return await ToResultAsync(response, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Mill {Mill} Settings proxy forward failed ({Path}).", millNo, path);
            return ErrorJson(502, $"Mill-{millNo} Settings proxy request failed.", ex.Message);
        }
    }

    private static async Task<HttpResponseMessage> SendWithTokenAsync(
        HttpClient client,
        HttpMethod method,
        Uri uri,
        string token,
        string? jsonBody,
        CancellationToken cancellationToken)
    {
        var request = new HttpRequestMessage(method, uri);
        request.Headers.TryAddWithoutValidation("X-Settings-Token", token);
        if (jsonBody is not null)
            request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
        return await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> EnsureMillTokenAsync(
        HttpClient client,
        Uri baseUri,
        int millNo,
        bool forceRefresh,
        CancellationToken cancellationToken)
    {
        if (!forceRefresh &&
            _tokens.TryGetValue(millNo, out var cached) &&
            cached.ExpiresUtc > DateTime.UtcNow.AddMinutes(1) &&
            !string.IsNullOrWhiteSpace(cached.Token))
        {
            return cached.Token;
        }

        var password = _bundle.CurrentValue.DashboardSettings?.AdminPassword;
        if (string.IsNullOrWhiteSpace(password))
            throw new InvalidOperationException("NdtBundle:DashboardSettings:AdminPassword is not set on Shared.");

        using var login = new HttpRequestMessage(HttpMethod.Post, new Uri(baseUri, "api/Settings/login"));
        login.Content = new StringContent(
            JsonSerializer.Serialize(new { password }),
            Encoding.UTF8,
            "application/json");

        using var response = await client.SendAsync(login, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Mill-{millNo} login returned {(int)response.StatusCode}: {Truncate(body, 200)}");
        }

        var parsed = JsonSerializer.Deserialize<LoginResponse>(body, JsonOptions)
            ?? throw new InvalidOperationException($"Mill-{millNo} login returned empty body.");
        if (string.IsNullOrWhiteSpace(parsed.Token))
            throw new InvalidOperationException($"Mill-{millNo} login response missing Token.");

        var expires = parsed.ExpiresUtc == default
            ? DateTime.UtcNow.AddHours(1)
            : parsed.ExpiresUtc.UtcDateTime;
        _tokens[millNo] = new CachedToken(parsed.Token, expires);
        return parsed.Token;
    }

    private static async Task<MillProxyHttpResult> ToResultAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        return new MillProxyHttpResult((int)response.StatusCode, contentType, body);
    }

    private static MillProxyHttpResult ErrorJson(int status, string message, string? error = null)
    {
        var payload = error is null
            ? JsonSerializer.Serialize(new { message })
            : JsonSerializer.Serialize(new { message, error });
        return new MillProxyHttpResult(status, "application/json", payload);
    }

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..max];

    private sealed record CachedToken(string Token, DateTime ExpiresUtc);

    private sealed class LoginResponse
    {
        public string? Token { get; set; }
        public DateTimeOffset ExpiresUtc { get; set; }
    }
}

/// <summary>No-op when mill workers own PLC locally (Mill / Monolith).</summary>
public sealed class NullMillSettingsPlcProxy : IMillSettingsPlcProxy
{
    public bool ShouldProxy(int millNo) => false;

    public Task<MillProxyHttpResult> ForwardAsync(
        int millNo,
        HttpMethod method,
        string relativePath,
        string? jsonBody,
        CancellationToken cancellationToken) =>
        Task.FromResult(new MillProxyHttpResult(
            500,
            "application/json",
            """{"message":"Mill Settings PLC proxy is not active on this instance."}"""));
}
