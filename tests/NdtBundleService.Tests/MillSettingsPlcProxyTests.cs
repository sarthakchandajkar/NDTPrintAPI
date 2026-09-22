using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.MillInstanceProxy;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillSettingsPlcProxyTests
{
    [Fact]
    public void ShouldProxy_only_when_Shared_enabled_and_base_url_configured()
    {
        var sut = NewProxy(
            SharedRole(),
            new MillInstanceProxyOptions
            {
                Enabled = true,
                BaseUrls = new Dictionary<string, string> { ["1"] = "http://127.0.0.1:5001" }
            });

        Assert.True(sut.ShouldProxy(1));
        Assert.False(sut.ShouldProxy(2));

        var millSut = NewProxy(
            MillRole(1),
            new MillInstanceProxyOptions
            {
                Enabled = true,
                BaseUrls = new Dictionary<string, string> { ["1"] = "http://127.0.0.1:5001" }
            });
        Assert.False(millSut.ShouldProxy(1));
    }

    [Fact]
    public async Task Forward_logs_in_then_posts_with_settings_token()
    {
        var handler = new ScriptedHandler(async (request, ct) =>
        {
            var path = request.RequestUri!.AbsolutePath;
            if (path.EndsWith("/api/Settings/login", StringComparison.OrdinalIgnoreCase))
            {
                var json = """{"token":"tok-abc","expiresUtc":"2099-01-01T00:00:00Z"}""";
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json")
                };
            }

            Assert.Equal("/api/Settings/plc/mill/1/connect", path);
            Assert.True(request.Headers.TryGetValues("X-Settings-Token", out var tokens));
            Assert.Equal("tok-abc", tokens.Single());
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"success":true,"millNo":1,"message":"connected"}""",
                    Encoding.UTF8,
                    "application/json")
            };
        });

        var sut = NewProxy(
            SharedRole(),
            new MillInstanceProxyOptions
            {
                Enabled = true,
                BaseUrls = new Dictionary<string, string> { ["1"] = "http://127.0.0.1:5001" },
                TimeoutSeconds = 10
            },
            handler,
            adminPassword: "secret");

        var result = await sut.ForwardAsync(
            1,
            HttpMethod.Post,
            "api/Settings/plc/mill/1/connect",
            jsonBody: null,
            CancellationToken.None);

        Assert.Equal(200, result.StatusCode);
        Assert.Contains("connected", result.Body, StringComparison.Ordinal);
        Assert.Equal(2, handler.CallCount);
    }

    [Fact]
    public async Task Forward_returns_502_when_mill_unreachable()
    {
        var handler = new ScriptedHandler((_, _) =>
            Task.FromException<HttpResponseMessage>(new HttpRequestException("connection refused")));

        var sut = NewProxy(
            SharedRole(),
            new MillInstanceProxyOptions
            {
                Enabled = true,
                BaseUrls = new Dictionary<string, string> { ["1"] = "http://127.0.0.1:5001" }
            },
            handler,
            adminPassword: "secret");

        var result = await sut.ForwardAsync(
            1,
            HttpMethod.Post,
            "api/Settings/plc/mill/1/connect",
            null,
            CancellationToken.None);

        Assert.Equal(502, result.StatusCode);
        Assert.Contains("Cannot reach Mill-1", result.Body, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_proxy_never_proxies()
    {
        var sut = new NullMillSettingsPlcProxy();
        Assert.False(sut.ShouldProxy(1));
    }

    private static MillSettingsPlcProxy NewProxy(
        InstanceRoleOptions role,
        MillInstanceProxyOptions proxy,
        HttpMessageHandler? handler = null,
        string adminPassword = "pw")
    {
        handler ??= new ScriptedHandler((_, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

        var factory = new FixedHttpClientFactory(handler);
        var bundle = Options.Create(new NdtBundleOptions
        {
            DashboardSettings = new DashboardSettingsOptions { AdminPassword = adminPassword },
            MillInstanceProxy = proxy
        });
        return new MillSettingsPlcProxy(
            factory,
            new BundleMonitor(bundle.Value),
            new RoleMonitor(role),
            NullLogger<MillSettingsPlcProxy>.Instance);
    }

    private static InstanceRoleOptions SharedRole() => new()
    {
        Mode = InstanceRoleModes.Shared,
        EnableDashboardApi = true,
        EnableMillWorkers = false
    };

    private static InstanceRoleOptions MillRole(int millNo) => new()
    {
        Mode = InstanceRoleModes.Mill,
        OwnedMillNos = [millNo],
        EnableMillWorkers = true,
        EnableDashboardApi = false
    };

    private sealed class FixedHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private sealed class ScriptedHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        public int CallCount { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return await respond(request, cancellationToken);
        }
    }

    private sealed class BundleMonitor(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }

    private sealed class RoleMonitor(InstanceRoleOptions value) : IOptionsMonitor<InstanceRoleOptions>
    {
        public InstanceRoleOptions CurrentValue { get; } = value;
        public InstanceRoleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<InstanceRoleOptions, string?> listener) => null;
    }
}
