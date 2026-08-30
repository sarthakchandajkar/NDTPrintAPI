using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleOptionsValidatorTests
{
    private readonly NdtBundleOptionsValidator _sut = new();

    [Fact]
    public void Valid_iso8601_succeeds_and_parses_utc()
    {
        const string raw = "2026-04-05T00:00:00Z";
        var result = _sut.Validate(null, new NdtBundleOptions { MinSourceFileLastWriteUtc = raw });
        Assert.True(result.Succeeded);

        Assert.True(SourceFileEligibility.TryParseMinUtcFromRaw(raw, out var minUtc));
        Assert.Equal(new DateTime(2026, 4, 5, 0, 0, 0, DateTimeKind.Utc), minUtc);
        Assert.Equal(DateTimeKind.Utc, minUtc!.Value.Kind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Empty_succeeds_as_no_floor(string? raw)
    {
        var result = _sut.Validate(null, new NdtBundleOptions { MinSourceFileLastWriteUtc = raw });
        Assert.True(result.Succeeded);

        Assert.True(SourceFileEligibility.TryParseMinUtcFromRaw(raw, out var minUtc));
        Assert.Null(minUtc);
        Assert.Null(SourceFileEligibility.ParseMinUtcFromRaw(raw));
    }

    [Fact]
    public void Malformed_fails_naming_the_bad_value()
    {
        const string bad = "not-a-date";
        var result = _sut.Validate(null, new NdtBundleOptions { MinSourceFileLastWriteUtc = bad });
        Assert.False(result.Succeeded);
        Assert.Contains(bad, result.Failures!.Single(), StringComparison.Ordinal);
        Assert.Contains("MinSourceFileLastWriteUtc", result.Failures!.Single(), StringComparison.Ordinal);

        Assert.False(SourceFileEligibility.TryParseMinUtcFromRaw(bad, out var minUtc));
        Assert.Null(minUtc);
    }

    [Fact]
    public async Task Valid_iso8601_logs_parsed_floor_at_startup()
    {
        var logger = new ListLogger();
        var sut = new SourceFileEligibilityStartupLog(
            Options.Create(new NdtBundleOptions { MinSourceFileLastWriteUtc = "2026-04-05T00:00:00Z" }),
            logger);

        await sut.StartAsync(CancellationToken.None);

        var line = Assert.Single(logger.Messages);
        Assert.Contains("MinSourceFileLastWriteUtc floor is", line, StringComparison.Ordinal);
        Assert.Contains(
            new DateTime(2026, 4, 5, 0, 0, 0, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
            line,
            StringComparison.Ordinal);
        Assert.DoesNotContain("none", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Empty_logs_none_floor_at_startup()
    {
        var logger = new ListLogger();
        var sut = new SourceFileEligibilityStartupLog(
            Options.Create(new NdtBundleOptions { MinSourceFileLastWriteUtc = "" }),
            logger);

        await sut.StartAsync(CancellationToken.None);

        var line = Assert.Single(logger.Messages);
        Assert.Contains("MinSourceFileLastWriteUtc floor is none", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Malformed_throws_on_startup_naming_the_bad_value()
    {
        const string bad = "not-a-date";
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["NdtBundle:MinSourceFileLastWriteUtc"] = bad
        });
        builder.Services.AddSingleton<IValidateOptions<NdtBundleOptions>, NdtBundleOptionsValidator>();
        builder.Services.AddOptions<NdtBundleOptions>()
            .Bind(builder.Configuration.GetSection("NdtBundle"))
            .ValidateOnStart();

        using var host = builder.Build();

        var accessEx = Assert.Throws<OptionsValidationException>(
            () => host.Services.GetRequiredService<IOptions<NdtBundleOptions>>().Value);
        Assert.Contains(bad, accessEx.Message, StringComparison.Ordinal);
        Assert.Contains("MinSourceFileLastWriteUtc", accessEx.Message, StringComparison.Ordinal);

        var startEx = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Contains(bad, startEx.Message, StringComparison.Ordinal);
    }

    private sealed class ListLogger : ILogger<SourceFileEligibilityStartupLog>
    {
        public List<string> Messages { get; } = new();

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => NullDisp.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception));
        }

        private sealed class NullDisp : IDisposable
        {
            public static readonly NullDisp Instance = new();
            public void Dispose() { }
        }
    }
}
