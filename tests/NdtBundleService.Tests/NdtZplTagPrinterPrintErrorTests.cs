using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtZplTagPrinterPrintErrorTests
{
    [Fact]
    public async Task Missing_printer_returns_no_printer_configured_for_mill_n()
    {
        var printer = CreatePrinter(configured: false, send: new PrinterSendResult(true));
        var result = await printer.PrintBundleTagAsync(Record(millNo: 3), 1, 10, false, CancellationToken.None);
        Assert.False(result.Success);
        Assert.Equal("no printer configured for mill 3", result.ErrorDetail);
    }

    [Fact]
    public async Task Unreachable_printer_includes_host_and_port()
    {
        var printer = CreatePrinter(
            configured: true,
            send: new PrinterSendResult(false, "No connection could be made"));
        var result = await printer.PrintBundleTagAsync(Record(millNo: 2), 1, 10, false, CancellationToken.None);
        Assert.False(result.Success);
        Assert.Contains("10.9.9.9:9100", result.ErrorDetail);
        Assert.Contains("No connection could be made", result.ErrorDetail);
    }

    private static NdtZplTagPrinter CreatePrinter(bool configured, PrinterSendResult send)
    {
        var options = Options.Create(new NdtBundleOptions { EnableNdtTagZplAndPrint = true });
        return new NdtZplTagPrinter(
            new OptionsMonitorStub(options.Value),
            new AlwaysOnZpl(),
            new EmptyWip(),
            new StubSender(send),
            new StubMillPrinters(configured),
            NullLogger<NdtZplTagPrinter>.Instance);
    }

    private static InputSlitRecord Record(int millNo) => new()
    {
        PoNumber = "1000000001",
        MillNo = millNo,
        SlitStartTime = new DateTime(2026, 8, 1, 8, 0, 0, DateTimeKind.Utc)
    };

    private sealed class AlwaysOnZpl : IZplGenerationToggle
    {
        public bool IsEnabled => true;
        public bool SetEnabled(bool enabled) => enabled;
    }

    private sealed class EmptyWip : IWipLabelProvider
    {
        public Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default) =>
            Task.FromResult<WipLabelInfo?>(null);
    }

    private sealed class StubSender(PrinterSendResult result) : INetworkPrinterSender
    {
        public Task<PrinterSendResult> SendAsync(string host, int port, byte[] data, CancellationToken cancellationToken = default) =>
            Task.FromResult(result);
    }

    private sealed class StubMillPrinters(bool configured) : IMillPrinterSettingsService
    {
        public Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<MillPrinterEndpoint>>([]);
        public Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public (string Address, int Port, bool Configured) ResolveForMill(int millNo) =>
            configured ? ("10.9.9.9", 9100, true) : (string.Empty, 9100, false);
    }

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
