using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Verifies Manual Reconcile / print-bundle reprint path: ZPL includes Reprint marker,
/// corrected pipe count, printer send, and Print_Status update.
/// </summary>
public sealed class ReconcileBundleTagServiceTests
{
    [Fact]
    public async Task ReprintAsync_sends_zpl_with_Reprint_marker_and_corrected_pcs()
    {
        var sender = new CapturingPrinterSender { Result = new PrinterSendResult(true) };
        var bundles = new CapturingBundleRepo();
        var sut = CreateSut(sender, bundles, zplEnabled: true, printerConfigured: true);

        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100001",
            PoNumber = "1000060363",
            MillNo = 1,
            TotalNdtPcs = 18,
            PrintedAt = new DateTime(2026, 9, 27, 10, 0, 0)
        };

        var result = await sut.ReprintAsync(bundle, CancellationToken.None);

        Assert.True(result.Success);
        Assert.Contains("Reprint", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(sender.Sends);
        var zpl = Encoding.UTF8.GetString(sender.Sends[0].Data);
        Assert.Contains("Reprint", zpl, StringComparison.Ordinal);
        Assert.Contains("Pcs. 18", zpl, StringComparison.Ordinal);
        Assert.Contains("1226100001", zpl, StringComparison.Ordinal);
        Assert.Equal(("1226100001", BundlePrintStatus.Printed), bundles.LastPrintStatus);
    }

    [Fact]
    public async Task ReprintAsync_first_print_path_would_omit_Reprint_marker_in_zpl_builder()
    {
        // Guard: isReprint:false must not say Reprint (first close path).
        var zpl = Encoding.UTF8.GetString(ZplNdtLabelBuilder.BuildNdtTagZpl(
            "1226100001", 1, "1000060363", "X52", "2\"", "0.25", "6.0", "500", "WIP",
            new DateTime(2026, 9, 27), 20, isReprint: false));
        Assert.DoesNotContain("Reprint", zpl, StringComparison.Ordinal);

        var reprintZpl = Encoding.UTF8.GetString(ZplNdtLabelBuilder.BuildNdtTagZpl(
            "1226100001", 1, "1000060363", "X52", "2\"", "0.25", "6.0", "500", "WIP",
            new DateTime(2026, 9, 27), 18, isReprint: true));
        Assert.Contains("Reprint", reprintZpl, StringComparison.Ordinal);
        Assert.Contains("Pcs. 18", reprintZpl, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReprintAsync_fails_when_zpl_toggle_disabled()
    {
        var sender = new CapturingPrinterSender();
        var sut = CreateSut(sender, new CapturingBundleRepo(), zplEnabled: false, printerConfigured: true);

        var result = await sut.ReprintAsync(SampleBundle(), CancellationToken.None);

        Assert.False(result.Success);
        Assert.Contains("disabled", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(sender.Sends);
    }

    [Fact]
    public async Task ReprintAsync_fails_when_printer_not_configured()
    {
        var sender = new CapturingPrinterSender();
        var sut = CreateSut(sender, new CapturingBundleRepo(), zplEnabled: true, printerConfigured: false);

        var result = await sut.ReprintAsync(SampleBundle(), CancellationToken.None);

        Assert.False(result.Success);
        Assert.Contains("Printer not configured", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(sender.Sends);
    }

    [Fact]
    public async Task ReprintAsync_marks_PrintFailed_when_send_fails()
    {
        var sender = new CapturingPrinterSender
        {
            Result = new PrinterSendResult(false, "connection refused")
        };
        var bundles = new CapturingBundleRepo();
        var sut = CreateSut(sender, bundles, zplEnabled: true, printerConfigured: true);

        var result = await sut.ReprintAsync(SampleBundle(), CancellationToken.None);

        Assert.False(result.Success);
        Assert.Equal("connection refused", result.ErrorDetail);
        Assert.Equal(BundlePrintStatus.PrintFailed, bundles.LastPrintStatus?.Status);
        Assert.Single(sender.Sends);
    }

    private static NdtBundleRecord SampleBundle() =>
        new()
        {
            BundleNo = "1226100001",
            PoNumber = "1000060363",
            MillNo = 1,
            TotalNdtPcs = 18
        };

    private static ReconcileBundleTagService CreateSut(
        CapturingPrinterSender sender,
        CapturingBundleRepo bundles,
        bool zplEnabled,
        bool printerConfigured) =>
        new(
            bundles,
            new StubWip(),
            sender,
            new StubPrinters(printerConfigured),
            Options.Create(new NdtBundleOptions()),
            new StubZplToggle(zplEnabled),
            NullLogger<ReconcileBundleTagService>.Instance);

    private sealed class CapturingPrinterSender : INetworkPrinterSender
    {
        public PrinterSendResult Result { get; set; } = new(true);
        public List<(string Host, int Port, byte[] Data)> Sends { get; } = new();

        public Task<PrinterSendResult> SendAsync(string host, int port, byte[] data, CancellationToken cancellationToken = default)
        {
            Sends.Add((host, port, data));
            return Task.FromResult(Result);
        }
    }

    private sealed class CapturingBundleRepo : StubBundleRepoBase
    {
        public (string BundleNo, string Status)? LastPrintStatus { get; private set; }

        public override Task UpdateBundlePrintStatusAsync(
            string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken)
        {
            LastPrintStatus = (bundleNo, printStatus);
            return Task.CompletedTask;
        }
    }

    private sealed class StubWip : IWipLabelProvider
    {
        public Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default) =>
            Task.FromResult<WipLabelInfo?>(new WipLabelInfo
            {
                PipeGrade = "X52",
                PipeSize = "2\"",
                PipeThickness = "0.250",
                PipeLength = "6.000",
                PipeWeightPerMeter = "10",
                PipeType = "WIP"
            });
    }

    private sealed class StubPrinters : IMillPrinterSettingsService
    {
        private readonly bool _configured;
        public StubPrinters(bool configured) => _configured = configured;

        public Task<IReadOnlyList<MillPrinterEndpoint>> GetAllAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<MillPrinterEndpoint>>(Array.Empty<MillPrinterEndpoint>());

        public Task SaveAllAsync(IReadOnlyList<MillPrinterEndpoint> mills, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public (string Address, int Port, bool Configured) ResolveForMill(int millNo) =>
            _configured ? ("192.168.0.125", 9100, true) : ("", 9100, false);
    }

    private sealed class StubZplToggle : IZplGenerationToggle
    {
        private readonly bool _enabled;
        public StubZplToggle(bool enabled) => _enabled = enabled;
        public bool IsEnabled => _enabled;
        public bool SetEnabled(bool enabled) => enabled;
    }
}
