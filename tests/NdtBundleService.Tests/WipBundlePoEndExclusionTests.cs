using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class WipBundlePoEndExclusionTests
{
    [Fact]
    public void TryEnqueueFileBasedPoChange_returns_false_for_plc_mill_without_enqueue()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, plcMillNo: 2);

        var result = InvokeTryEnqueueFileBasedPoChange(
            provider,
            millNo: 2,
            endedPo: "1000057001",
            newPo: "1000057002",
            wipFileName: "WIP_02_1000057002_1.csv");

        Assert.False(result);
        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "probe" }));
    }

    [Fact]
    public void TryApplyRunningPoUpdateUnsafe_plc_mill_updates_running_po_without_enqueue()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, plcMillNo: 2);
        var stamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(2, "1000057001", stamp, "WIP_02_1000057001_1.csv"));

        var result = InvokeTryApplyRunningPoUpdateUnsafe(
            provider,
            millNo: 2,
            newPo: "1000057002",
            wipStampUtc: stamp.AddMinutes(1),
            wipFileName: "WIP_02_1000057002_1.csv");

        Assert.True(result);
        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "probe" }));
    }

    [Fact]
    public void TryApplyRunningPoUpdateUnsafe_file_mill_enqueues_po_end()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, fileMillNo: 4);
        var stamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(4, "1000057001", stamp, "WIP_04_1000057001_1.csv"));

        var result = InvokeTryApplyRunningPoUpdateUnsafe(
            provider,
            millNo: 4,
            newPo: "1000057002",
            wipStampUtc: stamp.AddMinutes(1),
            wipFileName: "WIP_04_1000057002_1.csv");

        Assert.True(result);
        Assert.False(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 4, NewPo = "probe" }));
        queue.MarkCompleted(4);
    }

    private static WipBundleRunningPoProvider CreateProvider(
        FileBasedPoChangeQueue queue,
        int? plcMillNo = null,
        int? fileMillNo = null)
    {
        var mills = new List<MillConfig>();
        if (plcMillNo is { } plc)
            mills.Add(new MillConfig { MillNo = plc, PoEndSource = "Plc" });
        if (fileMillNo is { } file)
            mills.Add(new MillConfig { MillNo = file, PoEndSource = "File" });

        var options = Options.Create(new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions { Mills = mills }
        });

        return new WipBundleRunningPoProvider(
            options,
            NullLogger<WipBundleRunningPoProvider>.Instance,
            queue);
    }

    private static bool InvokeTryEnqueueFileBasedPoChange(
        WipBundleRunningPoProvider provider,
        int millNo,
        string endedPo,
        string newPo,
        string wipFileName)
    {
        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryEnqueueFileBasedPoChange",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        return (bool)method!.Invoke(
            provider,
            [millNo, endedPo, newPo, DateTime.UtcNow, wipFileName])!;
    }

    private static bool InvokeTryApplyRunningPoUpdateUnsafe(
        WipBundleRunningPoProvider provider,
        int millNo,
        string newPo,
        DateTime wipStampUtc,
        string wipFileName)
    {
        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryApplyRunningPoUpdateUnsafe",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        return (bool)method!.Invoke(
            provider,
            [millNo, newPo, wipStampUtc, wipFileName])!;
    }
}
