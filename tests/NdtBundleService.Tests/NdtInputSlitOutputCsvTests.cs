using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtInputSlitOutputCsvTests
{
    [Fact]
    public void BuildLines_includes_header_and_ndt_batch_column()
    {
        var record = new InputSlitRecord
        {
            PoNumber = "1000061839",
            SlitNo = "2606106_04",
            NdtPipes = 42,
            RejectedPipes = 1,
            SlitStartTime = new DateTime(2026, 9, 27, 10, 0, 6),
            SlitFinishTime = new DateTime(2026, 9, 27, 11, 0, 0),
            MillNo = 1,
            NdtShortLengthPipe = "",
            RejectedShortLengthPipe = ""
        };

        var lines = NdtInputSlitOutputCsv.BuildLines(new[] { (record, "1226100005") });

        Assert.Equal(NdtInputSlitOutputCsv.Header, lines[0]);
        Assert.Contains("NDT Batch No", lines[0]);
        Assert.Equal(2, lines.Count);
        Assert.EndsWith(",1226100005", lines[1]);
        Assert.Contains("27.09.2026 10:00:06", lines[1]);
        Assert.Contains("1000061839", lines[1]);
    }
}
