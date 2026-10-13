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
        Assert.Contains("Rejected Pipes", lines[0]);
        Assert.DoesNotContain("Rejected P,", lines[0]);
        Assert.Contains("NDT Batch No", lines[0]);
        Assert.Equal(2, lines.Count);
        Assert.EndsWith(",1226100005", lines[1]);
        Assert.Contains("27.09.2026 10:00:06", lines[1]);
        Assert.Contains("1000061839", lines[1]);
        Assert.Contains(",1,", lines[1]); // Rejected Pipes numeric (never blank)
    }

    [Fact]
    public void FormatDataLine_writes_zero_rejected_pipes_as_digit_not_blank()
    {
        var record = new InputSlitRecord
        {
            PoNumber = "1000061839",
            SlitNo = "2606106_04",
            NdtPipes = 10,
            RejectedPipes = 0,
            HasRejectedPipes = true,
            SlitStartTime = new DateTime(2026, 9, 27, 10, 0, 6),
            SlitFinishTime = new DateTime(2026, 9, 27, 11, 0, 0),
            MillNo = 1,
        };

        var line = NdtInputSlitOutputCsv.FormatDataLine(record, "1226100005");
        var cols = line.Split(',');
        Assert.Equal("0", cols[3]);
    }

    [Fact]
    public void FormatDataLine_strips_linked_po_91_from_slit_no()
    {
        var record = new InputSlitRecord
        {
            PoNumber = "1000062751",
            SlitNo = "2607388_01_91",
            NdtPipes = 4,
            RejectedPipes = 2,
            SlitStartTime = new DateTime(2026, 10, 10, 2, 34, 53),
            SlitFinishTime = new DateTime(2026, 10, 10, 2, 40, 54),
            MillNo = 1,
        };

        var line = NdtInputSlitOutputCsv.FormatDataLine(record, "1226100049");
        Assert.Contains(",2607388_01,", line);
        Assert.DoesNotContain("2607388_01_91", line);
    }
}
