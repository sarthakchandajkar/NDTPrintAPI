using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SlitOutputCsvFileNamingTests
{
    [Fact]
    public void BuildFileName_uses_slit_row_po_not_wip_override()
    {
        var rows = new[]
        {
            new InputSlitRecord
            {
                SlitNo = "2603597_02",
                PoNumber = "1000059208",
                MillNo = 2,
                SlitStartTime = new DateTime(2026, 6, 14)
            }
        };

        var name = SlitOutputCsvFileNaming.BuildFileName(@"C:\in\2603597_02_260614_1000059208.csv", rows);

        Assert.Equal("2603597_02_20260614_1000059208.csv", name);
    }
}
