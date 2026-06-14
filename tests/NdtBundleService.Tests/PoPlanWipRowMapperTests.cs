using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoPlanWipRowMapperTests
{
    [Fact]
    public void ToWipLabel_maps_thickness_and_weight_for_tags()
    {
        var row = new PoPlanWipRow
        {
            PipeGrade = "X52",
            PipeSize = "2.5",
            PipeThickness = "2.6",
            PipeLength = "6.000",
            PipeWeightPerMeter = "4.713",
            PipeType = "WIP"
        };

        var label = PoPlanWipRowMapper.ToWipLabel(row);

        Assert.Equal("2.6", label.PipeThickness);
        Assert.Equal("4.713", label.PipeWeightPerMeter);
        Assert.Equal("WIP", label.PipeType);
    }
}
