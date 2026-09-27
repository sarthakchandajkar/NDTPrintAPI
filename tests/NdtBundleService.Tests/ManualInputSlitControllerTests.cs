using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Controllers;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ManualInputSlitControllerTests : IDisposable
{
    private readonly string _folder;

    public ManualInputSlitControllerTests()
    {
        _folder = Path.Combine(Path.GetTempPath(), "manual-slit-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_folder);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_folder))
                Directory.Delete(_folder, recursive: true);
        }
        catch
        {
            /* ignore */
        }
    }

    [Fact]
    public async Task CreateManualFile_writes_csv_with_standard_header()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { InputSlitFolder = _folder }),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1000060999",
                MillNo = 1,
                SlitNo = "S1",
                NdtPipes = 12,
                RejectedPipes = 0,
                FileName = "manual_test_row.csv"
            },
            CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.NotNull(ok.Value);

        var path = Path.Combine(_folder, "manual_test_row.csv");
        Assert.True(File.Exists(path));
        var lines = await File.ReadAllLinesAsync(path);
        Assert.Equal(InputSlitsController.ManualCsvHeader, lines[0]);
        Assert.Contains("1000060999", lines[1], StringComparison.Ordinal);
        Assert.Contains(",12,", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateManualFile_rejects_invalid_mill()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { InputSlitFolder = _folder }),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest { PoNumber = "1", MillNo = 9, NdtPipes = 1 },
            CancellationToken.None);

        Assert.IsType<BadRequestObjectResult>(result);
    }
}
