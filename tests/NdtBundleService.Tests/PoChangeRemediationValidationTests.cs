using NdtBundleService.Services;
using NdtBundleService.Services.PoChangeRemediation;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoChangeRemediationValidationTests
{
    [Fact]
    public void Validate_rejects_slit_sum_mismatch()
    {
        var req = new PoChangeRemediationRequest
        {
            MillNo = 1,
            Reason = "test",
            FinishedBatches =
            {
                new PoChangeRemediationFinishedBatch
                {
                    BundleNo = "1226100027",
                    PoNumber = "1000062601",
                    TotalNdtPcs = 10,
                    Slits =
                    {
                        new PoChangeRemediationSlitRow
                        {
                            SlitNo = "2607102_01",
                            NdtPipes = 3,
                            RejectedPipes = 0,
                            SlitStartTime = "05.10.2026 12:00:00",
                            SlitFinishTime = "05.10.2026 12:10:00"
                        }
                    }
                }
            }
        };

        var result = PoChangeRemediationValidation.Validate(req);
        Assert.False(result.Ok);
        Assert.Contains(result.Errors, e => e.Contains("Slit sum", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_rejects_duplicate_bundle_between_finished_and_forming()
    {
        var req = new PoChangeRemediationRequest
        {
            MillNo = 1,
            Reason = "test",
            FinishedBatches =
            {
                new PoChangeRemediationFinishedBatch
                {
                    BundleNo = "1226100028",
                    PoNumber = "1000062601",
                    TotalNdtPcs = 3,
                    Slits =
                    {
                        new PoChangeRemediationSlitRow
                        {
                            SlitNo = "a_01",
                            NdtPipes = 3,
                            SlitStartTime = "05.10.2026 12:00:00",
                            SlitFinishTime = "05.10.2026 12:10:00"
                        }
                    }
                }
            },
            Forming = new PoChangeRemediationFormingBatch
            {
                BundleNo = "1226100028",
                PoNumber = "1000062605",
                TargetNdtPcs = 65,
                Slits = { }
            }
        };

        var result = PoChangeRemediationValidation.Validate(req);
        Assert.False(result.Ok);
        Assert.Contains(result.Errors, e => e.Contains("already used", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_computes_proposed_sequence_as_max()
    {
        var req = new PoChangeRemediationRequest
        {
            MillNo = 1,
            Reason = "test",
            FinishedBatches =
            {
                new PoChangeRemediationFinishedBatch
                {
                    BundleNo = "1226100027",
                    PoNumber = "1000062601",
                    TotalNdtPcs = 3,
                    Slits =
                    {
                        new PoChangeRemediationSlitRow
                        {
                            SlitNo = "a_01",
                            NdtPipes = 3,
                            SlitStartTime = "05.10.2026 12:00:00",
                            SlitFinishTime = "05.10.2026 12:10:00"
                        }
                    }
                }
            },
            Forming = new PoChangeRemediationFormingBatch
            {
                BundleNo = "1226100028",
                PoNumber = "1000062605",
                TargetNdtPcs = 65,
                Slits =
                {
                    new PoChangeRemediationSlitRow
                    {
                        SlitNo = "b_01",
                        NdtPipes = 4,
                        SlitStartTime = "05.10.2026 13:00:00",
                        SlitFinishTime = "05.10.2026 13:10:00"
                    }
                }
            }
        };

        var result = PoChangeRemediationValidation.Validate(req);
        Assert.True(result.Ok);
        Assert.Equal(28, result.ProposedMillSequence);
        Assert.NotNull(result.Forming);
        Assert.Equal(4, result.Forming!.CsvFilled);
    }

    [Theory]
    [InlineData("1226100029", CsvFillState.PlcClosed, "Pending", false, "1226100028", null, true)]
    [InlineData("1226100028", CsvFillState.CsvFilling, "Pending", false, "1226100028", null, false)]
    [InlineData("1226100029", CsvFillState.CsvComplete, "Printed", false, "1226100028", null, false)]
    [InlineData("1226100029", CsvFillState.PlcClosed, "Pending", true, "1226100028", null, false)]
    [InlineData("1226100030", CsvFillState.CsvFilling, "Pending", false, null, null, true)]
    [InlineData("1226100008", CsvFillState.CsvFilling, "Printed", false, null, null, false)]
    [InlineData("1226100031", CsvFillState.CsvOvershoot, "Pending", false, "1226100030", "CsvAdvance", true)]
    [InlineData("1226100030", CsvFillState.CsvOvershoot, "Pending", false, "1226100030", "CsvAdvance", false)]
    [InlineData("1226100029", CsvFillState.CsvComplete, "Pending", false, "1226100030", "Plc", false)]
    public void ShouldVoidAsExtraForming_enforces_single_forming(
        string bundleNo,
        string? fill,
        string? print,
        bool voided,
        string? keep,
        string? closeSource,
        bool expected) =>
        Assert.Equal(
            expected,
            PoChangeRemediationValidation.ShouldVoidAsExtraForming(
                bundleNo, fill, print, voided, keep, closeSource));

    [Fact]
    public void Validate_forming_only_consolidation_ok_with_empty_target()
    {
        var req = new PoChangeRemediationRequest
        {
            MillNo = 1,
            Reason = "dual forming cleanup",
            Forming = new PoChangeRemediationFormingBatch
            {
                BundleNo = "1226100030",
                PoNumber = "1000062606",
                TargetNdtPcs = null,
                Slits =
                {
                    new PoChangeRemediationSlitRow
                    {
                        SlitNo = "2607099_02",
                        NdtPipes = 4,
                        SlitStartTime = "05.10.2026 15:34:38",
                        SlitFinishTime = "05.10.2026 15:40:03",
                        SourceFileName = "2607099_02_261005_1000062606.csv"
                    },
                    new PoChangeRemediationSlitRow
                    {
                        SlitNo = "2607099_05",
                        NdtPipes = 5,
                        SlitStartTime = "05.10.2026 15:51:30",
                        SlitFinishTime = "05.10.2026 16:17:19",
                        SourceFileName = "2607099_05_261005_1000062606.csv"
                    }
                }
            }
        };

        var result = PoChangeRemediationValidation.Validate(req, defaultFormingTarget: 65);
        Assert.True(result.Ok);
        Assert.NotNull(result.Forming);
        Assert.Equal(65, result.Forming!.TargetNdtPcs);
        Assert.Equal(9, result.Forming.CsvFilled);
        Assert.Equal(30, result.ProposedMillSequence);
        Assert.Equal("2607099_05_261005_1000062606.csv", result.Forming.Slits[1].FileName);
    }

    [Fact]
    public void Validate_warns_on_sequence_gap()
    {
        var req = new PoChangeRemediationRequest
        {
            MillNo = 1,
            Reason = "gap",
            FinishedBatches =
            {
                new PoChangeRemediationFinishedBatch
                {
                    BundleNo = "1226100027",
                    PoNumber = "1000062601",
                    TotalNdtPcs = 1,
                    Slits =
                    {
                        new PoChangeRemediationSlitRow
                        {
                            SlitNo = "a",
                            NdtPipes = 1,
                            SlitStartTime = "05.10.2026 12:00:00",
                            SlitFinishTime = "05.10.2026 12:01:00"
                        }
                    }
                },
                new PoChangeRemediationFinishedBatch
                {
                    BundleNo = "1226100029",
                    PoNumber = "1000062602",
                    TotalNdtPcs = 1,
                    Slits =
                    {
                        new PoChangeRemediationSlitRow
                        {
                            SlitNo = "b",
                            NdtPipes = 1,
                            SlitStartTime = "05.10.2026 12:02:00",
                            SlitFinishTime = "05.10.2026 12:03:00"
                        }
                    }
                }
            }
        };

        var result = PoChangeRemediationValidation.Validate(req);
        Assert.True(result.Ok);
        Assert.Contains(result.Warnings, w => w.Contains("gap", StringComparison.OrdinalIgnoreCase));
    }
}
