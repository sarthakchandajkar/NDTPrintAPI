using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// SAP status derivation/transition rules for NDT Input Slit output CSVs
/// (docs/NDT_Input_Slit_SAP_Status_Design.md). Accepted is terminal; Rejected → Pending is a resubmit.
/// </summary>
public sealed class OutputSlitSapStatusPolicyTests
{
    [Theory]
    // Accepted folder wins over stale copies anywhere else.
    [InlineData(false, true, false, OutputSlitSapStatus.Accepted)]
    [InlineData(true, true, false, OutputSlitSapStatus.Accepted)]
    [InlineData(false, true, true, OutputSlitSapStatus.Accepted)]
    [InlineData(true, true, true, OutputSlitSapStatus.Accepted)]
    // Pending wins over a stale Rejected copy (operator copied rather than moved on resubmit).
    [InlineData(true, false, false, OutputSlitSapStatus.Pending)]
    [InlineData(true, false, true, OutputSlitSapStatus.Pending)]
    // Only in Rejected.
    [InlineData(false, false, true, OutputSlitSapStatus.Rejected)]
    public void DeriveObservedStatus_folder_precedence(
        bool inPending,
        bool inAccepted,
        bool inRejected,
        OutputSlitSapStatus expected)
    {
        Assert.Equal(expected, OutputSlitSapStatusPolicy.DeriveObservedStatus(inPending, inAccepted, inRejected));
    }

    [Theory]
    [InlineData(OutputSlitSapStatus.Pending)]
    [InlineData(OutputSlitSapStatus.Accepted)]
    [InlineData(OutputSlitSapStatus.Rejected)]
    public void Decide_no_stored_row_inserts(OutputSlitSapStatus observed)
    {
        Assert.Equal(
            OutputSlitSapStatusTransitionKind.InsertNew,
            OutputSlitSapStatusPolicy.Decide(null, observed));
    }

    [Theory]
    [InlineData(OutputSlitSapStatus.Pending)]
    [InlineData(OutputSlitSapStatus.Accepted)]
    [InlineData(OutputSlitSapStatus.Rejected)]
    public void Decide_same_status_is_no_change(OutputSlitSapStatus status)
    {
        Assert.Equal(
            OutputSlitSapStatusTransitionKind.NoChange,
            OutputSlitSapStatusPolicy.Decide(status, status));
    }

    [Theory]
    [InlineData(OutputSlitSapStatus.Pending, OutputSlitSapStatus.Accepted)]
    [InlineData(OutputSlitSapStatus.Pending, OutputSlitSapStatus.Rejected)]
    [InlineData(OutputSlitSapStatus.Rejected, OutputSlitSapStatus.Accepted)]
    public void Decide_regular_transitions(OutputSlitSapStatus current, OutputSlitSapStatus observed)
    {
        Assert.Equal(
            OutputSlitSapStatusTransitionKind.Transition,
            OutputSlitSapStatusPolicy.Decide(current, observed));
    }

    [Fact]
    public void Decide_rejected_back_to_pending_is_resubmit()
    {
        Assert.Equal(
            OutputSlitSapStatusTransitionKind.Resubmit,
            OutputSlitSapStatusPolicy.Decide(OutputSlitSapStatus.Rejected, OutputSlitSapStatus.Pending));
    }

    [Theory]
    // Accepted is frozen: no observation may regress it.
    [InlineData(OutputSlitSapStatus.Pending)]
    [InlineData(OutputSlitSapStatus.Rejected)]
    public void Decide_accepted_is_frozen(OutputSlitSapStatus observed)
    {
        Assert.Equal(
            OutputSlitSapStatusTransitionKind.IgnoreFrozenAccepted,
            OutputSlitSapStatusPolicy.Decide(OutputSlitSapStatus.Accepted, observed));
    }

    [Theory]
    [InlineData(OutputSlitSapStatus.Pending, "Pending")]
    [InlineData(OutputSlitSapStatus.Accepted, "Accepted")]
    [InlineData(OutputSlitSapStatus.Rejected, "Rejected")]
    public void Db_string_round_trips(OutputSlitSapStatus status, string dbValue)
    {
        Assert.Equal(dbValue, OutputSlitSapStatusPolicy.ToDbString(status));
        Assert.Equal(status, OutputSlitSapStatusPolicy.TryParse(dbValue));
        Assert.Equal(status, OutputSlitSapStatusPolicy.TryParse(dbValue.ToUpperInvariant()));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("Unknown")]
    public void TryParse_rejects_unknown_values(string? value)
    {
        Assert.Null(OutputSlitSapStatusPolicy.TryParse(value));
    }
}
