using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class InstanceRoleOptionsValidatorTests
{
    private readonly InstanceRoleOptionsValidator _sut = new();

    [Fact]
    public void Monolith_always_succeeds()
    {
        var result = _sut.Validate(null, new InstanceRoleOptions { Mode = InstanceRoleModes.Monolith });
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Mill_requires_exactly_one_owned_mill_and_strict_flags()
    {
        var ok = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [1],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false
        };
        Assert.True(_sut.Validate(null, ok).Succeeded);

        var bad = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [1, 2],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false
        };
        Assert.False(_sut.Validate(null, bad).Succeeded);
        Assert.Contains(_sut.Validate(null, bad).Failures!, f => f.Contains("exactly one", StringComparison.Ordinal));
    }

    [Fact]
    public void Mill_rejects_dashboard_or_import_enabled()
    {
        var options = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [2],
            EnableMillWorkers = true,
            EnableDashboardApi = true,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false
        };
        var result = _sut.Validate(null, options);
        Assert.False(result.Succeeded);
        Assert.Contains(result.Failures!, f => f.Contains("EnableDashboardApi=false", StringComparison.Ordinal));
    }

    [Fact]
    public void Shared_requires_inverse_flags_and_empty_owned()
    {
        var ok = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Shared,
            OwnedMillNos = [],
            EnableMillWorkers = false,
            EnableDashboardApi = true,
            EnablePoPlanWipImport = true,
            EnableUploadScheduler = true
        };
        Assert.True(_sut.Validate(null, ok).Succeeded);

        var withMill = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Shared,
            OwnedMillNos = [1],
            EnableMillWorkers = false,
            EnableDashboardApi = true,
            EnablePoPlanWipImport = true,
            EnableUploadScheduler = true
        };
        Assert.False(_sut.Validate(null, withMill).Succeeded);
    }

    [Fact]
    public void Rejects_duplicate_and_out_of_range_mills()
    {
        var dup = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [1, 1],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false
        };
        Assert.False(_sut.Validate(null, dup).Succeeded);

        var badRange = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [5],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false
        };
        Assert.False(_sut.Validate(null, badRange).Succeeded);
    }
}
