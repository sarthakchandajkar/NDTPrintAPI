using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Tests;

internal static class TestMillOwnership
{
    public static IMillOwnership Monolith() =>
        new MillOwnership(Options.Create(new InstanceRoleOptions { Mode = InstanceRoleModes.Monolith }));

    public static IMillOwnership Mill(int millNo) =>
        new MillOwnership(Options.Create(new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [millNo],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false
        }));

    public static IMillOwnership Shared() =>
        new MillOwnership(Options.Create(new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Shared,
            OwnedMillNos = [],
            EnableMillWorkers = false,
            EnableDashboardApi = true,
            EnablePoPlanWipImport = true
        }));
}
