using Microsoft.Extensions.Options;

namespace NdtBundleService.Configuration;

/// <summary>Rejects internally inconsistent <see cref="InstanceRoleOptions"/> at startup.</summary>
public sealed class InstanceRoleOptionsValidator : IValidateOptions<InstanceRoleOptions>
{
    public ValidateOptionsResult Validate(string? name, InstanceRoleOptions options)
    {
        if (options is null)
            return ValidateOptionsResult.Fail("InstanceRole options are required.");

        if (options.IsMonolith)
            return ValidateOptionsResult.Success;

        var errors = new List<string>();

        if (options.OwnedMillNos is not null)
        {
            var seen = new HashSet<int>();
            foreach (var mill in options.OwnedMillNos)
            {
                if (mill is < 1 or > 4)
                    errors.Add($"OwnedMillNos contains invalid mill number {mill}; must be 1–4.");
                else if (!seen.Add(mill))
                    errors.Add($"OwnedMillNos contains duplicate mill number {mill}.");
            }
        }

        if (options.IsMill)
        {
            if (options.OwnedMillNos is null || options.OwnedMillNos.Length != 1)
                errors.Add("Mode=Mill requires exactly one entry in OwnedMillNos.");

            if (options.EnableMillWorkers != true)
                errors.Add("Mode=Mill requires EnableMillWorkers=true.");

            if (options.EnableDashboardApi)
                errors.Add("Mode=Mill requires EnableDashboardApi=false.");

            if (options.EnablePoPlanWipImport)
                errors.Add("Mode=Mill requires EnablePoPlanWipImport=false.");
        }
        else if (options.IsShared)
        {
            if (options.OwnedMillNos is { Length: > 0 })
                errors.Add("Mode=Shared requires OwnedMillNos to be empty.");

            if (options.EnableMillWorkers)
                errors.Add("Mode=Shared requires EnableMillWorkers=false.");

            if (!options.EnableDashboardApi)
                errors.Add("Mode=Shared requires EnableDashboardApi=true.");

            if (!options.EnablePoPlanWipImport)
                errors.Add("Mode=Shared requires EnablePoPlanWipImport=true.");
        }
        else
        {
            errors.Add($"InstanceRole:Mode must be Monolith, Shared, or Mill (got '{options.Mode}').");
        }

        if (options.LeaseTtlSeconds is < 5 or > 600)
            errors.Add("LeaseTtlSeconds must be between 5 and 600.");

        if (options.LeaseRenewIntervalSeconds is < 1 or > 300)
            errors.Add("LeaseRenewIntervalSeconds must be between 1 and 300.");

        if (options.LeaseRenewMaxTransientAttempts is < 1 or > 20)
            errors.Add("LeaseRenewMaxTransientAttempts must be between 1 and 20.");

        if (options.LeaseRenewTransientRetryDelaySeconds is < 0 or > 60)
            errors.Add("LeaseRenewTransientRetryDelaySeconds must be between 0 and 60.");

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}
