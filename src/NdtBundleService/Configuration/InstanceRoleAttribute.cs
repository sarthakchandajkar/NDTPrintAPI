namespace NdtBundleService.Configuration;

/// <summary>Limits a controller to specific <see cref="InstanceRoleModes"/> values.</summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class InstanceRoleAttribute : Attribute
{
    public InstanceRoleAttribute(params string[] allowedModes)
    {
        AllowedModes = allowedModes ?? Array.Empty<string>();
    }

    public IReadOnlyList<string> AllowedModes { get; }
}
