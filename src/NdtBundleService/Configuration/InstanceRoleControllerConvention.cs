using Microsoft.AspNetCore.Mvc.ApplicationModels;

namespace NdtBundleService.Configuration;

/// <summary>Removes action selectors for controllers excluded by the current <see cref="InstanceRoleOptions.Mode"/>.</summary>
public sealed class InstanceRoleControllerConvention : IControllerModelConvention
{
    private readonly InstanceRoleOptions _role;

    public InstanceRoleControllerConvention(InstanceRoleOptions role)
    {
        _role = role;
    }

    public void Apply(ControllerModel controller)
    {
        if (_role.IsMonolith)
            return;

        var attr = controller.Attributes.OfType<InstanceRoleAttribute>().FirstOrDefault();
        if (attr is null)
            return;

        if (!attr.AllowedModes.Any(m => string.Equals(m, _role.Mode, StringComparison.OrdinalIgnoreCase)))
            controller.Selectors.Clear();
    }
}
