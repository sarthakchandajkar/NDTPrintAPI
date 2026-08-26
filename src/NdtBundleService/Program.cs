using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.DependencyInjection;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using QuestPDF.Infrastructure;
using Serilog;
using Serilog.Events;

QuestPDF.Settings.License = LicenseType.Community;
QuestPDF.Settings.EnableDebugging = true; // Better error location when layout constraints conflict

// Production hosting loads appsettings.Production.json (e.g. Z:\ Input Slit) even when debugging from the repo.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    EnvironmentName = Environments.Production
});

ConfigureSerilog(builder);

builder.Host.UseDefaultServiceProvider(options =>
{
    options.ValidateOnBuild = true;
    options.ValidateScopes = true;
});

builder.Services.AddWindowsService();

builder.Services.AddNdtBundleServices(builder.Configuration);

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

var instanceRole = builder.Configuration.GetSection(InstanceRoleOptions.SectionName).Get<InstanceRoleOptions>()
                   ?? new InstanceRoleOptions();

// Controllers & Swagger UI for testing
builder.Services.AddControllers()
    .AddJsonOptions(o =>
    {
        // Match ndtbundle-dashboard (expects mills, sourcePath, liveMillNdt).
        o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        o.JsonSerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.CamelCase;
    })
    .AddMvcOptions(o => o.Conventions.Add(new InstanceRoleControllerConvention(instanceRole)));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

try
{
    var app = builder.Build();

    if (app.Environment.IsDevelopment() || app.Configuration.GetValue<bool>("ShowSwagger"))
    {
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.UseCors();

    app.MapControllers();

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "NdtBundleService terminated unexpectedly.");
    throw;
}
finally
{
    Log.CloseAndFlush();
}

static void ConfigureSerilog(WebApplicationBuilder builder)
{
    var fileLogging = builder.Configuration.GetSection("Logging:File").Get<FileLoggingOptions>() ?? new FileLoggingOptions();
    var role = builder.Configuration.GetSection(InstanceRoleOptions.SectionName).Get<InstanceRoleOptions>()
               ?? new InstanceRoleOptions();
    var defaultLevel = builder.Configuration["Logging:LogLevel:Default"];
    var minLevel = Enum.TryParse<LogEventLevel>(defaultLevel, ignoreCase: true, out var parsed)
        ? parsed
        : LogEventLevel.Information;

    var ownedMill = role.OwnedMillNos.Length == 1 ? role.OwnedMillNos[0].ToString() : "-";
    var displayName = role.ResolveDisplayName();
    var outputTemplate =
        "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [{InstanceRole}/{OwnedMill}] {SourceContext}: {Message:lj}{NewLine}{Exception}";

    var loggerConfig = new LoggerConfiguration()
        .MinimumLevel.Is(minLevel)
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Information)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("InstanceRole", role.Mode)
        .Enrich.WithProperty("OwnedMill", ownedMill)
        .Enrich.WithProperty("InstanceDisplayName", displayName);

    if (fileLogging.Enabled)
    {
        var logFolder = string.IsNullOrWhiteSpace(fileLogging.Folder)
            ? Path.Combine(builder.Environment.ContentRootPath, "Logs")
            : fileLogging.Folder.Trim();
        Directory.CreateDirectory(logFolder);

        var prefix = string.IsNullOrWhiteSpace(fileLogging.FileNamePrefix) ? "ndtbundle" : fileLogging.FileNamePrefix.Trim();
        var logPath = Path.Combine(logFolder, $"{prefix}-.log");
        var retain = fileLogging.RetainFileCount > 0 ? fileLogging.RetainFileCount : 31;

        loggerConfig = loggerConfig.WriteTo.File(
            logPath,
            rollingInterval: RollingInterval.Day,
            retainedFileCountLimit: retain,
            shared: true,
            outputTemplate: outputTemplate);
    }

    if (OperatingSystem.IsWindows() && fileLogging.WriteToEventLog)
    {
        loggerConfig = loggerConfig.WriteTo.EventLog(
            role.IsMonolith ? "NdtBundleService" : $"NdtBundleService-{displayName}",
            manageEventSource: false,
            restrictedToMinimumLevel: LogEventLevel.Warning,
            outputTemplate: outputTemplate);
    }

    Log.Logger = loggerConfig.CreateLogger();
    builder.Host.UseSerilog();
}
