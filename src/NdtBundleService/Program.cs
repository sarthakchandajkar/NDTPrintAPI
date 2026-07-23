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

// Controllers & Swagger UI for testing
builder.Services.AddControllers().AddJsonOptions(o =>
{
    // Match ndtbundle-dashboard (expects mills, sourcePath, liveMillNdt).
    o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    o.JsonSerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.CamelCase;
});
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
    var defaultLevel = builder.Configuration["Logging:LogLevel:Default"];
    var minLevel = Enum.TryParse<LogEventLevel>(defaultLevel, ignoreCase: true, out var parsed)
        ? parsed
        : LogEventLevel.Information;

    var loggerConfig = new LoggerConfiguration()
        .MinimumLevel.Is(minLevel)
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Information)
        .Enrich.FromLogContext();

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
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}");
    }

    if (OperatingSystem.IsWindows() && fileLogging.WriteToEventLog)
    {
        loggerConfig = loggerConfig.WriteTo.EventLog(
            "NdtBundleService",
            manageEventSource: false,
            restrictedToMinimumLevel: LogEventLevel.Warning);
    }

    Log.Logger = loggerConfig.CreateLogger();
    builder.Host.UseSerilog();
}

