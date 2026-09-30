using System.Text.Json;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class UploadLookupCsvParserTests
{
    [Fact]
    public async Task SlitAcceptedCsvParser_writes_audit_and_lookup_rows()
    {
        var dir = Path.Combine(Path.GetTempPath(), "slit-acc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "slit.csv");
            await File.WriteAllLinesAsync(path,
            [
                "PO Number,Slit No,Slit Width,Slit Thick,NSS,Extra Col",
                "1000062356,2605848_03,122.5,2.60,Non-NSS,KEEP_ME"
            ]);

            var parsed = await SlitAcceptedCsvParser.ParseFileAsync(path, CancellationToken.None);
            Assert.Single(parsed.LookupRows);
            Assert.Equal("2605848_03", parsed.LookupRows[0].SlitNo);
            Assert.Equal("122.5", parsed.LookupRows[0].SlitWidth);

            Assert.Single(parsed.AuditRows);
            Assert.Equal(2, parsed.AuditRows[0].SourceRowNumber);
            Assert.Contains("Extra Col", parsed.AuditRows[0].HeaderLine, StringComparison.Ordinal);
            Assert.Contains("KEEP_ME", parsed.AuditRows[0].RowLine, StringComparison.Ordinal);

            var map = JsonSerializer.Deserialize<Dictionary<string, string>>(parsed.AuditRows[0].ColumnsJson)!;
            Assert.Equal("KEEP_ME", map["Extra Col"]);
            Assert.Equal("122.5", map["Slit Width"]);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* ignore */ }
        }
    }

    [Fact]
    public async Task FgBundleCsvParser_writes_audit_with_all_excel_columns()
    {
        var dir = Path.Combine(Path.GetTempPath(), "fg-acc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "FG_02_1000062356_2602021514_260930_135714.csv");
            await File.WriteAllLinesAsync(path,
            [
                "PO Number,Mill Line No,Pipe Grade,Pipe Size,Pipe Thickness,Pipe Length,Pipe Wt/mtr,Pipe Type,Act Pcs. in Bundle,Bundle No,Bundle Status,Slit 1 Num,Slit 1 OK Pcs,Slit 2 Num,Slit 2 OK Pcs,Slit 3 Num,Slit 3 OK Pcs,Slit 4 Num,Slit 4 OK Pcs,Bundle Wt.,Bundle Start,Bundle End,Operator Done,Non Standard Slit,PO_Specification",
                "1000062356,2,A,80 X 40,2.60,6.00,4.55,FG,16,2602021514,Partial,2605708_01,16,,,,,,2605708,0.44,30.09.2026 13:52:18,30.09.2026 13:54:51,30.09.2026 13:57:14,Non-NSS,RHS 2.80MM ASTM A500 GR A"
            ]);

            var parsed = await FgBundleCsvParser.ParseFileAsync(path, CancellationToken.None);
            Assert.Single(parsed.LookupRows);
            Assert.Equal("A", parsed.LookupRows[0].PipeGrade);
            Assert.Equal("2605708_01", parsed.LookupRows[0].Slit1Num);

            Assert.Single(parsed.AuditRows);
            var map = JsonSerializer.Deserialize<Dictionary<string, string>>(parsed.AuditRows[0].ColumnsJson)!;
            Assert.Equal("A", map["Pipe Grade"]);
            Assert.Equal("Partial", map["Bundle Status"]);
            Assert.Equal("RHS 2.80MM ASTM A500 GR A", map["PO_Specification"]);
            Assert.Equal(25, map.Count);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* ignore */ }
        }
    }
}
