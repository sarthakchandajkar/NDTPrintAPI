using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class CsvFolderReadOnlyIOTests
{
    [Fact]
    public void OpenRead_is_read_only_and_does_not_change_file()
    {
        var dir = Path.Combine(Path.GetTempPath(), "csv-ro-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "source.csv");
        const string contents = "A,B\n1,2\n";
        File.WriteAllText(path, contents);
        var beforeWrite = File.GetLastWriteTimeUtc(path);
        var beforeBytes = File.ReadAllBytes(path);

        try
        {
            using (var stream = CsvFolderReadOnlyIO.OpenRead(path))
            {
                Assert.True(stream.CanRead);
                Assert.False(stream.CanWrite);
                using var reader = new StreamReader(stream);
                Assert.Equal("A,B", reader.ReadLine());
            }

            Assert.Equal(beforeBytes, File.ReadAllBytes(path));
            Assert.Equal(beforeWrite, File.GetLastWriteTimeUtc(path));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* ignore */ }
        }
    }
}
