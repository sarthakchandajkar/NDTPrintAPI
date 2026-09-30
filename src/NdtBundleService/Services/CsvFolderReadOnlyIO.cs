namespace NdtBundleService.Services;

/// <summary>
/// Opens folder-source Excel/CSV files for <b>read-only</b> replication into SQL.
/// Never moves, deletes, renames, or writes the source file.
/// </summary>
internal static class CsvFolderReadOnlyIO
{
    /// <summary>
    /// Opens an existing source CSV with <see cref="FileAccess.Read"/> only.
    /// <see cref="FileShare.ReadWrite"/> / <see cref="FileShare.Delete"/> allow other apps
    /// (Excel/SAP) to keep the file open; this process still cannot write the handle.
    /// </summary>
    public static FileStream OpenRead(string filePath) =>
        new(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 4096,
            FileOptions.SequentialScan | FileOptions.Asynchronous);
}
