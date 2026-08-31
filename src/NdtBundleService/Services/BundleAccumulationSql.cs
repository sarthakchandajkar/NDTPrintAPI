using Microsoft.Data.SqlClient;

namespace NdtBundleService.Services;

/// <summary>
/// SQL for <c>dbo.Bundle_Accumulation</c>. Increment is a single MERGE (HOLDLOCK) so two
/// concurrent +delta writes cannot lose an update. Zero pcs never persist (CHECK + DELETE).
/// </summary>
public static class BundleAccumulationSql
{
    public const string IncrementMerge = @"
MERGE dbo.Bundle_Accumulation WITH (HOLDLOCK) AS t
USING (SELECT @Mill AS Mill_No, @Po AS Po_Number, @Size AS Size_Key) AS s
ON t.Mill_No = s.Mill_No AND t.Po_Number = s.Po_Number AND t.Size_Key = s.Size_Key
WHEN MATCHED AND t.Pcs + @Delta > 0 THEN
    UPDATE SET Pcs = t.Pcs + @Delta, Last_Activity_Utc = SYSUTCDATETIME()
WHEN MATCHED AND t.Pcs + @Delta <= 0 THEN
    DELETE
WHEN NOT MATCHED BY TARGET AND @Delta > 0 THEN
    INSERT (Mill_No, Po_Number, Size_Key, Pcs, Last_Activity_Utc)
    VALUES (@Mill, @Po, @Size, @Delta, SYSUTCDATETIME());";

    public const string AbsoluteMerge = @"
MERGE dbo.Bundle_Accumulation WITH (HOLDLOCK) AS t
USING (SELECT @Mill AS Mill_No, @Po AS Po_Number, @Size AS Size_Key) AS s
ON t.Mill_No = s.Mill_No AND t.Po_Number = s.Po_Number AND t.Size_Key = s.Size_Key
WHEN MATCHED AND @Pcs > 0 THEN
    UPDATE SET Pcs = @Pcs, Last_Activity_Utc = SYSUTCDATETIME()
WHEN MATCHED AND @Pcs <= 0 THEN
    DELETE
WHEN NOT MATCHED BY TARGET AND @Pcs > 0 THEN
    INSERT (Mill_No, Po_Number, Size_Key, Pcs, Last_Activity_Utc)
    VALUES (@Mill, @Po, @Size, @Pcs, SYSUTCDATETIME());";

    public const string SelectSizes = @"
SELECT Size_Key, Pcs, Last_Activity_Utc
FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public const string SelectOpenForMill = @"
SELECT Po_Number, Size_Key, Pcs, Last_Activity_Utc
FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill;";

    public const string ExistsOpenForMill = @"
SELECT TOP (1) 1
FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill;";

    public const string ExistsOpenAny = @"
SELECT TOP (1) 1
FROM dbo.Bundle_Accumulation;";

    public const string MaxActivityForPo = @"
SELECT MAX(Last_Activity_Utc)
FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public const string DeleteSize = @"
DELETE FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill AND Po_Number = @Po AND Size_Key = @Size;";

    public const string DeleteAllSizesForPo = @"
DELETE FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public const string RemainingSizeCount = @"
SELECT COUNT(1)
FROM dbo.Bundle_Accumulation
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public const string UpsertContext = @"
MERGE dbo.Bundle_Accumulation_Context WITH (HOLDLOCK) AS t
USING (SELECT @Mill AS Mill_No, @Po AS Po_Number) AS s
ON t.Mill_No = s.Mill_No AND t.Po_Number = s.Po_Number
WHEN MATCHED THEN UPDATE SET
    Slit_No = @SlitNo,
    Rejected_Pipes = @Rejected,
    Slit_Start_Time = @Start,
    Slit_Finish_Time = @Finish,
    Ndt_Short_Length_Pipe = @NdtShort,
    Rejected_Short_Length_Pipe = @RejShort,
    Last_Activity_Utc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (Mill_No, Po_Number, Slit_No, Rejected_Pipes, Slit_Start_Time, Slit_Finish_Time,
     Ndt_Short_Length_Pipe, Rejected_Short_Length_Pipe, Last_Activity_Utc)
VALUES
    (@Mill, @Po, @SlitNo, @Rejected, @Start, @Finish, @NdtShort, @RejShort, SYSUTCDATETIME());";

    public const string SelectContext = @"
SELECT Slit_No, Rejected_Pipes, Slit_Start_Time, Slit_Finish_Time,
       Ndt_Short_Length_Pipe, Rejected_Short_Length_Pipe, Last_Activity_Utc
FROM dbo.Bundle_Accumulation_Context
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public const string DeleteContext = @"
DELETE FROM dbo.Bundle_Accumulation_Context
WHERE Mill_No = @Mill AND Po_Number = @Po;";

    public static void AddMillPo(SqlCommand cmd, int millNo, string poNumber)
    {
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@Po", poNumber);
    }
}
