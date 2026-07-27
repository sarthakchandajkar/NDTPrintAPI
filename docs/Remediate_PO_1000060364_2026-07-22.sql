-- Data remediation for Mill-1 PO 1000060364 (2026-07-22 production incident)
-- Run in JazeeraMES_Prod after deploying FIFO recon + null-selector fix.
-- Review each section before executing in production.

SET NOCOUNT ON;

DECLARE @Po NVARCHAR(32) = N'1000060364';
DECLARE @Mill INT = 1;

-- 1) Re-open CSV recon for PLC-closed bundles 1–5 (adjust bundle numbers if your sequence differs)
UPDATE dbo.NDT_Bundle
SET Awaiting_Csv_Recon = 1,
    Count_Discrepancy = 0,
    Manual_Review = 0
WHERE PO_Number = @Po
  AND Mill_No = @Mill
  AND Bundle_No IN (
      N'1226100001', N'1226100002', N'1226100003', N'1226100004', N'1226100005'
  )
  AND Close_Source = N'Plc';

-- 2) Flag phantom partial bundle 1226100007 for void / manual review
UPDATE dbo.NDT_Bundle
SET Manual_Review = 1,
    Print_Status = N'Void'
WHERE Bundle_No = N'1226100007'
  AND PO_Number = @Po
  AND Mill_No = @Mill;

-- 3) Allow re-ingest of the three NRE-failed Input Slit files (delete trace rows for this file version)
--    After running, touch or re-drop the inbox files so SlitMonitoringWorker reprocesses them.
DELETE FROM dbo.Output_Slit_Row
WHERE Source_File IN (
    N'2604345_03_260722_1000060364.csv',
    N'2604352_02_260722_1000060364.csv',
    N'2604352_04_260722_1000060364.csv'
);

DELETE FROM dbo.Input_Slit_Row
WHERE Source_File IN (
    N'2604345_03_260722_1000060364.csv',
    N'2604352_02_260722_1000060364.csv',
    N'2604352_04_260722_1000060364.csv'
);

-- If Input_Slit_File_Seen exists, clear seen markers so F-5 reconcile re-queues:
IF OBJECT_ID(N'dbo.Input_Slit_File_Seen', N'U') IS NOT NULL
BEGIN
    DELETE FROM dbo.Input_Slit_File_Seen
    WHERE File_Path LIKE N'%2604345_03_260722_1000060364%'
       OR File_Path LIKE N'%2604352_02_260722_1000060364%'
       OR File_Path LIKE N'%2604352_04_260722_1000060364%';
END

-- 4) Verification: awaiting bundles oldest-first with slit sums from Output_Slit_Row
SELECT b.Bundle_No,
       b.Total_NDT_Pcs AS PlcTotal,
       b.Awaiting_Csv_Recon,
       b.Count_Discrepancy,
       b.Manual_Review,
       b.PrintedAt,
       ISNULL(s.SlitSum, 0) AS SlitSum
FROM dbo.NDT_Bundle b
OUTER APPLY (
    SELECT SUM(osr.NDT_Pipes) AS SlitSum
    FROM dbo.Output_Slit_Row osr
    WHERE osr.NDT_Batch_No = b.Bundle_No
) s
WHERE b.PO_Number = @Po
  AND b.Mill_No = @Mill
  AND b.Bundle_No IN (
      N'1226100001', N'1226100002', N'1226100003', N'1226100004',
      N'1226100005', N'1226100006', N'1226100007'
  )
ORDER BY b.PrintedAt ASC;
