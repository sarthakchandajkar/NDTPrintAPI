-- SUPERSEDED by docs/Printer_AddTable.sql.
-- Mill and station ZPL printers now live in one table: dbo.Printer.
-- Manual_Station_Run.Print_Status / Print_Error are also applied by that script.
-- This filename is kept so older checklists fail loudly instead of recreating dbo.Station_Printer.

RAISERROR(
    N'docs/Station_Printer_AddTable.sql is superseded. Run docs/Printer_AddTable.sql (unified dbo.Printer, including Manual_Station_Run print columns).',
    16,
    1);
GO
