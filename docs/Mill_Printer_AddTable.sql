-- SUPERSEDED by docs/Printer_AddTable.sql.
-- Mill and station ZPL printers now live in one table: dbo.Printer.
-- This filename is kept so older checklists fail loudly instead of recreating dbo.Mill_Printer.

RAISERROR(
    N'docs/Mill_Printer_AddTable.sql is superseded. Run docs/Printer_AddTable.sql (unified dbo.Printer for mills 1–4 and station printers).',
    16,
    1);
GO
