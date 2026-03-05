namespace NdtBundleService.Reports;

using System;
using Telerik.Reporting;
using Telerik.Reporting.Processing;

/// <summary>
/// NDT Bundle Tag Report - parameter-driven (no database).
/// Label data is supplied via report parameters; layout matches the Telerik design (100mm x 100mm).
/// </summary>
public partial class Rpt_NDTLabel : Report
{
    public Rpt_NDTLabel()
    {
        InitializeComponent();
    }

    private void Rpt_NDTLabel_NeedDataSource(object sender, EventArgs e)
    {
        var objReport = (Processing.Report)sender;
        var bundleNo = objReport.Parameters["BundleNo"].Value?.ToString() ?? "";
        var specification = objReport.Parameters["Specification"].Value?.ToString() ?? "";
        var type = objReport.Parameters["Type"].Value?.ToString() ?? "";
        var size = objReport.Parameters["Size"].Value?.ToString() ?? "";
        var length = objReport.Parameters["Length"].Value?.ToString() ?? "";
        var pcsBund = objReport.Parameters["PcsBund"].Value?.ToString() ?? "";
        var slitNo = objReport.Parameters["SlitNo"].Value?.ToString() ?? "";
        var isReprint = objReport.Parameters["isReprint"].Value is true;

        textSpecification.Value = specification;
        textType.Value = type;
        textSize.Value = string.IsNullOrEmpty(size) ? "" : size + "''";
        textLen.Value = string.IsNullOrEmpty(length) ? "" : length + "'";
        textPcsBund.Value = pcsBund;
        textBox3.Value = slitNo;
        textBundleNo.Value = bundleNo;
        barcode1.Value = bundleNo;
        barcode2.Value = bundleNo;
        reprintInd.Value = isReprint ? "R" : "";
    }
}
