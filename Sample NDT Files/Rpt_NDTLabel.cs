namespace IIOTReport
{
    using System;
    using System.ComponentModel;
    using System.Configuration;
    using Npgsql;
    using System.Drawing;
    using System.Windows.Forms;
    using Telerik.Reporting;
    using Telerik.Reporting.Drawing;

    /// <summary>
    /// Summary description for Rpt_NDTLabel.
    /// NDT Bundle Tag Report - Uses same design as Rpt_MillLabel but for NDT bundles
    /// </summary>
    public partial class Rpt_NDTLabel : Telerik.Reporting.Report
    {
        public Rpt_NDTLabel()
        {
            InitializeComponent();
        }

        private void Rpt_NDTLabel_NeedDataSource(object sender, EventArgs e)
        {
            Telerik.Reporting.Processing.Report objReport = (Telerik.Reporting.Processing.Report)sender;
            string Mill_Line = objReport.Parameters["MillLine"].Value.ToString();
            Int32 PO_Plan_Id = Int32.Parse(objReport.Parameters["MillPOId"].Value.ToString());
            Int32 NDTBundleID = Int32.Parse(objReport.Parameters["NDTBundleID"].Value.ToString());
            bool isReprint = Convert.ToBoolean(objReport.Parameters["isReprint"].Value.ToString());

            var connectionString = "";
            if (ConfigurationManager.AppSettings["DefaultConnection"] != null)
                connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            else
                connectionString = objReport.Parameters["ConnectionString"].Value.ToString();

            NpgsqlConnection sqlcon = new NpgsqlConnection(connectionString);
            NpgsqlCommand sqlcmd = new NpgsqlCommand
            {
                Connection = sqlcon
            };
            sqlcon.Open();
            
            string bundleNo = "";

            //Get PO_Plan details and NDT Bundle information:
            sqlcmd.CommandText = @"SELECT pop.""PO_No"" AS ""PONo"", mb.""Bundle_No"" AS ""BundleNo"", COALESCE(pop.""POSpecification"", '') AS ""POSpecification"",                            
                                COALESCE(pop.""Pipe_Type"", '') AS ""PipeType"", pop.""Pipe_Size"" AS ""PipeSize"", mb.""NDT_Pcs"" AS ""PcsPerBundle"",
                                CASE WHEN mb.""Parent_BundleNo"" IS NOT NULL AND mb.""Parent_BundleNo"" <> mb.""Bundle_No"" THEN COALESCE(mb.""Bundle_Wt"", 0) ELSE pop.""Pipe_Len"" END AS ""PipeLen""
                                FROM ""PO_Plan"" pop
                                LEFT OUTER JOIN ""M" + Mill_Line + "_NDTBundles"" mb ON mb.""PO_Plan_ID"" = pop.""PO_Plan_ID"" " +
                                "WHERE pop.""PO_Plan_ID"" = " + PO_Plan_Id.ToString() + " AND mb.""NDTBundle_ID"" = " + NDTBundleID.ToString();

            using (NpgsqlDataReader rdr = sqlcmd.ExecuteReader())
            {
                while (rdr.Read())
                {
                    bundleNo = rdr["BundleNo"].ToString();
                    this.textSpecification.Value = rdr["POSpecification"].ToString();
                    this.textType.Value = rdr["PipeType"].ToString();
                    this.textSize.Value = rdr["PipeSize"].ToString() + "''";
                    this.textLen.Value = rdr["PipeLen"].ToString() + "'";
                    this.textPcsBund.Value = rdr["PcsPerBundle"].ToString();
                }
                rdr.Close();
            }

            sqlcmd.CommandText = "SELECT ms.""Slit_No"" FROM \"M" + Mill_Line + "_Slit\" ms JOIN \"M" + Mill_Line + "_NDTBundles\" mb ON mb.\"Slit_ID\" = ms.\"Slit_ID\" WHERE mb.\"Bundle_No\" = '" + bundleNo.Replace("'", "''") + "' ORDER BY mb.\"NDTBundle_ID\" LIMIT 1";
            this.textBox3.Value = sqlcmd.ExecuteScalar()?.ToString() ?? "";

            // For NDT bundles, get the NDT pieces count from the bundle itself
            sqlcmd.CommandText = "SELECT COALESCE((SELECT SUM(\"NDT_Pcs\") FROM \"M" + Mill_Line + "_NDTBundles\" WHERE \"Bundle_No\" = '" + bundleNo.Replace("'", "''") + "'), 0) AS \"PcsBundle\"";
            var pcsResult = sqlcmd.ExecuteScalar();
            if (pcsResult != null && pcsResult != DBNull.Value)
            {
                this.textPcsBund.Value = pcsResult.ToString();
            }

            this.textBundleNo.Value = bundleNo;
            this.barcode1.Value = bundleNo;
            this.barcode2.Value = bundleNo;

            if (isReprint)
            {
                sqlcmd.CommandText = "UPDATE \"M" + Mill_Line + "_NDTBundles\" SET \"LastReprintDttm\" = CURRENT_TIMESTAMP WHERE \"Bundle_No\" = '" + bundleNo.Replace("'", "''") + "'";
                sqlcmd.ExecuteNonQuery();
                this.reprintInd.Value = "R";
            }
            else
                this.reprintInd.Value = "";

            sqlcmd.CommandText = "UPDATE \"M" + Mill_Line + @"_NDTBundles"" SET ""Status"" = CASE WHEN ""Status"" < 3 THEN 3 ELSE ""Status"" END WHERE ""Bundle_No"" = '" + bundleNo.Replace("'", "''") + "'";
            sqlcmd.ExecuteNonQuery();

            sqlcon.Close();
            sqlcon.Dispose();

        }
    }
}

