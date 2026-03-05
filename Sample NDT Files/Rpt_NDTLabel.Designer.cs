using System;
using System.Configuration;
using Npgsql;

namespace IIOTReport
{
    partial class Rpt_NDTLabel
    {
        #region Component Designer generated code
        /// <summary>
        /// Required method for telerik Reporting designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Rpt_NDTLabel));
            Telerik.Reporting.Barcodes.Code128Encoder code128Encoder1 = new Telerik.Reporting.Barcodes.Code128Encoder();
            Telerik.Reporting.Barcodes.Code128Encoder code128Encoder2 = new Telerik.Reporting.Barcodes.Code128Encoder();
            Telerik.Reporting.ReportParameter reportParameter1 = new Telerik.Reporting.ReportParameter();
            Telerik.Reporting.ReportParameter reportParameter2 = new Telerik.Reporting.ReportParameter();
            Telerik.Reporting.ReportParameter reportParameter3 = new Telerik.Reporting.ReportParameter();
            Telerik.Reporting.ReportParameter reportParameter4 = new Telerik.Reporting.ReportParameter();
            Telerik.Reporting.ReportParameter reportParameter5 = new Telerik.Reporting.ReportParameter();
            Telerik.Reporting.Drawing.StyleRule styleRule1 = new Telerik.Reporting.Drawing.StyleRule();
            this.detail = new Telerik.Reporting.DetailSection();
            this.panel1 = new Telerik.Reporting.Panel();
            this.textLen = new Telerik.Reporting.TextBox();
            this.textType = new Telerik.Reporting.TextBox();
            this.textSize = new Telerik.Reporting.TextBox();
            this.textPcsBund = new Telerik.Reporting.TextBox();
            this.textBundleNo = new Telerik.Reporting.TextBox();
            this.textBox5 = new Telerik.Reporting.TextBox();
            this.PictureBox1 = new Telerik.Reporting.PictureBox();
            this.textBox6 = new Telerik.Reporting.TextBox();
            this.textSpecification = new Telerik.Reporting.TextBox();
            this.textBox7 = new Telerik.Reporting.TextBox();
            this.textBox8 = new Telerik.Reporting.TextBox();
            this.textBox1 = new Telerik.Reporting.TextBox();
            this.textBox2 = new Telerik.Reporting.TextBox();
            this.textBox9 = new Telerik.Reporting.TextBox();
            this.textBox3 = new Telerik.Reporting.TextBox();
            this.textBox10 = new Telerik.Reporting.TextBox();
            this.barcode2 = new Telerik.Reporting.Barcode();
            this.barcode1 = new Telerik.Reporting.Barcode();
            this.textBox4 = new Telerik.Reporting.TextBox();
            this.reprintInd = new Telerik.Reporting.TextBox();
            this.eventLog1 = new System.Diagnostics.EventLog();
            ((System.ComponentModel.ISupportInitialize)(this.eventLog1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this)).BeginInit();
            // 
            // detail
            // 
            this.detail.Height = Telerik.Reporting.Drawing.Unit.Cm(9.6999998092651367D);
            this.detail.Items.AddRange(new Telerik.Reporting.ReportItemBase[] {
            this.panel1});
            this.detail.Name = "detail";
            this.detail.Style.Padding.Top = Telerik.Reporting.Drawing.Unit.Cm(0D);
            // 
            // panel1
            // 
            this.panel1.Items.AddRange(new Telerik.Reporting.ReportItemBase[] {
            this.textLen,
            this.textType,
            this.textSize,
            this.textPcsBund,
            this.textBundleNo,
            this.textBox5,
            this.PictureBox1,
            this.textBox6,
            this.textSpecification,
            this.textBox7,
            this.textBox8,
            this.textBox1,
            this.textBox2,
            this.textBox9,
            this.textBox3,
            this.textBox10,
            this.barcode2,
            this.barcode1,
            this.textBox4,
            this.reprintInd});
            this.panel1.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Mm(2D), Telerik.Reporting.Drawing.Unit.Mm(1.9999996423721314D));
            this.panel1.Name = "panel1";
            this.panel1.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Cm(9.6999998092651367D), Telerik.Reporting.Drawing.Unit.Cm(9.5D));
            this.panel1.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            // 
            // textLen
            // 
            this.textLen.Format = "{0}";
            this.textLen.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(4.80019998550415D), Telerik.Reporting.Drawing.Unit.Cm(3.5D));
            this.textLen.Name = "textLen";
            this.textLen.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1810239553451538D), Telerik.Reporting.Drawing.Unit.Inch(0.3487401008605957D));
            this.textLen.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textLen.Style.Font.Bold = false;
            this.textLen.Style.Font.Name = "Microsoft New Tai Lue";
            this.textLen.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textLen.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textLen.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textLen.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textLen.StyleName = "";
            this.textLen.Value = "";
            // 
            // textType
            // 
            this.textType.Format = "{0}";
            this.textType.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0.00020132276404183358D), Telerik.Reporting.Drawing.Unit.Cm(3.5D));
            this.textType.Name = "textType";
            this.textType.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1022830009460449D), Telerik.Reporting.Drawing.Unit.Inch(0.34874001145362854D));
            this.textType.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textType.Style.Font.Bold = false;
            this.textType.Style.Font.Name = "Microsoft New Tai Lue";
            this.textType.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textType.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textType.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textType.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textType.StyleName = "";
            this.textType.Value = "";
            // 
            // textSize
            // 
            this.textSize.Format = "{0}";
            this.textSize.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(2.8001999855041504D), Telerik.Reporting.Drawing.Unit.Cm(3.5D));
            this.textSize.Name = "textSize";
            this.textSize.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.7873227596282959D), Telerik.Reporting.Drawing.Unit.Inch(0.3487401008605957D));
            this.textSize.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textSize.Style.Font.Bold = false;
            this.textSize.Style.Font.Name = "Microsoft New Tai Lue";
            this.textSize.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textSize.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textSize.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textSize.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textSize.StyleName = "";
            this.textSize.Value = "";
            // 
            // textPcsBund
            // 
            this.textPcsBund.Format = "{0}";
            this.textPcsBund.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(7.8002004623413086D), Telerik.Reporting.Drawing.Unit.Cm(3.5D));
            this.textPcsBund.Name = "textPcsBund";
            this.textPcsBund.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.74791306257247925D), Telerik.Reporting.Drawing.Unit.Inch(0.3487401008605957D));
            this.textPcsBund.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textPcsBund.Style.Font.Bold = false;
            this.textPcsBund.Style.Font.Name = "Microsoft New Tai Lue";
            this.textPcsBund.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textPcsBund.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textPcsBund.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textPcsBund.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textPcsBund.StyleName = "";
            this.textPcsBund.Value = "";
            // 
            // textBundleNo
            // 
            this.textBundleNo.Format = "{0}";
            this.textBundleNo.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0D), Telerik.Reporting.Drawing.Unit.Cm(6.9999995231628418D));
            this.textBundleNo.Name = "textBundleNo";
            this.textBundleNo.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.2992126941680908D), Telerik.Reporting.Drawing.Unit.Inch(0.31496083736419678D));
            this.textBundleNo.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.None;
            this.textBundleNo.Style.Font.Bold = false;
            this.textBundleNo.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBundleNo.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textBundleNo.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBundleNo.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBundleNo.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBundleNo.StyleName = "";
            this.textBundleNo.Value = "";
            // 
            // textBox5
            // 
            this.textBox5.Format = "{0}";
            this.textBox5.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(4.80019998550415D), Telerik.Reporting.Drawing.Unit.Cm(-1.0513596659933455E-09D));
            this.textBox5.Name = "textBox5";
            this.textBox5.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1810239553451538D), Telerik.Reporting.Drawing.Unit.Inch(0.70858275890350342D));
            this.textBox5.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox5.Style.Font.Bold = true;
            this.textBox5.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox5.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(11D);
            this.textBox5.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox5.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Center;
            this.textBox5.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox5.StyleName = "";
            this.textBox5.Value = "AJSPC - OMAN";
            // 
            // PictureBox1
            // 
            this.PictureBox1.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Inch(3.0708663463592529D), Telerik.Reporting.Drawing.Unit.Inch(0D));
            this.PictureBox1.MimeType = "image/png";
            this.PictureBox1.Name = "PictureBox1";
            this.PictureBox1.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.74799174070358276D), Telerik.Reporting.Drawing.Unit.Inch(0.70858275890350342D));
            this.PictureBox1.Sizing = Telerik.Reporting.Drawing.ImageSizeMode.ScaleProportional;
            this.PictureBox1.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.PictureBox1.Style.Font.Name = "Arial";
            this.PictureBox1.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.PictureBox1.Value = ((object)(resources.GetObject("PictureBox1.Value")));
            // 
            // textBox6
            // 
            this.textBox6.Format = "{0}";
            this.textBox6.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0D), Telerik.Reporting.Drawing.Unit.Cm(1.8000000715255737D));
            this.textBox6.Name = "textBox6";
            this.textBox6.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1023622751235962D), Telerik.Reporting.Drawing.Unit.Inch(0.39362183213233948D));
            this.textBox6.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox6.Style.Font.Bold = true;
            this.textBox6.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox6.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textBox6.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox6.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox6.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox6.StyleName = "";
            this.textBox6.Value = "SPECIFICATION";
            // 
            // textSpecification
            // 
            this.textSpecification.Format = "{0}";
            this.textSpecification.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(2.8001999855041504D), Telerik.Reporting.Drawing.Unit.Cm(1.8000003099441528D));
            this.textSpecification.Name = "textSpecification";
            this.textSpecification.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(2.7164173126220703D), Telerik.Reporting.Drawing.Unit.Inch(0.39362174272537231D));
            this.textSpecification.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textSpecification.Style.Font.Bold = false;
            this.textSpecification.Style.Font.Name = "Microsoft New Tai Lue";
            this.textSpecification.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(11D);
            this.textSpecification.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textSpecification.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Center;
            this.textSpecification.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textSpecification.StyleName = "";
            this.textSpecification.Value = "";
            // 
            // textBox7
            // 
            this.textBox7.Format = "{0}";
            this.textBox7.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0D), Telerik.Reporting.Drawing.Unit.Cm(2.7999999523162842D));
            this.textBox7.Name = "textBox7";
            this.textBox7.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1023622751235962D), Telerik.Reporting.Drawing.Unit.Inch(0.27551168203353882D));
            this.textBox7.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox7.Style.Font.Bold = true;
            this.textBox7.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox7.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(10D);
            this.textBox7.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox7.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox7.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox7.StyleName = "";
            this.textBox7.Value = "Type";
            // 
            // textBox8
            // 
            this.textBox8.Format = "{0}";
            this.textBox8.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0.00020132276404183358D), Telerik.Reporting.Drawing.Unit.Cm(4.3860001564025879D));
            this.textBox8.Name = "textBox8";
            this.textBox8.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.29913330078125D), Telerik.Reporting.Drawing.Unit.Inch(0.35976362228393555D));
            this.textBox8.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.None;
            this.textBox8.Style.Font.Bold = true;
            this.textBox8.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox8.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textBox8.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox8.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox8.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox8.StyleName = "";
            this.textBox8.Value = "SLIT NUMBER";
            // 
            // textBox1
            // 
            this.textBox1.Format = "{0}";
            this.textBox1.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(2.8002004623413086D), Telerik.Reporting.Drawing.Unit.Cm(2.7999997138977051D));
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.7873225212097168D), Telerik.Reporting.Drawing.Unit.Inch(0.27551159262657166D));
            this.textBox1.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox1.Style.Font.Bold = true;
            this.textBox1.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox1.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(10D);
            this.textBox1.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox1.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox1.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox1.StyleName = "";
            this.textBox1.Value = "Size";
            // 
            // textBox2
            // 
            this.textBox2.Format = "{0}";
            this.textBox2.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(4.80019998550415D), Telerik.Reporting.Drawing.Unit.Cm(2.7999997138977051D));
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.1810238361358643D), Telerik.Reporting.Drawing.Unit.Inch(0.27551159262657166D));
            this.textBox2.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox2.Style.Font.Bold = true;
            this.textBox2.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox2.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(10D);
            this.textBox2.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox2.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox2.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox2.StyleName = "";
            this.textBox2.Value = "Length";
            // 
            // textBox9
            // 
            this.textBox9.Format = "{0}";
            this.textBox9.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(7.8002004623413086D), Telerik.Reporting.Drawing.Unit.Cm(2.7999997138977051D));
            this.textBox9.Name = "textBox9";
            this.textBox9.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.74791288375854492D), Telerik.Reporting.Drawing.Unit.Inch(0.27551159262657166D));
            this.textBox9.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox9.Style.Font.Bold = true;
            this.textBox9.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox9.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(10D);
            this.textBox9.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox9.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox9.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox9.StyleName = "";
            this.textBox9.Value = "Pcs/Bnd";
            // 
            // textBox3
            // 
            this.textBox3.Format = "{0}";
            this.textBox3.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0.00020145734015386552D), Telerik.Reporting.Drawing.Unit.Cm(5.2999997138977051D));
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.29913330078125D), Telerik.Reporting.Drawing.Unit.Inch(0.31496062874794006D));
            this.textBox3.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.None;
            this.textBox3.Style.Font.Bold = false;
            this.textBox3.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox3.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textBox3.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox3.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox3.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox3.StyleName = "";
            this.textBox3.Value = "";
            // 
            // textBox10
            // 
            this.textBox10.Format = "{0}";
            this.textBox10.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0.00020145734015386552D), Telerik.Reporting.Drawing.Unit.Cm(6.1002001762390137D));
            this.textBox10.Name = "textBox10";
            this.textBox10.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(1.29913330078125D), Telerik.Reporting.Drawing.Unit.Inch(0.354172945022583D));
            this.textBox10.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.None;
            this.textBox10.Style.Font.Bold = true;
            this.textBox10.Style.Font.Name = "Microsoft New Tai Lue";
            this.textBox10.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(9D);
            this.textBox10.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox10.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Left;
            this.textBox10.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox10.StyleName = "";
            this.textBox10.Value = "BUNDLE NUMBER";
            // 
            // barcode2
            // 
            this.barcode2.Angle = 90D;
            this.barcode2.Encoder = code128Encoder1;
            this.barcode2.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Mm(78.002006530761719D), Telerik.Reporting.Drawing.Unit.Mm(53D));
            this.barcode2.Name = "barcode2";
            this.barcode2.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Mm(18.996984481811523D), Telerik.Reporting.Drawing.Unit.Mm(41.998008728027344D));
            this.barcode2.Stretch = false;
            this.barcode2.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.barcode2.Style.Font.Name = "Calibri";
            this.barcode2.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(12D);
            this.barcode2.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.barcode2.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Center;
            this.barcode2.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Bottom;
            // 
            // barcode1
            // 
            this.barcode1.Encoder = code128Encoder2;
            this.barcode1.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Mm(0D), Telerik.Reporting.Drawing.Unit.Mm(0D));
            this.barcode1.Name = "barcode1";
            this.barcode1.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Mm(48D), Telerik.Reporting.Drawing.Unit.Mm(17.997997283935547D));
            this.barcode1.Stretch = false;
            this.barcode1.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.barcode1.Style.Font.Name = "Calibri";
            this.barcode1.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(12D);
            this.barcode1.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.barcode1.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Center;
            this.barcode1.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Bottom;
            // 
            // textBox4
            // 
            this.textBox4.Format = "{0}";
            this.textBox4.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0.00020145734015386552D), Telerik.Reporting.Drawing.Unit.Cm(8.5001020431518555D));
            this.textBox4.Name = "textBox4";
            this.textBox4.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(3.070786714553833D), Telerik.Reporting.Drawing.Unit.Inch(0.39362174272537231D));
            this.textBox4.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.Solid;
            this.textBox4.Style.Font.Bold = true;
            this.textBox4.Style.Font.Name = "Malgun Gothic";
            this.textBox4.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(12D);
            this.textBox4.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.textBox4.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Center;
            this.textBox4.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Middle;
            this.textBox4.StyleName = "";
            this.textBox4.Value = "MADE IN OMAN";
            // 
            // reprintInd
            // 
            this.reprintInd.Format = "{0}";
            this.reprintInd.Location = new Telerik.Reporting.Drawing.PointU(Telerik.Reporting.Drawing.Unit.Cm(0D), Telerik.Reporting.Drawing.Unit.Cm(8.5001020431518555D));
            this.reprintInd.Name = "reprintInd";
            this.reprintInd.Size = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Inch(0.39370083808898926D), Telerik.Reporting.Drawing.Unit.Inch(0.39362180233001709D));
            this.reprintInd.Style.BorderStyle.Default = Telerik.Reporting.Drawing.BorderType.None;
            this.reprintInd.Style.Font.Bold = false;
            this.reprintInd.Style.Font.Name = "Microsoft New Tai Lue";
            this.reprintInd.Style.Font.Size = Telerik.Reporting.Drawing.Unit.Point(25D);
            this.reprintInd.Style.LineWidth = Telerik.Reporting.Drawing.Unit.Point(3D);
            this.reprintInd.Style.TextAlign = Telerik.Reporting.Drawing.HorizontalAlign.Right;
            this.reprintInd.Style.VerticalAlign = Telerik.Reporting.Drawing.VerticalAlign.Top;
            this.reprintInd.StyleName = "";
            this.reprintInd.Value = "";
            // 
            // Rpt_NDTLabel
            // 
            this.Items.AddRange(new Telerik.Reporting.ReportItemBase[] {
            this.detail});
            this.Name = "Rpt_NDTLabel";
            this.PageSettings.ContinuousPaper = false;
            this.PageSettings.Landscape = false;
            this.PageSettings.Margins = new Telerik.Reporting.Drawing.MarginsU(Telerik.Reporting.Drawing.Unit.Mm(0D), Telerik.Reporting.Drawing.Unit.Mm(0D), Telerik.Reporting.Drawing.Unit.Mm(0D), Telerik.Reporting.Drawing.Unit.Mm(0D));
            this.PageSettings.PaperKind = System.Drawing.Printing.PaperKind.Custom;
            this.PageSettings.PaperSize = new Telerik.Reporting.Drawing.SizeU(Telerik.Reporting.Drawing.Unit.Mm(100D), Telerik.Reporting.Drawing.Unit.Mm(100D));
            reportParameter1.Name = "MillLine";
            reportParameter1.Type = Telerik.Reporting.ReportParameterType.Integer;
            reportParameter1.Value = "";
            reportParameter2.Name = "MillPOId";
            reportParameter2.Type = Telerik.Reporting.ReportParameterType.Integer;
            reportParameter2.Value = "";
            reportParameter3.Name = "NDTBundleID";
            reportParameter3.Type = Telerik.Reporting.ReportParameterType.Integer;
            reportParameter3.Value = "";
            reportParameter4.Name = "ConnectionString";
            reportParameter4.Text = "CS";
            reportParameter4.Value = "";
            reportParameter5.Name = "isReprint";
            reportParameter5.Type = Telerik.Reporting.ReportParameterType.Boolean;
            reportParameter5.Value = false;
            this.ReportParameters.Add(reportParameter1);
            this.ReportParameters.Add(reportParameter2);
            this.ReportParameters.Add(reportParameter3);
            this.ReportParameters.Add(reportParameter4);
            this.ReportParameters.Add(reportParameter5);
            styleRule1.Selectors.AddRange(new Telerik.Reporting.Drawing.ISelector[] {
            new Telerik.Reporting.Drawing.TypeSelector(typeof(Telerik.Reporting.TextItemBase)),
            new Telerik.Reporting.Drawing.TypeSelector(typeof(Telerik.Reporting.HtmlTextBox))});
            styleRule1.Style.Padding.Left = Telerik.Reporting.Drawing.Unit.Point(2D);
            styleRule1.Style.Padding.Right = Telerik.Reporting.Drawing.Unit.Point(2D);
            this.StyleSheet.AddRange(new Telerik.Reporting.Drawing.StyleRule[] {
            styleRule1});
            this.UnitOfMeasure = Telerik.Reporting.Drawing.UnitType.Mm;
            this.Width = Telerik.Reporting.Drawing.Unit.Cm(10D);
            this.NeedDataSource += new System.EventHandler(this.Rpt_NDTLabel_NeedDataSource);
            ((System.ComponentModel.ISupportInitialize)(this.eventLog1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this)).EndInit();

        }
        #endregion
        private Telerik.Reporting.DetailSection detail;
        private System.Diagnostics.EventLog eventLog1;
        internal Telerik.Reporting.TextBox textBundleNo;
        internal Telerik.Reporting.TextBox textType;
        internal Telerik.Reporting.TextBox textSize;
        internal Telerik.Reporting.TextBox textLen;
        internal Telerik.Reporting.TextBox textPcsBund;
        private Telerik.Reporting.Barcode barcode1;
        private Telerik.Reporting.Panel panel1;
        internal Telerik.Reporting.TextBox textBox5;
        internal Telerik.Reporting.PictureBox PictureBox1;
        internal Telerik.Reporting.TextBox textBox6;
        internal Telerik.Reporting.TextBox textSpecification;
        internal Telerik.Reporting.TextBox textBox7;
        internal Telerik.Reporting.TextBox textBox8;
        internal Telerik.Reporting.TextBox textBox1;
        internal Telerik.Reporting.TextBox textBox2;
        internal Telerik.Reporting.TextBox textBox9;
        internal Telerik.Reporting.TextBox textBox3;
        internal Telerik.Reporting.TextBox textBox10;
        private Telerik.Reporting.Barcode barcode2;
        internal Telerik.Reporting.TextBox textBox4;
        internal Telerik.Reporting.TextBox reprintInd;
    }
}

