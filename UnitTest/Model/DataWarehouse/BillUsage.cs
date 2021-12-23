using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;


namespace UnitTest.Model.DataWarehouse
{
    public class BillUsage    {
       

        private string _conexion;
        private string _testFileName;
        private DataSet myResponse, evaluatedData = new DataSet();

        public BillUsage(string conexion, string testFileName)
        {
            _conexion = conexion;
            _testFileName = testFileName;
        }

        /// <summary>
        /// Check if there are any bills generated on weekend or Holiday
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> GetBillsGeneratedOnWeekend(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("BillsGeneratedOnWeekend");
            string query = "SELECT B.BILLED_USAGE_KEY, B.SRC_BILL_ID, B.PER_KEY,"  + 
                "B.ACCT_KEY, D.BillDayofWeek, D.BillWorkDayCode, B.UDDGEN1 BillDate " + 
                "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_BillDate D ON B.BILL_DATE_KEY=D.BillDateKey " +
                "WHERE B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate AND BillWorkDayCode = 0";

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    String interpoledQuery = "SELECT B.BILLED_USAGE_KEY, B.SRC_BILL_ID, B.PER_KEY," +
                        "B.ACCT_KEY, D.BillDayofWeek, D.BillWorkDayCode, B.UDDGEN1 BillDate " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_BillDate D ON B.BILL_DATE_KEY=D.BillDateKey " +
                        "WHERE B.DATA_LOAD_DTTM BETWEEN '"+ startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '"+ endDate.ToString("yyyy-MM-dd HH:mm") + "' AND BillWorkDayCode = 0";

                    evaluatedData = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query, parameters.ToArray());

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Rows.Count > 0) ? "Test Failed" : "Test Passed"; 
                    myResponse.Tables[0].Rows[0][1] = "Check-New-Bills-On-Weekend";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, BillDate";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Rows.Count > 0) ? "There are bills generated on weekend or holidays" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpoledQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH - Validation: Test Name =>" + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result => " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    if (evaluatedData.Tables[0].Rows.Count > 0)
                    {
                        CSV DetaillogFile = new CSV(_testFileName + "_Detail_Bill_Generated_On_Weekend.csv");
                        DetaillogFile.writeNewOrExistingFile(evaluatedData.Tables[0]);
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString().Substring(0, 160));
                    myResponse.Tables[0].Rows[0][11] = ("Error: " + e.ToString().Substring(0, 160));
                    return myResponse;
                }
            });
        }


        /// <summary>
        /// Check if there are any Bill Generated on wron Fiscal Year.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Date</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> GetBillGeneratedOnWrongFiscalYear(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("BillGeneratedOnWrongFiscalYear");
            string query = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " + 
                "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY "+
                "WHERE (B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) ";            

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    evaluatedData = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query, parameters.ToArray());

                    string interpolatedQuery = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                        "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "WHERE (B.DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '"+ endDate.ToString("yyyy-MM-dd HH:mm") + "') ";

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "Warning" : "Test Passed";
                    myResponse.Tables[0].Rows[0][1] = "Get-Bill-Generated-On-Wrong-Fiscal-Year";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, Fiscal Year";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "There are bills generated on wrong fiscal year" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    //myResponse.Tables[0].Rows[0][6] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? ("BILLED_USAGE_KEY: " + Extensions.GetDelimitedString(evaluatedData.Tables[0], "IsCorrectFiscalYear = 0", "BILLED_USAGE_KEY", "|"))  : "N/A";
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery + "IsCorrectFiscalYear = 0";
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH - Validation: Test Name =>" + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result => " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //if the Test Fail write other file with the failed_data
                    if ((evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0))
                    {
                        CSV DetaillogFile = new CSV(_testFileName + "_Detail_Bill_Generated_On_Wrong_Fiscal_Year.csv");
                        DetaillogFile.writeNewOrExistingFile(evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").CopyToDataTable());
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString().Substring(0, 160));
                    myResponse.Tables[0].Rows[0][11] = ("Error: " + e.ToString().Substring(0, 160));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// Get Statistical Data and make a lineal regretion to compare data
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> GetStatisticalData(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("BillGeneratedOnWrongFiscalYear");
            string query = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " +
                "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                "WHERE (B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) ";

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    evaluatedData = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query, parameters.ToArray());

                    string interpolatedQuery = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                        "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "WHERE (B.DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "') ";

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "Warning" : "Test Passed";
                    myResponse.Tables[0].Rows[0][1] = "Get-Bill-Generated-On-Wrong-Fiscal-Year";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, Fiscal Year";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "There are bills generated on wrong fiscal year" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery + "IsCorrectFiscalYear = 0";
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH - Validation: Test Name =>" + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result => " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //if the Test Fail write other file with the failed_data
                    if ((evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0))
                    {
                        CSV DetaillogFile = new CSV(_testFileName + "_Detail_Bill_Generated_On_Wrong_Fiscal_Year.csv");
                        DetaillogFile.writeNewOrExistingFile(evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").CopyToDataTable());
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString().Substring(0, 160));
                    myResponse.Tables[0].Rows[0][11] = ("Error: " + e.ToString().Substring(0, 160));
                    return myResponse;
                }
            });
        }

    }
}