using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Microsoft.Extensions.Logging;
using Tools.Files;


namespace UnitTest.Model.DataWarehouse
{
    public class BillUsage    {
       

        private string _conexion;
        private string _testFileName;
        private DataSet myResponse, evaluatedData = new DataSet();

        public BillUsage(string conexion)
        {
            _conexion = conexion;
            _testFileName = @"C:\\ADW_UT\\UT_BI_ADWH_" + DateTime.Today.ToString("yyyy_MM_dd") + ".csv";
        }


        /// <summary>
        /// Prepara the response structure on Dataset
        /// </summary>
        /// <returns>a Dataset with a structure ready to be converted in JSON</returns>
        public  DataSet getResponseStructure(String dataTableName)
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable TestResult = new DataTable(dataTableName);
            //DataTable TestResultDetail = new DataTable("TestResultDetail");
            TestResult.Columns.Add("State");
            TestResult.Columns.Add("Test-Information");
            TestResult.Columns.Add("Entities-Involved");
            TestResult.Columns.Add("Test-Result-description");
            TestResult.Columns.Add("Initial Evaluated Date");
            TestResult.Columns.Add("End Evaluated Date");
            TestResult.Columns.Add("result_before_adf");
            TestResult.Columns.Add("result_after_adf");
            TestResult.Columns.Add("Effectuated Date");
            TestResult.Rows.Add(0, 0,"", "","","",0,0,"");
/*
            TestResultDetail.Columns.Add("TestResultID");
            TestResultDetail.Columns.Add("affected_keys_array");
            TestResultDetail.Columns.Add("affected_key_name");
            TestResultDetail.Columns.Add("database_name");
*/
            dsResult.Tables.Add(TestResult);
            //dsResult.Tables.Add(TestResultDetail);
            return dsResult;
        }

        /// <summary>
        /// Check if there are any bills generated on weekend or Holiday
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> GetBillsGeneratedOnWeekend(DateTime startDate, DateTime endDate)
        {
            myResponse = getResponseStructure("BillsGeneratedOnWeekend");
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

                    evaluatedData = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query, parameters.ToArray());

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Rows.Count > 0) ? "Test Failed" : "Test Passed"; 
                    myResponse.Tables[0].Rows[0][1] = "Check-New-Bills-On-Weekend";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, BillDate";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Rows.Count > 0) ? "There are bills generated on weekend or holidays" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = DateTime.Today.ToString("yyyy-MM-dd HH:mm");

                    //TestResult test = new TestResult(_conexion);

                    CSV logFile = new CSV(_testFileName);
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                  
                    return myResponse;

                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString());
                    return myResponse;
                }
            });
        }


        /// <summary>
        /// Check if there are any Bill Generated on wron Fiscal Year.
        /// 
        /// </summary>
        /// <param name="startDate">Initial Evaluated Date</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> GetBillGeneratedOnWrongFiscalYear(DateTime startDate, DateTime endDate)
        {
            myResponse = getResponseStructure("BillGeneratedOnWrongFiscalYear");
            string query = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " + 
                "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY "+
                "WHERE (B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate)";

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    evaluatedData = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query, parameters.ToArray());

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "Warning" : "Pass";
                    myResponse.Tables[0].Rows[0][1] = "Get-Bill-Generated-On-Wrong-Fiscal-Year";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, Fiscal Year";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Rows.Count > 0) ? "There are bills generated on wrong fiscal year" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = DateTime.Today.ToString("yyyy-MM-dd HH:mm");

                    //TestResult test = new TestResult(_conexion);
                    //test.writeTestResult(myResponse);

                    CSV logFile = new CSV(_testFileName);
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    return myResponse;

                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString());
                    return myResponse;
                }
            });
        }

    }
}