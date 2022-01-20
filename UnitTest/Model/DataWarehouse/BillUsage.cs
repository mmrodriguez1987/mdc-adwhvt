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
       

        private string _ccnDTW;
        private string _testFileName;
        private DataSet myResponse, evaluatedData = new DataSet();

        public BillUsage(string ccnDTW, string testFileName)
        {
            _ccnDTW = ccnDTW;
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
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();                   

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));


                    String interpoledQuery = "SELECT B.BILLED_USAGE_KEY, B.SRC_BILL_ID, B.PER_KEY," +
                        "B.ACCT_KEY, D.BillDayofWeek, D.BillWorkDayCode, B.UDDGEN1 BillDate " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_BillDate D ON B.BILL_DATE_KEY=D.BillDateKey " +
                        "WHERE B.DATA_LOAD_DTTM BETWEEN '"+ endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '"+ endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "' AND BillWorkDayCode = 0";

                    evaluatedData = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Rows.Count > 0) ? "Failed" : "OK!"; 
                    myResponse.Tables[0].Rows[0][1] = "Check-New-Bills-On-Weekend";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, BillDate";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Rows.Count > 0) ? "There are bills generated on weekend or holidays" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpoledQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");
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
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    evaluatedData = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    string interpolatedQuery = "SELECT  B.BILL_DATE_KEY, B.BILLED_USAGE_KEY, B.UDDGEN1, B.FISCAL_CAL_KEY, C.StartDate, " +
                        "C.EndDate, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "WHERE (B.DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '"+ endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "') ";

                    myResponse.Tables[0].Rows[0][0] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Get-Bill-Generated-On-Wrong-Fiscal-Year";
                    myResponse.Tables[0].Rows[0][2] = "BillUsage, Fiscal Year";
                    myResponse.Tables[0].Rows[0][3] = (evaluatedData.Tables[0].Select("IsCorrectFiscalYear = 0").Length > 0) ? "There are bills generated on wrong fiscal year" : "No bills Generated on weekend or holidays were found";
                    myResponse.Tables[0].Rows[0][4] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");                   
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = -1;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery + "IsCorrectFiscalYear = 0";
                    myResponse.Tables[0].Rows[0][10] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");
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
        /// Comparing the Daily BILL_ID Distinct Count with the Historical ILL_ID Distinct Count.       
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <param name="BU_MAX_COUNT_DISTINCT_BILL_IDs">Maximun Histic Count of Discint bill_id </param>
        /// <returns></returns>
        public Task<DataSet> GetCountDistinctBillOnDataLoadOverTheMaxHistric(DateTime startDate, DateTime endDate, Int32 BU_MAX_COUNT_DISTINCT_BILL_IDs)
        {
            myResponse = Extensions.getResponseStructure("GetCountDistinctBillOnDataLoad");

            string query = "SELECT COUNT(DISTINCT SRC_BILL_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " + 
                "FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    evaluatedData = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    int dtwCount = Convert.ToInt32(evaluatedData.Tables[0].Rows[0][0]);

                    string interpolatedQuery = " SELECT COUNT(DISTINCT SRC_BILL_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN '"
                    + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";
                    
                    myResponse.Tables[0].Rows[0][0] = dtwCount > BU_MAX_COUNT_DISTINCT_BILL_IDs ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Get-Count-Distinct-Bill-On-Data-Load-Over-The-Max-Historic";
                    myResponse.Tables[0].Rows[0][2] = "SRC_BILL_ID";
                    myResponse.Tables[0].Rows[0][3] = dtwCount > BU_MAX_COUNT_DISTINCT_BILL_IDs ? "Quantity of Distinct Bill_ID on this Day surpassed the historical maximum." : "Ok!";
                    myResponse.Tables[0].Rows[0][4] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH - Validation: Test Name =>" + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result => " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);
                   
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