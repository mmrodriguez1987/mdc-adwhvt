using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;
using Tools.Communication;

namespace UnitTest.Model.DataWarehouse
{
    public class Account
    {
        private string _ccnDTW, _ccnCDC, _testFileName, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();

        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="testFileName">File Name of the Test Result</param>
        public Account(string cnnDTW, string ccnCDC, string testFileName)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            _testFileName = testFileName;
            queryCDC = "cdc.sp_ci_acct_ct";
        }

        /// <summary>
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables ACCT
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> DistinctAccountCounts(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("AcctCounts");

            return Task.Run(() =>
            {
                try
                {

                    queryDTW = "SELECT COUNT(SRC_ACCT_ID) as DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN  @startDate AND @endDate";
                   

                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_ACCT_ID) as DTW_Count from [dwadm2].[CD_ACCT] where DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, parameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, parameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "Test Passed";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct ACCT_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct ACCT_ID counts on both sides are different" : "Distinct ACCT_ID counts on both sides are congruent";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
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



        /// <summary>
        /// Compare the New Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> NewAccountCounts(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("NewAccountCounts");

            return Task.Run(() =>
            {
                try
                {             
                    queryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate AND EFF_END_DTTM='3000-12-31'";

                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    string interpoledQueryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM [dwadm2].[CD_ACCT] where DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'  AND EFF_END_DTTM='3000-12-31'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, parameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, parameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=1 AND (Operation=1 OR Operation=2)").Length;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "Test ok!";
                    myResponse.Tables[0].Rows[0][1] = "Count New ACCT_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "New Record Count on ACCT are different on CDC and DTWH" : "New Record Count on ACCT are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
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


        /// <summary>
        /// Compare the updated Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> UpdatedAccountCounts(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("UpdatedAccountCounts");

            return Task.Run(() =>
            {
                try
                {                    
                    queryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate AND EFF_END_DTTM=DATA_LOAD_DTTM";

                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    string interpoledQueryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM [dwadm2].[CD_ACCT] where DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'  AND EFF_END_DTTM=DATA_LOAD_DTTM";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, parameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, parameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=0 AND Operation=4").Length;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "Test ok!";
                    myResponse.Tables[0].Rows[0][1] = "Count Updated ACCT_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Updated Record Count on ACCT are different on CDC and DTWH" : "New Record Count on ACCT are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
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


        /// <summary>
        /// Comparing the Daily ACCT_ID Distinct Count with the Historical ACCT_ID Distinct Count.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <param name="BU_MAX_COUNT_DISTINCT_ACCT_IDs">Maximun Histic Count of Discint bill_id </param>
        /// <returns></returns>
        public Task<DataSet> DistinctAcctCountOnDataLoadOverTheMaxHistricCount(DateTime startDate, DateTime endDate, Int32 BU_MAX_COUNT_DISTINCT_ACCT_IDs)
        {
            myResponse = Extensions.getResponseStructure("GetCountDistinctAcctOnDataLoad");

            string query = "SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                "FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, parameters.ToArray());

                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);


                    string interpolatedQuery = " SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '"
                    + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    myResponse.Tables[0].Rows[0][0] = dtwCount > BU_MAX_COUNT_DISTINCT_ACCT_IDs ? "Warning" : "Test Passed";
                    myResponse.Tables[0].Rows[0][1] = "Get-Count-Distinct-Acct-On-Data-Load-Over-The-Max-Historic";
                    myResponse.Tables[0].Rows[0][2] = "SRC_ACCT_ID";
                    myResponse.Tables[0].Rows[0][3] = dtwCount > BU_MAX_COUNT_DISTINCT_ACCT_IDs ? "Quantity of Distinct ACCT_ID on this Day surpassed the historical maximum." : "Ok!";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
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
