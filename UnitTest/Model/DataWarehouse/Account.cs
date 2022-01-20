using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;
using System.Linq;
using Tools;
using Tools.Statistics;
using UnitTest.DAL;

namespace UnitTest.Model.DataWarehouse
{
    public class Account
    {
        private string _ccnDTW, _ccnCDC, _ccnValTest, _testFileName, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();
        private CBDate ttime;
        private HistoricalIndicator _historicIndicators;
        private TestResults _test;

        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="ccnValTest">conexion to Validation Test Database</param>
        /// <param name="testFileName">File Name of the Test Result</param>
        public Account(string cnnDTW, string ccnCDC, string ccnValTest, string testFileName)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            _ccnValTest = ccnValTest;
            _testFileName = testFileName;

            //Initializa Historical Indicators Computing
            _historicIndicators = new HistoricalIndicator(_ccnValTest);
            _historicIndicators.EntityName = "ACCT";
            _historicIndicators.ColumnName = "ID";

            _test = new TestResults(_ccnValTest);

            queryCDC = "cdc.sp_ci_acct_ct";
            ttime = new CBDate();

        }

        /// <summary>
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables ACCT
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> DistinctAccountCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("AcctCounts");

            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";                   

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Unique Account Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Unique ACCT_ID counts on both sides are different" : "Distinct ACCT_ID counts on both sides are congruent";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

                    _historicIndicators.CalculatedDate = startDate;
                    _historicIndicators.DistinctCountVal = cdcCount;

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //recording on DB
                    _test = new TestResults(_ccnValTest);
                    _test.InfoID = 5;
                    _test.StateID = (cdcCount != dtwCount) ? 1 : 3;
                    _test.CountCDC = cdcCount;
                    _test.CountDTW = dtwCount;
                    _test.Entity = "ACCT";
                    _test.Result = (cdcCount != dtwCount) ? "Unique ACCT_ID counts on both sides are different" : "Distinct ACCT_ID counts on both sides are congruent";
                    _test.QueryCDC = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    _test.QueryDTW = interpoledQueryDTW;
                    _test.IniEvalDate = startDate;
                    _test.EndEvalDate = endDate;
                    _test.EffectDate = endDate;
                    _test.insert();

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(_test.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _test.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _test.Error.Substring(0, 160));
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
                    queryDTW = "SELECT  D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";
                                        
                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var NewAccountsOnDTW = (
                                        from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                        join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                        on cdc.Field<string>("ACCT_ID") equals dtw.Field<string>("SRC_ACCT_ID")
                                        where cdc.Field<Int32>("toInsert") == 1 && (cdc.Field<Int32>("__$operation") == 2 || cdc.Field<Int32>("__$operation") == 4)
                                        select new { 
                                            EmpId = dtw.Field<string>("SRC_ACCT_ID")  
                                        }).ToList();                    

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=1 AND ([__$operation]=2 OR [__$operation]=4)").Length;
                    int dtwCount = NewAccountsOnDTW.Count();

                    _historicIndicators.NewCountVal = cdcCount;

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count of New Accounts";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "New Records Counts on ACCT are different on CDC and DTWH" : "New Records Count on ACCT are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //recording on DB
                    _test = new TestResults(_ccnValTest);
                    _test.InfoID = 6;
                    _test.StateID = (cdcCount != dtwCount) ? 2 : 3;
                    _test.CountCDC = cdcCount;
                    _test.CountDTW = dtwCount;
                    _test.Entity = "ACCT";
                    _test.Result = (cdcCount != dtwCount) ? "New Records Counts on ACCT are different on CDC and DTWH" : "New Records Count on ACCT are congruents on CDC and DTWH";
                    _test.QueryCDC = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    _test.QueryDTW = interpoledQueryDTW;
                    _test.IniEvalDate = startDate;
                    _test.EndEvalDate = endDate;
                    _test.EffectDate = endDate;
                    _test.insert();

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(_test.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _test.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _test.Error.Substring(0, 160));
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
                    queryDTW = "SELECT  D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT  D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";


                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var UpdatedAccountsOnDTW = (
                                        from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                        join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                        on cdc.Field<string>("ACCT_ID") equals dtw.Field<string>("SRC_ACCT_ID")
                                        where cdc.Field<Int32>("toInsert") == 0 && cdc.Field<Int32>("__$operation") == 4
                                        select new
                                        {
                                            EmpId = dtw.Field<string>("SRC_ACCT_ID")
                                        }).ToList();

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=0 AND [__$operation]=4").Length;
                    int dtwCount = UpdatedAccountsOnDTW.Count();

                    _historicIndicators.UpdatedCountVal = cdcCount; 

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count of Updated Accounts";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Updated Records Counts on ACCT are different on CDC and DTWH" : "Updated Records Count on ACCT are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //recording on DB
                    _test = new TestResults(_ccnValTest);
                    _test.InfoID = 7;
                    _test.StateID = (cdcCount != dtwCount) ? 2 : 3;
                    _test.CountCDC = cdcCount;
                    _test.CountDTW = dtwCount;
                    _test.Entity = "ACCT";
                    _test.Result = (cdcCount != dtwCount) ? "Updated Records Counts on ACCT are different on CDC and DTWH" : "Updated Records Count on ACCT are congruents on CDC and DTWH";
                    _test.QueryCDC = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    _test.QueryDTW = interpoledQueryDTW;
                    _test.IniEvalDate = startDate;
                    _test.EndEvalDate = endDate;
                    _test.EffectDate = endDate;
                    _test.insert();

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(_test.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _test.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _test.Error.Substring(0, 160));
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
        /// Comparing the Daily ACCT_ID Distinct Count with the Historical ACCT_ID Distinct Count.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <param name="BU_MAX_COUNT_DISTINCT_ACCT_IDs">Maximun Histic Count of Discint bill_id </param>
        /// <returns></returns>
        public Task<DataSet> DistinctAcctCountOnDataLoadOverTheMaxHistricCount(DateTime startDate, DateTime endDate, Int32 BU_MAX_COUNT_DISTINCT_ACCT_IDs)
        {
            myResponse = Extensions.getResponseStructure("GetCountDistinctAcctOnDataLoad");           

            return Task.Run(() =>
            {
                try
                {
                    string query = "SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                     "FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    List<SqlParameter> dtwParameters = new List<SqlParameter>();                    

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    string interpolatedQuery = " SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '"
                    + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    myResponse.Tables[0].Rows[0][0] = dtwCount > BU_MAX_COUNT_DISTINCT_ACCT_IDs ? "Warning" : "OK!";
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
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    //recording on DB
                    _test = new TestResults(_ccnValTest);
                    _test.InfoID = 8;
                    _test.StateID = (dtwCount > BU_MAX_COUNT_DISTINCT_ACCT_IDs) ? 1 : 3;                   
                    _test.CountDTW = dtwCount;
                    _test.Entity = "ACCT";
                    _test.Result = dtwCount > BU_MAX_COUNT_DISTINCT_ACCT_IDs ? "Quantity of Distinct ACCT_ID on this Day surpassed the historical maximum." : "Ok!";                    
                    _test.QueryDTW = interpolatedQuery;
                    _test.IniEvalDate = startDate;
                    _test.EndEvalDate = endDate;
                    _test.EffectDate = endDate;
                    _test.insert();

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(_test.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _test.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _test.Error.Substring(0, 160));
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
        /// This method take the evaDate and and compare with the average of the Daily Distinct Count of account,
        /// taking a <paramref name="BU_ACCT_EVAL_DAY_FOR_AVERAGE"/> Days sample.
        /// </summary>
        /// <param name="evalDate">Evaluated Date</param>
        /// <param name="BU_ACCT_EVAL_DAY_FOR_AVERAGE">Days ago to be evaluated in the Sample Average</param>
        /// <returns></returns>
        public Task<DataSet> StatisticalComparison(DateTime evalDate, Int32 BU_ACCT_EVAL_DAY_FOR_AVERAGE, Double TOL_NUM_ON_VAR_ACCOUNT_AVER)
        {
            myResponse = Extensions.getResponseStructure("StatisticalComparison");
            

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> cdcParameters;
                  
                    // get the List with the Dates to evaluate
                    List<DateTime> evalrange = ttime.GetEvalRangeDate(evalDate, BU_ACCT_EVAL_DAY_FOR_AVERAGE,  false);  
                    List<StatisticalEvaluation> AccountEvaluation = new List<StatisticalEvaluation>();
                    StatisticalEvaluation se;

                    //Iterate the List to add the Hour to the Dates, and the Distinct Count of account
                    for (var i = 0; i < evalrange.Count; i++)
                    {
                        //adding the hour and minutes: 12:30
                        se = new StatisticalEvaluation();
                        se.IntialDate = evalrange[i].AddDays(-1).AddHours(12).AddMinutes(30);
                        se.EndDate = evalrange[i].AddDays(-1).AddHours(12).AddMinutes(30);
                        se.EvalDateIndex = i + 1;

                        cdcParameters = new List<SqlParameter>();

                        cdcParameters.Add(new SqlParameter("@startDate", se.IntialDate.ToString("yyyy-MM-dd HH:mm")));
                        cdcParameters.Add(new SqlParameter("@endDate", se.EndDate.ToString("yyyy-MM-dd HH:mm")));

                        //Distinct Acount Count
                        evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                        se.Val = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;
                        AccountEvaluation.Add(se);                        
                    }

                    //Computing the average of the Days
                    double averCountAccount = AccountEvaluation.Average(item => item.Val);

                    //Computing the Distinct Count of Accounts of the Evaluated Day

                    cdcParameters = new List<SqlParameter>();
                    cdcParameters.Add(new SqlParameter("@startDate", evalDate.AddDays(-1).AddHours(12).AddMinutes(30).ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", evalDate.AddHours(12).AddMinutes(30).ToString("yyyy-MM-dd HH:mm")));

                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //Distinct Acount count of Evaluated Day
                    int countAccount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;

                    //Incremental
                    double incremIndicator = (countAccount - averCountAccount) / averCountAccount;                

                    myResponse.Tables[0].Rows[0][0] = (Math.Abs(incremIndicator) > TOL_NUM_ON_VAR_ACCOUNT_AVER) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Statistical Average Accounts";
                    myResponse.Tables[0].Rows[0][2] = "cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (Math.Abs(incremIndicator) > TOL_NUM_ON_VAR_ACCOUNT_AVER) ? "The Acount Count is out of Teen Days Average Range " : "The Acount Count is into the Teen Days Average Range ";
                    myResponse.Tables[0].Rows[0][4] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = averCountAccount;
                    myResponse.Tables[0].Rows[0][7] = countAccount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = "";
                    myResponse.Tables[0].Rows[0][10] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);

                    // recording on DB
                    _test = new TestResults(_ccnValTest);
                    _test.InfoID = 9;
                    _test.StateID = (Math.Abs(incremIndicator) > TOL_NUM_ON_VAR_ACCOUNT_AVER) ? 1 : 3;
                    _test.CountCDC = Convert.ToInt64(Math.Round(averCountAccount));
                    _test.CountDTW = countAccount;
                    _test.Entity = "ACCT";
                    _test.Result = (Math.Abs(incremIndicator) > TOL_NUM_ON_VAR_ACCOUNT_AVER) ? "The Acount Count is out of Teen Days Average Range " : "The Acount Count is into the Teen Days Average Range ";                   
                    _test.IniEvalDate = evalDate;
                    _test.EndEvalDate = evalDate;
                    _test.EffectDate = evalDate;
                    _test.insert();

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(_test.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _test.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _test.Error.Substring(0, 160));
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


        public Task<DataSet> recordIndicators()
        {
            myResponse = Extensions.getResponseStructure("RecordingIndicators");

            return Task.Run(() =>
            {
                try
                {
                    myResponse.Tables[0].Rows[0][1] = "Recording Indicators";
                    _historicIndicators.insert();
                    if (!String.IsNullOrEmpty(_historicIndicators.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + _historicIndicators.Error.Substring(0, 160));
                        myResponse.Tables[0].Rows[0][11] = ("Error: " + _historicIndicators.Error.Substring(0, 160));
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
