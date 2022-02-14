﻿using System;
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
using UnitTest.Model.ValidationTest;
using System.Threading;

namespace UnitTest.Model.DataWarehouse
{
    public class Address
    {

        private string _ccnDTW, _ccnCDC, _testFileName, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();
        private CBDate tTime;
        private Historical historical;
        private TestResult results;

        public Address(string cnnDTW, string ccnCDC, string ccnValTest, string testFileName)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            _testFileName = testFileName;
            //Initializa Historical Indicators Computing
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);
            queryCDC = "cdc.sp_ci_acct_ct";
            tTime = new CBDate();
        }
        public Task<DataSet> DistinctAddresCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("AcctCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(15).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_ACCT_ID) DTW_Count FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(15).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;
                    int cdcCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("ACCT_ID")).Distinct().Count();
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    if (cdcCount == 0 && dtwCount == 0)
                        testInterpretation = "There is no data to evaluate for Distinct ACCT, CDC=0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount == 0)
                        testInterpretation = "There is no incremental data to evaluate for Distinct ACT, CDC > 0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount != dtwCount))
                        testInterpretation = "Distinct ACCT_ID count on both sides are different";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount))
                        testInterpretation = "Distinct ACCT_ID count on both sides are equals";
                    else
                        testInterpretation = "Error";

                    myResponse.Tables[0].Rows[0][0] = (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Distinct Account Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                    // 2 -SRC_ACCT_ID, 1- Distinct
                    historical.recordHistorical(2, 1, dtwCount, endDate);

                   
                    //recording on DB
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? 3 : 2);
                    results.TestDate = DateTime.Now;
                    results.TestID = 4; // Compare Dsitinct ACCT
                    results.recordUntitValidationTest(cdcCount, dtwCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving DistinctAccountCount test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving DistinctAccountCount test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on DistinctAccountCount process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on DistinctAccountCount process");
                    return myResponse;
                }
            });
        }



        /// <summary>
        /// In this proces we verify that the same New Address ID in CDC are also on DTW
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> NewAccountCounts(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("NewAccountCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    //This query reflect the "Universe" all the SRC_ACCT_ID for the evaluated date                   
                    queryDTW = "SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    string interpoledQueryDTW = "SELECT D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var NewAccountsOnDTW = (
                                        from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                        join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                        on cdc.Field<string>("ACCT_ID") equals dtw.Field<string>("SRC_ACCT_ID")
                                        where cdc.Field<Int32>("toInsert") == 1 && (cdc.Field<Int32>("__$operation") == 2 || cdc.Field<Int32>("__$operation") == 4)
                                        select new
                                        {
                                            SRC_ACCT_ID = dtw.Field<string>("SRC_ACCT_ID").Distinct()
                                        }).ToList();

                    var cdcFilteredRows = from row in evalDataCDC.Tables[0].AsEnumerable()
                                          where row.Field<Int32>("toInsert") == 1 && (row.Field<Int32>("__$operation") == 2 || row.Field<Int32>("__$operation") == 4)
                                          select row;

                    int cdcCount = cdcFilteredRows.Count();
                    int dtwCount = NewAccountsOnDTW.Count();

                    if (cdcCount == 0 && dtwCount == 0)
                        testInterpretation = "There is no data to evaluate for New ACCT, CDC=0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount == 0)
                        testInterpretation = "There is no incremental data to evaluate for New ACT, CDC > 0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount != dtwCount))
                        testInterpretation = "New ACCT_ID count on both sides are different";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount))
                        testInterpretation = "New ACCT_ID count on both sides are equals";
                    else
                        testInterpretation = "Error";

                    // 2 -SRC_ACCT_ID, 1- Distinct
                    historical.recordHistorical(2, 2, dtwCount, endDate);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "New ACCT Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                    
                    //recording on DB 
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? 3 : 2);
                    results.TestDate = DateTime.Now;
                    results.TestID = 5; // Compare Dsitinct ACCT
                    results.recordUntitValidationTest(cdcCount, dtwCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving NewAccountCounts test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving NewAccountCounts test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on NewAccountCounts process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on NewAccountCounts process");
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
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    string interpoledQueryDTW = "SELECT  D.SRC_ACCT_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_ACCT_ID FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_ACCT D ON D.SRC_ACCT_ID=T.SRC_ACCT_ID";


                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //LINQ query for Updated ACCT on DTW
                    var UpdatedAccountsOnDTW = (
                                                from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                                join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                                on cdc.Field<string>("ACCT_ID") equals dtw.Field<string>("SRC_ACCT_ID")
                                                where cdc.Field<Int32>("toInsert") == 0 && cdc.Field<Int32>("__$operation") == 4
                                                select new
                                                {
                                                    EmpId = dtw.Field<string>("SRC_ACCT_ID")
                                                }).ToList();

                    //LINQ query for Updated ACCT on CDC
                    var UpdatedAccountsOnCDC = from row in evalDataCDC.Tables[0].AsEnumerable()
                                               where row.Field<Int32>("toInsert") == 0 && row.Field<Int32>("__$operation") == 4
                                               select row;

                    int cdcCount = UpdatedAccountsOnCDC.Count();
                    int dtwCount = UpdatedAccountsOnDTW.Count();

                    if (cdcCount == 0 && dtwCount == 0)
                        testInterpretation = "There is no data to evaluate for Updated ACCT, CDC=0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount == 0)
                        testInterpretation = "There is no incremental data to evaluate for Updated ACCT, CDC > 0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount != dtwCount))
                        testInterpretation = "Updated ACCT_ID count on both sides are different";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount))
                        testInterpretation = "Updated ACCT_ID count on both sides are equals";
                    else
                        testInterpretation = "Error";

                    // 2 -SRC_ACCT_ID, 3-Updated
                    historical.recordHistorical(2, 3, dtwCount, endDate);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Count of Updated Accounts";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                   
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? 3 : 2);
                    results.TestDate = DateTime.Now;
                    results.TestID = 6;
                    results.recordUntitValidationTest(cdcCount, dtwCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving UpdatedAccountCounts test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving UpdatedAccountCounts test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on UpdatedAccountCounts process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on UpdatedAccountCounts process");
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
        public Task<DataSet> DistinctAcctCountOnDataLoadOverTheMaxHistricCount(DateTime startDate, DateTime endDate)
        {
            Int64 maxHistoricalCountAccountID = historical.GetMaximunHistorical(2, 1);

            myResponse = Extensions.getResponseStructure("GetCountDistinctAcctOnDataLoad");
            int dtwCount;
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    string query = "SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                     "FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    dtwCount = (evalDataDTW.Tables[0].Rows.Count > 0) ? Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]) : 0;

                    if (dtwCount > 0)
                        testInterpretation = "There is no data to evaluate in this Date Range, DTW=0";
                    else if (dtwCount > maxHistoricalCountAccountID)
                        testInterpretation = "Disctinc ACCT Count exceed the maximun historic";
                    else if (dtwCount < maxHistoricalCountAccountID)
                        testInterpretation = "Disctinc ACCT Count is less than the maximun historic";
                    else if (dtwCount == maxHistoricalCountAccountID)
                        testInterpretation = "Disctinc ACCT Count is equals to the maximun historic";
                    else
                        testInterpretation = "Error";

                    string interpolatedQuery = " SELECT COUNT(DISTINCT SRC_ACCT_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek FROM dwadm2.CD_ACCT WHERE DATA_LOAD_DTTM BETWEEN '"
                    + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    myResponse.Tables[0].Rows[0][0] = dtwCount <= maxHistoricalCountAccountID ? "OK!" : "Warning";
                    myResponse.Tables[0].Rows[0][1] = "Distinct ACCT Count vs Max Historic";
                    myResponse.Tables[0].Rows[0][2] = "SRC_ACCT_ID";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                    //recording on DB                                     
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((dtwCount <= maxHistoricalCountAccountID) ? 3 : 1);
                    results.TestDate = endDate;
                    results.TestID = 7; // Compare Dsitinct ACCT Over the maximun
                    results.recordHistoricalValidationTest(dtwCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving DistinctAcctCountOnDataLoadOverTheMaxHistricCount test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving DistinctAcctCountOnDataLoadOverTheMaxHistricCount test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on DistinctAcctCountOnDataLoadOverTheMaxHistricCount process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on DistinctAcctCountOnDataLoadOverTheMaxHistricCount process");
                    return myResponse;
                }
            });
        }


        /// <summary>
        /// This method take the evaDate and and compare with the average of the Daily Distinct Count of account,
        /// taking a <paramref name="DAY_RANGE_TO_BE_EVALUATED"/> Days sample.
        /// </summary>
        /// <param name="evalDate">Evaluated Date</param> 
        /// <param name="DAY_RANGE_TO_BE_EVALUATED">Quantity of Days to be evaluated in the Sample Average(days are counted to the past)</param>
        /// <returns></returns>
        public Task<DataSet> StatisticalAcountEvaluation(DateTime evalDate, Int32 DAY_RANGE_TO_BE_EVALUATED, Double TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION)
        {
            myResponse = Extensions.getResponseStructure("StatisticalComparison");
            string testInterpretation;

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> cdcParameters;

                    // get the List with the Dates to evaluate
                    List<DateTime> evalrange = tTime.GetEvalRangeDate(evalDate, DAY_RANGE_TO_BE_EVALUATED, false);
                    List<StatisticalEvaluation> AccountEvaluation = new List<StatisticalEvaluation>();
                    StatisticalEvaluation statisticalEvaluation;

                    //Iterate the List to add the Hour to the Dates, and the Distinct Count of account
                    for (var i = 0; i < evalrange.Count; i++)
                    {
                        //adding the hour and minutes: 12:30
                        statisticalEvaluation = new StatisticalEvaluation();
                        statisticalEvaluation.IntialDate = evalrange[i].Date.AddDays(-1).AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EndDate = evalrange[i].Date.AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EvalDateIndex = i + 1;

                        cdcParameters = new List<SqlParameter>();

                        cdcParameters.Add(new SqlParameter("@startDate", statisticalEvaluation.IntialDate.ToString("yyyy-MM-dd HH:mm")));
                        cdcParameters.Add(new SqlParameter("@endDate", statisticalEvaluation.EndDate.ToString("yyyy-MM-dd HH:mm")));

                        //Distinct Acount Count
                        evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                        statisticalEvaluation.DistinctCountValue = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("ACCT_ID")).Distinct().Count();

                        AccountEvaluation.Add(statisticalEvaluation);
                        Thread.Sleep(1000);
                    }

                    //Computing the average of the Days
                    double averCountAccount = AccountEvaluation.Average(item => item.DistinctCountValue);


                    //Distinct Acount Count
                    cdcParameters = new List<SqlParameter>();
                    cdcParameters.Add(new SqlParameter("@startDate", evalDate.Date.AddDays(-1).AddHours(12).AddMinutes(30).ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", evalDate.Date.AddHours(12).AddMinutes(30).ToString("yyyy-MM-dd HH:mm")));

                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //Distinct Acount count of Evaluated Day
                    int evaluatedCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("ACCT_ID")).Distinct().Count();


                    //Incremental
                    double incremIndicator = (evaluatedCount - averCountAccount) / averCountAccount;

                    if (Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100))
                        testInterpretation = "The Acount Count is out of Teen Days Average Range";
                    else
                        testInterpretation = "The Acount Count is into the Teen Days Average Range";

                    myResponse.Tables[0].Rows[0][0] = (Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100)) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Statistical Average Accounts";
                    myResponse.Tables[0].Rows[0][2] = "cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = averCountAccount;
                    myResponse.Tables[0].Rows[0][7] = evaluatedCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = string.Join(",", AccountEvaluation);
                    myResponse.Tables[0].Rows[0][10] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;
                   
                    //columnID: 2-SRC_ACCT_ID, indicatorType: 6-Average Weekly
                    historical.recordHistorical(2, 6, averCountAccount, evalDate);

                    //recording on DB                  
                    results.Description = testInterpretation;
                    results.StartDate = evalDate;
                    results.EndDate = evalDate;
                    results.StateID = (short)((Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100)) ? 1 : 3);
                    results.TestDate = DateTime.Now;
                    results.TestID = 8;
                    results.recordStatisticalValidationTest(averCountAccount, evaluatedCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving StatisticalAcountEvaluation test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving StatisticalAcountEvaluation test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on StatisticalAcountEvaluation process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on StatisticalAcountEvaluation process");
                    return myResponse;
                }
            });
        }


    }
}
