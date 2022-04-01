﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.DataConversion;
using System.Linq;
using Tools;
using UnitTest.Model.ValidationTest;
using Tools.Statistics;
using System.Threading;

namespace UnitTest.Model.DataWarehouse
{
    public class Person
    {
        private string _ccnDWH, _ccnCCB, queryCCB, queryDWH, appEnv;
        private DataSet myResponse, evalDataDWH, evalDataCCB;
        private CBDate tTime;
        private Historical historical;
        private TestResult results;
        private int ccbCount, dwhCount;

        /// <summary>
        /// Initialize the Person class with params required
        /// </summary>
        /// <param name="cnnDWH"></param>
        /// <param name="ccnCCB"></param>
        /// <param name="ccnValTest"></param>
        public Person(string cnnDWH, string ccnCCB, string ccnValTest)
        {
            _ccnDWH = cnnDWH;
            _ccnCCB = ccnCCB;            
            queryCCB = "cdc.sp_ci_per_ct";
            ccbCount = 0; dwhCount = 0;
            tTime = new CBDate();
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);
            tTime = new CBDate();
            appEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT").Substring(0, 3).ToUpper(); //
        }

        public Task<DataSet> PersonCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("PerCount");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDWH = "SELECT COUNT(SRC_PER_ID) DTW_Count FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between and UTC 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, queryDWH, dtwParameters.ToArray());
                    evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, queryCCB, cdcParameters.ToArray());

                    
                    ccbCount = evalDataCCB.Tables[0].AsEnumerable().Select(r => r.Field<string>("PER_ID")).Distinct().Count();
                    dwhCount = Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]);

                    bool stateTest = (ccbCount > 0 && dwhCount > 0 && (ccbCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPer, TestResult.TestGenericName.Distinct, ccbCount, dwhCount, appEnv, !stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = (stateTest ? 3 : 2);
                    dr["testID"] = 9;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = ccbCount;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);

                    return myResponse;
                }
                catch (Exception e)
                { 
                    return Extensions.getResponseWithErrorMsg("Error Reading PER Count from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }


        /// <summary>
        /// Compare the New Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>   
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result </returns>
        public Task<DataSet> NewPersonCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("NewPersonCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    //This query reflect the "Universe" all the SRC_PER_ID for the evaluated date                  

                    queryDWH = ";WITH "
                             + "Universe AS (SELECT * FROM dwadm2.CD_PER), "
                             + "AffecPER AS ( "
                             + "SELECT DISTINCT SRC_PER_ID "
                             + "FROM dwadm2.CD_PER "
                             + "WHERE DATA_LOAD_DTTM "
                             + "BETWEEN @startDate AND @endDate "
                             + ") "
                             + "SELECT "
                             + "U.SRC_PER_ID, COUNT(DISTINCT U.SRC_PER_ID) [Count] "
                             + "FROM Universe U "
                             + "INNER JOIN AffecPER AA ON U.SRC_PER_ID = AA.SRC_PER_ID "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " "
                             + "GROUP BY U.SRC_PER_ID "
                             + "HAVING COUNT(U.SRC_PER_ID) < 2 ";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, queryDWH, dtwParameters.ToArray());
                    evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, queryCCB, cdcParameters.ToArray());

                    var cdcFilteredRows = from row in evalDataCCB.Tables[0].AsEnumerable()
                                          where row.Field<Int32>("toInsert") == 1 && (row.Field<Int32>("__$operation") == 2 || row.Field<Int32>("__$operation") == 4)
                                          select row;

                    ccbCount = cdcFilteredRows.Count();
                    dwhCount = evalDataDWH.Tables[0].Rows.Count;

                    bool stateTest = (ccbCount > 0 && dwhCount > 0 && (ccbCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPer, TestResult.TestGenericName.New, ccbCount, dwhCount, appEnv, !stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = (stateTest ? 3 : 2);
                    dr["testID"] = 10;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = ccbCount;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                    
                    return myResponse;
                }
                catch (Exception e)
                {
                    return Extensions.getResponseWithErrorMsg("Error Reading New PER Count from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }


        /// <summary>
        /// Compare the updated Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param> 
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result </returns>
        public Task<DataSet> UpdatedPersonCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("UpdatedCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDWH = ";WITH "
                             + "Universe AS (SELECT * FROM dwadm2.CD_PER), "
                             + "AffecPER AS (SELECT DISTINCT SRC_PER_ID FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @endDate AND DATEADD(HOUR, 5, @endDate)) "
                             + "SELECT "
                             + "U.SRC_PER_ID, U.DATA_LOAD_DTTM, U.EFF_END_DTTM, U.EFF_START_DTTM, "
                             + "U.JOB_NBR,ROW_NUMBER() OVER(PARTITION BY U.SRC_PER_ID ORDER BY U.EFF_END_DTTM) as RankPos "
                             + "FROM Universe U "
                             + "INNER JOIN AffecPER AA ON U.SRC_PER_ID = AA.SRC_PER_ID "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " AND DATA_LOAD_DTTM < @endDate "
                             + "ORDER BY SRC_PER_ID, DATA_LOAD_DTTM ";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, queryDWH, dtwParameters.ToArray());
                    evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, queryCCB, cdcParameters.ToArray());

                    //LINQ query for Updated PER on CDC
                    var UpdatedPersonsOnCDC = from row in evalDataCCB.Tables[0].AsEnumerable()
                                               where row.Field<Int32>("toInsert") == 0 && row.Field<Int32>("__$operation") == 4
                                               select row;

                    ccbCount = UpdatedPersonsOnCDC.Count();
                    dwhCount = evalDataDWH.Tables[0].Rows.Count;

                    bool stateTest = (ccbCount > 0 && dwhCount > 0 && (ccbCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPer, TestResult.TestGenericName.Updated, ccbCount, dwhCount, appEnv, !stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = (stateTest ? 3 : 2);
                    dr["testID"] = 11;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = ccbCount;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                   
                    return myResponse;
                }
                catch (Exception e)
                {                    
                    return Extensions.getResponseWithErrorMsg("Error Reading Updated PER Count from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }

        /// <summary>
        /// Comparing the Daily PER_ID Distinct Count with the Historical PER_ID Distinct Count.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>              
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result</returns>
        public Task<DataSet> PersonCountVsMaxHistoric(DateTime startDate, DateTime endDate)
        {
            Int64 maxHistCountPersonID = 1000;

            myResponse = Extensions.getResponseStructure("GetCountDistinctPerOnDataLoad");           
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    string query = "SELECT COUNT(DISTINCT SRC_PER_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                     "FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, query, dtwParameters.ToArray());

                    dwhCount = (evalDataDWH.Tables[0].Rows.Count > 0) ? Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]) : 0;

                    bool stateTest = (dwhCount <= maxHistCountPersonID) && (dwhCount > 0) && (maxHistCountPersonID > 0);

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPer, TestResult.TestGenericName.DistinctVsHistoric, maxHistCountPersonID, dwhCount, appEnv, !stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = (stateTest ? 3 : 1);
                    dr["testID"] = 12;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = 0;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = maxHistCountPersonID;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                   
                    return myResponse;
                }
                catch (Exception e)
                {                   
                    return Extensions.getResponseWithErrorMsg("Error Reading Max Hist PER Count from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }

        /// <summary>
        /// This method take the evaDate and and compare with the average of the Daily Distinct Count of persons,
        /// taking a <paramref name="DAY_RANGE_TO_BE_EVALUATED"/> Days sample.
        /// </summary>
        /// <param name="DAY_RANGE_TO_BE_EVALUATED">Quantity of Days to be evaluated in the Sample Average(days are counted to the past)</param>
        /// <param name="evalDate">Evaluated Date</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result </returns>
        public Task<DataSet> StatisticalPersonEvaluation(DateTime evalDate, Int32 DAY_RANGE_TO_BE_EVALUATED, Double TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION)
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
                    List<StatisticalEvaluation> personEvaluation = new List<StatisticalEvaluation>();
                    StatisticalEvaluation statisticalEvaluation;

                    
                    for (var i = 0; i < evalrange.Count; i++)
                    {
                       
                        statisticalEvaluation = new StatisticalEvaluation();
                        statisticalEvaluation.IntialDate = evalrange[i].Date.AddDays(-1).AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EndDate = evalrange[i].Date.AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EvalDateIndex = i + 1;

                        cdcParameters = new List<SqlParameter>();

                        cdcParameters.Add(new SqlParameter("@startDate", statisticalEvaluation.IntialDate.ToString("yyyy-MM-dd HH:mm")));
                        cdcParameters.Add(new SqlParameter("@endDate", statisticalEvaluation.EndDate.ToString("yyyy-MM-dd HH:mm")));
                                               
                        evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, queryCCB, cdcParameters.ToArray());

                        statisticalEvaluation.CountValue = evalDataCCB.Tables[0].AsEnumerable().Select(r => r.Field<string>("PER_ID")).Distinct().Count();

                        personEvaluation.Add(statisticalEvaluation);
                        Thread.Sleep(1000);
                    }

                    //Computing the average of the Days
                    double averCountPer = personEvaluation.Average(item => item.CountValue);

                    //Distinct Acount Count
                    cdcParameters = new List<SqlParameter>();
                    cdcParameters.Add(new SqlParameter("@startDate", evalDate.Date.AddDays(-1).AddHours(10).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", evalDate.Date.AddHours(15).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));

                    evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, queryCCB, cdcParameters.ToArray());

                    //Distinct Acount count of Evaluated Day
                    ccbCount = evalDataCCB.Tables[0].AsEnumerable().Select(r => r.Field<string>("PER_ID")).Distinct().Count();

                    //Incremental
                    double incremIndicator = ((ccbCount - averCountPer) / averCountPer) * 100;

                    bool stateTest = (Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPer, TestResult.TestGenericName.Statistical, ccbCount, Convert.ToInt64(Math.Round(averCountPer)), appEnv, stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = (stateTest ? 1 : 3);
                    dr["testID"] = 13;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = ccbCount;
                    dr["DWHCount"] = 0;
                    dr["CCBAver"] = averCountPer;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                   
                    return myResponse;
                }
                catch (Exception e)
                {                    
                    return Extensions.getResponseWithErrorMsg("Error Reading Statistical PER Count from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }
    }
}
