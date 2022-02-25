using System;
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
    public class UnitOfMeasure
    {
        private string _ccnDTW, _ccnCDC, queryCDC, queryDTW, appEnv;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();
        private CBDate tTime;
        private Historical historical;
        private TestResult results;
        private int cdcCountTotal, cdcCountNew, cdcCountUpdated, ccbCountTotal, ccbCountNew, ccbCountUpdated;



        /// <summary>
        /// Initialize the class with params required
        /// </summary>
        /// <param name="cnnDTW"></param>
        /// <param name="ccnCDC"></param>
        /// <param name="ccnValTest"></param>
        public UnitOfMeasure(string cnnDTW, string ccnCDC, string ccnValTest)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            queryCDC = "cdc.sp_ci_uom_ct";
            tTime = new CBDate();
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);
            tTime = new CBDate();
            appEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT").Substring(0, 3).ToUpper(); //
        }

        public Task<DataSet> UOMCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("UOMCount");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(UOM_CD) DTW_Count FROM dwadm2.CD_UOM WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between and UTC 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());


                    cdcCountTotal = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("UOM_CD")).Distinct().Count();
                    ccbCountTotal = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    bool stateTest = (cdcCountTotal >= 0 && ccbCountTotal >= 0 && (cdcCountTotal == ccbCountTotal));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimUOM, TestResult.TestGenericName.Distinct, cdcCountTotal, ccbCountTotal, appEnv, !stateTest);



                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "UOM Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_UOM | cdcProdcc: cdc.sp_ci_uom_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCountTotal;
                    myResponse.Tables[0].Rows[0][7] = ccbCountTotal;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;


                    if (saveResult)
                    {                       
                        historical.recordHistorical(180, 1, ccbCountTotal, endDate);
                       
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.TestDate = DateTime.Now;
                        results.TestID = 40; 
                        results.recordUntitValidationTest(cdcCountTotal, ccbCountTotal);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving DistinctUOMCount test results in UOM");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving DistinctUOMCount test results in UOM");
                        }
                    }

                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error on DistinctUOMCount process: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error on DistinctUOMCount process: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }


        /// <summary>
        /// Compare the New Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>       
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns></returns>
        public Task<DataSet> NewUOMCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("NewUOMCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {                                    

                    queryDTW = ";WITH "
                             + "Universe AS (SELECT * FROM dwadm2.CD_UOM), "
                             + "AffecUOM AS ( "
                             + "SELECT DISTINCT UOM_CD "
                             + "FROM dwadm2.CD_UOM "
                             + "WHERE DATA_LOAD_DTTM "
                             + "BETWEEN @startDate AND @endDate "
                             + ") "
                             + "SELECT "
                             + "U.UOM_CD, COUNT(DISTINCT U.UOM_CD) [Count] "
                             + "FROM Universe U "
                             + "INNER JOIN AffecUOM AA ON U.UOM_CD = AA.UOM_CD "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " "
                             + "GROUP BY U.UOM_CD "
                             + "HAVING COUNT(U.UOM_CD) < 2 ";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); 

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var cdcFilteredRows = from row in evalDataCDC.Tables[0].AsEnumerable()
                                          where row.Field<Int32>("toInsert") == 1 && (row.Field<Int32>("__$operation") == 2 || row.Field<Int32>("__$operation") == 4)
                                          select row;

                    cdcCountNew = cdcFilteredRows.Count();
                    ccbCountNew = evalDataDTW.Tables[0].Rows.Count;

                    bool stateTest = (cdcCountNew >= 0 && ccbCountNew >= 0 && (cdcCountNew == ccbCountNew));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimUOM, TestResult.TestGenericName.New, cdcCountNew, ccbCountNew, appEnv, !stateTest);

                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "New UOM";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_UOM | cdcProdcc: cdc.sp_ci_uom_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCountNew;
                    myResponse.Tables[0].Rows[0][7] = ccbCountNew;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {
                        historical.recordHistorical(180, 2, ccbCountNew, endDate);
                        //recording on DB 
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.TestDate = DateTime.Now;
                        results.TestID = 41; 
                        results.recordUntitValidationTest(cdcCountNew, ccbCountNew);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving NewUOMCount test results in UOM");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving NewUOMCount test results in UOM");
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error on NewUOMCount process: " + e.ToString().Substring(0, 570));
                    myResponse.Tables[0].Rows[0][11] = ("Error on NewUOMCount process: " + e.ToString().Substring(0, 570));
                    return myResponse;
                }
            });
        }


        /// <summary>
        /// Compare the updated Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>       
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns></returns>
        public Task<DataSet> UpdatedUOMCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("UpdatedUOMCounts");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = ";WITH "
                             + "Universe AS (SELECT * FROM dwadm2.CD_UOM), "
                             + "AffecUOM AS (SELECT DISTINCT UOM_CD FROM dwadm2.CD_UOM WHERE DATA_LOAD_DTTM BETWEEN @endDate AND DATEADD(HOUR, 5, @endDate)) "
                             + "SELECT "
                             + "U.UOM_CD, U.DATA_LOAD_DTTM, "
                             + "U.JOB_NBR,ROW_NUMBER() OVER(PARTITION BY U.UOM_CD ORDER BY U.UPDATE_DTTM) as RankPos "
                             + "FROM Universe U "
                             + "INNER JOIN AffecUOM AA ON U.UOM_CD = AA.UOM_CD "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " AND DATA_LOAD_DTTM < @endDate "
                             + "ORDER BY UOM_CD, DATA_LOAD_DTTM ";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //LINQ query for Updated UOM on CDC
                    var UpdatedOnCDC = from row in evalDataCDC.Tables[0].AsEnumerable()
                                              where row.Field<Int32>("toInsert") == 0 && row.Field<Int32>("__$operation") == 4
                                              select row;

                    cdcCountUpdated = UpdatedOnCDC.Count();
                    ccbCountUpdated = evalDataDTW.Tables[0].Rows.Count;

                    bool stateTest = (cdcCountUpdated >= 0 && ccbCountUpdated >= 0 && (cdcCountUpdated == ccbCountUpdated));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimUOM, TestResult.TestGenericName.Updated, cdcCountUpdated, ccbCountUpdated, appEnv, !stateTest);

                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Updated UOM";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_UOM | cdcProdcc: cdc.sp_ci_uom_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCountUpdated;
                    myResponse.Tables[0].Rows[0][7] = ccbCountUpdated;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {                        
                        historical.recordHistorical(180, 3, ccbCountUpdated, endDate);

                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.TestDate = DateTime.Now;
                        results.TestID = 42;
                        results.recordUntitValidationTest(cdcCountUpdated, ccbCountUpdated);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving UpdatedUOMCount test results in UOM");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving UpdatedUOMCount test results in UOM");
                        }
                    }

                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error on UpdatedUOMCount process: " + e.ToString().Substring(0, 570));
                    myResponse.Tables[0].Rows[0][11] = ("Error on UpdatedUOMCount process: " + e.ToString().Substring(0, 570));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// Comparing the Daily Count with the Historical Count.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>       
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> UOMCountVsMaxHistoric(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            Int64 maxHistCountUOM_CD = historical.GetMaximunHistorical(180, 1);

            myResponse = Extensions.getResponseStructure("UOMCountVsMaxHistoric");
            int dtwCount;
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    string query = "SELECT COUNT(DISTINCT UOM_CD) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                     "FROM dwadm2.CD_UOM WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    dtwCount = (evalDataDTW.Tables[0].Rows.Count > 0) ? Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]) : 0;

                    bool stateTest = (dtwCount <= maxHistCountUOM_CD) && (dtwCount > 0) && (maxHistCountUOM_CD > 0);

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimUOM, TestResult.TestGenericName.DistinctVsHistoric, maxHistCountUOM_CD, dtwCount, appEnv, !stateTest);

                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Warning";
                    myResponse.Tables[0].Rows[0][1] = "UOM Count vs Max Historic Count";
                    myResponse.Tables[0].Rows[0][2] = "UOM_CD";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = maxHistCountUOM_CD;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = query;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {                                                           
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 1);
                        results.TestDate = endDate;
                        results.TestID = 43; 
                        results.recordHistoricalValidationTest(dtwCount);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving UomCountVsMaxHist test results in UOM");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving UomCountVsMaxHist test results in UOM");
                        }
                    }

                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error on UomCountVsMaxHist process: " + e.ToString());
                    myResponse.Tables[0].Rows[0][11] = ("Error on UomCountVsMaxHist process: " + e.ToString());
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// This method take the evaDate and and compare with the average of the Daily Distinct Count of UOM,
        /// taking a <paramref name="DAY_RANGE_TO_BE_EVALUATED"/> Days sample.
        /// </summary>
        /// <param name="DAY_RANGE_TO_BE_EVALUATED">Quantity of Days to be evaluated in the Sample Average(days are counted to the past)</param>
        /// <param name="evalDate">Evaluated Date</param>        
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> StatisticalUOMEvaluation(DateTime evalDate, Int32 DAY_RANGE_TO_BE_EVALUATED, Double TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION, Boolean saveResult)
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
                    List<StatisticalEvaluation> uomEvaluation = new List<StatisticalEvaluation>();
                    StatisticalEvaluation statisticalEvaluation;

                    //Iterate the List to add the Hour to the Dates, and the Distinct Count of UOM
                    for (var i = 0; i < evalrange.Count; i++)
                    {

                        statisticalEvaluation = new StatisticalEvaluation();
                        statisticalEvaluation.IntialDate = evalrange[i].Date.AddDays(-1).AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EndDate = evalrange[i].Date.AddHours(10).AddMinutes(30);
                        statisticalEvaluation.EvalDateIndex = i + 1;

                        cdcParameters = new List<SqlParameter>();

                        cdcParameters.Add(new SqlParameter("@startDate", statisticalEvaluation.IntialDate.ToString("yyyy-MM-dd HH:mm")));
                        cdcParameters.Add(new SqlParameter("@endDate", statisticalEvaluation.EndDate.ToString("yyyy-MM-dd HH:mm")));

                        evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                        statisticalEvaluation.CountValue = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("UOM_CD")).Distinct().Count();

                        uomEvaluation.Add(statisticalEvaluation);
                        Thread.Sleep(1000);
                    }

                    //Computing the average of the Days
                    double averCount = uomEvaluation.Average(item => item.CountValue);


                    //Distinct Acount Count
                    cdcParameters = new List<SqlParameter>();
                    cdcParameters.Add(new SqlParameter("@startDate", evalDate.Date.AddDays(-1).AddHours(10).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", evalDate.Date.AddHours(15).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));

                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //Distinct Acount count of Evaluated Day
                    int evaluatedCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("UOM_CD")).Distinct().Count();

                    //Incremental
                    double incremIndicator = ((evaluatedCount - averCount) / averCount) * 100;


                    bool stateTest = (Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimUOM, TestResult.TestGenericName.Statistical, evaluatedCount, Convert.ToInt64(Math.Round(averCount)), appEnv, stateTest);


                    myResponse.Tables[0].Rows[0][0] = stateTest ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Stat Aver UOM Count";
                    myResponse.Tables[0].Rows[0][2] = "cdcProdcc: cdc.sp_ci_uom_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = averCount;
                    myResponse.Tables[0].Rows[0][7] = evaluatedCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = queryCDC;
                    myResponse.Tables[0].Rows[0][10] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {                        
                        historical.recordHistorical(180, 6, averCount, evalDate);                                        
                        results.Description = testInterpretation;
                        results.StartDate = evalDate;
                        results.EndDate = evalDate;
                        results.StateID = (short)(stateTest ? 1 : 3);
                        results.TestDate = DateTime.Now;
                        results.TestID = 44;
                        results.recordStatisticalValidationTest(averCount, evaluatedCount);
                        
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving StatisticalUOMEvaluation test results in UOM");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving StatisticalUOMEvaluation test results in UOM");
                        }
                    }

                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error on StatisticalUOMEvaluation process: " + e.ToString().Substring(0, 570));
                    myResponse.Tables[0].Rows[0][11] = ("Error on StatisticalUOMEvaluation process: " + e.ToString().Substring(0, 570));
                    return myResponse;
                }
            });
        }


    }
}
