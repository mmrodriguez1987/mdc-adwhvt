using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.DataConversion;
using System.Linq;
using Tools;
using Tools.Statistics;
using UnitTest.Model.ValidationTest;
using System.Threading;

namespace UnitTest.Model.DataWarehouse
{
    public class Premise
    {
        private string _ccnDTW, _ccnCDC, queryCDC, queryDTW, appEnv;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();
        private CBDate tTime;
        private Historical historical;
        private TestResult results;



        /// <summary>
        /// Initialize the Premise class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param> 
        public Premise(string cnnDTW, string ccnCDC, string ccnValTest)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            //Initializa Historical Indicators Computing
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);
            queryCDC = "cdc.sp_ci_prem_ct";
            tTime = new CBDate();
            appEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT").Substring(0, 3).ToUpper(); // DEV or PRO
        }

        /// <summary>
        /// Task Dataset Executer for Get Distinct Premise of Premise on Tables PREM
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet>PremiseCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("PremiseCount");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(SRC_PREM_ID) DTW_Count FROM dwadm2.CD_PREM WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between and UTC 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "PREM_ID").Rows.Count;
                    int cdcCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("PREM_ID")).Distinct().Count();
                    int dwhCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    bool stateTest = (cdcCount >= 0 && dwhCount >= 0 && (cdcCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPrem, TestResult.TestGenericName.Distinct, cdcCount, dwhCount, appEnv, !stateTest);


                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "PREM Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PREM | cdcProdcc: cdc.sp_ci_prem_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dwhCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;


                    if (saveResult)
                    {
                        // 2 -SRC_PREM_ID, 1- Distinct
                        historical.recordHistorical(96, 1, dwhCount, endDate);

                        //recording on DB
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 14; // Compare Dsitinct PREM
                        results.recordUntitValidationTest(cdcCount, dwhCount);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error Saving PREM Count Test Result: " + results.Error.Substring(0, 200));
                            myResponse.Tables[0].Rows[0][11] = ("Error Saving PREM Count Test Result: " + results.Error.Substring(0, 200));
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error Reading PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error Reading PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// In this proces we verify that the same New Premise ID in CDC are also on DTW
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> NewPremiseCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("NewPremiseCount");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    //This query reflect the "Universe" all the SRC_PREM_ID for the evaluated date                                      
                    queryDTW = ";WITH "
                             + "Universe AS (SELECT * FROM dwadm2.CD_PREM), "
                             + "AffecPREM AS ( "
                             + "SELECT DISTINCT SRC_PREM_ID "
                             + "FROM dwadm2.CD_PREM "
                             + "WHERE DATA_LOAD_DTTM "
                             + "BETWEEN @startDate AND @endDate "
                             + ") "
                             + "SELECT "
                             + "U.SRC_PREM_ID, COUNT(DISTINCT U.SRC_PREM_ID) [Count] "
                             + "FROM Universe U "
                             + "INNER JOIN AffecPREM AA ON U.SRC_PREM_ID = AA.SRC_PREM_ID "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " "
                             + "GROUP BY U.SRC_PREM_ID "
                             + "HAVING COUNT(U.SRC_PREM_ID) < 2 ";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW


                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());


                    var cdcFilteredRows = from row in evalDataCDC.Tables[0].AsEnumerable()
                                          where row.Field<Int32>("toInsert") == 1 && (row.Field<Int32>("__$operation") == 2 || row.Field<Int32>("__$operation") == 4)
                                          select row;

                    int cdcCount = cdcFilteredRows.Count();
                    int dwhCount = evalDataDTW.Tables[0].Rows.Count;

                    bool stateTest = (cdcCount >= 0 && dwhCount >= 0 && (cdcCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPrem, TestResult.TestGenericName.New, cdcCount, dwhCount, appEnv, !stateTest);



                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "New PREM";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PREM | cdcProdcc: cdc.sp_ci_prem_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dwhCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {
                        historical.recordHistorical(96, 2, dwhCount, endDate);
                        //recording on DB 
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 15; // Compare Dsitinct PREM
                        results.recordUntitValidationTest(cdcCount, dwhCount);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error Saving New PREM Count Test Result: " + results.Error.Substring(0, 200));
                            myResponse.Tables[0].Rows[0][11] = ("Error Saving New PREM Count Test Result: " + results.Error.Substring(0, 200));
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error Reading New PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error Reading New PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// Compare the updated Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> UpdatedPremiseCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("UpdatedPremiseCount");
            String testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = ";WITH "
                             + "Universe AS(SELECT * FROM dwadm2.CD_PREM), "
                             + "AffecPREM AS( SELECT DISTINCT SRC_PREM_ID FROM dwadm2.CD_PREM WHERE DATA_LOAD_DTTM BETWEEN @endDate AND DATEADD(HOUR, 5, @endDate)) "
                             + "SELECT "
                             + "U.SRC_PREM_ID, COUNT(DISTINCT U.SRC_PREM_ID) [Count] "
                             + "FROM Universe U "
                             + "INNER JOIN AffecPREM AA ON U.SRC_PREM_ID = AA.SRC_PREM_ID "
                             + "WHERE JOB_NBR = " + endDate.ToString("yyyyMMddHH") + " "
                             + "GROUP BY U.SRC_PREM_ID "
                             + "HAVING COUNT(U.SRC_PREM_ID) > 1";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //LINQ query for Updated PREM on CDC
                    var UpdatedPremisesOnCDC = from row in evalDataCDC.Tables[0].AsEnumerable()
                                               where row.Field<Int32>("toInsert") == 0 && row.Field<Int32>("__$operation") == 4
                                               select row;

                    int cdcCount = UpdatedPremisesOnCDC.Count();
                    int dwhCount = evalDataDTW.Tables[0].Rows.Count;

                    bool stateTest = (cdcCount >= 0 && dwhCount >= 0 && (cdcCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPrem, TestResult.TestGenericName.Updated, cdcCount, dwhCount, appEnv, !stateTest);

                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Updated PREM";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PREM | cdcProdcc: cdc.sp_ci_prem_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dwhCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {
                        // 96 -SRC_prem_ID, 3-Updated
                        historical.recordHistorical(96, 3, dwhCount, endDate);

                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 2);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 16;
                        results.recordUntitValidationTest(cdcCount, dwhCount);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error Saving Updated PREM Count Test Result: " + results.Error.Substring(0, 200));
                            myResponse.Tables[0].Rows[0][11] = ("Error Saving Updated PREM Count Test Result: " + results.Error.Substring(0, 200));
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error Reading Updated PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error Reading Updated PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// Comparing the Daily PREM_ID Distinct Count with the Historical PREM_ID Distinct Count.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>       
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        /// <returns></returns>
        public Task<DataSet> PremCountVsMaxHist(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            Int64 maxHistoricalCountPremID = historical.GetMaximunHistorical(96, 1);

            myResponse = Extensions.getResponseStructure("TotalPremCountVsMaxHist");
            int dwhCount;
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    string query = "SELECT COUNT(DISTINCT SRC_PREM_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek " +
                     "FROM dwadm2.CD_PREM WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    dwhCount = (evalDataDTW.Tables[0].Rows.Count > 0) ? Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]) : 0;

                    bool stateTest = (dwhCount <= maxHistoricalCountPremID) && (dwhCount > 0) && (maxHistoricalCountPremID > 0);

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPrem, TestResult.TestGenericName.DistinctVsHistoric, maxHistoricalCountPremID, dwhCount, appEnv, !stateTest);

                    myResponse.Tables[0].Rows[0][0] = stateTest ? "OK!" : "Warning";
                    myResponse.Tables[0].Rows[0][1] = "PREM Count vs Max Historic Count";
                    myResponse.Tables[0].Rows[0][2] = "SRC_PREM_ID";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = maxHistoricalCountPremID;
                    myResponse.Tables[0].Rows[0][7] = dwhCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = query;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {
                        //recording on DB                                     
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)(stateTest ? 3 : 1);
                        results.CalcDate = endDate;
                        results.TestID = 17; // Compare Dsitinct PREM Over the maximun
                        results.recordHistoricalValidationTest(dwhCount);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error Saving Max Hist PREM Count Test Result: " + results.Error.Substring(0, 200));
                            myResponse.Tables[0].Rows[0][11] = ("Error Saving Max Hist PREM Count Test Result: " + results.Error.Substring(0, 200));
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error Reading Max Hist PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error Reading Max Hist PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }

        /// <summary>
        /// This method take the evaDate and and compare with the average of the Daily Distinct Prem of Premise,
        /// taking a <paramref name="DAY_RANGE_TO_BE_EVALUATED"/> Days sample.
        /// </summary>
        /// <param name="DAY_RANGE_TO_BE_EVALUATED">Quantity of Days to be evaluated in the Sample Average(days are counted to the past)</param>
        /// <param name="evalDate">Evaluated Date</param>        
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> StatisticalPremiseEvaluation(DateTime evalDate, Int32 DAY_RANGE_TO_BE_EVALUATED, Double TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION, Boolean saveResult)
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
                    List<StatisticalEvaluation> PremiseEvaluation = new List<StatisticalEvaluation>();
                    StatisticalEvaluation statisticalEvaluation;

                    //Iterate the List to add the Hour to the Dates, and the Distinct Count of Premise
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

                        statisticalEvaluation.CountValue = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("PREM_ID")).Distinct().Count();

                        PremiseEvaluation.Add(statisticalEvaluation);
                        Thread.Sleep(1000);
                    }

                    //Computing the average of the Days
                    double averCountPremise = PremiseEvaluation.Average(item => item.CountValue);


                    //Distinct Acount Count
                    cdcParameters = new List<SqlParameter>();
                    cdcParameters.Add(new SqlParameter("@startDate", evalDate.Date.AddDays(-1).AddHours(10).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", evalDate.Date.AddHours(15).AddMinutes(00).ToString("yyyy-MM-dd HH:mm")));

                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    //Premise count of Evaluated Day
                    int evaluatedCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("PREM_ID")).Distinct().Count();

                    //Incremental
                    double incremIndicator = ((evaluatedCount - averCountPremise) / averCountPremise) * 100;


                    bool stateTest = (Math.Abs(incremIndicator) > (TOLERANCE_PERCENTAGE_IN_AVERAGE_VARIATION * 100));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.DimPrem, TestResult.TestGenericName.Statistical, evaluatedCount, Convert.ToInt64(Math.Round(averCountPremise)), appEnv, stateTest);


                    myResponse.Tables[0].Rows[0][0] = stateTest ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Stat Aver PREM Count";
                    myResponse.Tables[0].Rows[0][2] = "cdcProdcc: cdc.sp_ci_prem_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = averCountPremise;
                    myResponse.Tables[0].Rows[0][7] = evaluatedCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = queryCDC;
                    myResponse.Tables[0].Rows[0][10] = evalDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    if (saveResult)
                    {
                        //columnID: 3-SRC_PREM_ID, indicatorType: 6-Average Weekly
                        historical.recordHistorical(96, 6, averCountPremise, evalDate);

                        //recording on DB                  
                        results.Description = testInterpretation;
                        results.StartDate = evalDate;
                        results.EndDate = evalDate;
                        results.StateID = (short)(stateTest ? 1 : 3);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 18;
                        results.recordStatisticalValidationTest(averCountPremise, evaluatedCount);
                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error Saving Statistical PREM Count Test Result: " + results.Error.Substring(0, 200));
                            myResponse.Tables[0].Rows[0][11] = ("Error Saving Statistical PREM Count Test Result: " + results.Error.Substring(0, 200));
                        }
                    }
                    return myResponse;
                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error Reading Statistical PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    myResponse.Tables[0].Rows[0][11] = ("Error Reading Statistical PREM Count from CCB: " + e.ToString().Substring(0, 198));
                    return myResponse;
                }
            });
        }
    }
}
