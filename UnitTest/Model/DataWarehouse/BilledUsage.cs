using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.DataConversion;
using UnitTest.Model.ValidationTest;
using Tools;
using System.Linq;

namespace UnitTest.Model.DataWarehouse
{
    public class BilledUsage    
    {
        private string _ccnDTW, _ccnCDC, testInterpretation, sp_ccb_bseg, queryDWH, appEnv;        
        private DataSet myResponse = new DataSet(), evalDataDWH = new DataSet(), evalDataCDC = new DataSet();
        private CBDate tTime;
        private Historical historical;
        private TestResult results;
        private int dtwCount = 0;

        /// <summary>
        /// Initialize the Billed Usage class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="ccnValTest">conexion to Validation Test Database</param>       
        public BilledUsage(string cnnDTW, string ccnCDC, string ccnValTest)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);           
            sp_ccb_bseg = "cdc.sp_ci_bseg_ct";
            tTime = new CBDate();
            appEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT").Substring(0, 3).ToUpper(); // DEV or PRO
        }

        #region Business Validations Rules
        /// <summary>
        /// Check if there are any bills generated on weekend or Holiday
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> BillsGeneratedOnWeekend(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("BillsGeneratedOnWeekend");
            string query = "SELECT DISTINCT B.SRC_BILL_ID FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_BillDate D ON B.BILL_DATE_KEY=D.BillDateKey " +
                "WHERE B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate AND BillWorkDayCode = 0";
            
            startDate = startDate.AddHours(5); // adding the dates to UTC
            endDate = endDate.AddHours(5);  ///adding the hours to UTC
            List<string> affectedIDs = new List<string>();

            return Task.Run(() =>
            {
                try
                {                   
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();                   

                    dtwParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    String interpoledQuery = "SELECT B.SRC_BILL_ID, D.BillDayofWeek, B.UDDGEN1 BillDate  " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_BillDate D ON B.BILL_DATE_KEY=D.BillDateKey " +
                        "WHERE B.DATA_LOAD_DTTM BETWEEN '"+ endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '"+ endDate.ToString("yyyy-MM-dd HH:mm") + "' AND BillWorkDayCode = 0";

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    dtwCount = evalDataDWH.Tables[0].Rows.Count; 
                   
                    testInterpretation = (dtwCount > 0) ? ("There are " + dtwCount + " bills that were generated on weekends or holidays") : "No bills were generated on weekend or holidays";
                    
                    if (dtwCount > 0)            
                        foreach (DataRow row in evalDataDWH.Tables[0].Rows) affectedIDs.Add((string)Convert.ToString(row["SRC_BILL_ID"]));

                    myResponse.Tables[0].Rows[0][0] = (dtwCount > 0) ? "Failed" : "OK!"; 
                    myResponse.Tables[0].Rows[0][1] = "Bills Generated on Weekend";
                    myResponse.Tables[0].Rows[0][2] = "dt-ttdp: dwadm2.CF_BILLED_USAGE";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpoledQuery;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                    if (saveResult)
                    {
                        //recording on DB 
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)((dtwCount == 0) ? 3 : 2);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 1; // Bills Generated On Weekend
                        results.recordBilledUsageBusinessRuleValidationTest(dtwCount, "CF_BILLED_USAGE.SRC_BILL_ID", (dtwCount > 0) ? affectedIDs.ToArray() : null);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving NewAccountCounts test results in Account");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving NewAccountCounts test results in Account");
                        }
                    }
                    
                    return myResponse;
                }
                catch (Exception )
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on NewAccountCounts process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on NewAccountCounts process");
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
        public Task<DataSet> BillGeneratedOnWrongFiscalYear(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("BillGeneratedOnWrongFiscalYear");
            string query = "SELECT T.SRC_BILL_ID FROM (SELECT top(100) B.SRC_BILL_ID, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear " +
                "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                "WHERE (B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate)) AS T WHERE T.IsCorrectFiscalYear = 0 ";

            startDate = startDate.AddHours(5); // adding the dates to UTC
            endDate = endDate.AddHours(5);  ///adding the hours to UTC
            List<string> affectedIDs = new List<string>();

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    string interpolatedQuery = "SELECT T.SRC_BILL_ID FROM (SELECT top(100) B.SRC_BILL_ID, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear  " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "WHERE (B.DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "')) AS T ";

                    dtwCount = evalDataDWH.Tables[0].Rows.Count;

                    testInterpretation = (dtwCount > 0) ? ("There are " + dtwCount + " bills that were generated on wrong fiscal year") : "No bills were generated on wrong fiscal year";

                    if (dtwCount > 0)
                        foreach (DataRow row in evalDataDWH.Tables[0].Rows) affectedIDs.Add((string)Convert.ToString(row["SRC_BILL_ID"]));


                    myResponse.Tables[0].Rows[0][0] = (dtwCount > 0) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Get Bill Generated On Wrong Fiscal Year";
                    myResponse.Tables[0].Rows[0][2] = "dt-ttdp: dwadm2.CF_BILLED_USAGE";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");                   
                    myResponse.Tables[0].Rows[0][6] = -1;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "";
                    myResponse.Tables[0].Rows[0][9] = interpolatedQuery + "IsCorrectFiscalYear = 0";
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;                   

                    if (saveResult)
                    {
                        //recording on DB 
                        results.Description = testInterpretation;
                        results.StartDate = startDate;
                        results.EndDate = endDate;
                        results.StateID = (short)((dtwCount == 0) ? 3 : 2);
                        results.CalcDate = DateTime.Now;
                        results.TestID = 2; // Bills Generated On Wrong Fiscal Year
                        results.recordBilledUsageBusinessRuleValidationTest(dtwCount, "CF_BILLED_USAGE.SRC_BILL_ID", (dtwCount > 0) ? affectedIDs.ToArray() : null);

                        // if there re any error on recording db
                        if (!String.IsNullOrEmpty(results.Error))
                        {
                            myResponse.Tables[0].Rows[0][3] = ("Error saving NewAccountCounts test results in Account");
                            myResponse.Tables[0].Rows[0][11] = ("Error saving NewAccountCounts test results in Account");
                        }
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
        /// Comparing the Daily BILL_ID Distinct Count with the Historical ILL_ID Distinct Count.       
        /// </summary>
        /// <param name="startDate">Initial Evaluated Data</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <param name="BU_MAX_COUNT_DISTINCT_BILL_IDs">Maximun Histic Count of Discint bill_id </param>
        /// <returns></returns>
        public Task<DataSet> BillCountVsMaxHistoric(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            Int32 BU_MAX_COUNT_DISTINCT_BILL_IDs = 100000;
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

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, query, dtwParameters.ToArray());

                    int dtwCount = Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]);

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

        #endregion

        #region Data Validations Test
        /// <summary>       
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables BSEG
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> BillSegmentCount(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            myResponse = Extensions.getResponseStructure("BilledSegmentCount");
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDWH = "SELECT COUNT(DISTINCT SRC_BSEG_ID) DTW_Count FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    //For DTW use a hour range because the data load is on the morning one time between and UTC 
                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm"))); //Take the variability on the DATA_LOAD_TIME on DTW

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDWH, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, sp_ccb_bseg, cdcParameters.ToArray());

                    
                    int cdcCount = evalDataCDC.Tables[0].AsEnumerable().Select(r => r.Field<string>("BSEG_ID")).Distinct().Count();
                    int dwhCount = Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]);

                    bool stateTest = (cdcCount >= 0 && dwhCount >= 0 && (cdcCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.FactBilledUsage, TestResult.TestGenericName.BillSegmentCountOnFact, cdcCount, dwhCount, appEnv, !stateTest);


                    myResponse.Tables[0].Rows[0][0] = (cdcCount > 0 && dwhCount > 0 && (cdcCount == dwhCount)) ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Bill Segment Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CF_BILLED_USAGE | cdcProdcc: cdc.sp_ci_bseg_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dwhCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + sp_ccb_bseg + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = queryDWH;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = testInterpretation;

                    // 195 -SRC_BSEG_ID, 1- Distinct
                    historical.recordHistorical(243, 1, dwhCount, endDate);

                    //recording on DB
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((cdcCount > 0 && dwhCount > 0 && (cdcCount == dwhCount)) ? 3 : 2);
                    results.CalcDate = DateTime.Now;
                    results.TestID = 34; // Compare Dsitinct ACCT
                    results.recordUntitValidationTest(cdcCount, dwhCount);

                    // if there re any error on recording db
                    if (!String.IsNullOrEmpty(results.Error))
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error saving BillSegmentCountOnFact test results in Account");
                        myResponse.Tables[0].Rows[0][11] = ("Error saving BillSegmentCountOnFact test results in Account");
                    }
                    return myResponse;
                }
                catch (Exception)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Exception error on BillSegmentCountOnFact process");
                    myResponse.Tables[0].Rows[0][11] = ("Exception error on BillSegmentCountOnFact process");
                    return myResponse;
                }
            });
        }
        #endregion


    }
}