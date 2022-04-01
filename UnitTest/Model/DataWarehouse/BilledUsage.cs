using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.DataConversion;
using UnitTest.Model.ValidationTest;
using System.Linq;

namespace UnitTest.Model.DataWarehouse
{
    public class BilledUsage    
    {
        private string _ccnDWH, _ccnCCB, testInterpretation, sp_ccb_bseg, queryDWH, appEnv;        
        private DataSet myResponse, evalDataDWH, evalDataCCB;
        private TestResult results;
        private int dwhCount;

        /// <summary>
        /// Initialize the Billed Usage class with params required
        /// </summary>
        /// <param name="cnnDWH">conexion to Datawarehouse</param>
        /// <param name="ccnCCB">conexion to CDC Database</param>
        /// <param name="ccnValTest">conexion to Validation Test Database</param>       
        public BilledUsage(string cnnDWH, string ccnCCB, string ccnValTest)
        {
            _ccnDWH = cnnDWH;
            _ccnCCB = ccnCCB;
            dwhCount = 0;            
            results = new TestResult(ccnValTest);           
            sp_ccb_bseg = "cdc.sp_ci_bseg_ct";
            appEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT").Substring(0, 3).ToUpper(); // DEV or PRO
        }

        #region Business Validations Rules
        /// <summary>
        /// Check if there are any bills generated on weekend or Holiday
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> BillsGeneratedOnWeekend(DateTime startDate, DateTime endDate)
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

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, query, dtwParameters.ToArray());

                    dwhCount = evalDataDWH.Tables[0].Rows.Count; 
                   
                    testInterpretation = (dwhCount > 0) ? ("There are " + dwhCount + " bills that were generated on weekends or holidays") : "No bills were generated on weekend or holidays";
                    
                    if (dwhCount > 0)            
                        foreach (DataRow row in evalDataDWH.Tables[0].Rows) affectedIDs.Add((string)Convert.ToString(row["SRC_BILL_ID"]));


                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = ((dwhCount == 0) ? 3 : 2);
                    dr["testID"] = 1;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = 0;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr); 
                    return myResponse;
                }
                catch (Exception e)
                {
                    return Extensions.getResponseWithErrorMsg("Error Reading BU: BillsGeneratedOnWeekend from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }
        /// <summary>
        /// Check if there are any Bill Generated on wron Fiscal Year.
        /// </summary>
        /// <param name="startDate">Initial Evaluated Date</param>
        /// <param name="endDate">Final Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> BillGeneratedOnWrongFiscalYear(DateTime startDate, DateTime endDate)
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

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, query, dtwParameters.ToArray());

                    string interpolatedQuery = "SELECT T.SRC_BILL_ID FROM (SELECT top(100) B.SRC_BILL_ID, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS IsCorrectFiscalYear  " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY " +
                        "WHERE (B.DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "')) AS T ";

                    dwhCount = evalDataDWH.Tables[0].Rows.Count;

                    testInterpretation = (dwhCount > 0) ? ("There are " + dwhCount + " bills that were generated on wrong fiscal year") : "No bills were generated on wrong fiscal year";

                    if (dwhCount > 0)
                        foreach (DataRow row in evalDataDWH.Tables[0].Rows) affectedIDs.Add((string)Convert.ToString(row["SRC_BILL_ID"]));

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = ((dwhCount == 0) ? 3 : 2);
                    dr["testID"] = 2;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = 0;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);                   
                    return myResponse;
                }
                catch (Exception e)
                {
                    return Extensions.getResponseWithErrorMsg("Error Reading BU: BillGeneratedOnWrongFiscalYear from CCB: " + e.ToString().Substring(0, 198));
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
        public Task<DataSet> BillCountVsMaxHistoric(DateTime startDate, DateTime endDate)
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

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, query, dtwParameters.ToArray());

                    int dtwCount = Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]);

                    string interpolatedQuery = " SELECT COUNT(DISTINCT SRC_BILL_ID) DwCount, CONVERT(VARCHAR,DATA_LOAD_DTTM,1) DATA_LOAD_DTTM, FORMAT(DATA_LOAD_DTTM,'dddd') DayofWeek FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN '"
                    + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CONVERT(VARCHAR, DATA_LOAD_DTTM,1), FORMAT(DATA_LOAD_DTTM,'dddd') ORDER BY DATA_LOAD_DTTM DESC";

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = dtwCount > BU_MAX_COUNT_DISTINCT_BILL_IDs ? 1 : 3;
                    dr["testID"] = 32;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = 0;
                    dr["DWHCount"] = dtwCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                                      
                    return myResponse;
                }
                catch (Exception e)
                {
                    return Extensions.getResponseWithErrorMsg("Error Reading BU: BillCountVsMaxHistoric from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }

        #endregion

        #region Data Validations Test
        /// <summary>       
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables BSEG
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>       
        /// <param name="saveResult">'True' to save test result on database, else 'false' to dont save its</param>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public Task<DataSet> BillSegmentCount(DateTime startDate, DateTime endDate)
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

                    evalDataDWH = SqlHelper.ExecuteDataset(_ccnDWH, CommandType.Text, queryDWH, dtwParameters.ToArray());
                    evalDataCCB = SqlHelper.ExecuteDataset(_ccnCCB, CommandType.StoredProcedure, sp_ccb_bseg, cdcParameters.ToArray());
                    
                    int cdcCount = evalDataCCB.Tables[0].AsEnumerable().Select(r => r.Field<string>("BSEG_ID")).Distinct().Count();
                    int dwhCount = Convert.ToInt32(evalDataDWH.Tables[0].Rows[0][0]);

                    bool stateTest = (cdcCount > 0 && dwhCount > 0 && (cdcCount == dwhCount));

                    testInterpretation = results.createMessageNotification(TestResult.BusinessStar.BU, TestResult.Entity.FactBilledUsage, TestResult.TestGenericName.BillSegmentCountOnFact, cdcCount, dwhCount, appEnv, !stateTest);

                    DataRow dr = myResponse.Tables[0].NewRow();
                    dr["stateID"] = stateTest ? 3 : 2;
                    dr["testID"] = 34;
                    dr["description"] = testInterpretation;
                    dr["startDate"] = startDate.ToString("yyyy-MM-dd HH:mm");
                    dr["endDate"] = endDate.ToString("yyyy-MM-dd HH:mm");
                    dr["CCBCount"] = cdcCount;
                    dr["DWHCount"] = dwhCount;
                    dr["CCBAver"] = 0;
                    dr["CCBMax"] = 0;
                    dr["calcDate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows.Add(dr);
                  
                    return myResponse;
                }
                catch (Exception e)
                {
                    return Extensions.getResponseWithErrorMsg("Error Reading BU: BillSegmentCountOnFact from CCB: " + e.ToString().Substring(0, 198));
                }
            });
        }

        #endregion
    }
}