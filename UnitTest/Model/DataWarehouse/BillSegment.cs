using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;
using UnitTest.Model.ValidationTest;
using Tools;

namespace UnitTest.Model.DataWarehouse
{
    public class BillSegment
    {
        private string _ccnDTW, _ccnCDC, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();
        private CBDate tTime;
        private Historical historical;
        private TestResult results;

        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>       
        public BillSegment(string cnnDTW, string ccnCDC, string ccnValTest)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;           
            queryCDC = "cdc.sp_ci_bseg_ct";
            historical = new Historical(ccnValTest);
            results = new TestResult(ccnValTest);
            tTime = new CBDate();
        }

        /// <summary>       
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables BSEG
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> DistinctBillSegmentCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("BilledSegmentCount");
            string testInterpretation;
            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(DISTINCT SRC_BSEG_ID) DTW_Count FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.AddHours(10).ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(10).ToString("yyyy-MM-dd HH:mm")));


                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_BSEG_ID) DTW_Count from dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "BSEG_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    if (cdcCount == 0 && dtwCount == 0)
                        testInterpretation = "There is no data to evaluate for Distinct BSEG_ID, CDC=0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount == 0)
                        testInterpretation = "There is no incremental data to evaluate for Distinct BSEG_ID, CDC > 0 and DTW=0";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount != dtwCount))
                        testInterpretation = "Distinct BSEG_ID count on both sides are different";
                    else if (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount))
                        testInterpretation = "Distinct BSEG_ID count on both sides are equals";
                    else
                        testInterpretation = "Error";

                    myResponse.Tables[0].Rows[0][0] = (cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? "OK!" : "Failed";
                    myResponse.Tables[0].Rows[0][1] = "Distinct Bill Segment Count";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CF_BILLED_USAGE | cdcProdcc: cdc.sp_ci_bseg_ct";
                    myResponse.Tables[0].Rows[0][3] = testInterpretation;
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Val=> T.N: " + myResponse.Tables[0].Rows[0][1].ToString() + ", T.R: " + testInterpretation;

                    // 195 -SRC_BSEG_ID, 1- Distinct
                    historical.recordHistorical(195, 1, dtwCount, endDate);

                    //recording on DB
                    results.Description = testInterpretation;
                    results.StartDate = startDate;
                    results.EndDate = endDate;
                    results.StateID = (short)((cdcCount > 0 && dtwCount > 0 && (cdcCount == dtwCount)) ? 3 : 2);
                    results.TestDate = DateTime.Now;
                    results.TestID = 34; // Compare Dsitinct ACCT
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

    }
}
