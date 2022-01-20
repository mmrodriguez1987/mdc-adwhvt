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
    public class BillSegment
    {
        private string _ccnDTW, _ccnCDC, _testFileName, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();


        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="testFileName">File Name of the Test Result</param>
        public BillSegment(string cnnDTW, string ccnCDC, string testFileName)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            _testFileName = testFileName;
            queryCDC = "cdc.sp_ci_bseg_ct";
        }

        /// <summary>       
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables BSEG
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> UniqueBillSegmentCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("BilledSegmentCount");

            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(DISTINCT SRC_BSEG_ID) DTW_Count FROM dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));


                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_BSEG_ID) DTW_Count from dwadm2.CF_BILLED_USAGE WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "BSEG_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct BSEG_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CF_BILLED_USAGE | cdcProdcc: cdc.sp_ci_bseg_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct BSEG_ID counts on both sides are different" : "Distinct BSEG_ID counts on both sides are congruent";
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
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString().Substring(0,160));
                    myResponse.Tables[0].Rows[0][11] = ("Error: " + e.ToString().Substring(0,160));
                    return myResponse;
                }
            });
        }

    }
}
