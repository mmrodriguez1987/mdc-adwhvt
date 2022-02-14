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
    public class Premise
    {
        private string _ccnDTW, _ccnCDC, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();


        /// <summary>
        /// Initialize the Premise class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param> 
        public Premise(string cnnDTW, string ccnCDC)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;                 
            queryCDC = "cdc.sp_ci_prem_ct";
        }

        /// <summary>       
        /// Task Dataset Execute for Get Distinct Counts of PREM on Tables PREM
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> UniquePremisesCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("UniquePremises");

            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(DISTINCT SRC_PREM_ID) AS DTW_Count FROM dwadm2.CD_PREM WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_PREM_ID) as DTW_Count FROM dwadm2.CD_PREM WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "PER_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct PREM on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PREM | cdcProd: cdc.sp_ci_prem_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct PREM counts on both sides are different" : "Distinct PREM counts on both sides are congruent";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
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
    }
}
