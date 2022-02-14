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
    public class ServiceAgreement
    {
        private string _ccnDTW, _ccnCDC,queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();


        /// <summary>
        /// Initialize the Premise class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>        
        public ServiceAgreement(string cnnDTW, string ccnCDC)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;            
            queryDTW = "SELECT COUNT(DISTINCT SRC_SA_ID) AS DTW_Count " +
               " from dwadm2.CD_SA WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";
            queryCDC = "cdc.sp_ci_sa_ct";
        }

        /// <summary>       
        /// Task Dataset Execute for Get Distinct Counts of Persons on Tables PER
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> GetCountServiceAgreement(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("ModifiedServiceAgreement");

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_SA_ID) as DTW_Count FROM dwadm2.CD_SA WHERE DATA_LOAD_DTTM BETWEEN '" + startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, parameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, parameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "SA_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct SA_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_SA | cdcProd: cdc.sp_ci_sa_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct SA_ID counts on both sides are different" : "Distinct SA_ID counts on both sides are congruent";
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
