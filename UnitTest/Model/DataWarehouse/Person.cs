using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;
using System.Linq;

namespace UnitTest.Model.DataWarehouse
{
    public class Person
    {
        private string _ccnDTW, _ccnCDC, queryCDC, queryDTW;
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();


        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="testFileName">File Name of the Test Result</param>
        public Person(string cnnDTW, string ccnCDC)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;            
            queryCDC = "cdc.sp_ci_per_ct";
        }

        /// <summary>       
        /// Task Dataset Execute for Get Distinct Counts of Persons on Tables PER
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> UniquePersonsCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("ModifiedPerson");

            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT COUNT(DISTINCT SRC_PER_ID) AS DTW_Count FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT COUNT(DISTINCT SRC_PER_ID) as DTW_Count FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + "'";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "PER_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataDTW.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct PER_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PER | cdcProd: cdc.sp_ci_per_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct PER_ID counts on both sides are different" : "Distinct PER_ID counts on both sides are congruent";
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

        /// <summary>
        /// Compare the New Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> NewPersonsCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("NewPersonCounts");

            return Task.Run(() =>
            {
                try
                {
                    queryDTW = "SELECT D.SRC_PER_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_PER_ID FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) AS T INNER JOIN dwadm2.CD_PER D ON D.SRC_PER_ID=T.SRC_PER_ID";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT D.SRC_PER_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_PER_ID FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_PER D ON D.SRC_PER_ID=T.SRC_PER_ID";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var NewAccountsOnDTW = (
                                        from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                        join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                        on cdc.Field<string>("PER_ID") equals dtw.Field<string>("SRC_PER_ID")
                                        where cdc.Field<Int32>("toInsert") == 1 && (cdc.Field<Int32>("__$operation") == 2 || cdc.Field<Int32>("__$operation") == 4)
                                        select new
                                        {
                                            SRC_PER_ID = dtw.Field<string>("SRC_PER_ID")
                                        }).ToList();

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=1 AND ([__$operation]=2 OR [__$operation]=4)").Length;
                    int dtwCount = NewAccountsOnDTW.Count();

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count of New Persons";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PER | cdcProdcc: cdc.sp_ci_per_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "New Records Counts on PER are different on CDC and DTWH" : "New Records Count on PER are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

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


        /// <summary>
        /// Compare the updated Record Count between datawarehouse and CDC
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public Task<DataSet> UpdatedPersonsCount(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("UpdatedAccountCounts");

            return Task.Run(() =>
            {
                try
                {

                    queryDTW = "SELECT D.SRC_PER_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_PER_ID FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN @startDate AND @endDate) AS T INNER JOIN dwadm2.CD_PER D ON D.SRC_PER_ID=T.SRC_PER_ID";

                    List<SqlParameter> cdcParameters = new List<SqlParameter>();
                    List<SqlParameter> dtwParameters = new List<SqlParameter>();

                    cdcParameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    cdcParameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    dtwParameters.Add(new SqlParameter("@startDate", endDate.ToString("yyyy-MM-dd HH:mm")));
                    dtwParameters.Add(new SqlParameter("@endDate", endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm")));

                    string interpoledQueryDTW = "SELECT D.SRC_PER_ID, D.DATA_LOAD_DTTM, D.EFF_START_DTTM, D.EFF_END_DTTM FROM (SELECT SRC_PER_ID FROM dwadm2.CD_PER WHERE DATA_LOAD_DTTM BETWEEN '" + endDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + endDate.AddHours(5).ToString("yyyy-MM-dd HH:mm") + ") AS T INNER JOIN dwadm2.CD_PER D ON D.SRC_PER_ID=T.SRC_PER_ID";

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, dtwParameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, cdcParameters.ToArray());

                    var UpdatedPersonsOnDTW = (
                                        from cdc in evalDataCDC.Tables[0].AsEnumerable()
                                        join dtw in evalDataDTW.Tables[0].AsEnumerable()
                                        on cdc.Field<string>("PER_ID") equals dtw.Field<string>("SRC_PER_ID")
                                        where cdc.Field<Int32>("toInsert") == 0 && cdc.Field<Int32>("__$operation") == 4
                                        select new
                                        {
                                            SRC_PER_ID = dtw.Field<string>("SRC_PER_ID")
                                        }).ToList();

                    int cdcCount = evalDataCDC.Tables[0].Select("toInsert=0 AND [__$operation]=4").Length;
                    int dtwCount = UpdatedPersonsOnDTW.Count();

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Failed" : "OK!";
                    myResponse.Tables[0].Rows[0][1] = "Count of Updated Persons";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_PER | cdcProdcc: cdc.sp_ci_per_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Updated Records Counts on PER are different on CDC and DTWH" : "Updated Records Count on PER are congruents on CDC and DTWH";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = "EXEC " + queryCDC + " @startDate='" + startDate.ToString("yyyy-MM-dd HH:mm") + "', @endDate= '" + endDate.ToString("yyyy-MM-dd HH:mm") + "'";
                    myResponse.Tables[0].Rows[0][9] = interpoledQueryDTW;
                    myResponse.Tables[0].Rows[0][10] = endDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][11] = "ADTWH-Validation => Test Name: " + myResponse.Tables[0].Rows[0][1].ToString() + ", Test Result: " + myResponse.Tables[0].Rows[0][0].ToString();

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
