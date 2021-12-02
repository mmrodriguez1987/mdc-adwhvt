﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;


namespace UnitTest.Model.DataWarehouse
{
    public class Account
    {
        private string _ccnDTW, _ccnCDC, _testFileName, queryCDC, queryDTW;      
        private DataSet myResponse, evalDataDTW, evalDataCDC = new DataSet();


        /// <summary>
        /// Initialize the account class with params required
        /// </summary>
        /// <param name="cnnDTW">conexion to Datawarehouse</param>
        /// <param name="ccnCDC">conexion to CDC Database</param>
        /// <param name="testFileName">File Name of the Test Result</param>
        public Account(string cnnDTW, string ccnCDC, string testFileName)
        {
            _ccnDTW = cnnDTW;
            _ccnCDC = ccnCDC;
            _testFileName = testFileName;
            queryDTW = "SELECT  COUNT (DISTINCT SRC_ACCT_ID) as DTW_Count " +
               " from [dwadm2].[CD_ACCT] where DATA_LOAD_DTTM BETWEEN  @startDate AND @endDate";
              

            queryCDC = "cdc.sp_ci_acct_ct";
        }

        /// <summary>
        /// Task Dataset Executer for Get Distinct Counts of Accounts on Tables ACCT
        /// </summary>
        /// <param name="startDate">Start Evaluated Date</param>
        /// <param name="endDate">End Evaluated Date</param>
        /// <returns></returns>
        public Task<DataSet> GetCountAccounts(DateTime startDate, DateTime endDate)
        {
            myResponse = Extensions.getResponseStructure("AcctCounts");
           

            return Task.Run(() =>
            {
                try
                {
                    List<SqlParameter> parameters = new List<SqlParameter>();

                    parameters.Add(new SqlParameter("@startDate", startDate.ToString("yyyy-MM-dd HH:mm")));
                    parameters.Add(new SqlParameter("@endDate", endDate.ToString("yyyy-MM-dd HH:mm")));

                    evalDataDTW = SqlHelper.ExecuteDataset(_ccnDTW, CommandType.Text, queryDTW, parameters.ToArray());
                    evalDataCDC = SqlHelper.ExecuteDataset(_ccnCDC, CommandType.StoredProcedure, queryCDC, parameters.ToArray());
                    
                    int cdcCount = evalDataCDC.Tables[0].DefaultView.ToTable(true, "ACCT_ID").Rows.Count;
                    int dtwCount = Convert.ToInt32(evalDataCDC.Tables[0].Rows[0][0]);

                    myResponse.Tables[0].Rows[0][0] = (cdcCount != dtwCount) ? "Warning" : "Test Passed";
                    myResponse.Tables[0].Rows[0][1] = "Count Distinct ACCT_ID on DTW and CDC";
                    myResponse.Tables[0].Rows[0][2] = "dw-ttdp: dwadm2.CD_ACCT | cdcProdcc: cdc.sp_ci_acct_ct";
                    myResponse.Tables[0].Rows[0][3] = (cdcCount != dtwCount) ? "Distinct ACCT_ID counts on both sides are different" : "Distinct ACCT_ID counts on both sides are congruent";
                    myResponse.Tables[0].Rows[0][4] = startDate.ToString("yyyy-MM-dd HH:mm");
                    myResponse.Tables[0].Rows[0][5] = endDate.ToString("yyyy-MM-dd HH:mm");                    
                    myResponse.Tables[0].Rows[0][6] = cdcCount;
                    myResponse.Tables[0].Rows[0][7] = dtwCount;
                    myResponse.Tables[0].Rows[0][8] = DateTime.Now.ToString("yyyy-MM-dd HH:mm");

                    CSV logFile = new CSV(_testFileName + ".csv");
                    logFile.writeNewOrExistingFile(myResponse.Tables[0]);                  

                    return myResponse;

                }
                catch (Exception e)
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString());
                    return myResponse;
                }
            });
        }
    }
}