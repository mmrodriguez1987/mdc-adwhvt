using DBHelper.SqlHelper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;


namespace UnitTest.Model
{
    public class IncrLoadTest
    {
        private SqlHelper helper;
        private readonly ILogger<IncrLoadTest> _log;
        private static string query;
        private static DataSet result = new DataSet();
        private static string _conexion;
        private static string _tableIdentifier;
        private static DateTime _startDate, _endDate;
        private static DataSet myResponse, sqlCDCResponse = new DataSet();


        public IncrLoadTest(string conexion, string tableIdentifier, DateTime startDate, DateTime endDate)
        {
            helper = new SqlHelper(conexion);
            _conexion = conexion;
            _tableIdentifier = tableIdentifier;
            _startDate = startDate;
            _endDate = endDate;
        }

        /// <summary>
        /// Prepare the query that will by executed on facebook
        /// </summary>
        /// <returns>The query string already done</returns>
        private static string prepareQuery()
        {
            string tableName;

            switch (_tableIdentifier.Trim().ToUpper())
            {
                case "UOM":
                    tableName = "cdc.CISADM_CI_UOM_CT"; break;                    
                case "BSEG":
                    tableName = "cdc.CISADM_CI_BSEG_CT"; break;                   
                case "BSEG_CALC":
                    tableName = "cdc.CISADM_CI_BSEG_CALC_CT"; break;
                case "BSEG_CALC_LN":
                    tableName = "cdc.CISADM_CI_BSEG_CALC_LN_CT"; break;
                case "BSEG_SQ":
                    tableName = "cdc.CISADM_CI_BSEG_SQ_CT"; break;
                case "CAL_PERIOD":
                    tableName = "cdc.CISADM_CI_CAL_PERIOD_CT"; break;
                case "FT":
                    tableName = "cdc.CISADM_CI_FT_CT"; break;
                case "PER":
                    tableName = "cdc.CISADM_CI_PER_CT"; break;
                case "PREM":
                    tableName = "cdc.CISADM_CI_PREM_CT"; break;
                case "RS":
                    tableName = "cdc.CISADM_CI_RS_CT"; break;
                case "SA":
                    tableName = "cdc.CISADM_CI_SA_CT"; break;
                case "SQI":
                    tableName = "cdc.CISADM_CI_SQI_CT"; break;
                case "ACCT":
                    tableName = "cdc.CISADM_CI_ACCT_CT"; break;
                default:
                    tableName = "wrong table name"; break;
            }
            
            if (tableName.StartsWith("wrong"))      
                return tableName;

     
            query = "SELECT CT.__$operation AS Operation, COUNT(CT.__$operation) AS [Count] FROM " + tableName + " CT " +
               "LEFT JOIN cdc.lsn_time_mapping T ON CT.__$start_lsn=T.start_lsn WHERE T.tran_begin_time " +
               " BETWEEN '" + _startDate.ToString("yyyy-MM-dd HH:mm") + "' AND '" + _endDate.ToString("yyyy-MM-dd HH:mm") + "' GROUP BY CT.__$operation";
            return query; //2021-01-20 10:00
        }

        /// <summary>
        /// Prepara the response structure on Dataset
        /// 
        /// ----------------------------------------
        /// delete | insert | update |     msg     |
        /// ----------------------------------------
        ///     0  |    0   |   0    |             |
        /// ----------------------------------------
        /// </summary>
        /// <returns>a Dataset with a structure ready to be converted in JSON</returns>
        public static DataSet setResponseStructure()
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");
            dtResult.Columns.Add("insert"); //Item array 0
            dtResult.Columns.Add("update"); //Item array 1
            dtResult.Columns.Add("delete"); //Item array 2
            dtResult.Columns.Add("msg");    //Item array 3
            dtResult.Rows.Add(0, 0, 0, "");
            
            dsResult.Tables.Add(dtResult);
            return dsResult;
        }


        /// <summary>
        /// Get all the CDC operations for the specified table on the class
        /// </summary>
        /// <returns>Dataset with the information requested</returns>
        public static Task<DataSet> GetOperationsCount()
        {            
            myResponse = setResponseStructure();
           
            return Task.Run(() =>
            {
                if (prepareQuery().StartsWith("wrong"))
                {
                    myResponse.Tables[0].Rows[0][3] = ("wrong table name");
                    return myResponse;
                }
                else
                {
                    try
                    {
                        sqlCDCResponse = SqlHelper.ExecuteDataset(_conexion, CommandType.Text, query);

                        
                        foreach (DataRow row in sqlCDCResponse.Tables[0].Rows)
                        {
                            if (int.Parse(row[0].ToString()) == 1) // DELETE                                                                 
                                myResponse.Tables[0].Rows[0][2] = row[1].ToString();

                            if (int.Parse(row[0].ToString()) == 2) // INSERT                                                    
                                myResponse.Tables[0].Rows[0][0] = row[1].ToString();

                            if (int.Parse(row[0].ToString()) == 4) // UPDATE                        
                                myResponse.Tables[0].Rows[0][1] = row[1].ToString();
                        }
                        return myResponse;
                                   
                    }
                    catch (Exception e)
                    {
                        myResponse.Tables[0].Rows[0][3] = ("wrong connection error: " + e.ToString());
                        return myResponse;
                    }
                }
            });
        }
    }   
}
