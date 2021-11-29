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
  
        private static string storedProcedureName;
        private static DataSet result = new DataSet();
        private static string _conexion;
        private static string _identifier;
        private static DateTime _startDate, _endDate;
        private static DataSet myResponse, sqlCDCResponse = new DataSet();
        //private static Boolean _IsSPorQuery;
        private static Int64 _adfNewRowsCount, _adfUpdatedRowsCount;

        public IncrLoadTest(string conexion, string tableIdentifier, DateTime startDate, DateTime endDate, Int64 adfNewRowsCount, Int64 adfUpdatedRowsCount)
        {
            
            _conexion = conexion;
            _identifier = tableIdentifier;
            _startDate = startDate;
            _endDate = endDate;
            _adfNewRowsCount = adfNewRowsCount;
            _adfUpdatedRowsCount = adfUpdatedRowsCount;
            helper = new SqlHelper(_conexion);
            // _IsSPorQuery = IsSPorQuery;
        }

        /// <summary>
        /// Prepare the query that will by executed on facebook
        /// </summary>
        /// <returns>The query string already done</returns>
        private static string prepareQuery()
        {
            switch (_identifier.Trim().ToUpper())
            {
                case "ACCT":
                    storedProcedureName = "cdc.sp_ci_acct_ct"; break;
                case "BSEG":
                    storedProcedureName = "cdc.sp_ci_bseg_ct"; break;
                case "BSEG_CALC":
                    storedProcedureName = "cdc.sp_ci_bseg_calc_ct"; break;
                case "BSEG_CALC_LN":
                    storedProcedureName = "cdc.sp_ci_bseg_calc_ln_ct"; break;
                case "BSEG_SQ":
                    storedProcedureName = "cdc.sp_ci_bseg_sq_ct"; break;
                case "CAL_PERIOD":
                    storedProcedureName = "cdc.sp_ci_cal_period_ct"; break;
                case "FT":
                    storedProcedureName = "cdc.sp_ci_ft_ct"; break;
                case "PREM":
                    storedProcedureName = "cdc.sp_ci_prem_ct"; break;
                case "RS":
                    storedProcedureName = "cdc.sp_ci_rs_ct"; break;
                case "SA":
                    storedProcedureName = "cdc.sp_ci_sa_ct"; break;
                case "SQI":
                    storedProcedureName = "cdc.sp_ci_sqi_ct"; break;
                case "UOM":
                    storedProcedureName = "cdc.sp_ci_uom_ct"; break;
                case "PER":
                    storedProcedureName = "cdc.sp_ci_per_ct"; break;
                default:
                    storedProcedureName = "Error: No stored procedure finded"; break;
            }

            return storedProcedureName; 
        }

        /// <summary>
        /// Prepara the response structure on Dataset
        /// 
        /// ----------------------------------------------------------------------------------
        /// delete | insert | update |     msg     |  ADF Inserted RC   |    ADF Updated RC  |
        /// ----------------------------------------------------------------------------------
        ///     0  |    0   |   0    |             |         0          |           0        |
        /// ----------------------------------------------------------------------------------
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
            dtResult.Columns.Add("adf-insert"); //Item array 4
            dtResult.Columns.Add("adf-update"); //Item array 5
            dtResult.Rows.Add(0, 0, 0, "", 0);
            
            dsResult.Tables.Add(dtResult);
            return dsResult;
        }


        /// <summary>
        /// Get all the CDC operations for the specified table on the class
        /// </summary>
        /// <returns>Dataset with the information requested</returns>
        public static Task<DataSet> GetOperationsCount()
        {
            Int64 diffInsertedRowsCount, diffUpdatedRowsCount;

            myResponse = setResponseStructure();
           
            return Task.Run(() =>
            {
                if (prepareQuery().StartsWith("Error"))
                {
                    myResponse.Tables[0].Rows[0][3] = ("Error: wrong table name");
                    return myResponse;
                }
                else
                {
                    try
                    {

                        Object[] param = { _startDate.ToString("yyyy-MM-dd HH:mm"), _endDate.ToString("yyyy-MM-dd HH:mm") };
                          
                        sqlCDCResponse = SqlHelper.ExecuteDataset(_conexion, storedProcedureName, param);
                            
                        //New Rows, Inserts Count
                        myResponse.Tables[0].Rows[0][0] = sqlCDCResponse.Tables[0].Select("toInsert = 1").Length;
                            
                        //Updated Rows, Modified Rows Count
                        myResponse.Tables[0].Rows[0][1] = sqlCDCResponse.Tables[0].Select("toInsert = 0 and [__$operation] = 4").Length;

                        myResponse.Tables[0].Rows[0][4] = _adfNewRowsCount;
                        myResponse.Tables[0].Rows[0][5] = _adfUpdatedRowsCount;


                        //Compute the diference between the ADF Rows Count and the SP-DW Rows Count, if the differences is 0 everithing is OK!
                        diffInsertedRowsCount = Math.Abs(Convert.ToInt64(myResponse.Tables[0].Rows[0][4]) - Convert.ToInt64(myResponse.Tables[0].Rows[0][0]));

                        //Compute the diference between the ADF Rows Count and the SP-DW Rows Count, if the differences is 0 everithing is OK!
                        diffUpdatedRowsCount = Math.Abs(Convert.ToInt64(myResponse.Tables[0].Rows[0][1]) - Convert.ToInt64(myResponse.Tables[0].Rows[0][5]));

                        //If any Rows Count that come from ADF is different from 0 and the ADF have changes
                        if ((diffInsertedRowsCount != 0 || diffUpdatedRowsCount != 0)  && (Convert.ToInt64(myResponse.Tables[0].Rows[0][0]) != 0 || Convert.ToInt64(myResponse.Tables[0].Rows[0][0]) != 0))                            
                            myResponse.Tables[0].Rows[0][3] = "There is a discrepancy: Inserted Rows Differences = "+ diffInsertedRowsCount + ", Updated Rows Difference = " + diffUpdatedRowsCount + ", please check the the process.";                        
                        else                        
                            myResponse.Tables[0].Rows[0][3] = "OK";                                             
                            
                        return myResponse;
                      
                    }
                    catch (Exception e)
                    {
                        myResponse.Tables[0].Rows[0][3] = ("Error: " + e.ToString());
                        return myResponse;
                    }
                }
            });
        }
    }   
}
