using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UnitTestController : ControllerBase
    {
        public IConfiguration _conf { get; }
        public UnitTestController(IConfiguration configuration)
        {
            _conf = configuration;
        }


        [HttpPost]
        public IActionResult PostUnitTest(string dtwTable, string ccbTable)
        {
            if (String.IsNullOrEmpty(dtwTable) || String.IsNullOrEmpty(ccbTable))
                return NotFound("Datawarehouse or CCB Tables are messing");

            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");
            dtResult.Columns.Add(dtwTable);
            dtResult.Columns.Add(ccbTable);
            dtResult.Columns.Add("OK");
            dtResult.Columns.Add("Any Error?");
            dtResult.Rows.Add(0, 0, 0, "Clean");
            dsResult.Tables.Add(dtResult);


            SqlConnection myConnection = null;
            SqlCommand command = null;
            SqlDataAdapter dataAdapter = null;
            DataSet results = new DataSet();
            string query = "SELECT COUNT(*) as count FROM " + dtwTable;



            OracleConnection myOracleConnection = null;
            OracleCommand commandOracle = null;
            OracleDataAdapter dataAdapterOracle = null;
            DataSet resultsOracle = new DataSet();
            string queryOracle = "SELECT COUNT(*) as count FROM " + ccbTable;





            try
            {
                //Conexion to Datawarehouse
                using (myConnection = new SqlConnection(_conf.GetConnectionString("DTWttdpConnection")))
                {
                    myConnection.Open();
                    using (command = new SqlCommand(query, myConnection))
                    {
                        using (dataAdapter = new SqlDataAdapter(command))
                        {
                            dataAdapter.Fill(results);
                            dsResult.Tables[0].Rows[0][0] = results.Tables[0].Rows[0].ItemArray[0];
                        }
                    }
                }

            }
            catch (SqlException conexSQLException)
            {   // Return -1 in Colum "OK" and the Error description in "Any Error" column
                Console.Write("Datawarehouse - SQL  Connection Exception: " + conexSQLException);
                dsResult.Tables[0].Rows[0][2] = -1;
                dsResult.Tables[0].Rows[0][3] = ("Datawarehouse - SQL  Connection Exception: " + conexSQLException.ToString());
                return Ok(Util.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            finally
            {
                myConnection.Close();
            }



            try
            {
                //Conexion to CCB 
                using (myOracleConnection = new OracleConnection(_conf.GetConnectionString("CCBProdConnection")))
                {
                    myOracleConnection.Open();
                    using (commandOracle = new OracleCommand(queryOracle, myOracleConnection))
                    {
                        using (dataAdapterOracle = new OracleDataAdapter(commandOracle))
                        {
                            dataAdapterOracle.Fill(resultsOracle);
                            dsResult.Tables[0].Rows[0][1] = resultsOracle.Tables[0].Rows[0].ItemArray[0];
                        }
                    }
                }
                myOracleConnection.Close();
            }
            catch (OracleException conexOracleException)
            {
                // Return -1 in Colum "OK" and the Error description in "Any Error" column
                Console.Write("CCB - Oracle Connection Exception: " + conexOracleException);
                dsResult.Tables[0].Rows[0][2] = -1;
                dsResult.Tables[0].Rows[0][3] = ("CCB - Oracle Connection Exception: " + conexOracleException.ToString());
                return Ok(Util.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            finally
            {
                myOracleConnection.Close();
            }


            //if count(ccb.table.count == datawarehouse.table.count) then return 1 in columnt "0"   
            if (Convert.ToInt64(dsResult.Tables[0].Rows[0][0]) == Convert.ToInt64(dsResult.Tables[0].Rows[0][1]))
            {
                dsResult.Tables[0].Rows[0][2] = 1;
            }


            return Ok(Util.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));

        }
    }
}
