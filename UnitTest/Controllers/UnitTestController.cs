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


        /// <summary>
        /// Datawarehouse Controller 
        /// </summary>
        /// <param name="tableIdentifier"></param> 
        /// <returns></returns>       
        [HttpGet]
        public IActionResult Get(string tableIdentifier)
        {
            string dtwTable, ccbTable;


            switch (tableIdentifier.Trim())
            {
                case "UOM":
                    dtwTable = "dwadm2.CD_UOM";
                    ccbTable = "CISADM.CI_UOM";
                    break;
                case "FISCAL_CAL": case "CAL_PERIOD":
                    dtwTable = "dwadm2.CD_FISCAL_CAL";
                    ccbTable = "CISADM.CI_CAL_PERIOD";
                    break;

                case "ACCT":
                    dtwTable = "dwadm2.CD_ACCT";
                    ccbTable = "CISADM.CI_ACCT";
                    break;

                case "ADDR":
                    dtwTable = "dwadm2.CD_PREM";
                    ccbTable = "CISADM.CI_ADDR";
                    break;

                case "PREM":
                    dtwTable = "dwadm2.CD_PREM";
                    ccbTable = "CISADM.CI_PREM";
                    break;

                case "RATE":
                    dtwTable = "dwadm2.CD_RATE";
                    ccbTable = "CISADM.CI_RS";
                    break;

                case "SA":
                    dtwTable = "dwadm2.CD_SA";
                    ccbTable = "CISADM.CI_SA";
                    break;

                case "PER":
                    dtwTable = "dwadm2.CD_PER";
                    ccbTable = "CISADM.CI_PER";
                    break;

                case "SQI":
                    dtwTable = "dwadm2.CD_SQI";
                    ccbTable = "CISADM.CI_SQI";
                    break;

                default:
                    dtwTable = "";
                    ccbTable = "";
                    break;
            }


            if (String.IsNullOrEmpty(dtwTable) || String.IsNullOrEmpty(ccbTable))
                return NotFound("Datawarehouse or CCB Tables are messing");

            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");
            dtResult.Columns.Add(dtwTable);
            dtResult.Columns.Add(ccbTable);
            dtResult.Columns.Add("result");
            dtResult.Columns.Add("Error");
            dtResult.Rows.Add(0, 0, 0, "");
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
            catch (Exception dte )
            {   // Return -1 in Colum "OK" and the Error description in "Any Error" column
                Console.Write("Conexion DTW: " + _conf.GetConnectionString("DTWttdpConnection") + "\n");
                Console.Write("Exception: " + dte + "\n");
                dsResult.Tables[0].Rows[0][2] = -1;
                dsResult.Tables[0].Rows[0][3] = ("Exception: " + dte.ToString());
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
            }
            catch (Exception ccbe)
            {
                // Return -1 in Colum "OK" and the Error description in "Any Error" column
                
                Console.Write("Conexion CCB: " + _conf.GetConnectionString("CCBProdConnection") + "\n");
                Console.Write("Exception: " + ccbe);
                dsResult.Tables[0].Rows[0][2] = -1;
                dsResult.Tables[0].Rows[0][3] = ("Exception: " + ccbe.ToString());
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
