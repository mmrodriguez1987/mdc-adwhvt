using Microsoft.AspNetCore.Mvc;
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

        
        [HttpGet]
        public IActionResult Get(string dtwTable, string ccbTable)
        {
            if (String.IsNullOrEmpty(dtwTable) || String.IsNullOrEmpty(ccbTable))
                return NotFound("Datawarehouse or CCB Tables are messing");

            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");           
            dtResult.Columns.Add(dtwTable);
            dtResult.Columns.Add(ccbTable);
            dtResult.Columns.Add("OK");
            dtResult.Rows.Add(0,0,0);            
            dsResult.Tables.Add(dtResult);
       

            SqlConnection myConnection = null;
            SqlCommand command = null;
            SqlDataAdapter dataAdapter = null;
            DataSet results = new DataSet();
            string query = "SELECT COUNT(*) as count FROM " + dtwTable;
            string ccn = "Data Source=sql-prod-mdcttdp.database.windows.net;Initial Catalog=dw-ttdp;Persist Security Info=True;User ID=CCBUser;Password=CCBDadeUser2020!";


            OracleConnection myOracleConnection = null;
            OracleCommand commandOracle = null;
            OracleDataAdapter dataAdapterOracle = null;
            DataSet resultsOracle = new DataSet();
            string queryOracle = "SELECT COUNT(*) as count FROM " + ccbTable;
            string ccnOracle = "TNS_ADMIN=C:\\oracle\\product\\19.0.0\\client_1\\network\\admin;USER ID=POWERBI_USER;DATA SOURCE=CCBPROD;PERSIST SECURITY INFO=True; PASSWORD=U4aBI#2020";


        

            try
            {
                //Conexion to Datawarehouse
                using (myConnection = new SqlConnection(ccn))
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

                myConnection.Close();


                //Conexion to CCB
                using (myOracleConnection = new OracleConnection(ccnOracle))
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
                if (Convert.ToInt64(dsResult.Tables[0].Rows[0][0]) == Convert.ToInt64(dsResult.Tables[0].Rows[0][1]))
                {
                    dsResult.Tables[0].Rows[0][2] = 1;
                } 
            
                   
                return Ok(Util.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            catch (Exception e)
            {
                Console.Write(e);
                return NotFound(e);
            }   
        }
    }
}
