using Microsoft.AspNetCore.Mvc;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;



namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CCBTableController : ControllerBase
    {

        [HttpGet]
        public IActionResult Get(string tableName)
        {
            OracleConnection myOracleConnection = null;
            OracleCommand commandOracle = null;
            OracleDataAdapter dataAdapterOracle = null;
            DataSet resultsOracle = new DataSet();
            string queryOracle = "SELECT COUNT(*) as count FROM " + tableName;
            string ccnOracle = "TNS_ADMIN=C:\\oracle\\product\\19.0.0\\client_1\\network\\admin;USER ID=POWERBI_USER;DATA SOURCE=CCBPROD;PERSIST SECURITY INFO=True; PASSWORD=U4aBI#2020";

            if (String.IsNullOrEmpty(tableName))
                return Content("No Table selected");


            try
            {
                using (myOracleConnection = new OracleConnection(ccnOracle))
                {
                    myOracleConnection.Open();
                    using (commandOracle = new OracleCommand(queryOracle, myOracleConnection))
                    {
                        using (dataAdapterOracle = new OracleDataAdapter(commandOracle))
                        {
                            dataAdapterOracle.Fill(resultsOracle);
                            return Ok(resultsOracle.Tables[0].Rows[0].ItemArray[0]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.Write(e);
                return NotFound(e);
            }
            finally
            {
                myOracleConnection.Close();
            }

        }
    }
}