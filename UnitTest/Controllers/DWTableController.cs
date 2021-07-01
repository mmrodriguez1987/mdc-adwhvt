using Microsoft.AspNetCore.Mvc;
using System;
using System.Data.SqlClient;
using System.Data;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DWTableController : ControllerBase
    {
        
        [HttpGet]
        public IActionResult Get(string tableName)
        {

            SqlConnection myConnection = null;
            SqlCommand command = null;
            SqlDataAdapter dataAdapter = null;
            DataSet results = new DataSet();
            string query = "SELECT COUNT(*) as count FROM " + tableName;
            string ccn = "Data Source=sql-prod-mdcttdp.database.windows.net;Initial Catalog=dw-ttdp;Persist Security Info=True;User ID=CCBUser;Password=CCBDadeUser2020!";
            
            if (String.IsNullOrEmpty(tableName))            
                return Content("No Table selected");
            

            try
            {
                using (myConnection = new SqlConnection(ccn))
                {
                    myConnection.Open();
                    using (command = new SqlCommand(query, myConnection))
                    {
                        using (dataAdapter = new SqlDataAdapter(command))
                        {
                            dataAdapter.Fill(results);
                            return Ok(results.Tables[0].Rows[0].ItemArray[0]);
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
                myConnection.Close();
            }


           
        }
    }
}
