using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.Mechanism;
using Tools.DataConversion;
using UnitTest.Model;
using Tools.Documentation;


namespace UnitTest.Controllers
{
    
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "09/15/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class IncrLoadTestController : ControllerBase
    {
        private IConfiguration _conf { get; }
        private static IncrLoadTest incrLT;
        private static DDictionary myDict;
        private readonly ILogger<IncrLoadTestController> _log;

        /// <summary>
        /// Initialize the controller with logger option and access to the project config enviroment
        /// </summary>
        /// <param name="configuration">The configuration object from the enviroment to have access to the configuration project configuration file</param>
        /// <param name="log"> Log object for have access to loggin class</param>
        public IncrLoadTestController(IConfiguration configuration, ILogger<IncrLoadTestController> log)
        {
            _conf = configuration;
            _log = log;
            myDict = new DDictionary();
        }


        /// <summary>
        /// Execution of the Get  Method of Asynchronus
        /// </summary>
        /// <remarks>Async Taks </remarks>
        /// <param name="tableIdentifier">ACCT, UOM, ADDR, etc...</param>
        /// <param name="enviroment">CCB or STGE</param>
        /// <param name="startDate"> Initial date to evaluate</param>
        /// <param name="endDate">End Date to evaluate</param>
        /// <param name="adfNewRowsCount"> Inserted Rows Count from the Azure Data Factory</param>
        /// <param name="adfUpdatedRowsCount"> Updated Rows Count from the Azure Data Factory</param>
        /// <returns></returns>
        [Description("Execution of the Get  Method of Asynchronus", "Async Taks","tableIdentifier: ACCT, UOM, ADDR, etc...", "enviroment: CCB or STGE", "startDate: 2021-09-01 10:00:00.00", "endDate: 2021-09-02 10:00:00.00", "adfNewRowsCount: Inserted Rows Count from the Azure Data Factory", "adfUpdatedRowsCount: Updated Rows Count from the Azure Data Factory")]
        [HttpGet]
        public async Task<IActionResult> Get(string tableIdentifier, string enviroment, DateTime startDate, DateTime endDate, Int64 adfNewRowsCount, Int64 adfUpdatedRowsCount)
        {            
            DataSet dsResult = new DataSet();
            dsResult = IncrLoadTest.setResponseStructure();

            //Validating that the TableIdentifier not be empty or null
            if (String.IsNullOrEmpty(tableIdentifier) || String.IsNullOrEmpty(enviroment))
            {                              
                _log.LogError(myDict.messages["UT.BadIdent"]);                        
                return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadIdent"]));
            }

            _log.LogInformation("Table Identifier: " + tableIdentifier);
            _log.LogInformation("Enviroment: " + enviroment);

            //Validating the dates            
            if (DateTime.Compare(endDate, DateTime.Now) > 0)
            {                        
                _log.LogError(myDict.messages["UT.BadEndDate"]);               
                return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadEndDate"]));               
            }

            //Validating the dates            
            if (DateTime.Compare(startDate, endDate) > 0)
            {
                _log.LogError(myDict.messages["UT.BadStartDate"]);
                return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadStartDate"]));
            }


            // validating the enviroment
            switch (enviroment.Trim().ToUpper())
            {
                case "PROD":
                case "CDCPROD":
                case "CDCPRODUCTION":
                    _log.LogInformation(myDict.messages["UT.CDCProdCnx"] + _conf.GetConnectionString("CDCProdConnection"));
                    incrLT = new IncrLoadTest(_conf.GetConnectionString("CDCProdConnection"), tableIdentifier, startDate, endDate, adfNewRowsCount, adfUpdatedRowsCount);
                    break;

                case "STGE":
                case "CDCSTGE":
                case "CDCSTAGE":
                    _log.LogInformation(myDict.messages["UT.CDCStgeCnx"] + _conf.GetConnectionString("CDCStgeConnection"));
                    incrLT = new IncrLoadTest(_conf.GetConnectionString("CDCStgeConnection"), tableIdentifier, startDate, endDate, adfNewRowsCount, adfUpdatedRowsCount);
                    break;
             
                default:                    
                    _log.LogError(myDict.messages["UT.BadEnv"]);
                    _log.LogInformation(myDict.messages["UT.BadEnv"]);
                    return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadEnv"]));                   
            }

            dsResult = await IncrLoadTest.GetOperationsCount();

            // If any error come from GetOperationCount()
            if (dsResult.Tables[0].Rows[0].ItemArray[3].ToString().StartsWith("Error"))            
                return base.BadRequest(Extensions.messageToJSON(dsResult.Tables[0].Rows[0][3].ToString()));            
                
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));            
                       
                
        }
    }
}
