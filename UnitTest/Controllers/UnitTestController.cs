using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Mechanism;


namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UnitTestController : ControllerBase
    {
        public IConfiguration _conf { get; }
        public static Models.UnitTest unitTest;
        private static DDictionary myDict;
   
        readonly ILogger<UnitTestController> _log;
        public UnitTestController(IConfiguration configuration, ILogger<UnitTestController> log)
        {
            _conf = configuration;
            _log = log;
            myDict = new DDictionary();
        }
 
       
        [HttpGet]
        public async Task<IActionResult> Get(string tableIdentifier, string enviroment)
        {
            Int64 diffCount;
            DataSet dsResult = new DataSet("dsResults");
            // Prepare the DataSet for return in the response
            dsResult = Models.UnitTest.setResponseStructure();

            _log.LogInformation(myDict.utMsgs["UTC.BeginTest"]);
            if (String.IsNullOrEmpty(tableIdentifier) || String.IsNullOrEmpty(enviroment))
            {
                              
                dsResult.Tables[0].Rows[0].ItemArray[3] = myDict.utMsgs["UTC.BadEnvOrIdent"];
                _log.LogError(myDict.utMsgs["UTC.BadEnvOrIdent"]);
                _log.LogInformation(myDict.utMsgs["UTC.EndTest"]);
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            } 
            

            _log.LogInformation("Table Identifier: " + tableIdentifier);
            _log.LogInformation("Enviroment: "+ enviroment);


            // Initialize the Model for identify conexion's string and tables to use
            switch (enviroment.Trim().ToUpper())
            {
                case "CCBPROD":
                case "PROD":
                case "PRODUCTION":
                    _log.LogInformation(myDict.utMsgs["UTC.CCBConxStr"] + _conf.GetConnectionString("CCBProdConnection"));                  
                    unitTest = new Models.UnitTest(_conf.GetConnectionString("CCBProdConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    
                    break;

                case "CCBSTGE":
                case "STGE":
                case "STAGE":
                    _log.LogInformation(myDict.utMsgs["UTC.CCBConxStr"] + _conf.GetConnectionString("CCBStgeConnection"));
                    unitTest = new Models.UnitTest(_conf.GetConnectionString("CCBStgeConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    break;

                case "CCBRPTS":
                case "RPTS":
                case "REPORTS":
                    _log.LogInformation(myDict.utMsgs["UTC.CCBConxStr"] + _conf.GetConnectionString("CCBRptsConnection"));
                    unitTest = new Models.UnitTest(_conf.GetConnectionString("CCBRptsConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    break;
                default:
                    
                    dsResult.Tables[0].Rows[0][3] = myDict.utMsgs["UTC.BadEnv"];
                    _log.LogError(myDict.utMsgs["UTC.BadEnv"]);
                    _log.LogInformation(myDict.utMsgs["UTC.EndTest"]);
                    return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            _log.LogInformation(myDict.utMsgs["UTC.DTWHConxStr"] + _conf.GetConnectionString("DTWttdpConnection"));


            // Validation if tableIdentifier has an invalid value
            if (String.IsNullOrEmpty(unitTest.DtwTable) || String.IsNullOrEmpty(unitTest.CcbTable)) {                
                dsResult.Tables[0].Rows[0][3] = myDict.utMsgs["UTC.BadIdent"];
                _log.LogError(myDict.utMsgs["UTC.BadIdent"]);
                _log.LogInformation(myDict.utMsgs["UTC.EndTest"]);
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }

            //Executing count by Thread
            DataSet resultCCB = await Models.UnitTest.GetRowsCountFromCCB();
            DataSet resultDTWH = await Models.UnitTest.GetRowsCountFromDTWH();

            //if the response from Datawarehouse contain an Error
            if (resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-1"))
            {
                dsResult.Tables[0].Rows[0][3] = resultDTWH.Tables[0].Rows[0][0];
                _log.LogError(myDict.utMsgs["UTC.DTWHConxStrErr"] + resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString());
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
                

            //if the response from CCB  contain an Error
            if (resultCCB.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-1"))
            {
                dsResult.Tables[0].Rows[0][3] = resultCCB.Tables[0].Rows[0][0];
                _log.LogError(myDict.utMsgs["UTC.CCBConxStrErr"] + resultCCB.Tables[0].Rows[0].ItemArray[0].ToString());
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }


            dsResult.Tables[0].Rows[0][0] = resultDTWH.Tables[0].Rows[0].ItemArray[0];
            dsResult.Tables[0].Rows[0][1] = resultCCB.Tables[0].Rows[0].ItemArray[0];

            // if the CCB and DTW have the same value count tables
            if (Convert.ToInt64(dsResult.Tables[0].Rows[0][0]) == Convert.ToInt64(dsResult.Tables[0].Rows[0][1]))
            {
                 _log.LogInformation("Unit Test Successfully ==>> DTWH Count : " + dsResult.Tables[0].Rows[0][0].ToString() + ", CCB Count : " + dsResult.Tables[0].Rows[0][1].ToString());
                dsResult.Tables[0].Rows[0][2] = 1;
            }                    
            else
            {
                diffCount = Math.Abs(Convert.ToInt64(dsResult.Tables[0].Rows[0][0]) - Convert.ToInt64(dsResult.Tables[0].Rows[0][1]));
                dsResult.Tables[0].Rows[0][4] = diffCount;
                string response = "Unit Test fail for " + tableIdentifier + ", there is a difference of " + diffCount.ToString()
                    + ". CCB Count: " + resultCCB.Tables[0].Rows[0].ItemArray[0].ToString()
                    + ". DTW Count: " + resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString();
                _log.LogInformation(response);
                dsResult.Tables[0].Rows[0][3] = response;
            }
            _log.LogInformation(myDict.utMsgs["UTC.EndTest"]);
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
           
        }
    }
}
