using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Mechanism;
using UnitTest.Model;


namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class InitLoadTestController : ControllerBase
    {
        private IConfiguration _conf { get; }
        private static InitialLoadTest initialLoadTest;
        private static DDictionary myDict;

        private ILogger<InitLoadTestController> _log;
        public InitLoadTestController(IConfiguration configuration, ILogger<InitLoadTestController> log)
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
            dsResult = InitialLoadTest.setResponseStructure();

            _log.LogInformation(myDict.messages["UT.BeginTest"]);
            if (String.IsNullOrEmpty(tableIdentifier) || String.IsNullOrEmpty(enviroment))
            {
                dsResult.Tables[0].Rows[0].ItemArray[3] = myDict.messages["UT.BadEnvOrIdent"];
                _log.LogError(myDict.messages["UT.BadEnvOrIdent"]);
                _log.LogInformation(myDict.messages["UT.EndTest"]);
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }


            _log.LogInformation("Table Identifier: " + tableIdentifier);
            _log.LogInformation("Enviroment: " + enviroment);


            // Initialize the Model for identify conexion's string and tables to use
            switch (enviroment.Trim().ToUpper())
            {
                case "CCBPROD":
                case "PROD":
                case "PRODUCTION":
                    _log.LogInformation(myDict.messages["UT.CCBConxStr"] + _conf.GetConnectionString("CCBProdConnection"));
                    initialLoadTest = new InitialLoadTest(_conf.GetConnectionString("CCBProdConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    break;

                case "CCBSTGE":
                case "STGE":
                case "STAGE":
                    _log.LogInformation(myDict.messages["UT.CCBConxStr"] + _conf.GetConnectionString("CCBStgeConnection"));
                    initialLoadTest = new InitialLoadTest(_conf.GetConnectionString("CCBStgeConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    break;

                case "CCBRPTS":
                case "RPTS":
                case "REPORTS":
                    _log.LogInformation(myDict.messages["UT.CCBConxStr"] + _conf.GetConnectionString("CCBRptsConnection"));
                    initialLoadTest = new InitialLoadTest(_conf.GetConnectionString("CCBRptsConnection"), _conf.GetConnectionString("DTWttdpConnection"), tableIdentifier);
                    break;
                default:
                    dsResult.Tables[0].Rows[0][3] = myDict.messages["UT.BadEnv"];
                    _log.LogError(myDict.messages["UT.BadEnv"]);
                    _log.LogInformation(myDict.messages["UT.EndTest"]);
                    return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            _log.LogInformation(myDict.messages["UT.DTWHConxStr"] + _conf.GetConnectionString("DTWttdpConnection"));

            // Validation if tableIdentifier has an invalid value
            if (String.IsNullOrEmpty(initialLoadTest.DtwTable) || String.IsNullOrEmpty(initialLoadTest.CcbTable))
            {
                dsResult.Tables[0].Rows[0][3] = myDict.messages["UT.BadIdent"];
                _log.LogError(myDict.messages["UT.BadIdent"]);
                _log.LogInformation(myDict.messages["UT.EndTest"]);
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }

            //Executing count by Thread
            DataSet resultCCB = await InitialLoadTest.GetRowsCountFromCCB();
            DataSet resultDTWH = await InitialLoadTest.GetRowsCountFromDTWH();

            //if the response from Datawarehouse contain an Error
            if (resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-1"))
            {
                dsResult.Tables[0].Rows[0][3] = resultDTWH.Tables[0].Rows[0][0];
                _log.LogError(myDict.messages["UT.DTWHConxStrErr"] + resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString());
                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }


            //if the response from CCB  contain an Error
            if (resultCCB.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-1"))
            {
                dsResult.Tables[0].Rows[0][3] = resultCCB.Tables[0].Rows[0][0];
                _log.LogError(myDict.messages["UT.CCBConxStrErr"] + resultCCB.Tables[0].Rows[0].ItemArray[0].ToString());
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
            _log.LogInformation(myDict.messages["UT.EndTest"]);
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
        }
    }
}
