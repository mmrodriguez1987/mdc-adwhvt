using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using UnitTest.Model;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class EnvironmentInformationController : ControllerBase
    {
        private readonly ILogger<EnvironmentInformationController> _log;        
        private Global gbl;
        private EnviromentInformation envInfo;

        public EnvironmentInformationController(IConfiguration conf, ILogger<EnvironmentInformationController> log)
        {
            gbl = new Global(conf);
            _log = log;

        }
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            try
            {         
                DataSet dsResult = new DataSet("dsResults");
                DataRow dr;
            
                dsResult = Extensions.getResponseEnviromentInfoStructure("Info");
                envInfo = new EnviromentInformation();
            
                // DTW
                Boolean r = await envInfo.GetInformationTest(gbl.CcnDTW);
                dr = dsResult.Tables[0].NewRow();
                dr[0] = r ? "T" : "F";
                dr[1] = "CCN_DTWH";
                dr[2] = "Datawarehouse connection";
                dr[3] = gbl.CcnDTW;
                dsResult.Tables[0].Rows.Add(dr);

                // CDC
                r = await envInfo.GetInformationTest(gbl.CcnCDC);
                dr = dsResult.Tables[0].NewRow();
                dr[0] = r ? "T" : "F";
                dr[1] = "CCN_CDC";
                dr[2] = "CDC connection";
                dr[3] = gbl.CcnCDC;
                dsResult.Tables[0].Rows.Add(dr);

                //Validation DB
                r = await envInfo.GetInformationTest(gbl.CcnDVT);
                dr = dsResult.Tables[0].NewRow();
                dr[0] = r ? "T" : "F";
                dr[1] = "CCN_ValDB";
                dr[2] = "Validation DB connection";
                dr[3] = gbl.CcnDVT;
                dsResult.Tables[0].Rows.Add(dr);

                String envVar = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");  //ASPNETCORE_ENVIRONMENT

                r = !String.IsNullOrWhiteSpace(envVar);
                dr = dsResult.Tables[0].NewRow();
                dr[0] = r ? "T" : "F";
                dr[1] = "Enviroment";
                dr[2] = "Validate the Enviroment";
                dr[3] = envVar;
                dsResult.Tables[0].Rows.Add(dr);
                   
                
                dr = dsResult.Tables[0].NewRow();
                dr[0] = r ? "T" : "F";
                dr[1] = "Phone Numbers for SMS Notifications";
                dr[2] = "Validate Phones";
                dr[3] = String.Join(",", gbl.BiTeamPhoneNumbers); 
                dsResult.Tables[0].Rows.Add(dr);


                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
            catch (Exception e)
            {                
                return base.BadRequest(e.ToString());
            }            
        }
    }
}
