using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Communication;
using UnitTest.Model.DataWarehouse;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class ServiceAgreementTestController : ControllerBase
    {     
        private DataSet finalResultDS, dsResult;       
        private SMS mySMS;
        private Global gbl;

        public ServiceAgreementTestController(IConfiguration conf)
        {
            gbl = new Global(conf);   
            finalResultDS = new DataSet();
            dsResult = new DataSet();          
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify)
        {

            mySMS = new SMS(gbl.CcnACS);

            ServiceAgreement sa = new ServiceAgreement(gbl.CcnDTW, gbl.CcnCDC);

            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("PremiseTestResult");
          
            //Validation: Get Accounts Count
            dsResult = await sa.GetCountServiceAgreement(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());           

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
